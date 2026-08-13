#!/usr/bin/env python3
"""
Shared logic for the Snowflake dashboard and drilldown query runners.

Snowflake (snowflake-connector-python) analogue of the ClickHouse shell runners
run_dashboard.sh / run_drilldown.sh. Produces BYTE-IDENTICAL JSONL records given
the same metadata, so one analysis pipeline ingests output from all systems.

Each iteration: ISO-8601 UTC start -> COUNT(*) raw (QUOTES) -> COUNT(*) mv
(QUOTES_DAILY) -> run each query once capturing SERVER-SIDE timings from
QUERY_HISTORY (ms -> seconds) -> ISO end -> append one JSONL line. Runs until
Ctrl-C / SIGTERM.

Scheduling is FIXED-RATE: iteration N fires at (loop start + N*--interval), i.e. every
--interval seconds from the previous iteration's SCHEDULED start, independent of how long the
queries take. (Earlier this was fixed-delay — sleep --interval AFTER finishing — which drifted:
a run taking T seconds made the gap --interval+T. A dashboard that "refreshes every 10 min"
must re-fire 10 min after the previous start, not 10 min after it finishes.) If a run overruns
--interval the next fires immediately (one connection can't overlap runs) and the drift is logged.

Three timings are recorded per query, each in the same [[v], [v], ...] shape:
  "result"           TOTAL_ELAPSED_TIME  — the reported latency (compile + queue + execute)
  "compilation_time" COMPILATION_TIME
  "execution_time"   EXECUTION_TIME
`result` is TOTAL_ELAPSED_TIME, not EXECUTION_TIME: querying a streaming-fed interactive
MV, compilation ran ~10x execution (T2 RUN8 median 10.3s compile vs 1.0s execute), so
execution alone understated user-visible latency by >12x. The split is kept in separate
fields so that effect stays visible instead of being buried in one number.

Auth: key-pair (env SF_ACCOUNT / SF_USER / SF_KEY), reader warehouse SF_WAREHOUSE,
schema SF_SCHEMA. Requires: pip install snowflake-connector-python cryptography
"""

import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import snowflake.connector as sc
from cryptography.hazmat.primitives import serialization

ACCOUNT   = os.environ["SF_ACCOUNT"]
USER      = os.environ["SF_USER"]
KEY_PATH  = os.environ.get("SF_KEY", "/home/ubuntu/bench/keys/rsa_key.p8")
WAREHOUSE = os.environ.get("SF_WAREHOUSE", "BENCH2COST_SMALL_GEN2")
# Timed dashboard/drilldown queries run on WAREHOUSE (the *measured* warehouse — e.g. the
# interactive read warehouse). All support/tracking queries (row-count COUNT(*), the
# QUERY_HISTORY timing lookup) run on TRACK_WAREHOUSE so they add no load/cost to the measured
# warehouse and never hit its 5s interactive timeout. Defaults to WAREHOUSE (single-wh behaviour).
TRACK_WAREHOUSE = os.environ.get("SF_TRACK_WAREHOUSE", WAREHOUSE)
SCHEMA    = os.environ.get("SF_SCHEMA", "STOCKHOUSE")   # tables live in BENCH2COST.STOCKHOUSE

# Tables the runner COUNT(*)s for volume context (raw_rows / mv_rows). Override for the
# interactive-table experiment, e.g. SF_MV_TABLE=QUOTES_DAILY_IT.
RAW_TABLE = os.environ.get("SF_RAW_TABLE", "QUOTES")
MV_TABLE  = os.environ.get("SF_MV_TABLE", "QUOTES_DAILY")
TAGS = ["managed", "aws", "snowflake"]


class _Stop:
    """Tracks Ctrl-C / SIGTERM so the loop exits cleanly mid-sleep."""
    def __init__(self):
        self.requested = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum, frame):
        self.requested = True

    def sleep(self, seconds):
        slept = 0.0
        while slept < seconds and not self.requested:
            chunk = min(0.5, seconds - slept)
            time.sleep(chunk)
            slept += chunk


def _pkb():
    pk = serialization.load_pem_private_key(open(KEY_PATH, "rb").read(), password=None)
    return pk.private_bytes(serialization.Encoding.DER,
                            serialization.PrivateFormat.PKCS8,
                            serialization.NoEncryption())


def connect(database):
    """Key-pair connection; set warehouse + database + schema. QUERY_HISTORY()
    and the unqualified query-file references both need a schema in context."""
    con = sc.connect(account=ACCOUNT, user=USER, private_key=_pkb(),
                     database=database, login_timeout=30)
    cur = con.cursor()
    # Disable the result cache so every timed query actually executes (else an identical
    # repeated query returns ~0ms with no compute, faking latency and bypassing the 5s
    # interactive timeout). The warehouse DATA cache (warm partitions) is intentionally kept.
    cur.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")
    cur.execute(f"USE WAREHOUSE {TRACK_WAREHOUSE}")   # default context = tracking wh (counts/lookups)
    cur.execute(f"USE DATABASE {database}")
    cur.execute(f"USE SCHEMA {database}.{SCHEMA}")
    cur.close()
    return con


def _strip_line_comments(text):
    out = []
    for line in text.splitlines():
        idx = line.find("--")
        out.append(line if idx < 0 else line[:idx])
    return "\n".join(out)


def parse_queries(path):
    """Split on ';', trim, collapse internal whitespace, drop empties — matches
    the CH runners' sed+awk pipeline."""
    text = _strip_line_comments(Path(path).read_text())
    queries = []
    for chunk in text.split(";"):
        q = chunk.strip()
        if q:
            queries.append(" ".join(q.split()))
    return queries


def _query_timings(cur, database, sfqid, tries=4, delay=1.0):
    """Server-side timings for a query id -> (total_s, compile_s, exec_s), seconds floats.
    Retries a few times because a just-finished query can take ~1s to appear in
    QUERY_HISTORY. Any component QUERY_HISTORY has no value for comes back None (-> JSON
    null); a query killed during compilation has a NULL EXECUTION_TIME, so retry on the
    ROW being absent, never on an individual column being NULL. (RESULT_LIMIT is explicit
    because QUERY_HISTORY() windows to the N newest rows *before* the WHERE applies.)"""
    for attempt in range(tries):
        try:
            cur.execute(
                "SELECT TOTAL_ELAPSED_TIME, COMPILATION_TIME, EXECUTION_TIME "
                f"FROM TABLE({database}.INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 100)) "
                "WHERE QUERY_ID = %s",
                (sfqid,),
            )
            row = cur.fetchone()
            if row is not None:
                return tuple(None if v is None else float(v) / 1000.0 for v in row)
        except Exception as exc:
            print(f"  WARN: timing lookup failed for {sfqid}: {exc}", file=sys.stderr, flush=True)
            return (None, None, None)
        if attempt < tries - 1:
            time.sleep(delay)
    return (None, None, None)


# An interactive warehouse aborts any SELECT that exceeds its (max 5s) query timeout.
# Record the string "timeout" for those instead of failing/null, so the JSONL distinguishes
# "couldn't meet the interactive latency bar" from a genuine error.
_TIMEOUT_MARKERS = ("timeout", "exceeded", "canceled", "cancelled")


def time_query(cur, database, query):
    """Run one query once; return (result, compile_s, exec_s). `result` is server-side
    TOTAL_ELAPSED_TIME in seconds (float), "timeout" if the interactive warehouse aborted it
    on the timeout limit, or None on any other error. Never raises."""
    sfqid = None
    prior_sfqid = None
    timed_out = False
    try:
        if WAREHOUSE != TRACK_WAREHOUSE:
            cur.execute(f"USE WAREHOUSE {WAREHOUSE}")   # run the timed query on the measured wh
        prior_sfqid = getattr(cur, "sfqid", None)       # the USE statement's id, not the query's
        cur.execute(query)
        cur.fetchall()              # drain so the engine fully executes it
        sfqid = cur.sfqid
    except Exception as exc:
        # Keep the query id from the failure path too: a timed-out query IS in QUERY_HISTORY, and
        # its COMPILATION_TIME / TOTAL_ELAPSED_TIME are exactly what explains why it timed out.
        # Read it here, before the finally block's USE WAREHOUSE overwrites cur.sfqid.
        sfqid = getattr(exc, "sfqid", None) or getattr(cur, "sfqid", None)
        if sfqid is not None and sfqid == prior_sfqid:
            sfqid = None            # driver never got an id for OUR query (e.g. a network error);
                                    # don't report the preceding USE WAREHOUSE's timings as ours
        msg = str(exc).lower()
        code = getattr(exc, "errno", None)
        if code in (604, 630) or any(m in msg for m in _TIMEOUT_MARKERS):
            print(f"  QUERY TIMEOUT (interactive limit): {exc}", file=sys.stderr, flush=True)
            timed_out = True
        else:
            print(f"  QUERY ERROR: {exc}", file=sys.stderr, flush=True)
            return (None, None, None)
    finally:
        if WAREHOUSE != TRACK_WAREHOUSE:                # back to tracking wh for counts/lookups
            try:
                cur.execute(f"USE WAREHOUSE {TRACK_WAREHOUSE}")
            except Exception:
                pass

    if sfqid is None:
        return ("timeout" if timed_out else None, None, None)
    total_s, compile_s, exec_s = _query_timings(cur, database, sfqid)
    # A timeout stays flagged as "timeout" in `result` — it's a censored observation (the query
    # was killed at the cap), not a latency measurement — but its compile/exec split is recorded.
    return ("timeout" if timed_out else total_s, compile_s, exec_s)


def scalar_query(cur, query):
    try:
        cur.execute(query)
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception as exc:
        print(f"  COUNT ERROR: {exc}", file=sys.stderr, flush=True)
        return 0


def server_version(cur):
    try:
        cur.execute("SELECT CURRENT_VERSION()")
        row = cur.fetchone()
        return str(row[0]) if row and row[0] is not None else "unknown"
    except Exception:
        return "unknown"


def _now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_record(output_path, record):
    line = json.dumps(record, separators=(",", ":"), ensure_ascii=False)
    with open(output_path, "a") as f:
        f.write(line + "\n")


def build_arg_parser(runner_name, default_queries, default_interval):
    ap = argparse.ArgumentParser(prog=f"run_{runner_name}.py",
                                 description=f"Snowflake {runner_name} query runner (long-running, JSONL).")
    ap.add_argument("--database", required=True)
    ap.add_argument("--queries", default=default_queries)
    ap.add_argument("--interval", type=int, default=default_interval)
    ap.add_argument("--output", default="")
    ap.add_argument("--output-dir", default="./runner_output")
    ap.add_argument("system"); ap.add_argument("machine")
    ap.add_argument("cluster_size")   # string: accept any value (e.g. "1", "2.7", labels)
    ap.add_argument("comment"); ap.add_argument("extra_flag")
    return ap


def run(runner_name, default_queries, default_interval, comment_flavor, argv=None):
    args = build_arg_parser(runner_name, default_queries, default_interval).parse_args(argv)
    database = args.database

    if args.output:
        output_path = args.output
    else:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output_path = os.path.join(args.output_dir, f"{runner_name}_{ts}.jsonl")
    Path(os.path.dirname(output_path) or ".").mkdir(parents=True, exist_ok=True)

    queries = parse_queries(args.queries)
    total = len(queries)
    if total == 0:
        print(f"ERROR: No queries found in {args.queries}", file=sys.stderr)
        sys.exit(1)

    comment = f"{args.comment} ({comment_flavor}, {args.extra_flag})"
    print(f"Parsed {total} queries from {args.queries}", file=sys.stderr)
    print(f"Writing JSONL to {output_path}", file=sys.stderr)
    print(f"Query warehouse {WAREHOUSE}, tracking warehouse {TRACK_WAREHOUSE}, "
          f"database {database}, schema {SCHEMA}.", file=sys.stderr)
    print(f"Interval {args.interval}s. Ctrl-C to stop.", file=sys.stderr)

    stop = _Stop()
    iteration = 0
    period = args.interval
    # FIXED-RATE scheduling: each iteration fires `period` seconds after the previous iteration's
    # SCHEDULED start — NOT after it finishes. The premise is a dashboard that refreshes every
    # `period`s regardless of how long a refresh takes; fixed-delay (sleep after finish) would push
    # the cadence out by each run's duration and silently drift (e.g. a 5-min run -> 15-min gap).
    # If a run overruns `period`, the next fires immediately (a single connection can't overlap runs)
    # and the drift is logged rather than accumulated.
    next_fire = time.monotonic()
    while not stop.requested:
        delay = next_fire - time.monotonic()
        if delay > 0:
            stop.sleep(delay)
        if stop.requested:
            break

        iteration += 1
        ts_start = _now_iso()
        print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] iter {iteration} starting...",
              file=sys.stderr, flush=True)

        con = None
        connected = False
        try:
            con = connect(database)
            cur = con.cursor()
            connected = True
        except Exception as exc:
            print(f"  CONNECTION ERROR: {exc}", file=sys.stderr, flush=True)
            write_record(output_path, {
                "iteration": iteration, "iteration_started_at": ts_start,
                "iteration_finished_at": _now_iso(), "raw_rows": 0, "mv_rows": 0,
                "system": args.system, "machine": args.machine,
                "cluster_size": args.cluster_size, "comment": comment, "tags": TAGS,
                "result": [[None] for _ in range(total)],
                "compilation_time": [[None] for _ in range(total)],
                "execution_time": [[None] for _ in range(total)],
            })
            if con is not None:
                try: con.close()
                except Exception: pass

        if connected:
            try:
                raw_rows = scalar_query(cur, f"SELECT COUNT(*) FROM {database}.{SCHEMA}.{RAW_TABLE}")
                mv_rows = scalar_query(cur, f"SELECT COUNT(*) FROM {database}.{SCHEMA}.{MV_TABLE}")
                print(f"  raw_rows={raw_rows}  mv_rows={mv_rows}", file=sys.stderr, flush=True)
                result, compile_times, exec_times = [], [], []
                for i, q in enumerate(queries):
                    d, c, e = time_query(cur, database, q)
                    result.append([d]); compile_times.append([c]); exec_times.append([e])
                    fmt = lambda v: "null" if v is None else v
                    print(f"  q{i + 1}/{total}: {fmt(d)}s  (compile {fmt(c)}s, exec {fmt(e)}s)",
                          file=sys.stderr, flush=True)
                write_record(output_path, {
                    "iteration": iteration, "iteration_started_at": ts_start,
                    "iteration_finished_at": _now_iso(), "raw_rows": raw_rows, "mv_rows": mv_rows,
                    "system": args.system, "machine": args.machine,
                    "cluster_size": args.cluster_size, "comment": comment, "tags": TAGS,
                    "result": result,
                    "compilation_time": compile_times,
                    "execution_time": exec_times,
                })
            finally:
                try: cur.close()
                except Exception: pass
                try: con.close()
                except Exception: pass

        # Next fire = this iteration's SCHEDULED time + period (fixed rate), NOT now (fixed delay).
        next_fire += period
        now = time.monotonic()
        if next_fire <= now:
            behind = now - next_fire
            print(f"  WARN: iteration exceeded the {period}s cadence by {behind:.1f}s; next fires "
                  f"immediately (a single connection can't overlap runs).", file=sys.stderr, flush=True)
            next_fire = now
        else:
            print(f"  done. next iteration in {next_fire - now:.0f}s (fixed {period}s cadence).",
                  file=sys.stderr, flush=True)

    print(f"\nStopped after {iteration} iterations.", file=sys.stderr)
