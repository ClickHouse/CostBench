#!/usr/bin/env python3
"""
Shared logic for the Snowflake dashboard and drilldown query runners.

Snowflake (snowflake-connector-python) analogue of the ClickHouse shell runners
run_dashboard.sh / run_drilldown.sh. Produces BYTE-IDENTICAL JSONL records given
the same metadata, so one analysis pipeline ingests output from all systems.

Each iteration: ISO-8601 UTC start -> COUNT(*) raw (QUOTES) -> COUNT(*) mv
(QUOTES_DAILY) -> run each query once capturing SERVER-SIDE execution time
(EXECUTION_TIME from QUERY_HISTORY, ms -> seconds) -> ISO end -> append one JSONL
line -> sleep --interval. Runs until Ctrl-C / SIGTERM.

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


def _execution_seconds(cur, database, sfqid, tries=4, delay=1.0):
    """Server-side EXECUTION_TIME (ms) for a query id -> seconds float. Retries
    a few times because a just-finished query can take ~1s to appear in
    QUERY_HISTORY. None on persistent failure (-> JSON null)."""
    for attempt in range(tries):
        try:
            cur.execute(
                f"SELECT EXECUTION_TIME FROM TABLE({database}.INFORMATION_SCHEMA.QUERY_HISTORY()) "
                f"WHERE QUERY_ID = %s",
                (sfqid,),
            )
            row = cur.fetchone()
            if row is not None and row[0] is not None:
                return float(row[0]) / 1000.0
        except Exception as exc:
            print(f"  WARN: timing lookup failed for {sfqid}: {exc}", file=sys.stderr, flush=True)
            return None
        if attempt < tries - 1:
            time.sleep(delay)
    return None


# An interactive warehouse aborts any SELECT that exceeds its (max 5s) query timeout.
# Record the string "timeout" for those instead of failing/null, so the JSONL distinguishes
# "couldn't meet the interactive latency bar" from a genuine error.
_TIMEOUT_MARKERS = ("timeout", "exceeded", "canceled", "cancelled")


def time_query(cur, database, query):
    """Run one query once; return server-side seconds (float), "timeout" if the interactive
    warehouse aborted it on the timeout limit, or None on any other error. Never raises."""
    try:
        if WAREHOUSE != TRACK_WAREHOUSE:
            cur.execute(f"USE WAREHOUSE {WAREHOUSE}")   # run the timed query on the measured wh
        cur.execute(query)
        cur.fetchall()              # drain so the engine fully executes it
        sfqid = cur.sfqid
    except Exception as exc:
        msg = str(exc).lower()
        code = getattr(exc, "errno", None)
        if code in (604, 630) or any(m in msg for m in _TIMEOUT_MARKERS):
            print(f"  QUERY TIMEOUT (interactive limit): {exc}", file=sys.stderr, flush=True)
            return "timeout"
        print(f"  QUERY ERROR: {exc}", file=sys.stderr, flush=True)
        return None
    finally:
        if WAREHOUSE != TRACK_WAREHOUSE:                # back to tracking wh for counts/lookups
            try:
                cur.execute(f"USE WAREHOUSE {TRACK_WAREHOUSE}")
            except Exception:
                pass
    return _execution_seconds(cur, database, sfqid)


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
    while not stop.requested:
        iteration += 1
        ts_start = _now_iso()
        print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] iter {iteration} starting...",
              file=sys.stderr, flush=True)

        try:
            con = connect(database)
            cur = con.cursor()
        except Exception as exc:
            print(f"  CONNECTION ERROR: {exc}", file=sys.stderr, flush=True)
            write_record(output_path, {
                "iteration": iteration, "iteration_started_at": ts_start,
                "iteration_finished_at": _now_iso(), "raw_rows": 0, "mv_rows": 0,
                "system": args.system, "machine": args.machine,
                "cluster_size": args.cluster_size, "comment": comment, "tags": TAGS,
                "result": [[None] for _ in range(total)],
            })
            stop.sleep(args.interval)
            continue

        try:
            raw_rows = scalar_query(cur, f"SELECT COUNT(*) FROM {database}.{SCHEMA}.{RAW_TABLE}")
            mv_rows = scalar_query(cur, f"SELECT COUNT(*) FROM {database}.{SCHEMA}.{MV_TABLE}")
            print(f"  raw_rows={raw_rows}  mv_rows={mv_rows}", file=sys.stderr, flush=True)
            result = []
            for i, q in enumerate(queries):
                d = time_query(cur, database, q)
                result.append([d])
                print(f"  q{i + 1}/{total}: {'null' if d is None else d}s", file=sys.stderr, flush=True)
            write_record(output_path, {
                "iteration": iteration, "iteration_started_at": ts_start,
                "iteration_finished_at": _now_iso(), "raw_rows": raw_rows, "mv_rows": mv_rows,
                "system": args.system, "machine": args.machine,
                "cluster_size": args.cluster_size, "comment": comment, "tags": TAGS,
                "result": result,
            })
        finally:
            try: cur.close()
            except Exception: pass
            try: con.close()
            except Exception: pass

        print(f"  done. sleeping {args.interval}s...", file=sys.stderr, flush=True)
        stop.sleep(args.interval)

    print(f"\nStopped after {iteration} iterations.", file=sys.stderr)
