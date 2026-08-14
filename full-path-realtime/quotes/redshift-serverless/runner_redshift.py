#!/usr/bin/env python3
"""
Redshift Serverless dashboard/drilldown query runner (long-running, JSONL).

The Redshift analogue of the Snowflake ../snowflake/runner_common.py + run_dashboard.py /
run_drilldown.py. Produces the SAME JSONL schema so the shared _viz pipeline ingests it under the
"Redshift" vendor. One self-contained file (dashboard vs drilldown differ only by --queries/--interval).

Each iteration: ISO-8601 UTC start -> volume counters -> run each query once, capturing SERVER-SIDE
timings from SYS_QUERY_HISTORY (microseconds -> seconds) -> ISO end -> append one JSONL line.
Runs until Ctrl-C / SIGTERM.

THREE TARGET ROLES (--role), one runner process each, all writing the same schema:
  dashboard        queries_dashboard.sql        -> quotes_daily     (the (sym,day) rollup)
  drilldown_super  queries_drilldown_super.sql  -> quotes_streamed  (live SUPER payload)
  drilldown_typed  queries_drilldown_typed.sql  -> quotes_typed     (typed columns)
The two drilldown roles run the SAME logic against different physical representations, so their
latency series are directly comparable — that comparison is the point of the T2 Redshift run.

TRACKING OVERHEAD IS KEPT OFF THE MEASURED PATH (--counts-mode, default "rollup"):
a COUNT(*) on the multi-billion-row streaming MV is itself a real workload and would pollute the
workgroup being measured. Instead the volume axis is derived from the small rollup:
  raw_rows = SUM(n_quotes) FROM quotes_daily,  mv_rows = COUNT(*) FROM quotes_daily
which is a scan of ~thousands of rows, not billions. Use --counts-mode exact only when you
deliberately want the true COUNT(*) (and accept it lands in the measured workgroup's usage), or
--counts-mode none to emit nulls and take the volume axis from the controller's lag file.

Three timings per query, matching the Snowflake schema:
  "result"           elapsed_time    -- total server-side latency (the reported number)
  "compilation_time" compile_time    -- Redshift compiles query segments (first run can be seconds)
  "execution_time"   execution_time

Scheduling is FIXED-RATE (iteration N fires at loop_start + N*--interval), identical to the fixed
Snowflake runner: a slow query set does NOT push the cadence out. See ../snowflake/runner_common.py.

Auth: direct SQL over the Serverless endpoint with a DB user/password (no AWS creds needed). Env:
  RS_HOST      workgroup endpoint. Point this at the READER workgroup for the measured runs — it
               serves the shared objects on its own RPUs, isolating read cost from ingest/refresh.
               NOTE the reader is NOT publicly accessible (a publicly-accessible consumer cannot
               read datashare objects), so the runner must execute from inside the VPC.
  RS_PORT      5439            RS_DB    quotes
  RS_USER      cbadmin         RS_PASSWORD  <the admin password>   (from terraform.tfstate)
  RS_MV_TABLE  quotes_daily    -- small rollup used for the volume counters
When reading through the datashare, set RS_MV_TABLE to the qualified name, e.g.
  RS_MV_TABLE=shared_public.quotes_daily
Requires: pip install redshift_connector
"""
import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import redshift_connector
except ImportError:
    sys.exit("ERROR: pip install redshift_connector  (into the runner's venv)")

HOST = os.environ["RS_HOST"]
PORT = int(os.environ.get("RS_PORT", "5439"))
DB   = os.environ.get("RS_DB", "quotes")
USER = os.environ["RS_USER"]
PASSWORD  = os.environ["RS_PASSWORD"]
MV_TABLE  = os.environ.get("RS_MV_TABLE", "quotes_daily")
RAW_TABLE = os.environ.get("RS_RAW_TABLE", "quotes_streamed")   # only used by --counts-mode exact
SEARCH_PATH = os.environ.get("RS_SEARCH_PATH", "")              # e.g. shared_public (reader/datashare)
TAGS = ["managed", "aws", "redshift"]

# role -> (default query file, the object the queries actually read). The runner does not enforce
# the pairing; it records it so each output file is self-describing.
ROLES = {
    "dashboard":       ("sql/queries_dashboard.sql",       "quotes_daily"),
    "drilldown_super": ("sql/queries_drilldown_super.sql", "quotes_streamed"),
    "drilldown_typed": ("sql/queries_drilldown_typed.sql", "quotes_typed"),
}


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


def connect():
    con = redshift_connector.connect(host=HOST, port=PORT, database=DB, user=USER, password=PASSWORD)
    con.autocommit = True
    cur = con.cursor()
    # Disable the result cache so every timed query actually executes (else a repeated identical
    # query returns ~0ms from cache, faking latency) — the analogue of Snowflake USE_CACHED_RESULT=FALSE.
    cur.execute("SET enable_result_cache_for_session TO off")
    # On the READER the objects are reached through the datashare's external schema, while the query
    # files use unqualified names (so ONE set of files works against both endpoints). Setting
    # search_path resolves them. e.g. RS_SEARCH_PATH=shared_public
    if SEARCH_PATH:
        cur.execute(f"SET search_path TO {SEARCH_PATH}")
    cur.close()
    return con


def _strip_line_comments(text):
    out = []
    for line in text.splitlines():
        idx = line.find("--")
        out.append(line if idx < 0 else line[:idx])
    return "\n".join(out)


def parse_queries(path):
    text = _strip_line_comments(Path(path).read_text())
    return [" ".join(q.split()) for q in text.split(";") if q.strip()]


def _query_timings(cur, qid, tries=5, delay=0.8):
    """(elapsed_s, compile_s, exec_s) for a query id from SYS_QUERY_HISTORY (microseconds -> s).
    Retries because a just-finished query can take ~1s to land in the view."""
    for attempt in range(tries):
        try:
            cur.execute(
                "SELECT elapsed_time, compile_time, execution_time "
                "FROM SYS_QUERY_HISTORY WHERE query_id = %s", (qid,))
            row = cur.fetchone()
            if row is not None and row[0] is not None:
                return tuple(None if v is None else float(v) / 1e6 for v in row)
        except Exception as exc:
            print(f"  WARN: timing lookup failed for {qid}: {exc}", file=sys.stderr, flush=True)
            return (None, None, None)
        if attempt < tries - 1:
            time.sleep(delay)
    return (None, None, None)


def time_query(cur, query):
    """Run one query once; return (result, compile_s, exec_s). result = server-side elapsed_time
    (seconds), or None on error. Never raises."""
    try:
        cur.execute(query)
        try:
            cur.fetchall()          # drain so the engine fully executes it
        except Exception:
            pass                    # some statements return no rows
        cur.execute("SELECT pg_last_query_id()")
        qid = cur.fetchone()[0]
    except Exception as exc:
        print(f"  QUERY ERROR: {exc}", file=sys.stderr, flush=True)
        return (None, None, None)
    if qid is None or int(qid) < 0:
        return (None, None, None)
    return _query_timings(cur, qid)


def scalar_query(cur, query):
    try:
        cur.execute(query)
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception as exc:
        print(f"  COUNT ERROR: {exc}", file=sys.stderr, flush=True)
        return 0


def volume_counters(cur, mode):
    """(raw_rows, mv_rows) for the volume axis — see --counts-mode in the module docstring.

    'rollup' derives the ingested-row count from the rollup's own SUM(n_quotes) so the measured
    workgroup never scans the multi-billion-row streaming MV just for telemetry. n_quotes is a
    COUNT(*) per (sym, day), so the sum is exactly the number of rows the rollup has absorbed —
    i.e. the volume as of the last daily refresh, which is the right x-axis for these queries.
    """
    if mode == "none":
        return None, None
    if mode == "exact":
        return (scalar_query(cur, f"SELECT COUNT(*) FROM {RAW_TABLE}"),
                scalar_query(cur, f"SELECT COUNT(*) FROM {MV_TABLE}"))
    return (scalar_query(cur, f"SELECT COALESCE(SUM(n_quotes), 0) FROM {MV_TABLE}"),
            scalar_query(cur, f"SELECT COUNT(*) FROM {MV_TABLE}"))


def _now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_record(output_path, record):
    with open(output_path, "a") as f:
        f.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n")


def main(argv=None):
    ap = argparse.ArgumentParser(prog="runner_redshift.py",
                                 description="Redshift Serverless query runner (long-running, JSONL).")
    ap.add_argument("--role", default="dashboard",
                    help="target role(s): dashboard | drilldown_super | drilldown_typed. "
                         "Sets the default query file and labels the output. Accepts a COMMA-SEPARATED "
                         "list, e.g. --role drilldown_typed,drilldown_super — those roles then run "
                         "BACK-TO-BACK inside one iteration (sequentially, never concurrently) so they "
                         "see the same data volume, each still writing its OWN JSONL file.")
    ap.add_argument("--queries", default="",
                    help="override the query file (only valid with a single --role)")
    ap.add_argument("--interval", type=int, default=600)
    ap.add_argument("--counts-mode", choices=("rollup", "exact", "none"), default="rollup",
                    help="how to get the volume axis. rollup (default) = SUM(n_quotes)/COUNT(*) on "
                         "the small rollup, keeping tracking off the measured path; exact = "
                         "COUNT(*) on the streaming MV (expensive); none = emit nulls.")
    ap.add_argument("--output", default="")
    ap.add_argument("--output-dir", default="./out_redshift")
    ap.add_argument("system"); ap.add_argument("machine")
    ap.add_argument("cluster_size"); ap.add_argument("comment"); ap.add_argument("extra_flag")
    args = ap.parse_args(argv)

    roles = [r.strip() for r in args.role.split(",") if r.strip()]
    unknown = [r for r in roles if r not in ROLES]
    if unknown:
        sys.exit(f"ERROR: unknown role(s) {unknown}; choose from {sorted(ROLES)}")
    if args.queries and len(roles) > 1:
        sys.exit("ERROR: --queries can only override a single --role")
    if args.output and len(roles) > 1:
        sys.exit("ERROR: --output can only be used with a single --role")

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    # One "target" per role: its query list, the object it reads, and its own output file.
    targets = []
    for role in roles:
        default_queries, target_object = ROLES[role]
        qpath = args.queries or default_queries
        qs = parse_queries(qpath)
        if not qs:
            sys.exit(f"ERROR: no queries in {qpath}")
        opath = args.output or os.path.join(args.output_dir, f"{role}_{ts}.jsonl")
        Path(os.path.dirname(opath) or ".").mkdir(parents=True, exist_ok=True)
        targets.append({
            "role": role, "queries": qs, "object": target_object, "output": opath,
            "comment": f"{args.comment} ({role} on {target_object}, {args.extra_flag})",
        })
        print(f"{len(qs)} queries from {qpath} -> {opath}", file=sys.stderr)
        print(f"  role={role} target={target_object} counts-mode={args.counts_mode}", file=sys.stderr)
    if len(targets) > 1:
        print(f"roles run BACK-TO-BACK per iteration, in this order: "
              f"{' -> '.join(t['role'] for t in targets)}", file=sys.stderr)
    print(f"Redshift {HOST}:{PORT}/{DB} user {USER}. Interval {args.interval}s (fixed-rate). Ctrl-C to stop.",
          file=sys.stderr)

    stop = _Stop()
    iteration = 0
    period = args.interval
    next_fire = time.monotonic()          # FIXED-RATE: fire at loop_start + N*period, not finish+period
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
            con = connect()
            cur = con.cursor()
            connected = True
        except Exception as exc:
            print(f"  CONNECTION ERROR: {exc}", file=sys.stderr, flush=True)
            for t in targets:
                n = len(t["queries"])
                write_record(t["output"], {
                    "iteration": iteration, "iteration_started_at": ts_start,
                    "iteration_finished_at": _now_iso(), "raw_rows": 0, "mv_rows": 0,
                    "system": args.system, "machine": args.machine, "cluster_size": args.cluster_size,
                    "comment": t["comment"], "tags": TAGS,
                    "result": [[None] for _ in range(n)],
                    "compilation_time": [[None] for _ in range(n)],
                    "execution_time": [[None] for _ in range(n)],
                })
            if con is not None:
                try: con.close()
                except Exception: pass

        if connected:
            try:
                # One volume read per iteration, shared by every role, so the back-to-back roles are
                # stamped with the SAME volume — that's the point of running them together.
                raw_rows, mv_rows = volume_counters(cur, args.counts_mode)
                print(f"  raw_rows={raw_rows}  mv_rows={mv_rows}  ({args.counts_mode})",
                      file=sys.stderr, flush=True)
                for t in targets:
                    n = len(t["queries"])
                    role_start = _now_iso()
                    result, compile_times, exec_times = [], [], []
                    for i, q in enumerate(t["queries"]):
                        d, c, e = time_query(cur, q)
                        result.append([d]); compile_times.append([c]); exec_times.append([e])
                        fmt = lambda v: "null" if v is None else v
                        label = f"{t['role']} q{i + 1}/{n}" if len(targets) > 1 else f"q{i + 1}/{n}"
                        print(f"  {label}: {fmt(d)}s  (compile {fmt(c)}s, exec {fmt(e)}s)",
                              file=sys.stderr, flush=True)
                    write_record(t["output"], {
                        "iteration": iteration, "iteration_started_at": role_start,
                        "iteration_finished_at": _now_iso(), "raw_rows": raw_rows, "mv_rows": mv_rows,
                        "system": args.system, "machine": args.machine,
                        "cluster_size": args.cluster_size,
                        "comment": t["comment"], "tags": TAGS,
                        "result": result, "compilation_time": compile_times,
                        "execution_time": exec_times,
                    })
            finally:
                try: cur.close()
                except Exception: pass
                try: con.close()
                except Exception: pass

        next_fire += period
        now = time.monotonic()
        if next_fire <= now:
            print(f"  WARN: iteration exceeded the {period}s cadence by {now - next_fire:.1f}s; "
                  f"next fires immediately.", file=sys.stderr, flush=True)
            next_fire = now
        else:
            print(f"  done. next iteration in {next_fire - now:.0f}s (fixed {period}s cadence).",
                  file=sys.stderr, flush=True)

    print(f"\nStopped after {iteration} iterations.", file=sys.stderr)


if __name__ == "__main__":
    main()
