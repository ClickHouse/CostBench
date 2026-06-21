#!/usr/bin/env python3
"""
Drilldown runner — loops the raw quotes query every --interval seconds
(default hourly), appending one JSONL record per iteration to --output.
Run in parallel to the ingest script; Ctrl-C when ingest finishes.

Usage:
    export DATABRICKS_HOST="https://dbc-....cloud.databricks.com"
    export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<id>"
    export DATABRICKS_TOKEN="dapi..."

    python3 run_drilldown.py \
        --system "Databricks" \
        --machine "Small warehouse" \
        --cluster-size 1 \
        --comment "10B rows" \
        --interval 3600 \
        --output drilldown.jsonl
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

_http_path    = os.environ.get("DATABRICKS_HTTP_PATH", "")
_warehouse_id = _http_path.rstrip("/").split("/")[-1] if _http_path else ""

_hostname = os.environ.get("DATABRICKS_SERVER_HOSTNAME", "")
if _hostname and not os.environ.get("DATABRICKS_HOST"):
    host = f"https://{_hostname}" if not _hostname.startswith("http") else _hostname
    os.environ["DATABRICKS_HOST"] = host


def run_sql(client, warehouse_id, statement):
    resp = client.statement_execution.execute_statement(
        statement=statement, warehouse_id=warehouse_id, wait_timeout="0s")
    stmt_id = resp.statement_id
    state = resp.status.state
    while state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(0.2)
        resp = client.statement_execution.get_statement(statement_id=stmt_id)
        state = resp.status.state
    if state == StatementState.FAILED:
        raise RuntimeError(resp.status.error.message)
    return resp


def time_query(client, warehouse_id, statement):
    t0 = time.time()
    try:
        run_sql(client, warehouse_id, statement)
        return round(time.time() - t0, 3)
    except Exception as exc:
        print(f"  query error: {exc}", file=sys.stderr)
        return None


def scalar_query(client, warehouse_id, statement):
    try:
        resp = run_sql(client, warehouse_id, statement)
        data = resp.result.data_array if resp.result else None
        val = data[0][0] if data else None
        return int(val) if val is not None else 0
    except Exception:
        return 0


def parse_queries(path):
    text = Path(path).read_text()
    text = re.sub(r'--[^\n]*', '', text)
    queries = [q.strip() for q in text.split(';')]
    return [q for q in queries if q]


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queries",      default="queries_raw.sql")
    parser.add_argument("--interval",     type=float, default=3600.0)
    parser.add_argument("--output",       default="")
    parser.add_argument("--output-dir",   default=".")
    parser.add_argument("--warehouse",    default=_warehouse_id)
    parser.add_argument("--system",       default="Databricks")
    parser.add_argument("--cluster-size", default="1")
    parser.add_argument("--comment",      default="")
    args = parser.parse_args()

    if not args.warehouse:
        sys.exit("ERROR: --warehouse or DATABRICKS_HTTP_PATH is required")

    queries = parse_queries(args.queries)
    if not queries:
        sys.exit(f"ERROR: no queries found in {args.queries}")
    print(f"Parsed {len(queries)} queries from {args.queries}", file=sys.stderr)

    output = args.output or f"{args.output_dir}/drilldown_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.jsonl"
    Path(output).parent.mkdir(parents=True, exist_ok=True)
    print(f"Writing JSONL to {output}", file=sys.stderr)
    print(f"Interval {args.interval}s. Ctrl-C to stop.", file=sys.stderr)

    client = WorkspaceClient()

    try:
        resp = run_sql(client, args.warehouse, "SELECT version()")
        data = resp.result.data_array if resp.result else None
        version = data[0][0] if data else "unknown"
    except Exception:
        version = "unknown"

    iteration = 0
    try:
        while True:
            iteration += 1
            ts_start = now_utc()
            print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] iter {iteration} starting...", file=sys.stderr)

            raw_rows = scalar_query(client, args.warehouse, "SELECT count(*) FROM workspace.benchmarking.quotes")
            mv_rows  = scalar_query(client, args.warehouse, "SELECT count(*) FROM workspace.benchmarking.quotes_daily")
            print(f"  raw_rows={raw_rows:,}  mv_rows={mv_rows:,}", file=sys.stderr)

            results = []
            for i, q in enumerate(queries):
                t = time_query(client, args.warehouse, q)
                results.append([t])
                print(f"  q{i+1}/{len(queries)}: {t}s", file=sys.stderr)

            ts_end = now_utc()

            record = {
                "iteration": iteration,
                "iteration_started_at": ts_start,
                "iteration_finished_at": ts_end,
                "raw_rows": raw_rows,
                "mv_rows": mv_rows,
                "system": args.system,
                "version": version,
                "cluster_size": args.cluster_size,
                "comment": f"{args.comment} (drilldown)",
                "result": results,
            }
            with open(output, "a") as f:
                f.write(json.dumps(record) + "\n")

            print(f"  done. sleeping {args.interval}s...", file=sys.stderr)
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print(f"\nStopped after {iteration} iterations.", file=sys.stderr)


if __name__ == "__main__":
    main()
