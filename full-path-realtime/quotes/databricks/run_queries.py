#!/usr/bin/env python3
"""
Runs each .sql file in a queries directory on a fixed interval, recording:
  - query name
  - row count in the table at time of run
  - query duration

Results are appended to a CSV file.

Usage:
    export DATABRICKS_HOST="https://dbc-....cloud.databricks.com"
    export DATABRICKS_TOKEN="dapi..."

    python3 run_queries.py \
        --queries-dir ./queries \
        --interval 60 \
        --output results.csv
"""

import argparse
import csv
import os
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


def make_client() -> WorkspaceClient:
    return WorkspaceClient()


def run_sql(client: WorkspaceClient, warehouse_id: str, statement: str):
    resp  = client.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        wait_timeout="0s",
    )
    state = resp.status.state
    while state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(0.2)
        resp  = client.statement_execution.get_statement(statement_id=resp.statement_id)
        state = resp.status.state

    if state == StatementState.FAILED:
        raise RuntimeError(resp.status.error.message)
    return resp


def get_row_count(client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str, table: str) -> int:
    try:
        resp = run_sql(client, warehouse_id, f"SELECT count(*) FROM {catalog}.{schema}.{table}")
        data = resp.result.data_array if resp.result else None
        return int(data[0][0]) if data else 0
    except Exception:
        return 0


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def main():
    parser = argparse.ArgumentParser(description="Run benchmark queries on an interval")
    parser.add_argument("--queries-dir", required=True)
    parser.add_argument("--interval",    type=float, default=60.0,
                        help="Seconds between full rounds of all queries")
    parser.add_argument("--output",      default="results.csv")
    parser.add_argument("--catalog",     default="workspace")
    parser.add_argument("--schema",      default="benchmarking")
    parser.add_argument("--table",       default="quotes")
    parser.add_argument("--warehouse",   default=_warehouse_id)
    parser.add_argument("--rounds",      type=int, default=0,
                        help="Number of rounds to run. 0 = run forever.")
    args = parser.parse_args()

    if not args.warehouse:
        raise SystemExit("ERROR: --warehouse or DATABRICKS_HTTP_PATH is required")

    client      = make_client()
    queries_dir = Path(args.queries_dir)
    query_files = sorted(queries_dir.glob("*.sql"))
    if not query_files:
        raise SystemExit(f"ERROR: no .sql files found in {queries_dir}")

    output_path  = Path(args.output)
    write_header = not output_path.exists()

    print(f"Queries:   {', '.join(f.stem for f in query_files)}")
    print(f"Interval:  {args.interval}s")
    print(f"Output:    {output_path}")
    print(f"Target:    {args.catalog}.{args.schema}.{args.table}")
    print()

    round_num = 0
    with open(output_path, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        if write_header:
            writer.writerow(["timestamp", "round", "query", "row_count", "duration_s"])

        while args.rounds == 0 or round_num < args.rounds:
            round_num += 1
            row_count  = get_row_count(client, args.warehouse, args.catalog, args.schema, args.table)
            print(f"[{now_utc()}] round {round_num}  row_count={row_count:,}")

            for qf in query_files:
                sql     = qf.read_text()
                t_start = time.time()
                try:
                    run_sql(client, args.warehouse, sql)
                    duration = time.time() - t_start
                    status   = f"{duration:.2f}s"
                except Exception as exc:
                    duration = time.time() - t_start
                    status   = f"ERROR: {exc}"

                ts = now_utc()
                writer.writerow([ts, round_num, qf.stem, row_count, f"{duration:.3f}"])
                csvfile.flush()
                print(f"  {qf.stem:<40} {status}")

            if args.rounds == 0 or round_num < args.rounds:
                time.sleep(args.interval)


if __name__ == "__main__":
    main()
