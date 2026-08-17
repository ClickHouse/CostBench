#!/usr/bin/env python3
"""Sample BigQuery MV refresh metadata on a fixed cadence."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from google.cloud import bigquery

from bq_common import (
    append_jsonl,
    iso_utc,
    job_labels,
    query_job_stats,
    read_progress,
    utc_now,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT"))
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--location", default="US")
    parser.add_argument("--mv-table", default="quotes_daily")
    parser.add_argument("--progress-file", type=Path)
    parser.add_argument("--interval", type=float, default=60.0)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--output-dir", type=Path, default=Path("results/freshness"))
    parser.add_argument("--iterations", type=int, default=0)
    parser.add_argument("--run-id")
    args = parser.parse_args()
    if not args.project:
        parser.error("--project or GOOGLE_CLOUD_PROJECT is required")
    if args.interval <= 0 or args.iterations < 0:
        parser.error("--interval must be > 0 and --iterations must be >= 0")
    return args


def main() -> int:
    args = parse_args()
    run_id = args.run_id or f"bq-{utc_now().strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    if args.output is None:
        args.output = args.output_dir / f"mv_freshness_{utc_now().strftime('%Y%m%dT%H%M%SZ')}.jsonl"
    args.output = args.output.expanduser().resolve()
    if args.progress_file:
        args.progress_file = args.progress_file.expanduser().resolve()

    information_schema = f"`{args.project}`.`{args.dataset}`.INFORMATION_SCHEMA"
    sql = f"""
    SELECT
      table_name,
      last_refresh_time,
      refresh_watermark,
      TO_JSON_STRING(last_refresh_status) AS last_refresh_status_json
    FROM {information_schema}.MATERIALIZED_VIEWS
    WHERE table_name = @table_name
    """
    config = bigquery.QueryJobConfig(
        use_query_cache=False,
        labels=job_labels("freshness", run_id),
        query_parameters=[bigquery.ScalarQueryParameter("table_name", "STRING", args.mv_table)],
    )
    client = bigquery.Client(project=args.project, location=args.location)
    stopped = False

    def request_stop(_signum: int, _frame: Any) -> None:
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    iteration = 0
    next_fire = time.monotonic()
    print(f"Writing freshness JSONL to {args.output}; fixed interval {args.interval:g}s", file=sys.stderr)
    while not stopped and (args.iterations == 0 or iteration < args.iterations):
        delay = next_fire - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        if stopped:
            break
        iteration += 1
        observed = utc_now()
        row = None
        error = None
        job = None
        started = time.monotonic()
        try:
            job = client.query(sql, job_config=config, location=args.location, job_id_prefix="fpra_freshness_")
            rows = list(job.result(max_results=1))
            row = dict(rows[0].items()) if rows else None
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            if job is not None:
                try:
                    job.reload()
                except Exception:
                    pass
        stats = query_job_stats(job, client_wall_s=time.monotonic() - started, error=error)
        watermark = row.get("refresh_watermark") if row else None
        progress = read_progress(args.progress_file)
        record = {
            "schema_version": 1,
            "run_id": run_id,
            "iteration": iteration,
            "observed_at": iso_utc(observed),
            "project": args.project,
            "dataset": args.dataset,
            "materialized_view": args.mv_table,
            "last_refresh_time": iso_utc(row.get("last_refresh_time")) if row else None,
            "refresh_watermark": iso_utc(watermark) if watermark else None,
            "watermark_lag_sec": (observed - watermark).total_seconds() if watermark else None,
            "last_refresh_status": (
                json.loads(row["last_refresh_status_json"])
                if row and row.get("last_refresh_status_json")
                else None
            ),
            "ingest_progress_updated_at": progress.get("updated_at") if progress else None,
            "ingest_acknowledged_rows": progress.get("acknowledged_rows") if progress else None,
            "metadata_query_job": stats,
        }
        append_jsonl(args.output, record)
        print(
            f"[{observed.strftime('%H:%M:%S')}] watermark={record['refresh_watermark']} "
            f"lag={record['watermark_lag_sec']}s status={record['last_refresh_status']} error={error}",
            file=sys.stderr,
        )
        next_fire += args.interval
        if next_fire <= time.monotonic():
            next_fire = time.monotonic()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        sys.exit(1)
