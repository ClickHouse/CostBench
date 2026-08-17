#!/usr/bin/env python3
"""Fixed-rate BigQuery dashboard/drill-down runner with job-level accounting."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import sys
import time
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

from google.cloud import bigquery

from bq_common import (
    aligned_metric_arrays,
    append_jsonl,
    iso_utc,
    job_labels,
    load_queries,
    query_job_stats,
    read_progress,
    table_snapshot,
    utc_now,
)

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULTS = {
    "dashboard": (SCRIPT_DIR / "queries_mv.sql", 600),
    "drilldown": (SCRIPT_DIR / "queries_raw.sql", 3600),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runner-name", choices=sorted(DEFAULTS), required=True)
    parser.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT"))
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--location", default="US")
    parser.add_argument("--queries", type=Path)
    parser.add_argument("--interval", type=float)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--output-dir", type=Path, default=Path("."))
    parser.add_argument("--progress-file", type=Path)
    parser.add_argument("--raw-table", default="quotes")
    parser.add_argument("--mv-table", default="quotes_daily")
    parser.add_argument("--query-timeout", type=float, default=0.0, help="0 means no client-side timeout")
    parser.add_argument("--iterations", type=int, default=0, help="0 means continue until interrupted")
    parser.add_argument("--run-id")
    parser.add_argument("--print-queries", action="store_true", help="Render SQL and exit without contacting BigQuery")
    parser.add_argument("system")
    parser.add_argument("machine_desc")
    parser.add_argument("cluster_size")
    parser.add_argument("base_comment")
    parser.add_argument(
        "compatibility_flag",
        help="Retained to keep the ClickHouse wrapper's five positional metadata arguments; recorded but not used.",
    )
    args = parser.parse_args()
    if not args.project:
        parser.error("--project or GOOGLE_CLOUD_PROJECT is required")
    default_queries, default_interval = DEFAULTS[args.runner_name]
    args.queries = args.queries or default_queries
    args.interval = args.interval or default_interval
    if args.interval <= 0 or args.query_timeout < 0 or args.iterations < 0:
        parser.error("--interval must be > 0; --query-timeout and --iterations must be >= 0")
    return args


def run_query(
    client: bigquery.Client,
    sql: str,
    query_number: int,
    runner_name: str,
    run_id: str,
    location: str,
    timeout: float,
) -> dict[str, Any]:
    labels = job_labels(runner_name, run_id)
    labels["query_no"] = str(query_number)
    config = bigquery.QueryJobConfig(
        use_query_cache=False,
        priority=bigquery.QueryPriority.INTERACTIVE,
        labels=labels,
    )
    job = None
    error = None
    statistics_reload_error = None
    started = time.monotonic()
    query_finished = None
    try:
        prefix = f"fpra_{runner_name[:8]}_q{query_number}_"
        job = client.query(sql, job_config=config, location=location, job_id_prefix=prefix)
        kwargs = {"max_results": 1}
        if timeout:
            kwargs["timeout"] = timeout
        job.result(**kwargs)
        query_finished = time.monotonic()
        # Match the existing Bench2Cost `bq show -j` accounting. Very short
        # queries can already be DONE in the initial response, causing the
        # Python client to skip jobs.get and leave billing fields absent.
        # Refresh only after stopping the client-side query timer.
        try:
            job.reload()
        except Exception as exc:
            statistics_reload_error = f"{type(exc).__name__}: {exc}"
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        if job is not None:
            try:
                if timeout and not job.done():
                    job.cancel()
                job.reload()
            except Exception:
                pass
    client_wall = (query_finished or time.monotonic()) - started
    stats = query_job_stats(job, client_wall_s=client_wall, error=error)
    stats["statistics_reload_error"] = statistics_reload_error
    stats["query_number"] = query_number
    stats["sql_sha256"] = hashlib.sha256(sql.encode("utf-8")).hexdigest()
    return stats


def metadata_snapshot(
    client: bigquery.Client,
    args: argparse.Namespace,
) -> dict[str, Any]:
    raw_id = f"{args.project}.{args.dataset}.{args.raw_table}"
    mv_id = f"{args.project}.{args.dataset}.{args.mv_table}"
    data: dict[str, Any] = {}
    try:
        data["raw_table"] = table_snapshot(client, raw_id)
    except Exception as exc:
        data["raw_table"] = {"table_id": raw_id, "error": str(exc), "num_rows": None}
    try:
        data["materialized_view"] = table_snapshot(client, mv_id)
    except Exception as exc:
        data["materialized_view"] = {"table_id": mv_id, "error": str(exc), "num_rows": None}

    progress = read_progress(args.progress_file)
    data["ingest_progress"] = progress
    progress_matches = (
        progress
        and progress.get("project") == args.project
        and progress.get("dataset") == args.dataset
        and progress.get("table") == args.raw_table
    )
    if progress_matches and progress.get("logical_raw_rows") is not None:
        data["raw_rows"] = int(progress["logical_raw_rows"])
        data["raw_rows_source"] = "write_api_ack_progress"
    else:
        data["raw_rows"] = data["raw_table"].get("num_rows")
        data["raw_rows_source"] = "tables.get_metadata"
    data["mv_rows"] = data["materialized_view"].get("num_rows")
    data["mv_rows_source"] = "tables.get_metadata"
    return data


def main() -> int:
    args = parse_args()
    queries = load_queries(args.queries, args.project, args.dataset)
    expected = 4 if args.runner_name == "dashboard" else 2
    if len(queries) != expected:
        raise RuntimeError(f"{args.queries} contains {len(queries)} queries; expected {expected}")
    if args.print_queries:
        for index, query in enumerate(queries, start=1):
            print(f"-- query {index}\n{query};\n")
        return 0

    run_id = args.run_id or f"bq-{utc_now().strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    if args.output is None:
        stamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
        args.output = args.output_dir / f"{args.runner_name}_{stamp}.jsonl"
    args.output = args.output.expanduser().resolve()
    if args.progress_file:
        args.progress_file = args.progress_file.expanduser().resolve()

    client = bigquery.Client(project=args.project, location=args.location)
    stopped = False
    signal_count = 0

    def request_stop(_signum: int, _frame: Any) -> None:
        nonlocal stopped, signal_count
        signal_count += 1
        stopped = True
        if signal_count >= 2:
            raise KeyboardInterrupt

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    print(f"Parsed {len(queries)} queries from {args.queries}", file=sys.stderr)
    print(f"Writing JSONL to {args.output}", file=sys.stderr)
    print(f"Run ID {run_id}; fixed-rate interval {args.interval:g}s; Ctrl-C to stop.", file=sys.stderr)

    iteration = 0
    schedule_anchor_wall = utc_now()
    schedule_anchor_monotonic = time.monotonic()
    next_fire = schedule_anchor_monotonic
    while not stopped and (args.iterations == 0 or iteration < args.iterations):
        delay = next_fire - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        if stopped:
            break

        iteration += 1
        scheduled_fire = next_fire
        scheduled_start = schedule_anchor_wall + timedelta(
            seconds=scheduled_fire - schedule_anchor_monotonic
        )
        actual_start_monotonic = time.monotonic()
        iteration_started = utc_now()
        print(f"[{iteration_started.strftime('%H:%M:%S')}] iteration {iteration} starting", file=sys.stderr)
        metadata = metadata_snapshot(client, args)
        print(
            f"  raw_rows={metadata['raw_rows']} ({metadata['raw_rows_source']}) "
            f"mv_rows={metadata['mv_rows']}",
            file=sys.stderr,
        )

        job_stats: list[dict[str, Any]] = []
        interrupted = False
        for query_number, sql in enumerate(queries, start=1):
            if stopped:
                interrupted = True
                break
            stats = run_query(
                client,
                sql,
                query_number,
                args.runner_name,
                run_id,
                args.location,
                args.query_timeout,
            )
            job_stats.append(stats)
            print(
                f"  q{query_number}/{len(queries)}: runtime={stats['runtime_sec']}s "
                f"slot={stats['billed_slot_sec']}s bytes={stats['total_bytes_billed']} "
                f"cache={stats['cache_hit']} job={stats['job_id']} error={stats['error']}",
                file=sys.stderr,
            )
        if interrupted or len(job_stats) != len(queries):
            print(f"  interrupted; discarding incomplete iteration {iteration}", file=sys.stderr)
            break

        # These three arrays are deliberately shape-aligned by query and trial,
        # matching Bench2Cost's BigQuery result format. One scheduled iteration
        # contains one trial per query.
        metric_arrays = aligned_metric_arrays(job_stats)

        record = {
            "schema_version": 2,
            "run_id": run_id,
            "runner": args.runner_name,
            "iteration": iteration,
            "scheduled_start_at": iso_utc(scheduled_start),
            "start_lag_sec": max(0.0, actual_start_monotonic - scheduled_fire),
            "iteration_started_at": iso_utc(iteration_started),
            "iteration_finished_at": iso_utc(utc_now()),
            "iteration_elapsed_sec": time.monotonic() - actual_start_monotonic,
            "scheduled_interval_sec": args.interval,
            "raw_rows": metadata["raw_rows"],
            "raw_rows_source": metadata["raw_rows_source"],
            "raw_rows_metadata": metadata["raw_table"].get("num_rows"),
            "mv_rows": metadata["mv_rows"],
            "mv_rows_source": metadata["mv_rows_source"],
            "ingest_progress_updated_at": (
                metadata["ingest_progress"].get("updated_at") if metadata["ingest_progress"] else None
            ),
            "ingest_acknowledged_rows": (
                metadata["ingest_progress"].get("acknowledged_rows") if metadata["ingest_progress"] else None
            ),
            "ingestion_finished": (
                metadata["ingest_progress"].get("finished") if metadata["ingest_progress"] else None
            ),
            "active_ingestion": (
                not metadata["ingest_progress"].get("finished") if metadata["ingest_progress"] else None
            ),
            "mv_last_refresh_time": metadata["materialized_view"].get("mview_last_refresh_time"),
            "mv_refresh_watermark": metadata["materialized_view"].get("mview_refresh_watermark"),
            "system": args.system,
            "version": "managed",
            "machine": args.machine_desc,
            "cluster_size": args.cluster_size,
            "comment": (
                f"{args.base_comment} ({args.runner_name}, query_cache=false, priority=INTERACTIVE, "
                f"compatibility_flag={args.compatibility_flag})"
            ).strip(),
            "tags": ["serverless", "column-oriented", "gcp", "managed"],
            **metric_arrays,
            "query_jobs": job_stats,
        }
        append_jsonl(args.output, record)

        next_fire += args.interval
        now = time.monotonic()
        if next_fire <= now:
            behind = now - next_fire
            print(
                f"  WARN: iteration exceeded cadence by {behind:.1f}s; next starts immediately; this runner does not overlap itself.",
                file=sys.stderr,
            )
            next_fire = now
        else:
            print(f"  done; next iteration in {next_fire - now:.1f}s", file=sys.stderr)

    print(f"Stopped after {iteration} iteration(s).", file=sys.stderr)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        sys.exit(130)
    except Exception as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        sys.exit(1)
