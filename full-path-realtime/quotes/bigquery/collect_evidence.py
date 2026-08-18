#!/usr/bin/env python3
"""Export bounded query, refresh, ingestion, storage, and table evidence."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from google.cloud import bigquery

from bq_common import (
    iso_utc,
    job_labels,
    json_default,
    query_job_stats,
    table_snapshot,
    utc_now,
)


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            json.dump(row, handle, default=json_default, separators=(",", ":"))
            handle.write("\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT"))
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--location", default="US")
    parser.add_argument("--raw-table", default="quotes")
    parser.add_argument("--mv-table", default="quotes_daily")
    parser.add_argument("--since", type=parse_timestamp)
    parser.add_argument("--until", type=parse_timestamp)
    parser.add_argument("--hours", type=float, default=48.0)
    parser.add_argument("--run-id", default="evidence")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if not args.project:
        parser.error("--project or GOOGLE_CLOUD_PROJECT is required")
    if args.hours <= 0:
        parser.error("--hours must be > 0")
    return args


def execute(
    client: bigquery.Client,
    sql: str,
    parameters: list[Any],
    location: str,
    run_id: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    config = bigquery.QueryJobConfig(
        use_query_cache=False,
        labels=job_labels("evidence", run_id),
        query_parameters=parameters,
    )
    job = client.query(sql, job_config=config, location=location, job_id_prefix="fpra_evidence_")
    rows = [dict(row.items()) for row in job.result()]
    return rows, query_job_stats(job)


def main() -> int:
    args = parse_args()
    args.output_dir = args.output_dir.expanduser().resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    since = args.since or (utc_now() - timedelta(hours=args.hours))
    until = args.until or utc_now()
    if until <= since:
        raise ValueError("--until must be later than --since")
    region_view = f"`{args.project}`.`region-{args.location.lower()}`.INFORMATION_SCHEMA"
    client = bigquery.Client(project=args.project, location=args.location)
    parameters = [
        bigquery.ScalarQueryParameter("since", "TIMESTAMP", since),
        bigquery.ScalarQueryParameter("until", "TIMESTAMP", until),
        bigquery.ScalarQueryParameter("project", "STRING", args.project),
        bigquery.ScalarQueryParameter("dataset", "STRING", args.dataset),
        bigquery.ScalarQueryParameter("raw_table", "STRING", args.raw_table),
        bigquery.ScalarQueryParameter("mv_table", "STRING", args.mv_table),
    ]
    collector_jobs: list[dict[str, Any]] = []
    errors: list[str] = []

    jobs_sql = f"""
    SELECT
      job_id, creation_time, start_time, end_time, state, job_type,
      statement_type, priority, cache_hit, total_bytes_processed,
      total_bytes_billed, total_slot_ms, reservation_id, error_result,
      destination_table, labels
    FROM {region_view}.JOBS_BY_PROJECT
    WHERE creation_time >= @since
      AND creation_time < @until
      AND (
        EXISTS (
          SELECT 1 FROM UNNEST(labels)
          WHERE key = 'benchmark' AND value = 'full-path-realtime'
        )
        OR (
          STARTS_WITH(job_id, 'materialized_view_refresh')
          AND EXISTS (
            SELECT 1
            FROM UNNEST(referenced_tables) AS referenced_table
            WHERE referenced_table.project_id = @project
              AND referenced_table.dataset_id = @dataset
              AND referenced_table.table_id = @mv_table
          )
        )
      )
    ORDER BY creation_time, job_id
    """
    try:
        jobs, stats = execute(client, jobs_sql, parameters, args.location, args.run_id)
        collector_jobs.append(stats)
        write_jsonl(args.output_dir / "query_and_mv_jobs.jsonl", jobs)
        mv_jobs = [row for row in jobs if str(row.get("job_id", "")).startswith("materialized_view_refresh")]
        write_jsonl(args.output_dir / "mv_refresh_jobs.jsonl", mv_jobs)
    except Exception as exc:
        errors.append(f"jobs export: {type(exc).__name__}: {exc}")

    write_sql = f"""
    SELECT
      start_timestamp, project_id, dataset_id, table_id, stream_type,
      error_code, total_requests, total_rows, total_input_bytes
    FROM {region_view}.WRITE_API_TIMELINE_BY_PROJECT
    WHERE start_timestamp >= @since
      AND start_timestamp < @until
      AND dataset_id = @dataset
      AND table_id = @raw_table
    ORDER BY start_timestamp, stream_type, error_code
    """
    try:
        timeline, stats = execute(client, write_sql, parameters, args.location, args.run_id)
        collector_jobs.append(stats)
        write_jsonl(args.output_dir / "write_api_timeline.jsonl", timeline)
    except Exception as exc:
        errors.append(f"Write API timeline export: {type(exc).__name__}: {exc}")

    storage_sql = f"""
    SELECT
      project_id, table_schema, table_name, creation_time, table_type,
      total_rows, total_logical_bytes, active_logical_bytes,
      long_term_logical_bytes, current_physical_bytes,
      total_physical_bytes, active_physical_bytes,
      long_term_physical_bytes, time_travel_physical_bytes,
      fail_safe_physical_bytes, storage_last_modified_time, deleted
    FROM {region_view}.TABLE_STORAGE_BY_PROJECT
    WHERE table_schema = @dataset
      AND table_name IN (@raw_table, @mv_table)
      AND NOT deleted
    ORDER BY table_name
    """
    try:
        storage, stats = execute(client, storage_sql, parameters, args.location, args.run_id)
        collector_jobs.append(stats)
        write_jsonl(args.output_dir / "table_storage.jsonl", storage)
    except Exception as exc:
        errors.append(f"table storage export: {type(exc).__name__}: {exc}")

    snapshots = {}
    for name in (args.raw_table, args.mv_table):
        table_id = f"{args.project}.{args.dataset}.{name}"
        try:
            snapshots[name] = table_snapshot(client, table_id)
        except Exception as exc:
            snapshots[name] = {"table_id": table_id, "error": str(exc)}

    summary = {
        "collected_at": iso_utc(utc_now()),
        "since": iso_utc(since),
        "until": iso_utc(until),
        "project": args.project,
        "dataset": args.dataset,
        "location": args.location,
        "table_snapshots": snapshots,
        "collector_query_jobs": collector_jobs,
        "errors": errors,
    }
    (args.output_dir / "evidence_summary.json").write_text(
        json.dumps(summary, indent=2, default=json_default, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    for error in errors:
        print(error, file=sys.stderr)
    print(f"Evidence written to {args.output_dir}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
