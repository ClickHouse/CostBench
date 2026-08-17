#!/usr/bin/env python3
"""Validate local SQL and, optionally, BigQuery objects without running data queries."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from bq_common import json_default, load_queries, render_sql, split_sql_statements

SCRIPT_DIR = Path(__file__).resolve().parent
EXPECTED_SCHEMA = [
    ("sym", "STRING", "NULLABLE"),
    ("bx", "INT64", "NULLABLE"),
    ("bp", "FLOAT64", "NULLABLE"),
    ("bs", "INT64", "NULLABLE"),
    ("ax", "INT64", "NULLABLE"),
    ("ap", "FLOAT64", "NULLABLE"),
    ("as", "INT64", "NULLABLE"),
    ("c", "INT64", "NULLABLE"),
    ("i", "INT64", "REPEATED"),
    ("t", "INT64", "NULLABLE"),
    ("q", "INT64", "NULLABLE"),
    ("z", "INT64", "NULLABLE"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT", "offline-project"))
    parser.add_argument("--dataset", default="offline_dataset")
    parser.add_argument("--location", default="US")
    parser.add_argument("--online", action="store_true", help="Authenticate, inspect objects, and dry-run all six queries")
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def offline_checks(project: str, dataset: str) -> dict[str, Any]:
    mv_queries = load_queries(SCRIPT_DIR / "queries_mv.sql", project, dataset)
    raw_queries = load_queries(SCRIPT_DIR / "queries_raw.sql", project, dataset)
    ddl = split_sql_statements(
        render_sql((SCRIPT_DIR / "create.sql").read_text(encoding="utf-8"), project, dataset)
    )
    checks = {
        "create_statement_count": len(ddl),
        "dashboard_query_count": len(mv_queries),
        "drilldown_query_count": len(raw_queries),
        "cache_policy": "runner forces QueryJobConfig(use_query_cache=False)",
    }
    errors = []
    if len(ddl) != 2:
        errors.append(f"expected 2 DDL statements, found {len(ddl)}")
    if len(mv_queries) != 4:
        errors.append(f"expected 4 dashboard queries, found {len(mv_queries)}")
    if len(raw_queries) != 2:
        errors.append(f"expected 2 drilldown queries, found {len(raw_queries)}")
    checks["errors"] = errors
    return checks


def online_checks(project: str, dataset: str, location: str) -> dict[str, Any]:
    from google.cloud import bigquery

    client = bigquery.Client(project=project, location=location)
    report: dict[str, Any] = {}
    dataset_obj = client.get_dataset(f"{project}.{dataset}")
    report["dataset_location"] = dataset_obj.location
    if dataset_obj.location.lower() != location.lower():
        report.setdefault("errors", []).append(
            f"dataset location {dataset_obj.location!r} does not match requested {location!r}"
        )

    raw = client.get_table(f"{project}.{dataset}.quotes")
    type_aliases = {"INTEGER": "INT64", "FLOAT": "FLOAT64"}
    actual_schema = [
        (field.name, type_aliases.get(field.field_type, field.field_type), field.mode)
        for field in raw.schema
    ]
    report["raw_schema"] = actual_schema
    if actual_schema != EXPECTED_SCHEMA:
        report.setdefault("errors", []).append("raw table schema differs from create.sql contract")
    report["raw_clustering_fields"] = raw.clustering_fields
    if raw.clustering_fields != ["sym", "t"]:
        report.setdefault("errors", []).append("raw table must be clustered by ['sym', 't']")

    mv = client.get_table(f"{project}.{dataset}.quotes_daily")
    report["mv_enable_refresh"] = mv.mview_enable_refresh
    report["mv_refresh_interval_sec"] = (
        mv.mview_refresh_interval.total_seconds() if mv.mview_refresh_interval else None
    )
    report["mv_last_refresh_time"] = mv.mview_last_refresh_time
    if mv.mview_query is None:
        report.setdefault("errors", []).append("quotes_daily is not a materialized view")

    dry_runs = []
    all_queries = load_queries(SCRIPT_DIR / "queries_mv.sql", project, dataset) + load_queries(
        SCRIPT_DIR / "queries_raw.sql", project, dataset
    )
    for number, sql in enumerate(all_queries, start=1):
        config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = client.query(sql, job_config=config, location=location)
        dry_runs.append(
            {
                "query_number": number,
                "estimated_bytes_processed": int(job.total_bytes_processed or 0),
                "cache_disabled": not config.use_query_cache,
            }
        )
    report["dry_runs"] = dry_runs
    return report


def main() -> int:
    args = parse_args()
    report = {
        "project": args.project,
        "dataset": args.dataset,
        "location": args.location,
        "offline": offline_checks(args.project, args.dataset),
    }
    if args.online:
        try:
            report["online"] = online_checks(args.project, args.dataset, args.location)
        except Exception as exc:
            report["online"] = {"errors": [f"{type(exc).__name__}: {exc}"]}
    rendered = json.dumps(report, indent=2, default=json_default, sort_keys=True)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    errors = report["offline"].get("errors", []) + report.get("online", {}).get("errors", [])
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
