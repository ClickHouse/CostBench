#!/usr/bin/env python3
"""Create the BigQuery dataset, raw table, and incremental materialized view."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from bq_common import job_labels, render_sql, split_sql_statements

SCRIPT_DIR = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT"), required=False)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--location", default="US")
    parser.add_argument("--create-sql", type=Path, default=SCRIPT_DIR / "create.sql")
    parser.add_argument("--run-id", default="setup")
    parser.add_argument(
        "--print-only",
        action="store_true",
        help="Render and print SQL without authenticating or changing BigQuery.",
    )
    args = parser.parse_args()
    if not args.project:
        parser.error("--project or GOOGLE_CLOUD_PROJECT is required")
    return args


def apply_schema(project: str, dataset: str, location: str, sql_path: Path, run_id: str) -> None:
    from google.cloud import bigquery

    client = bigquery.Client(project=project, location=location)
    dataset_ref = bigquery.Dataset(f"{project}.{dataset}")
    dataset_ref.location = location
    client.create_dataset(dataset_ref, exists_ok=True)

    rendered = render_sql(sql_path.read_text(encoding="utf-8"), project, dataset)
    statements = split_sql_statements(rendered)
    for index, statement in enumerate(statements, start=1):
        config = bigquery.QueryJobConfig(
            use_query_cache=False,
            labels=job_labels("setup", run_id),
        )
        print(f"DDL {index}/{len(statements)}: {statement.splitlines()[0][:100]}", flush=True)
        client.query(statement, job_config=config, location=location).result()


def main() -> int:
    args = parse_args()
    rendered = render_sql(args.create_sql.read_text(encoding="utf-8"), args.project, args.dataset)
    if args.print_only:
        print(rendered)
        return 0
    apply_schema(args.project, args.dataset, args.location, args.create_sql, args.run_id)
    print(f"Ready: {args.project}.{args.dataset}.quotes and quotes_daily")
    return 0


if __name__ == "__main__":
    sys.exit(main())
