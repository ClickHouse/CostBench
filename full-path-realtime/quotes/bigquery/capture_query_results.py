#!/usr/bin/env python3
"""Capture untimed correctness evidence for all six canonical queries."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from google.cloud import bigquery

from bq_common import (
    iso_utc,
    job_labels,
    json_default,
    load_queries,
    query_job_stats,
    utc_now,
)

SCRIPT_DIR = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT"))
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--location", default="US")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", default="validation")
    args = parser.parse_args()
    if not args.project:
        parser.error("--project or GOOGLE_CLOUD_PROJECT is required")
    return args


def canonical_row(row: Any) -> str:
    return json.dumps(
        dict(row.items()),
        default=json_default,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def main() -> int:
    args = parse_args()
    args.output_dir = args.output_dir.expanduser().resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    client = bigquery.Client(project=args.project, location=args.location)
    suites = [
        ("dashboard", load_queries(SCRIPT_DIR / "queries_mv.sql", args.project, args.dataset)),
        ("drilldown", load_queries(SCRIPT_DIR / "queries_raw.sql", args.project, args.dataset)),
    ]
    manifest = {
        "schema_version": 1,
        "captured_at": iso_utc(utc_now()),
        "project": args.project,
        "dataset": args.dataset,
        "location": args.location,
        "run_id": args.run_id,
        "queries": [],
    }
    failed = False
    for suite, queries in suites:
        for query_number, sql in enumerate(queries, start=1):
            output_path = args.output_dir / f"{suite}_q{query_number}.jsonl"
            config = bigquery.QueryJobConfig(
                use_query_cache=False,
                priority=bigquery.QueryPriority.INTERACTIVE,
                labels=job_labels("validation", args.run_id),
            )
            job = None
            error = None
            row_count = 0
            digest = hashlib.sha256()
            started = time.monotonic()
            try:
                job = client.query(
                    sql,
                    job_config=config,
                    location=args.location,
                    job_id_prefix=f"fpra_validate_{suite[:4]}_q{query_number}_",
                )
                with output_path.open("w", encoding="utf-8") as handle:
                    for row in job.result():
                        line = canonical_row(row)
                        handle.write(line + "\n")
                        digest.update(line.encode("utf-8"))
                        digest.update(b"\n")
                        row_count += 1
            except Exception as exc:
                failed = True
                error = f"{type(exc).__name__}: {exc}"
                if job is not None:
                    try:
                        job.reload()
                    except Exception:
                        pass
            stats = query_job_stats(job, client_wall_s=time.monotonic() - started, error=error)
            item = {
                "suite": suite,
                "query_number": query_number,
                "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
                "output_file": output_path.name,
                "row_count": row_count,
                "canonical_rows_sha256": digest.hexdigest() if error is None else None,
                "query_job": stats,
            }
            manifest["queries"].append(item)
            print(
                f"{suite} q{query_number}: rows={row_count} sha256={item['canonical_rows_sha256']} error={error}",
                file=sys.stderr,
            )
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, default=json_default, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
