#!/usr/bin/env python3
"""Summarize Redshift runner latency and committed hourly compute allocation."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
from typing import Any

from _common import portable_path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def scalar(value: Any, *, context: str) -> float:
    if not isinstance(value, list) or len(value) != 1:
        raise ValueError(f"expected one scalar trial at {context}; got {value!r}")
    item = value[0]
    if isinstance(item, bool) or not isinstance(item, (int, float)):
        raise ValueError(f"missing/non-numeric value at {context}: {item!r}")
    result = float(item)
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"invalid nonnegative value at {context}: {result}")
    return result


def load_records(path: Path, limit: int | None) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, 1):
            if not raw.strip():
                continue
            record = json.loads(raw)
            if not isinstance(record, dict):
                raise ValueError(f"expected JSON object at {path}:{line_number}")
            records.append(record)
            if limit is not None and len(records) == limit:
                break
    if not records:
        raise ValueError(f"no records in {path}")
    if limit is not None and len(records) != limit:
        raise ValueError(f"requested {limit} iterations but {path} has only {len(records)}")
    return records


def allocation_csv_metadata(path: Path) -> dict[str, Any]:
    rows = 0
    allocated = 0.0
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {"query_id", "start_time", "elapsed_s", "billed_rpu_seconds"}
        missing = required - set(reader.fieldnames or ())
        if missing:
            raise ValueError(f"{path} missing allocation columns: {sorted(missing)}")
        for row in reader:
            rows += 1
            allocated += float(row["billed_rpu_seconds"])
    return {
        "path": portable_path(path),
        "sha256": sha256(path),
        "statement_rows": rows,
        "total_allocated_compute_rpu_seconds": allocated,
        "historical_column_name": "billed_rpu_seconds",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--iterations", type=int)
    parser.add_argument(
        "--pricing",
        type=Path,
        default=Path(__file__).with_name("pricing_eu-west-2.json"),
    )
    parser.add_argument("--allocation-csv", type=Path, required=True)
    args = parser.parse_args()
    if args.iterations is not None and args.iterations <= 0:
        raise ValueError("--iterations must be positive")

    source = args.input.expanduser().resolve()
    output = args.output.expanduser().resolve()
    pricing_path = args.pricing.expanduser().resolve()
    allocation_path = args.allocation_csv.expanduser().resolve()
    pricing = json.loads(pricing_path.read_text(encoding="utf-8"))
    rate = float(pricing["redshift_serverless"]["rpu_hour_usd"])
    records = load_records(source, args.iterations)

    first = records[0]
    query_count: int | None = None
    runtime = compile_time = execution_time = allocated = 0.0
    for record_number, record in enumerate(records, 1):
        results = record.get("result")
        compiles = record.get("compilation_time")
        executions = record.get("execution_time")
        billed = record.get("billed_times")
        if not all(isinstance(value, list) for value in (results, compiles, executions, billed)):
            raise ValueError(f"missing result/timing/allocation arrays at {source}:{record_number}")
        lengths = {len(results), len(compiles), len(executions), len(billed)}
        if len(lengths) != 1:
            raise ValueError(f"query array length mismatch at {source}:{record_number}")
        if query_count is None:
            query_count = len(results)
        elif len(results) != query_count:
            raise ValueError(f"query count changed at {source}:{record_number}")
        for query_number in range(len(results)):
            context = f"{source}:{record_number}:q{query_number + 1}"
            runtime += scalar(results[query_number], context=f"{context}:result")
            compile_time += scalar(compiles[query_number], context=f"{context}:compile")
            execution_time += scalar(executions[query_number], context=f"{context}:execution")
            allocated += scalar(billed[query_number], context=f"{context}:billed_times")

    assert query_count is not None
    query_jobs = len(records) * query_count
    query_cost = allocated / 3600 * rate
    payload = {
        "schema_version": 1,
        "source_file": portable_path(source),
        "source_sha256": sha256(source),
        "system": first.get("system"),
        "machine": first.get("machine"),
        "cluster_size": first.get("cluster_size"),
        "iterations_included": len(records),
        "queries_per_iteration": query_count,
        "query_jobs": query_jobs,
        "total_runtime_seconds": round(runtime, 9),
        "total_compilation_seconds": round(compile_time, 9),
        "total_execution_seconds": round(execution_time, 9),
        "allocated_compute_rpu_seconds": round(allocated, 9),
        "costs": [
            {
                "tier": "Standard",
                "price_usd": rate,
                "price_unit": "RPU-hour",
                "allocated_compute_rpu_seconds": round(allocated, 9),
                "total_compute_cost_usd": round(query_cost, 9),
            }
        ],
        "usage_allocation_contract": {
            "source_usage_metric": "SYS_SERVERLESS_USAGE.compute_seconds",
            "bucket": "hour",
            "allocation_driver": "statement elapsed_time / total statement elapsed_time in the statement start-hour",
            "runner_field": "billed_times (historical name; values are allocated compute RPU-seconds)",
            "interpretation": "normalized query-cost allocation, not a literal invoice reconstruction",
            "live_sys_serverless_usage_dependency": False,
            "input_mode": "committed JSONL allocation populated post hoc from the committed hourly allocation artifact",
        },
        "pricing": {
            "path": portable_path(pricing_path),
            "sha256": sha256(pricing_path),
            "region": pricing["region"],
            "effective_date": pricing["effective_date"],
        },
        "allocation_evidence": allocation_csv_metadata(allocation_path),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Written: {output}")
    print(
        f"iterations={len(records)} jobs={query_jobs} runtime={runtime:.3f}s "
        f"allocated={allocated:.3f} RPU-s cost=${query_cost:.6f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
