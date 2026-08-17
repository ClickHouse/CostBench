#!/usr/bin/env python3
"""Price the shared Redshift writer + MSK complete-ingest path from uptime."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from _common import portable_path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--start", default="2026-08-12T16:53:56Z")
    parser.add_argument("--end", default="2026-08-14T00:21:03Z")
    parser.add_argument("--rows", type=int, default=113_219_565_734)
    parser.add_argument("--writer-rpu", type=float, default=128)
    parser.add_argument("--msk-brokers", type=int, default=3)
    parser.add_argument("--msk-storage-gb-per-broker", type=float, default=500)
    parser.add_argument("--msk-partitions", type=int, default=24)
    parser.add_argument(
        "--pricing",
        type=Path,
        default=Path(__file__).with_name("pricing_eu-west-2.json"),
    )
    args = parser.parse_args()
    start = parse_time(args.start)
    end = parse_time(args.end)
    duration_seconds = (end - start).total_seconds()
    if duration_seconds <= 0:
        raise ValueError("--end must be after --start")
    if args.rows <= 0 or args.writer_rpu <= 0 or args.msk_brokers <= 0:
        raise ValueError("rows, writer RPU, and broker count must be positive")

    pricing_path = args.pricing.expanduser().resolve()
    pricing = json.loads(pricing_path.read_text(encoding="utf-8"))
    hours = duration_seconds / 3600
    redshift_rate = float(pricing["redshift_serverless"]["rpu_hour_usd"])
    broker_rate = float(pricing["msk_provisioned"]["broker_hour_usd"])
    storage_rate = float(pricing["msk_provisioned"]["storage_gb_month_usd"])
    month_hours = float(pricing["month_hours"])

    writer_rpu_seconds = args.writer_rpu * duration_seconds
    writer_cost = writer_rpu_seconds / 3600 * redshift_rate
    broker_cost = args.msk_brokers * hours * broker_rate
    total_storage_gb = args.msk_brokers * args.msk_storage_gb_per_broker
    storage_cost = total_storage_gb * storage_rate * hours / month_hours
    msk_total = broker_cost + storage_cost
    total = writer_cost + msk_total
    payload: dict[str, Any] = {
        "schema_version": 1,
        "system": "Redshift Serverless",
        "scope": "complete-ingest fresh-data path",
        "rows_ingested": args.rows,
        "window": {
            "start": args.start,
            "end": args.end,
            "duration_seconds": duration_seconds,
            "duration_hours": hours,
            "basis": "producer start through producer DONE line (113,219,565,734 rows in 113,227 seconds)",
        },
        "shared_path_contract": {
            "shared_between_read_variants": True,
            "read_variants": ["dashboard + SUPER drill-down", "dashboard + typed drill-down"],
            "attribution": "The writer and MSK path is charged once, reused unchanged by both counterfactual read-path comparisons, and never split or doubled.",
        },
        "components": {
            "writer_workgroup": {
                "base_rpu": args.writer_rpu,
                "capacity_time_rpu_seconds": writer_rpu_seconds,
                "rpu_hour_rate_usd": redshift_rate,
                "cost_usd": writer_cost,
                "model": "declared base capacity multiplied by full producer uptime",
            },
            "msk": {
                "brokers": args.msk_brokers,
                "broker_instance": pricing["msk_provisioned"]["broker_instance"],
                "partitions": args.msk_partitions,
                "storage_gb_per_broker": args.msk_storage_gb_per_broker,
                "total_storage_gb": total_storage_gb,
                "broker_hour_rate_usd": broker_rate,
                "storage_gb_month_rate_usd": storage_rate,
                "broker_cost_usd": broker_cost,
                "storage_cost_usd": storage_cost,
                "inter_broker_transfer_cost_usd": 0.0,
                "client_cross_az_cost_usd": 0.0,
                "client_cross_az_included": False,
                "total_cost_usd": msk_total,
            },
        },
        "total_cost_usd": total,
        "excluded": {
            "redshift_managed_storage": "Final storage snapshot is not time-integrated and is excluded from the main fresh-data-path comparison.",
            "client_cross_az_transfer": "Excluded by benchmark-owner policy, consistently with other systems.",
            "reader_queries": "Reported separately from committed hourly allocation evidence.",
        },
        "pricing": {
            "path": portable_path(pricing_path),
            "sha256": sha256(pricing_path),
            "region": pricing["region"],
            "effective_date": pricing["effective_date"],
            "sources": pricing["sources"],
        },
    }
    output = args.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Written: {output}")
    print(
        f"writer=${writer_cost:.6f} MSK=${msk_total:.6f} "
        f"shared fresh-data path=${total:.6f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
