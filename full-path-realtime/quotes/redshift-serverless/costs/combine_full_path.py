#!/usr/bin/env python3
"""Combine matched query summaries with the shared fresh-data path."""

from __future__ import annotations

import argparse
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


def load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def tier_cost(summary: dict[str, Any], tier: str) -> float:
    matches = [item for item in summary["costs"] if item.get("tier") == tier]
    if len(matches) != 1:
        raise ValueError(f"expected one {tier} cost; got {len(matches)}")
    return float(matches[0]["total_compute_cost_usd"])


def validate_pair(ch: dict[str, Any], rs: dict[str, Any], workload: str) -> None:
    ch_iterations = int(ch["iterations_included"])
    rs_iterations = int(rs["iterations_included"])
    ch_qpi = int(ch["queries_per_iteration"])
    rs_qpi = int(rs["queries_per_iteration"])
    if (ch_iterations, ch_qpi) != (rs_iterations, rs_qpi):
        raise ValueError(
            f"{workload} matched evidence differs: "
            f"ClickHouse={ch_iterations}x{ch_qpi}, Redshift={rs_iterations}x{rs_qpi}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("super", "typed"), required=True)
    parser.add_argument("--clickhouse-dashboard", type=Path, required=True)
    parser.add_argument("--clickhouse-drilldown", type=Path, required=True)
    parser.add_argument("--redshift-dashboard", type=Path, required=True)
    parser.add_argument("--redshift-drilldown", type=Path, required=True)
    parser.add_argument("--clickhouse-fresh-path", type=Path, required=True)
    parser.add_argument("--redshift-fresh-path", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--clickhouse-tier", default="Enterprise")
    args = parser.parse_args()

    paths = {
        "clickhouse_dashboard": args.clickhouse_dashboard.expanduser().resolve(),
        "clickhouse_drilldown": args.clickhouse_drilldown.expanduser().resolve(),
        "redshift_dashboard": args.redshift_dashboard.expanduser().resolve(),
        "redshift_drilldown": args.redshift_drilldown.expanduser().resolve(),
        "clickhouse_fresh_path": args.clickhouse_fresh_path.expanduser().resolve(),
        "redshift_fresh_path": args.redshift_fresh_path.expanduser().resolve(),
    }
    data = {key: load(path) for key, path in paths.items()}
    validate_pair(data["clickhouse_dashboard"], data["redshift_dashboard"], "dashboard")
    validate_pair(data["clickhouse_drilldown"], data["redshift_drilldown"], "drill-down")
    if data["redshift_fresh_path"]["shared_path_contract"]["shared_between_read_variants"] is not True:
        raise ValueError("Redshift fresh path is not marked shared between read variants")

    ch_runtime = sum(
        float(data[key]["total_runtime_seconds"])
        for key in ("clickhouse_dashboard", "clickhouse_drilldown")
    )
    rs_runtime = sum(
        float(data[key]["total_runtime_seconds"])
        for key in ("redshift_dashboard", "redshift_drilldown")
    )
    ch_query_cost = sum(
        tier_cost(data[key], args.clickhouse_tier)
        for key in ("clickhouse_dashboard", "clickhouse_drilldown")
    )
    rs_query_cost = sum(
        tier_cost(data[key], "Standard")
        for key in ("redshift_dashboard", "redshift_drilldown")
    )
    ch_fresh = tier_cost(data["clickhouse_fresh_path"], args.clickhouse_tier)
    rs_fresh = float(data["redshift_fresh_path"]["total_cost_usd"])

    ch_total = ch_fresh + ch_query_cost
    rs_total = rs_fresh + rs_query_cost
    ch_score = ch_total * ch_runtime
    rs_score = rs_total * rs_runtime
    if not all(value > 0 for value in (ch_runtime, rs_runtime, ch_total, rs_total, ch_score, rs_score)):
        raise ValueError("full-path values must be positive")
    label = "Redshift · SUPER" if args.variant == "super" else "Redshift · Typed"
    rows = [
        {
            "label": "ClickHouse",
            "runtime_sec": ch_runtime,
            "fresh_data_path_cost": ch_fresh,
            "query_cost": ch_query_cost,
            "total_cost": ch_total,
            "score": ch_score,
            "relative_to_clickhouse": 1.0,
        },
        {
            "label": label,
            "runtime_sec": rs_runtime,
            "fresh_data_path_cost": rs_fresh,
            "query_cost": rs_query_cost,
            "total_cost": rs_total,
            "score": rs_score,
            "relative_to_clickhouse": rs_score / ch_score,
        },
    ]
    payload = {
        "schema_version": 1,
        "chart": "full_path_cost_performance_clickhouse_vs_redshift",
        "redshift_read_variant": args.variant,
        "formula": "(complete-ingest fresh-data-path cost + matched query cost) × accumulated matched query runtime",
        "rows": rows,
        "shared_redshift_fresh_data_path": data["redshift_fresh_path"],
        "redshift_query_cost_contract": data["redshift_dashboard"]["usage_allocation_contract"],
        "matching": {
            "dashboard_iterations": int(data["redshift_dashboard"]["iterations_included"]),
            "dashboard_queries_per_iteration": int(data["redshift_dashboard"]["queries_per_iteration"]),
            "drilldown_iterations": int(data["redshift_drilldown"]["iterations_included"]),
            "drilldown_queries_per_iteration": int(data["redshift_drilldown"]["queries_per_iteration"]),
        },
        "sources": {
            key: {"path": portable_path(path), "sha256": sha256(path)}
            for key, path in paths.items()
        },
    }
    output = args.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Written: {output}")
    print(
        f"{label}: runtime={rs_runtime:.3f}s cost=${rs_total:.3f} "
        f"score={rs_score:,.3f} ({rs_score / ch_score:,.1f}x ClickHouse)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
