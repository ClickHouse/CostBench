#!/usr/bin/env python3
"""Summarize Snowflake JSONL query results as normalized query cost."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("pricing", type=Path, help="Pricing JSON for the recorded warehouse")
    parser.add_argument("output", type=Path)
    parser.add_argument("--cloud", default="aws")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--iterations", type=int)
    parser.add_argument(
        "--fallback-pricing",
        type=Path,
        help="Pricing JSON for fallback-priced query jobs.",
    )
    parser.add_argument(
        "--fallback-warehouse",
        help="Fallback warehouse name in --fallback-pricing, for example 'Gen2 Small'.",
    )
    parser.add_argument(
        "--fallback-threshold-seconds",
        type=float,
        help="Strict elapsed-time threshold: jobs above this value are fallback-priced.",
    )
    args = parser.parse_args()
    fallback_values = (
        args.fallback_pricing,
        args.fallback_warehouse,
        args.fallback_threshold_seconds,
    )
    if any(value is not None for value in fallback_values) and not all(
        value is not None for value in fallback_values
    ):
        parser.error(
            "--fallback-pricing, --fallback-warehouse, and "
            "--fallback-threshold-seconds must be supplied together"
        )
    if args.iterations is not None and args.iterations < 1:
        parser.error("--iterations must be positive")
    if args.fallback_threshold_seconds is not None and args.fallback_threshold_seconds < 0:
        parser.error("--fallback-threshold-seconds must be non-negative")
    return args


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object at {path}")
    return value


def load_records(path: Path, limit: int | None) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            if not raw_line.strip():
                continue
            value = json.loads(raw_line)
            if not isinstance(value, dict):
                raise ValueError(f"expected a JSON object at {path}:{line_number}")
            records.append(value)
            if limit is not None and len(records) == limit:
                break
    if not records:
        raise ValueError(f"no JSONL records found in {path}")
    if limit is not None and len(records) != limit:
        raise ValueError(f"requested {limit} iterations from {path}, found {len(records)}")
    return records


def runtime(value: Any, *, context: str) -> float:
    if not isinstance(value, list) or len(value) != 1:
        raise ValueError(f"expected exactly one trial at {context}; got {value!r}")
    scalar = value[0]
    if isinstance(scalar, bool) or not isinstance(scalar, (int, float)):
        raise ValueError(f"expected a numeric runtime at {context}; got {scalar!r}")
    result = float(scalar)
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"invalid runtime at {context}: {result!r}")
    return result


def rounded(value: float, digits: int) -> float:
    return round(value + 10 ** (-(digits + 4)), digits)


def plan_map(document: dict[str, Any], *, cloud: str, region: str, path: Path) -> dict[str, dict[str, Any]]:
    blocks = [
        block
        for block in document.get("pricing", [])
        if str(block.get("cloud")) == cloud and str(block.get("region")) == region
    ]
    result: dict[str, dict[str, Any]] = {}
    for block in blocks:
        tier = str(block.get("plan") or "")
        if not tier or tier in result:
            raise ValueError(f"duplicate or missing pricing plan in {path}: {tier!r}")
        result[tier] = block
    if not result:
        raise ValueError(f"no pricing for {cloud}/{region} in {path}")
    return result


def warehouse_by_credits(block: dict[str, Any], credits: float, *, context: str) -> dict[str, Any]:
    matches = [
        item
        for item in block.get("warehouses", [])
        if math.isclose(float(item.get("credits_per_hour", -1)), credits, abs_tol=1e-12)
    ]
    if len(matches) != 1:
        raise ValueError(f"expected one {credits:g}-credits/hour warehouse in {context}; found {len(matches)}")
    return matches[0]


def warehouse_by_name(block: dict[str, Any], name: str, *, context: str) -> dict[str, Any]:
    matches = [item for item in block.get("warehouses", []) if str(item.get("name")) == name]
    if len(matches) != 1:
        raise ValueError(f"expected one warehouse {name!r} in {context}; found {len(matches)}")
    return matches[0]


def component(
    *,
    role: str,
    rule: str,
    warehouse: dict[str, Any],
    pricing_file: Path,
    query_jobs: int,
    runtime_seconds: float,
    credit_price: float,
) -> dict[str, Any]:
    credits_per_hour = float(warehouse["credits_per_hour"])
    cost = runtime_seconds / 3600 * credits_per_hour * credit_price
    return {
        "role": role,
        "pricing_rule": rule,
        "pricing_file": pricing_file.name,
        "warehouse_size": str(warehouse["name"]),
        "query_jobs": query_jobs,
        "runtime_seconds": rounded(runtime_seconds, 3),
        "credits_per_hour": credits_per_hour,
        "credit_price_per_hour": credit_price,
        "total_compute_cost_usd": rounded(cost, 5),
    }


def main() -> int:
    args = parse_args()
    records = load_records(args.input, args.iterations)
    first = records[0]
    try:
        primary_credits = float(first["cluster_size"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(f"missing/non-numeric cluster_size in {args.input}") from error

    first_results = first.get("result")
    if not isinstance(first_results, list) or not first_results:
        raise ValueError(f"missing result array in {args.input}:1")
    query_count = len(first_results)
    per_query_runtimes: list[list[float]] = [[] for _ in range(query_count)]
    for iteration_number, record in enumerate(records, start=1):
        results = record.get("result")
        if not isinstance(results, list) or len(results) != query_count:
            raise ValueError(
                f"expected {query_count} query results at {args.input}:{iteration_number}"
            )
        for query_index, trial in enumerate(results):
            per_query_runtimes[query_index].append(
                runtime(trial, context=f"{args.input}:{iteration_number} query {query_index + 1}")
            )

    fallback_enabled = args.fallback_pricing is not None
    threshold = float(args.fallback_threshold_seconds or 0)

    def is_fallback(value: float) -> bool:
        return fallback_enabled and value > threshold

    all_runtimes = [value for values in per_query_runtimes for value in values]
    primary_runtimes = [value for value in all_runtimes if not is_fallback(value)]
    fallback_runtimes = [value for value in all_runtimes if is_fallback(value)]
    total_runtime = sum(all_runtimes)
    primary_runtime = sum(primary_runtimes)
    fallback_runtime = sum(fallback_runtimes)

    per_query_attribution = []
    for query_index, values in enumerate(per_query_runtimes, start=1):
        primary_values = [value for value in values if not is_fallback(value)]
        fallback_values = [value for value in values if is_fallback(value)]
        per_query_attribution.append(
            {
                "query_number": query_index,
                "query_jobs": len(values),
                "total_runtime_seconds": rounded(sum(values), 3),
                "primary_priced_query_jobs": len(primary_values),
                "primary_priced_runtime_seconds": rounded(sum(primary_values), 3),
                "fallback_priced_query_jobs": len(fallback_values),
                "fallback_priced_runtime_seconds": rounded(sum(fallback_values), 3),
            }
        )

    primary_document = load_json(args.pricing)
    primary_plans = plan_map(
        primary_document, cloud=args.cloud, region=args.region, path=args.pricing
    )
    fallback_plans = (
        plan_map(
            load_json(args.fallback_pricing),
            cloud=args.cloud,
            region=args.region,
            path=args.fallback_pricing,
        )
        if fallback_enabled
        else {}
    )
    if fallback_enabled and set(primary_plans) != set(fallback_plans):
        raise ValueError(
            "primary and fallback pricing files expose different plans for "
            f"{args.cloud}/{args.region}"
        )

    costs: list[dict[str, Any]] = []
    primary_warehouse_name: str | None = None
    fallback_credits: float | None = None
    for tier, primary_plan in primary_plans.items():
        primary_warehouse = warehouse_by_credits(
            primary_plan,
            primary_credits,
            context=f"{args.pricing} plan {tier}",
        )
        primary_warehouse_name = str(primary_warehouse["name"])
        primary_price = float(primary_plan["credit_price_per_hour"])
        primary_component = component(
            role="primary",
            rule=(
                f"elapsed_seconds <= {threshold:g}"
                if fallback_enabled
                else "all query jobs"
            ),
            warehouse=primary_warehouse,
            pricing_file=args.pricing,
            query_jobs=len(primary_runtimes),
            runtime_seconds=primary_runtime,
            credit_price=primary_price,
        )
        components = [primary_component]
        if fallback_enabled:
            fallback_plan = fallback_plans[tier]
            fallback_warehouse = warehouse_by_name(
                fallback_plan,
                str(args.fallback_warehouse),
                context=f"{args.fallback_pricing} plan {tier}",
            )
            fallback_credits = float(fallback_warehouse["credits_per_hour"])
            components.append(
                component(
                    role="fallback",
                    rule=f"elapsed_seconds > {threshold:g}; price full elapsed time",
                    warehouse=fallback_warehouse,
                    pricing_file=args.fallback_pricing,
                    query_jobs=len(fallback_runtimes),
                    runtime_seconds=fallback_runtime,
                    credit_price=float(fallback_plan["credit_price_per_hour"]),
                )
            )
        total_cost = sum(float(item["total_compute_cost_usd"]) for item in components)
        costs.append(
            {
                "tier": tier,
                "warehouse_size": (
                    f"{primary_warehouse['name']} + {args.fallback_warehouse} fallback"
                    if fallback_enabled
                    else str(primary_warehouse["name"])
                ),
                "credits_per_hour": None if fallback_enabled else primary_credits,
                "credit_price_per_hour": primary_price,
                "total_compute_cost_usd": rounded(total_cost, 5),
                "components": components,
            }
        )

    query_jobs = len(all_runtimes)
    output = {
        "schema_version": 2,
        "source_file": args.input.name,
        "system": first.get("system"),
        "version": first.get("version"),
        "machine": first.get("machine"),
        "cluster_size": primary_credits,
        "cloud": args.cloud,
        "region": args.region,
        "iterations_included": len(records),
        "queries_per_iteration": query_count,
        "total_runtime_seconds": rounded(total_runtime, 3),
        "query_cost_model": {
            "metric": "normalized query cost",
            "attribution_method": (
                "elapsed_threshold_effective_warehouse"
                if fallback_enabled
                else "single_recorded_warehouse_rate"
            ),
            "elapsed_time_source": "result (end-to-end query elapsed seconds)",
            "compilation_treatment": "included in elapsed time; not priced separately",
            "standing_capacity_treatment": "excluded",
            "primary_warehouse": primary_warehouse_name,
            "primary_pricing_file": args.pricing.name,
            "fallback_is_proxy": fallback_enabled,
            "fallback_condition": (
                f"elapsed_seconds > {threshold:g}" if fallback_enabled else None
            ),
            "fallback_threshold_seconds": threshold if fallback_enabled else None,
            "fallback_warehouse": args.fallback_warehouse if fallback_enabled else None,
            "fallback_pricing_file": (
                args.fallback_pricing.name if fallback_enabled else None
            ),
            "fallback_credits_per_hour": fallback_credits,
            "fallback_runtime_treatment": (
                "full elapsed time is priced at the fallback warehouse rate"
                if fallback_enabled
                else None
            ),
            "primary_attempt_cost_treatment": (
                "no additional primary-warehouse charge for fallback-priced jobs"
                if fallback_enabled
                else None
            ),
        },
        "runtime_attribution": {
            "query_jobs": query_jobs,
            "primary_priced_query_jobs": len(primary_runtimes),
            "primary_priced_runtime_seconds": rounded(primary_runtime, 3),
            "fallback_priced_query_jobs": len(fallback_runtimes),
            "fallback_priced_runtime_seconds": rounded(fallback_runtime, 3),
            "fallback_priced_job_share": (
                rounded(len(fallback_runtimes) / query_jobs, 6) if query_jobs else 0
            ),
        },
        "per_query_attribution": per_query_attribution,
        "costs": costs,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")

    print("→ Summarizing Snowflake query results")
    print(f"  Input  : {args.input}")
    print(f"  Pricing: {args.pricing}")
    print(f"  Cloud  : {args.cloud} / {args.region}")
    print(f"  Iterations: {len(records)}")
    if fallback_enabled:
        print(
            f"  Fallback proxy: elapsed > {threshold:g}s → "
            f"{args.fallback_warehouse} for full elapsed time"
        )
        print(
            f"  Fallback-priced jobs: {len(fallback_runtimes)}/{query_jobs} "
            f"({100 * len(fallback_runtimes) / query_jobs:.1f}%)"
        )
    print(f"✅ Written to {args.output}")
    print(f"⏱  Total runtime: {output['total_runtime_seconds']}s")
    print("💰 Normalized compute cost per tier:")
    for item in costs:
        print(f"  {item['tier']} ({item['warehouse_size']}): ${item['total_compute_cost_usd']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
