#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Plot complete-run ingest and background maintenance costs.

ClickHouse is rendered as one bundled write-service cost. BigQuery is rendered
as one bar with alternative Capacity and On-demand endpoints. Automatic
reclustering is disclosed as a zero separate charge.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import matplotlib
import matplotlib.font_manager as font_manager
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Rectangle

from _layout import configure_figure, resolve_layout, save_figure, write_json


WHITE = "#F7F7F7"
MUTED = "#A0A0A0"
CLICKHOUSE = "#FDFF62"
BIGQUERY_INGEST = "#4285F4"
BIGQUERY_MV = "#1557A6"

matplotlib.rcParams["font.family"] = (
    "Inter"
    if any(font.name == "Inter" for font in font_manager.fontManager.ttflist)
    else "DejaVu Sans"
)
matplotlib.rcParams["font.sans-serif"] = ["Inter", "DejaVu Sans"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clickhouse-cost", type=Path, required=True)
    parser.add_argument("--bigquery-ingest-cost", type=Path, required=True)
    parser.add_argument("--bigquery-mv-refresh-cost", type=Path, required=True)
    parser.add_argument("--bigquery-serverless-pricing", type=Path, required=True)
    parser.add_argument("--bigquery-write-api-pricing", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--tier", default="Enterprise")
    parser.add_argument("--region", default="us")
    parser.add_argument(
        "--basename",
        default="ingest_fresh_path_cost_clickhouse_vs_bigquery",
    )
    parser.add_argument("--dpi", type=int, default=300)
    parser.add_argument(
        "--wide",
        action="store_true",
        help="Render the Keynote-native 5156x2900 staged variant and suffix outputs with _wide.",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object in {path}")
    return value


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def one(
    items: list[dict[str, Any]], *, field: str, value: str, context: str
) -> dict[str, Any]:
    matches = [item for item in items if item.get(field) == value]
    if len(matches) != 1:
        raise ValueError(
            f"expected exactly one {field}={value!r} entry in {context}; "
            f"found {len(matches)}"
        )
    return matches[0]


def close(actual: float, expected: float, *, context: str) -> None:
    if not math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-6):
        raise ValueError(f"cost mismatch for {context}: {actual} != {expected}")


def money(value: float) -> str:
    return f"\\${value:,.2f}"


def rounded_bar(
    axis: Any, x: float, y: float, width: float, height: float, color: str
) -> FancyBboxPatch:
    patch = FancyBboxPatch(
        (x, y),
        width,
        height,
        boxstyle=f"round,pad=0,rounding_size={height * 0.12}",
        linewidth=0,
        facecolor=color,
    )
    axis.add_patch(patch)
    return patch


def main() -> int:
    args = parse_args()
    paths = {
        "clickhouse_cost": args.clickhouse_cost.expanduser().resolve(),
        "bigquery_ingest_cost": args.bigquery_ingest_cost.expanduser().resolve(),
        "bigquery_mv_refresh_cost": args.bigquery_mv_refresh_cost.expanduser().resolve(),
        "bigquery_serverless_pricing": args.bigquery_serverless_pricing.expanduser().resolve(),
        "bigquery_write_api_pricing": args.bigquery_write_api_pricing.expanduser().resolve(),
    }
    values = {name: load_json(path) for name, path in paths.items()}

    ch = values["clickhouse_cost"]
    bq_ingest = values["bigquery_ingest_cost"]
    bq_mv = values["bigquery_mv_refresh_cost"]
    serverless = values["bigquery_serverless_pricing"]
    write_pricing = values["bigquery_write_api_pricing"]

    ch_tier = one(
        ch["costs"],
        field="tier",
        value=args.tier,
        context=str(paths["clickhouse_cost"]),
    )
    ch_cost = float(ch_tier["total_compute_cost_usd"])

    if bq_ingest.get("component") != "storage_write_api_ingest":
        raise ValueError("BigQuery ingest summary has the wrong component")
    bq_ingest_cost = float(bq_ingest["pricing"]["total_cost_usd"])
    write_rate = float(write_pricing["pricing"]["price_usd"])
    close(
        float(bq_ingest["pricing"]["price_usd"]),
        write_rate,
        context="Storage Write API price",
    )
    close(
        bq_ingest_cost,
        float(bq_ingest["total_input_gib"]) * write_rate,
        context="Storage Write API total",
    )

    if bq_mv.get("component") != "mv_refresh":
        raise ValueError("BigQuery MV summary has the wrong component")
    if int(bq_mv.get("failed_jobs") or 0) != 0:
        raise ValueError("BigQuery MV summary contains failed jobs")
    bq_mv_tier = one(
        bq_mv["costs"],
        field="tier",
        value=args.tier,
        context=str(paths["bigquery_mv_refresh_cost"]),
    )
    bq_mv_capacity_cost = float(bq_mv_tier["total_compute_cost_usd"])
    bq_mv_on_demand = one(
        bq_mv["costs"],
        field="tier",
        value="OnDemand",
        context=str(paths["bigquery_mv_refresh_cost"]),
    )
    bq_mv_on_demand_cost = float(bq_mv_on_demand["total_compute_cost_usd"])
    pricing_tiers = serverless["regions"][args.region]["pricing_compute"]["capacity"][
        "default"
    ]["hourly"]["tiers"]
    pricing_tier = one(
        pricing_tiers,
        field="name",
        value=args.tier,
        context=str(paths["bigquery_serverless_pricing"]),
    )
    slot_hour_rate = float(pricing_tier["price_usd"])
    close(
        float(bq_mv_tier["price_usd"]),
        slot_hour_rate,
        context=f"BigQuery {args.tier} slot price",
    )
    close(
        bq_mv_capacity_cost,
        float(bq_mv["total_billed_slot_sec"]) / 3600 * slot_hour_rate,
        context=f"BigQuery {args.tier} MV refresh total",
    )
    on_demand_pricing = serverless["regions"][args.region]["pricing_compute"][
        "on_demand"
    ]["monthly"]
    on_demand_rate = float(on_demand_pricing["price_usd"])
    on_demand_unit_bytes = float(on_demand_pricing["price_unit_bytes"])
    close(
        float(bq_mv_on_demand["price_usd"]),
        on_demand_rate,
        context="BigQuery On-demand price",
    )
    close(
        bq_mv_on_demand_cost,
        float(bq_mv["total_bytes_billed"]) / on_demand_unit_bytes * on_demand_rate,
        context="BigQuery On-demand MV refresh total",
    )

    bq_capacity_total = bq_ingest_cost + bq_mv_capacity_cost
    bq_on_demand_total = bq_ingest_cost + bq_mv_on_demand_cost
    max_total = max(ch_cost, bq_capacity_total, bq_on_demand_total)

    basename, figure_size, layout = resolve_layout(
        args.basename, args.wide, (11, 5.2), dpi=args.dpi
    )
    fig, axis = plt.subplots(
        figsize=figure_size, dpi=args.dpi if args.wide else None
    )
    plot_background = configure_figure(fig, wide=args.wide)
    axis.set_facecolor(plot_background)
    axis.set_xlim(0, 1)
    axis.set_ylim(0, 1)
    axis.axis("off")

    left = 0.035 if args.wide else 0.04
    max_width = 0.76 if args.wide else 0.72
    value_gap = 0.018
    bar_height = 0.16 if args.wide else 0.145
    label_fontsize = 31 if args.wide else 24
    price_fontsize = 23 if args.wide else 17
    description_fontsize = 18 if args.wide else 14
    bigquery_description_fontsize = 17 if args.wide else 13.5

    rows = (
        ("ClickHouse", 0.68, ch_cost, CLICKHOUSE),
        ("BigQuery", 0.25, bq_on_demand_total, BIGQUERY_INGEST),
    )
    for label, y, _total, color in rows:
        axis.text(
            left,
            y + 0.205,
            label,
            color=color,
            fontsize=label_fontsize,
            fontweight="bold",
            va="center",
        )

    ch_width = max_width * ch_cost / max_total
    rounded_bar(axis, left, rows[0][1], ch_width, bar_height, CLICKHOUSE)
    axis.text(
        left + ch_width + value_gap,
        rows[0][1] + bar_height / 2,
        money(ch_cost),
        color=WHITE,
        fontsize=price_fontsize,
        fontweight="bold",
        ha="left",
        va="center",
    )
    axis.text(
        left,
        rows[0][1] - 0.065,
        f"Bundled write service · ingest, sorting, merges & incremental MV · ${ch_cost:,.2f}",
        color=MUTED,
        fontsize=description_fontsize,
        va="center",
    )

    bq_on_demand_width = max_width * bq_on_demand_total / max_total
    bq_capacity_width = max_width * bq_capacity_total / max_total
    bq_clip = rounded_bar(
        axis, left, rows[1][1], bq_on_demand_width, bar_height, BIGQUERY_MV
    )
    capacity_segment = Rectangle(
        (left, rows[1][1]),
        bq_capacity_width,
        bar_height,
        linewidth=0,
        facecolor=BIGQUERY_INGEST,
    )
    capacity_segment.set_clip_path(bq_clip)
    axis.add_patch(capacity_segment)
    axis.plot(
        [left + bq_capacity_width, left + bq_capacity_width],
        [rows[1][1], rows[1][1] + bar_height],
        color=plot_background,
        linewidth=1.1,
        alpha=0.65,
        solid_capstyle="butt",
    )
    axis.text(
        left + bq_capacity_width - 0.008,
        rows[1][1] + bar_height + 0.035,
        f"{money(bq_capacity_total)} Capacity",
        color=WHITE,
        fontsize=price_fontsize,
        fontweight="bold",
        ha="right",
        va="bottom",
    )
    axis.text(
        left + bq_on_demand_width + value_gap,
        rows[1][1] + bar_height / 2,
        f"{money(bq_on_demand_total)} On-demand",
        color=WHITE,
        fontsize=price_fontsize,
        fontweight="bold",
        ha="left",
        va="center",
    )
    axis.text(
        left,
        rows[1][1] - 0.065,
        (
            f"Storage Write API \\${bq_ingest_cost:,.2f} · "
            f"MV refresh: \\${bq_mv_capacity_cost:,.2f} Capacity / "
            f"\\${bq_mv_on_demand_cost:,.2f} On-demand · reclustering \\$0"
        ),
        color=MUTED,
        fontsize=bigquery_description_fontsize,
        va="center",
    )
    if args.wide:
        fig.subplots_adjust(left=0.055, right=0.945, bottom=0.070, top=0.755)

    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    png_path = output_dir / f"{basename}.png"
    svg_path = output_dir / f"{basename}.svg"
    summary_path = output_dir / f"{basename}_summary.json"
    png_path, svg_path = save_figure(
        fig, output_dir, basename, args.dpi, wide=args.wide
    )
    plt.close(fig)

    ch_rows = int(ch["rows_ingested"])
    bq_rows = int(bq_ingest["successful_rows"])
    row_difference = ch_rows - bq_rows
    summary = {
        "schema_version": 1,
        "chart": "complete_ingest_and_background_fresh_path_cost",
        "layout": layout,
        "pricing_tier": args.tier,
        "pricing_region": args.region,
        "scope": {
            "included": [
                "write ingestion",
                "sorting/merges/incremental MV bundled in ClickHouse write service",
                "BigQuery automatic MV refresh",
                "BigQuery automatic reclustering at zero separate charge",
            ],
            "excluded": [
                "storage",
                "dashboard queries",
                "drill-down queries",
                "BigQuery query-time delta merge",
            ],
        },
        "systems": {
            "clickhouse": {
                "rows_ingested": ch_rows,
                "duration_hours": ch["duration_hours"],
                "bundled_write_service_cost_usd": ch_cost,
                "total_cost_usd": ch_cost,
            },
            "bigquery": {
                "provider_successful_rows": bq_rows,
                "storage_write_api_cost_usd": bq_ingest_cost,
                "enterprise_mv_refresh_cost_usd": bq_mv_capacity_cost,
                "on_demand_mv_refresh_cost_usd": bq_mv_on_demand_cost,
                "automatic_reclustering_separate_cost_usd": 0,
                "capacity_total_cost_usd": bq_capacity_total,
                "on_demand_total_cost_usd": bq_on_demand_total,
            },
        },
        "row_scope_reconciliation": {
            "clickhouse_rows": ch_rows,
            "bigquery_provider_successful_rows": bq_rows,
            "absolute_difference_rows": abs(row_difference),
            "difference_as_pct_of_clickhouse": abs(row_difference) / ch_rows * 100,
        },
        "sources": {
            name: {"path": str(path), "sha256": sha256(path)}
            for name, path in paths.items()
        },
        "rendering": {
            "png": str(png_path),
            "svg": str(svg_path),
            "total_label_placement": "directly_after_bar_end; Capacity anchored to its internal endpoint",
            "maximum_bar_width_axis_fraction": max_width,
        },
    }
    write_json(summary_path, summary)

    for path in (png_path, svg_path, summary_path):
        print(f"Wrote {path}")
    print(
        f"ClickHouse {args.tier}: ${ch_cost:,.2f}; "
        f"BigQuery ingest: ${bq_ingest_cost:,.2f}; "
        f"BigQuery {args.tier} MV refresh: ${bq_mv_capacity_cost:,.2f}; "
        f"BigQuery On-demand MV refresh: ${bq_mv_on_demand_cost:,.2f}; "
        f"BigQuery totals: ${bq_capacity_total:,.2f} Capacity / "
        f"${bq_on_demand_total:,.2f} On-demand"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
