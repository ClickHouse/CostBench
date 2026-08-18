#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Plot accumulated query runtime and modeled query cost.

ClickHouse is priced at the selected service tier. BigQuery shows two
alternative pricing models for the same query work: normalized capacity cost
from billed slot-seconds and on-demand cost from billed bytes. The two
BigQuery costs are alternatives and are never added together.
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
from _layout import configure_figure, resolve_layout, save_figure, write_json
from matplotlib.patches import FancyBboxPatch

WHITE = "#F7F7F7"
MUTED = "#A0A0A0"
FAINT = "#777777"
BORDER = "#B5B5B5"
CLICKHOUSE = "#FDFF62"
BIGQUERY = "#4285F4"

matplotlib.rcParams["font.family"] = (
    "Inter"
    if any(font.name == "Inter" for font in font_manager.fontManager.ttflist)
    else "DejaVu Sans"
)
matplotlib.rcParams["font.sans-serif"] = ["Inter", "DejaVu Sans"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clickhouse-dashboard-cost", type=Path, required=True)
    parser.add_argument("--clickhouse-drilldown-cost", type=Path, required=True)
    parser.add_argument("--bigquery-dashboard-cost", type=Path, required=True)
    parser.add_argument("--bigquery-drilldown-cost", type=Path, required=True)
    parser.add_argument("--bigquery-serverless-pricing", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--tier", default="Enterprise")
    parser.add_argument("--region", default="us")
    parser.add_argument(
        "--basename",
        default="query_runtime_cost_clickhouse_vs_bigquery",
    )
    parser.add_argument("--dpi", type=int, default=300)
    parser.add_argument("--wide", action="store_true", help="Render the Keynote-native 5156x2900 staged variant and suffix outputs with _wide.")
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
    items: list[dict[str, Any]],
    *,
    context: str,
    **conditions: str,
) -> dict[str, Any]:
    matches = [
        item
        for item in items
        if all(str(item.get(field)) == value for field, value in conditions.items())
    ]
    if len(matches) != 1:
        rendered = ", ".join(f"{field}={value!r}" for field, value in conditions.items())
        raise ValueError(f"expected exactly one {rendered} entry in {context}; found {len(matches)}")
    return matches[0]


def close(actual: float, expected: float, *, context: str) -> None:
    if not math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-6):
        raise ValueError(f"cost mismatch for {context}: {actual} != {expected}")


def money(value: float) -> str:
    if value < 1:
        return f"${value:,.3f}"
    return f"${value:,.2f}"


def duration(value: float) -> str:
    if value < 60:
        return f"{value:.1f}s"
    if value < 3600:
        return f"{value / 60:.1f}m"
    return f"{value / 3600:.1f}h"


def rounded_box(
    axis: Any,
    x: float,
    y: float,
    width: float,
    height: float,
    *,
    color: str,
    linewidth: float = 0,
    fill: bool = True,
    rounding: float = 0.01,
    linestyle: str = "solid",
) -> FancyBboxPatch:
    patch = FancyBboxPatch(
        (x, y),
        width,
        height,
        boxstyle=f"round,pad=0,rounding_size={rounding}",
        linewidth=linewidth,
        edgecolor=color,
        facecolor=color if fill else "none",
        linestyle=linestyle,
    )
    axis.add_patch(patch)
    return patch


def validate_bigquery(
    summary: dict[str, Any],
    *,
    component: str,
    path: Path,
    tier: str,
    capacity_rate: float,
    on_demand_rate: float,
    on_demand_unit_bytes: float,
) -> dict[str, float | int]:
    if summary.get("system") != "BigQuery" or summary.get("component") != component:
        raise ValueError(f"wrong BigQuery component in {path}")
    for field in ("failed_jobs", "cache_hits", "missing_billed_bytes", "missing_slot_seconds"):
        if int(summary.get(field) or 0) != 0:
            raise ValueError(f"{field} is nonzero in {path}")

    capacity = one(
        summary["costs"],
        context=str(path),
        tier=tier,
        compute_model="capacity",
        pricing_variant="default",
        billing_period="hourly",
    )
    on_demand = one(
        summary["costs"],
        context=str(path),
        tier="OnDemand",
        compute_model="on_demand",
    )
    close(float(capacity["price_usd"]), capacity_rate, context=f"{component} capacity rate")
    close(float(on_demand["price_usd"]), on_demand_rate, context=f"{component} on-demand rate")
    close(
        float(capacity["total_compute_cost_usd"]),
        float(summary["total_billed_slot_sec"]) / 3600 * capacity_rate,
        context=f"{component} capacity total",
    )
    close(
        float(on_demand["total_compute_cost_usd"]),
        float(summary["total_bytes_billed"]) / on_demand_unit_bytes * on_demand_rate,
        context=f"{component} on-demand total",
    )
    return {
        "iterations": int(summary["iterations_included"]),
        "query_jobs": int(summary["query_jobs"]),
        "runtime_sec": float(summary["total_runtime_seconds"]),
        "capacity_cost_usd": float(capacity["total_compute_cost_usd"]),
        "on_demand_cost_usd": float(on_demand["total_compute_cost_usd"]),
        "billed_slot_sec": float(summary["total_billed_slot_sec"]),
        "billed_bytes": int(summary["total_bytes_billed"]),
    }


def validate_clickhouse(
    summary: dict[str, Any],
    *,
    component: str,
    path: Path,
    tier: str,
) -> dict[str, float | int]:
    if "ClickHouse" not in str(summary.get("system") or ""):
        raise ValueError(f"not a ClickHouse summary: {path}")
    cost = one(summary["costs"], context=str(path), tier=tier)
    iterations = int(summary["iterations_included"])
    queries_per_iteration = int(summary["queries_per_iteration"])
    return {
        "iterations": iterations,
        "query_jobs": iterations * queries_per_iteration,
        "runtime_sec": float(summary["total_runtime_seconds"]),
        "enterprise_cost_usd": float(cost["total_compute_cost_usd"]),
        "component": component,
    }


def add_panel(
    axis: Any,
    *,
    bottom: float,
    height: float,
    title: str,
    subtitle: str,
    clickhouse: dict[str, float | int],
    bigquery: dict[str, float | int],
) -> None:
    left = 0.035
    right = 0.965
    rounded_box(
        axis,
        left,
        bottom,
        right - left,
        height,
        color=BORDER,
        linewidth=1.15,
        fill=False,
        rounding=0.012,
        linestyle=(0, (1.2, 1.5)),
    )
    axis.text(left + 0.012, bottom + height - 0.025, title, color=MUTED, fontsize=15, fontweight="bold", va="top")
    axis.text(left + 0.012, bottom + height - 0.055, subtitle, color=MUTED, fontsize=11.5, va="top")

    bar_left = left + 0.012
    bar_max_width = 0.675
    cost_left = 0.79
    cost_label_left = 0.865
    runtime_max = max(float(clickhouse["runtime_sec"]), float(bigquery["runtime_sec"]))
    # Leave a clear header band for the panel title/subtitle, then place the
    # two system rows in the lower two-thirds of the panel.
    row_y = (bottom + height * 0.37, bottom + height * 0.06)
    rows = (
        ("ClickHouse", CLICKHOUSE, clickhouse, row_y[0]),
        ("BigQuery", BIGQUERY, bigquery, row_y[1]),
    )
    bar_height = height * 0.125

    for label, color, values, y in rows:
        query_jobs = int(values["query_jobs"])
        axis.text(bar_left, y + bar_height + 0.026, label, color=color, fontsize=16, fontweight="bold", va="center")
        metadata = (
            f"· {query_jobs} matched query executions"
            if title == "COMBINED"
            else f"· {int(values['iterations'])} iterations · {query_jobs} query executions"
        )
        axis.text(
            bar_left + (0.16 if label == "ClickHouse" else 0.132),
            y + bar_height + 0.026,
            metadata,
            color=MUTED,
            fontsize=10.5,
            va="center",
        )

        width = bar_max_width * float(values["runtime_sec"]) / runtime_max
        width = max(width, 0.006)
        rounded_box(axis, bar_left, y, width, bar_height, color=color, rounding=bar_height * 0.13)
        runtime_text_x = min(bar_left + width + 0.014, bar_left + bar_max_width + 0.012)
        axis.text(
            runtime_text_x,
            y + bar_height / 2,
            duration(float(values["runtime_sec"])),
            color=color,
            fontsize=14,
            fontweight="bold",
            va="center",
        )

        if label == "ClickHouse":
            axis.text(
                cost_left,
                y + bar_height + 0.026,
                money(float(values["enterprise_cost_usd"])),
                color=MUTED,
                fontsize=11.5,
                ha="left",
                va="center",
            )
        else:
            axis.text(
                cost_left,
                y + bar_height + 0.038,
                money(float(values["capacity_cost_usd"])),
                color=MUTED,
                fontsize=11.5,
                ha="left",
                va="center",
            )
            axis.text(
                cost_label_left,
                y + bar_height + 0.038,
                "Capacity",
                color=MUTED,
                fontsize=11.5,
                ha="left",
                va="center",
            )
            axis.text(
                cost_left,
                y + bar_height + 0.014,
                money(float(values["on_demand_cost_usd"])),
                color=MUTED,
                fontsize=11.5,
                ha="left",
                va="center",
            )
            axis.text(
                cost_label_left,
                y + bar_height + 0.014,
                "On-demand",
                color=MUTED,
                fontsize=11.5,
                ha="left",
                va="center",
            )


def main() -> int:
    args = parse_args()
    paths = {
        "clickhouse_dashboard_cost": args.clickhouse_dashboard_cost.expanduser().resolve(),
        "clickhouse_drilldown_cost": args.clickhouse_drilldown_cost.expanduser().resolve(),
        "bigquery_dashboard_cost": args.bigquery_dashboard_cost.expanduser().resolve(),
        "bigquery_drilldown_cost": args.bigquery_drilldown_cost.expanduser().resolve(),
        "bigquery_serverless_pricing": args.bigquery_serverless_pricing.expanduser().resolve(),
    }
    values = {name: load_json(path) for name, path in paths.items()}
    pricing = values["bigquery_serverless_pricing"]
    compute = pricing["regions"][args.region]["pricing_compute"]
    capacity_tier = one(
        compute["capacity"]["default"]["hourly"]["tiers"],
        context=str(paths["bigquery_serverless_pricing"]),
        name=args.tier,
    )
    on_demand = compute["on_demand"]["monthly"]
    capacity_rate = float(capacity_tier["price_usd"])
    on_demand_rate = float(on_demand["price_usd"])
    on_demand_unit_bytes = float(on_demand["price_unit_bytes"])

    ch_dashboard = validate_clickhouse(
        values["clickhouse_dashboard_cost"],
        component="dashboard",
        path=paths["clickhouse_dashboard_cost"],
        tier=args.tier,
    )
    ch_drilldown = validate_clickhouse(
        values["clickhouse_drilldown_cost"],
        component="drilldown",
        path=paths["clickhouse_drilldown_cost"],
        tier=args.tier,
    )
    bq_dashboard = validate_bigquery(
        values["bigquery_dashboard_cost"],
        component="dashboard",
        path=paths["bigquery_dashboard_cost"],
        tier=args.tier,
        capacity_rate=capacity_rate,
        on_demand_rate=on_demand_rate,
        on_demand_unit_bytes=on_demand_unit_bytes,
    )
    bq_drilldown = validate_bigquery(
        values["bigquery_drilldown_cost"],
        component="drilldown",
        path=paths["bigquery_drilldown_cost"],
        tier=args.tier,
        capacity_rate=capacity_rate,
        on_demand_rate=on_demand_rate,
        on_demand_unit_bytes=on_demand_unit_bytes,
    )

    for component, ch, bq in (
        ("dashboard", ch_dashboard, bq_dashboard),
        ("drilldown", ch_drilldown, bq_drilldown),
    ):
        if ch["iterations"] != bq["iterations"] or ch["query_jobs"] != bq["query_jobs"]:
            raise ValueError(
                f"{component} execution-count mismatch: ClickHouse "
                f"{ch['iterations']}/{ch['query_jobs']} vs BigQuery "
                f"{bq['iterations']}/{bq['query_jobs']}"
            )

    ch_combined = {
        "dashboard_iterations": int(ch_dashboard["iterations"]),
        "drilldown_iterations": int(ch_drilldown["iterations"]),
        "query_jobs": int(ch_dashboard["query_jobs"]) + int(ch_drilldown["query_jobs"]),
        "runtime_sec": float(ch_dashboard["runtime_sec"]) + float(ch_drilldown["runtime_sec"]),
        "enterprise_cost_usd": float(ch_dashboard["enterprise_cost_usd"]) + float(ch_drilldown["enterprise_cost_usd"]),
    }
    bq_combined = {
        "dashboard_iterations": int(bq_dashboard["iterations"]),
        "drilldown_iterations": int(bq_drilldown["iterations"]),
        "query_jobs": int(bq_dashboard["query_jobs"]) + int(bq_drilldown["query_jobs"]),
        "runtime_sec": float(bq_dashboard["runtime_sec"]) + float(bq_drilldown["runtime_sec"]),
        "capacity_cost_usd": float(bq_dashboard["capacity_cost_usd"]) + float(bq_drilldown["capacity_cost_usd"]),
        "on_demand_cost_usd": float(bq_dashboard["on_demand_cost_usd"]) + float(bq_drilldown["on_demand_cost_usd"]),
        "billed_slot_sec": float(bq_dashboard["billed_slot_sec"]) + float(bq_drilldown["billed_slot_sec"]),
        "billed_bytes": int(bq_dashboard["billed_bytes"]) + int(bq_drilldown["billed_bytes"]),
    }

    basename, figure_size, layout = resolve_layout(
        args.basename, args.wide, (12.5, 11.3), dpi=args.dpi
    )
    fig, axis = plt.subplots(
        figsize=figure_size, dpi=args.dpi if args.wide else None
    )
    plot_background = configure_figure(fig, wide=args.wide)
    axis.set_facecolor(plot_background)
    axis.set_xlim(0, 1)
    axis.set_ylim(0, 1)
    axis.axis("off")
    add_panel(
        axis,
        bottom=0.69,
        height=0.295,
        title="INTERACTIVE AGGREGATE QUERIES",
        subtitle="4 queries · every 10 min · pre-aggregated data · accumulated runtime and cost",
        clickhouse=ch_dashboard,
        bigquery=bq_dashboard,
    )
    add_panel(
        axis,
        bottom=0.365,
        height=0.295,
        title="DRILL-DOWN QUERIES",
        subtitle="2 queries · every hour · full raw data table · accumulated runtime and cost",
        clickhouse=ch_drilldown,
        bigquery=bq_drilldown,
    )
    add_panel(
        axis,
        bottom=0.04,
        height=0.295,
        title="COMBINED",
        subtitle="interactive aggregate + drill-down · accumulated runtime and cost",
        clickhouse=ch_combined,
        bigquery=bq_combined,
    )
    if args.wide:
        fig.subplots_adjust(left=0.050, right=0.950, bottom=0.050, top=0.755)

    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    png_path = output_dir / f"{basename}.png"
    svg_path = output_dir / f"{basename}.svg"
    summary_path = output_dir / f"{basename}_summary.json"
    png_path, svg_path = save_figure(
        fig, output_dir, basename, args.dpi, wide=args.wide
    )
    plt.close(fig)

    summary = {
        "schema_version": 1,
        "chart": "matched_active_ingestion_accumulated_query_runtime_and_modeled_cost",
        "layout": layout,
        "pricing": {
            "clickhouse": f"{args.tier} normalized runtime allocation",
            "bigquery_capacity": f"{args.tier} normalized billed-slot-second allocation",
            "bigquery_on_demand": "gross modeled billed-byte cost",
            "bigquery_models_are_alternatives": True,
        },
        "workloads": {
            "dashboard": {"clickhouse": ch_dashboard, "bigquery": bq_dashboard},
            "drilldown": {"clickhouse": ch_drilldown, "bigquery": bq_drilldown},
            "combined": {"clickhouse": ch_combined, "bigquery": bq_combined},
        },
        "sources": {
            name: {"path": str(path), "sha256": sha256(path)}
            for name, path in paths.items()
        },
        "rendering": {"png": str(png_path), "svg": str(svg_path)},
    }
    write_json(summary_path, summary)

    for path in (png_path, svg_path, summary_path):
        print(f"Wrote {path}")
    print(
        "Combined: "
        f"ClickHouse {duration(float(ch_combined['runtime_sec']))}, {money(float(ch_combined['enterprise_cost_usd']))}; "
        f"BigQuery {duration(float(bq_combined['runtime_sec']))}, "
        f"{money(float(bq_combined['capacity_cost_usd']))} capacity or "
        f"{money(float(bq_combined['on_demand_cost_usd']))} on-demand"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
