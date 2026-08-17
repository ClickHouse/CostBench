#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Plot full-path cost-performance for ClickHouse and BigQuery.

The score is `(fresh-data path cost + matched query cost) * accumulated
query runtime`; lower is better. BigQuery capacity and on-demand are rendered
as alternative pricing models and are never added together.
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
from matplotlib.patches import FancyBboxPatch

from _layout import configure_figure, resolve_layout, save_figure, write_json


WHITE = "#F7F7F7"
MUTED = "#A0A0A0"
FAINT = "#505050"
CLICKHOUSE = "#FDFF62"
BIGQUERY_CAPACITY = "#4285F4"
BIGQUERY_ON_DEMAND = BIGQUERY_CAPACITY

matplotlib.rcParams["font.family"] = (
    "Inter"
    if any(font.name == "Inter" for font in font_manager.fontManager.ttflist)
    else "DejaVu Sans"
)
matplotlib.rcParams["font.sans-serif"] = ["Inter", "DejaVu Sans"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clickhouse-ingest-cost", type=Path, required=True)
    parser.add_argument("--clickhouse-dashboard-cost", type=Path, required=True)
    parser.add_argument("--clickhouse-drilldown-cost", type=Path, required=True)
    parser.add_argument("--bigquery-ingest-cost", type=Path, required=True)
    parser.add_argument("--bigquery-mv-refresh-cost", type=Path, required=True)
    parser.add_argument("--bigquery-dashboard-cost", type=Path, required=True)
    parser.add_argument("--bigquery-drilldown-cost", type=Path, required=True)
    parser.add_argument("--bigquery-serverless-pricing", type=Path, required=True)
    parser.add_argument("--bigquery-write-api-pricing", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--tier", default="Enterprise")
    parser.add_argument("--region", default="us")
    parser.add_argument(
        "--basename",
        default="full_path_cost_performance_clickhouse_vs_bigquery",
    )
    parser.add_argument(
        "--wide",
        action="store_true",
        help="Render the Keynote-native 5156x2900 staged variant and append _wide to the basename.",
    )
    parser.add_argument("--dpi", type=int, default=300)
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


def one(items: list[dict[str, Any]], *, context: str, **conditions: str) -> dict[str, Any]:
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
    return f"${value:,.3f}" if value < 1 else f"${value:,.2f}"


def duration(value: float) -> str:
    if value < 60:
        return f"{value:.1f}s"
    if value < 3600:
        return f"{value / 60:.1f}m"
    return f"{value / 3600:.1f}h"


def rounded_bar(axis: Any, x: float, y: float, width: float, height: float, color: str) -> None:
    axis.add_patch(
        FancyBboxPatch(
            (x, y),
            width,
            height,
            boxstyle=f"round,pad=0,rounding_size={height * 0.12}",
            linewidth=0,
            facecolor=color,
        )
    )


def validate_query_summary(summary: dict[str, Any], *, system: str, component: str, path: Path) -> None:
    if system == "BigQuery":
        if summary.get("system") != "BigQuery" or summary.get("component") != component:
            raise ValueError(f"wrong BigQuery query component in {path}")
        for field in ("failed_jobs", "cache_hits", "missing_billed_bytes", "missing_slot_seconds"):
            if int(summary.get(field) or 0) != 0:
                raise ValueError(f"{field} is nonzero in {path}")
    elif "ClickHouse" not in str(summary.get("system") or ""):
        raise ValueError(f"not a ClickHouse summary: {path}")


def main() -> int:
    args = parse_args()
    paths = {
        "clickhouse_ingest_cost": args.clickhouse_ingest_cost.expanduser().resolve(),
        "clickhouse_dashboard_cost": args.clickhouse_dashboard_cost.expanduser().resolve(),
        "clickhouse_drilldown_cost": args.clickhouse_drilldown_cost.expanduser().resolve(),
        "bigquery_ingest_cost": args.bigquery_ingest_cost.expanduser().resolve(),
        "bigquery_mv_refresh_cost": args.bigquery_mv_refresh_cost.expanduser().resolve(),
        "bigquery_dashboard_cost": args.bigquery_dashboard_cost.expanduser().resolve(),
        "bigquery_drilldown_cost": args.bigquery_drilldown_cost.expanduser().resolve(),
        "bigquery_serverless_pricing": args.bigquery_serverless_pricing.expanduser().resolve(),
        "bigquery_write_api_pricing": args.bigquery_write_api_pricing.expanduser().resolve(),
    }
    values = {name: load_json(path) for name, path in paths.items()}

    ch_ingest = values["clickhouse_ingest_cost"]
    ch_dashboard = values["clickhouse_dashboard_cost"]
    ch_drilldown = values["clickhouse_drilldown_cost"]
    bq_ingest = values["bigquery_ingest_cost"]
    bq_mv = values["bigquery_mv_refresh_cost"]
    bq_dashboard = values["bigquery_dashboard_cost"]
    bq_drilldown = values["bigquery_drilldown_cost"]
    serverless = values["bigquery_serverless_pricing"]
    write_pricing = values["bigquery_write_api_pricing"]

    validate_query_summary(
        ch_dashboard,
        system="ClickHouse",
        component="dashboard",
        path=paths["clickhouse_dashboard_cost"],
    )
    validate_query_summary(
        ch_drilldown,
        system="ClickHouse",
        component="drilldown",
        path=paths["clickhouse_drilldown_cost"],
    )
    validate_query_summary(
        bq_dashboard,
        system="BigQuery",
        component="dashboard",
        path=paths["bigquery_dashboard_cost"],
    )
    validate_query_summary(
        bq_drilldown,
        system="BigQuery",
        component="drilldown",
        path=paths["bigquery_drilldown_cost"],
    )

    for component, ch, bq in (
        ("dashboard", ch_dashboard, bq_dashboard),
        ("drilldown", ch_drilldown, bq_drilldown),
    ):
        ch_jobs = int(ch["iterations_included"]) * int(ch["queries_per_iteration"])
        if ch_jobs != int(bq["query_jobs"]):
            raise ValueError(f"{component} query-count mismatch: ClickHouse {ch_jobs} vs BigQuery {bq['query_jobs']}")

    ch_fresh = float(
        one(ch_ingest["costs"], context=str(paths["clickhouse_ingest_cost"]), tier=args.tier)[
            "total_compute_cost_usd"
        ]
    )
    ch_query = sum(
        float(one(item["costs"], context="ClickHouse query cost", tier=args.tier)["total_compute_cost_usd"])
        for item in (ch_dashboard, ch_drilldown)
    )
    ch_runtime = sum(float(item["total_runtime_seconds"]) for item in (ch_dashboard, ch_drilldown))

    if bq_ingest.get("component") != "storage_write_api_ingest":
        raise ValueError("BigQuery ingest summary has the wrong component")
    bq_write = float(bq_ingest["pricing"]["total_cost_usd"])
    write_rate = float(write_pricing["pricing"]["price_usd"])
    close(float(bq_ingest["pricing"]["price_usd"]), write_rate, context="Storage Write API rate")
    close(bq_write, float(bq_ingest["total_input_gib"]) * write_rate, context="Storage Write API total")

    if bq_mv.get("component") != "mv_refresh" or int(bq_mv.get("failed_jobs") or 0) != 0:
        raise ValueError("invalid BigQuery MV refresh summary")

    compute = serverless["regions"][args.region]["pricing_compute"]
    capacity_rate = float(
        one(
            compute["capacity"]["default"]["hourly"]["tiers"],
            context=str(paths["bigquery_serverless_pricing"]),
            name=args.tier,
        )["price_usd"]
    )
    on_demand_rate = float(compute["on_demand"]["monthly"]["price_usd"])
    on_demand_unit_bytes = float(compute["on_demand"]["monthly"]["price_unit_bytes"])

    def bq_cost(summary: dict[str, Any], model: str) -> float:
        if model == "capacity":
            entry = one(
                summary["costs"],
                context="BigQuery cost",
                tier=args.tier,
                compute_model="capacity",
                pricing_variant="default",
                billing_period="hourly",
            )
            close(float(entry["price_usd"]), capacity_rate, context="BigQuery capacity rate")
            return float(entry["total_compute_cost_usd"])
        entry = one(
            summary["costs"],
            context="BigQuery cost",
            tier="OnDemand",
            compute_model="on_demand",
        )
        close(float(entry["price_usd"]), on_demand_rate, context="BigQuery on-demand rate")
        return float(entry["total_compute_cost_usd"])

    bq_mv_capacity = bq_cost(bq_mv, "capacity")
    bq_mv_on_demand = bq_cost(bq_mv, "on_demand")
    close(
        bq_mv_capacity,
        float(bq_mv["total_billed_slot_sec"]) / 3600 * capacity_rate,
        context="BigQuery MV capacity total",
    )
    close(
        bq_mv_on_demand,
        float(bq_mv["total_bytes_billed"]) / on_demand_unit_bytes * on_demand_rate,
        context="BigQuery MV on-demand total",
    )

    bq_query_capacity = sum(bq_cost(item, "capacity") for item in (bq_dashboard, bq_drilldown))
    bq_query_on_demand = sum(bq_cost(item, "on_demand") for item in (bq_dashboard, bq_drilldown))
    bq_runtime = sum(float(item["total_runtime_seconds"]) for item in (bq_dashboard, bq_drilldown))

    rows = [
        {
            "label": "ClickHouse",
            "color": CLICKHOUSE,
            "fresh_cost": ch_fresh,
            "query_cost": ch_query,
            "runtime_sec": ch_runtime,
        },
        {
            "label": "BigQuery · Capacity",
            "color": BIGQUERY_CAPACITY,
            "fresh_cost": bq_write + bq_mv_capacity,
            "query_cost": bq_query_capacity,
            "runtime_sec": bq_runtime,
        },
        {
            "label": "BigQuery · On-demand",
            "color": BIGQUERY_ON_DEMAND,
            "fresh_cost": bq_write + bq_mv_on_demand,
            "query_cost": bq_query_on_demand,
            "runtime_sec": bq_runtime,
        },
    ]
    for row in rows:
        row["total_cost"] = float(row["fresh_cost"]) + float(row["query_cost"])
        row["score"] = float(row["total_cost"]) * float(row["runtime_sec"])
    baseline = float(rows[0]["score"])
    for row in rows:
        row["relative_to_clickhouse"] = float(row["score"]) / baseline

    basename, figure_size, layout = resolve_layout(
        args.basename,
        args.wide,
        (11.5, 7.3),
        dpi=args.dpi,
    )
    fig, axis = plt.subplots(
        figsize=figure_size, dpi=args.dpi if args.wide else None
    )
    plot_background = configure_figure(fig, wide=args.wide)
    axis.set_facecolor(plot_background)
    axis.set_xlim(0, 1)
    axis.set_ylim(0, 1)
    axis.axis("off")

    left = 0.035 if args.wide else 0.045
    # Keep a dedicated right-side gutter for the relative-result label.  The
    # maximum bar still fills the entire bar lane, but can no longer collide
    # with labels such as "512x worse" in either layout.
    bar_right = 0.82 if args.wide else 0.78
    result_gap = 0.018 if args.wide else 0.020
    max_width = bar_right - left
    bar_height = 0.125 if args.wide else 0.105
    label_fontsize = 28 if args.wide else 21
    result_fontsize = 22 if args.wide else 17
    formula_fontsize = 16 if args.wide else 12.5
    max_relative = max(float(row["relative_to_clickhouse"]) for row in rows)
    log_span = math.log10(max_relative)
    y_positions = (0.70, 0.38, 0.06)
    axis.text(
        left,
        0.97,
        "Full-path cost-performance score = (fresh-data path cost + query cost) × total query runtime\nLOG SCALE · LOWER IS BETTER",
        color=MUTED,
        fontsize=13 if args.wide else 9.5,
        va="top",
    )

    for index, (row, y) in enumerate(zip(rows, y_positions)):
        color = str(row["color"])
        axis.text(left, y + 0.14, str(row["label"]), color=color, fontsize=label_fontsize, fontweight="bold", va="center")
        relative = float(row["relative_to_clickhouse"])
        width = max_width * math.log10(relative) / log_span if relative > 1 else 0.0
        if width:
            rounded_bar(axis, left, y, width, bar_height, color)
        else:
            axis.scatter(left, y + bar_height / 2, s=155 if args.wide else 95, color=color, zorder=4)

        result = "best" if index == 0 else f"{relative:,.0f}× worse"
        axis.text(
            left + width + result_gap,
            y + bar_height / 2,
            result,
            color=WHITE,
            fontsize=result_fontsize,
            fontweight="bold",
            va="center",
        )
        fresh_cost_text = money(float(row["fresh_cost"])).replace("$", r"\$")
        query_cost_text = money(float(row["query_cost"])).replace("$", r"\$")
        axis.text(
            left,
            y - 0.055,
            (
                f"({fresh_cost_text} fresh data path + "
                f"{query_cost_text} queries cost) × "
                f"{duration(float(row['runtime_sec']))} queries runtime = {float(row['score']):,.0f}"
            ),
            color=MUTED,
            fontsize=formula_fontsize,
            va="center",
        )
        if index < len(rows) - 1:
            axis.plot([left, 0.955], [y - 0.115, y - 0.115], color=FAINT, linewidth=0.8, alpha=0.55)

    if args.wide:
        fig.subplots_adjust(left=0.055, right=0.945, bottom=0.050, top=0.755)

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
        "chart": "full_path_cost_performance",
        "formula": "(fresh_path_cost_usd + query_cost_usd) * accumulated_query_runtime_sec",
        "lower_is_better": True,
        "pricing_tier": args.tier,
        "pricing_region": args.region,
        "bigquery_models_are_alternatives": True,
        "layout": layout,
        "score_scale": "log10 relative to ClickHouse",
        "baseline_bar_width": 0,
        "rows": [
            {
                key: value
                for key, value in row.items()
                if key != "color"
            }
            for row in rows
        ],
        "sources": {
            name: {"path": str(path), "sha256": sha256(path)}
            for name, path in paths.items()
        },
        "rendering": {"png": str(png_path), "svg": str(svg_path)},
    }
    write_json(summary_path, summary)

    for path in (png_path, svg_path, summary_path):
        print(f"Wrote {path}")
    for row in rows:
        print(
            f"{row['label']}: score={float(row['score']):,.0f}, "
            f"relative={float(row['relative_to_clickhouse']):,.1f}x"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
