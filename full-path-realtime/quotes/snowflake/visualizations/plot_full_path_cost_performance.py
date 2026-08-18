#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Plot full-path cost-performance for ClickHouse and Snowflake."""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import matplotlib.pyplot as plt

from _common import CLICKHOUSE, MUTED, SNOWFLAKE, configure_figure, duration, money, read_json, resolve_layout, rounded_bar, save_figure, sha256, tier_cost, validate_snowflake_credit_cost, validate_snowflake_query_cost, write_json


FAINT = "#505050"


def args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clickhouse-ingest-cost", type=Path, required=True)
    parser.add_argument("--clickhouse-dashboard-cost", type=Path, required=True)
    parser.add_argument("--clickhouse-drilldown-cost", type=Path, required=True)
    parser.add_argument("--snowflake-snowpipe-cost", type=Path, required=True)
    parser.add_argument("--snowflake-mv-refresh-cost", type=Path, required=True)
    parser.add_argument("--snowflake-dashboard-cost", type=Path, required=True)
    parser.add_argument("--snowflake-drilldown-cost", type=Path, required=True)
    parser.add_argument("--snowflake-dashboard-pricing", type=Path, required=True)
    parser.add_argument("--snowflake-drilldown-pricing", type=Path, required=True)
    parser.add_argument("--snowflake-fallback-pricing", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--tier", default="enterprise")
    parser.add_argument("--basename", default="full_path_cost_performance_clickhouse_vs_snowflake")
    parser.add_argument(
        "--query-cost-display",
        choices=("total", "attribution"),
        default="total",
        help=(
            "Show total query cost only, or also render the Snowflake "
            "fallback-attribution footer. Cost validation and provenance "
            "remain unchanged."
        ),
    )
    parser.add_argument("--wide", action="store_true", help="Render the Keynote-native 5156x2900 staged variant and suffix outputs with _wide.")
    parser.add_argument("--dpi", type=int, default=300)
    return parser.parse_args()


def main() -> int:
    a = args()
    raw_paths = {
        "clickhouse_ingest": a.clickhouse_ingest_cost, "clickhouse_dashboard": a.clickhouse_dashboard_cost,
        "clickhouse_drilldown": a.clickhouse_drilldown_cost, "snowflake_snowpipe": a.snowflake_snowpipe_cost,
        "snowflake_mv_refresh": a.snowflake_mv_refresh_cost, "snowflake_dashboard": a.snowflake_dashboard_cost,
        "snowflake_drilldown": a.snowflake_drilldown_cost, "snowflake_dashboard_pricing": a.snowflake_dashboard_pricing,
        "snowflake_drilldown_pricing": a.snowflake_drilldown_pricing,
        "snowflake_fallback_pricing": a.snowflake_fallback_pricing,
    }
    paths = {name: path.expanduser().resolve() for name, path in raw_paths.items()}
    values = {name: read_json(path) for name, path in paths.items()}
    ch_fresh = float(tier_cost(values["clickhouse_ingest"], a.tier, context=str(paths["clickhouse_ingest"]))["total_compute_cost_usd"])
    ch_query = sum(float(tier_cost(values[name], a.tier, context=str(paths[name]))["total_compute_cost_usd"]) for name in ("clickhouse_dashboard", "clickhouse_drilldown"))
    ch_runtime = sum(float(values[name]["total_runtime_seconds"]) for name in ("clickhouse_dashboard", "clickhouse_drilldown"))
    sf_fresh = validate_snowflake_credit_cost(values["snowflake_snowpipe"], a.tier, context=str(paths["snowflake_snowpipe"])) + validate_snowflake_credit_cost(values["snowflake_mv_refresh"], a.tier, context=str(paths["snowflake_mv_refresh"]))
    sf_dashboard = validate_snowflake_query_cost(
        values["snowflake_dashboard"], values["snowflake_dashboard_pricing"],
        a.tier, context=str(paths["snowflake_dashboard"]),
        fallback_pricing=values["snowflake_fallback_pricing"],
    )
    sf_drilldown = validate_snowflake_query_cost(
        values["snowflake_drilldown"], values["snowflake_drilldown_pricing"],
        a.tier, context=str(paths["snowflake_drilldown"]),
        fallback_pricing=values["snowflake_fallback_pricing"],
    )
    for ch_name, sf in (("clickhouse_dashboard", sf_dashboard), ("clickhouse_drilldown", sf_drilldown)):
        ch_summary = values[ch_name]
        if int(ch_summary["iterations_included"]) * int(ch_summary["queries_per_iteration"]) != int(sf["query_jobs"]):
            raise ValueError(f"matched query-count mismatch for {ch_name}")
    sf_query = float(sf_dashboard["cost_usd"]) + float(sf_drilldown["cost_usd"])
    sf_runtime = float(sf_dashboard["runtime_sec"]) + float(sf_drilldown["runtime_sec"])
    rows = [
        {"label": "ClickHouse", "color": CLICKHOUSE, "fresh_cost": ch_fresh, "query_cost": ch_query, "runtime_sec": ch_runtime},
        {"label": "Snowflake", "color": SNOWFLAKE, "fresh_cost": sf_fresh, "query_cost": sf_query, "runtime_sec": sf_runtime},
    ]
    for row in rows:
        row["total_cost"] = row["fresh_cost"] + row["query_cost"]
        row["score"] = row["total_cost"] * row["runtime_sec"]
    baseline = rows[0]["score"]
    for row in rows: row["relative_to_clickhouse"] = row["score"] / baseline
    basename, figure_size, layout = resolve_layout(
        a.basename, a.wide, (11.5, 5.7), dpi=a.dpi
    )
    fig, axis = plt.subplots(
        figsize=figure_size, dpi=a.dpi if a.wide else None
    )
    plot_background = configure_figure(fig, wide=a.wide)
    axis.set_facecolor(plot_background)
    axis.set_xlim(0, 1); axis.set_ylim(0, 1); axis.axis("off")
    left, max_width, bar_height = .025, .86, .105
    system_size = 30 if a.wide else 23
    result_size = 23 if a.wide else 18
    detail_size = 18 if a.wide else 13
    max_relative = max(float(row["relative_to_clickhouse"]) for row in rows)
    log_span = math.log10(max_relative)
    axis.text(
        left,
        .965,
        "Full-path cost-performance score = (fresh-data path cost + query cost) × total query runtime\nLOG SCALE · LOWER IS BETTER",
        color=MUTED,
        fontsize=14 if a.wide else 10,
        va="top",
    )
    for index, (row, y) in enumerate(zip(rows, (.61, .14))):
        axis.text(left, y + .16, row["label"], color=row["color"], fontsize=system_size, fontweight="bold", va="center")
        relative = float(row["relative_to_clickhouse"])
        width = max_width * math.log10(relative) / log_span if relative > 1 else 0.0
        if width:
            rounded_bar(axis, left, y, width, bar_height, row["color"])
        else:
            axis.scatter(left, y + bar_height / 2, s=155 if a.wide else 95, color=row["color"], zorder=4)
        result = "best" if index == 0 else f"{row['relative_to_clickhouse']:,.0f}× worse"
        axis.text(min(left + width + .018, .90), y + bar_height / 2, result, color="white", fontsize=result_size, fontweight="bold", va="center")
        detail = f"({money(row['fresh_cost'])} fresh data path + {money(row['query_cost'])} queries cost) × {duration(row['runtime_sec'])} queries runtime = {row['score']:,.0f}".replace("$", r"\$")
        axis.text(left, y - .07, detail, color=MUTED, fontsize=detail_size, va="center")
        if index == 0: axis.plot([left, .985], [y - .15, y - .15], color=FAINT, linewidth=.8, alpha=.55)
    if a.query_cost_display == "attribution":
        axis.text(
            left,
            .015,
            "Snowflake normalized query-cost proxy: jobs >5s are priced for full elapsed time at Gen2 Small; no added Interactive charge",
            color=MUTED,
            fontsize=10.5 if a.wide else 7.8,
            va="bottom",
        )
    if a.wide:
        fig.subplots_adjust(left=.055, right=.945, bottom=.070, top=.755)
    else:
        fig.subplots_adjust(left=.008, right=.997, bottom=.02, top=.995)
    output = a.output_dir.expanduser().resolve(); png, svg = save_figure(fig, output, basename, a.dpi, wide=a.wide); plt.close(fig)
    summary = output / f"{basename}_summary.json"
    write_json(summary, {"schema_version": 1, "chart": "full_path_cost_performance", "layout": layout, "formula": "(fresh_data_path_cost_usd + queries_cost_usd) * queries_runtime_sec", "lower_is_better": True, "tier": a.tier,
                         "presentation": {"query_cost_display": a.query_cost_display, "cost_attribution_visible_on_chart": a.query_cost_display == "attribution", "score_scale": "log10 relative to ClickHouse", "baseline_bar_width": 0},
                         "rows": [{key: value for key, value in row.items() if key != "color"} for row in rows],
                         "snowflake_normalized_query_cost_contract": {"dashboard": sf_dashboard["query_cost_model"], "drilldown": sf_drilldown["query_cost_model"], "dashboard_attribution": {key: sf_dashboard[key] for key in ("query_jobs", "primary_priced_query_jobs", "primary_priced_runtime_sec", "fallback_priced_query_jobs", "fallback_priced_runtime_sec", "fallback_priced_job_share")}, "drilldown_attribution": {key: sf_drilldown[key] for key in ("query_jobs", "primary_priced_query_jobs", "primary_priced_runtime_sec", "fallback_priced_query_jobs", "fallback_priced_runtime_sec", "fallback_priced_job_share")}},
                         "snowflake_fresh_path_contract": "Snowpipe Streaming + serverless MV refresh; no ingest warehouse",
                         "sources": {name: {"path": str(path), "sha256": sha256(path)} for name, path in paths.items()}, "outputs": {"png": str(png), "svg": str(svg)}})
    for path in (png, svg, summary): print(f"Wrote {path}")
    return 0


if __name__ == "__main__": raise SystemExit(main())
