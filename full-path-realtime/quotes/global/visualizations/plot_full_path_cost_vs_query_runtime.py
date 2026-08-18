#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Plot full-path cost against accumulated query runtime on inverted log axes."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, LogLocator

from _common import (
    GRID, MUTED, configure_figure, load_manifest, resolve_layout, resolve_source,
    save_figure, sha256, validate_required_labels, write_csv, write_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path(__file__).with_name("manifest.json"))
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--basename", default="full_path_cost_vs_query_runtime_all_systems")
    parser.add_argument(
        "--query-cost-display",
        choices=("total", "attribution"),
        default="total",
        help=(
            "Show total query cost only, or also render the Snowflake "
            "fallback-attribution footer. Pairwise provenance is retained "
            "in both modes."
        ),
    )
    parser.add_argument(
        "--wide",
        action="store_true",
        help="Render the Keynote-native 5156x2900 staged variant and append _wide to the basename.",
    )
    parser.add_argument("--dpi", type=int, default=300)
    return parser.parse_args()


def accepted_row(payload: dict[str, Any], label: str) -> dict[str, Any]:
    try:
        return next(row for row in payload["rows"] if row["label"] == label)
    except StopIteration as error:
        raise ValueError(f"missing accepted row {label!r}") from error


def dollars(value: float, _position: float | None = None) -> str:
    return f"${value:,.0f}"


def seconds(value: float, _position: float | None = None) -> str:
    if value >= 3600:
        return f"{value / 3600:.1f}h"
    if value >= 60:
        return f"{value / 60:.1f}m"
    return f"{value:.1f}s"


def main() -> int:
    args = parse_args()
    manifest, manifest_path = load_manifest(args.manifest)
    config = manifest["cost_performance"]
    sf_path = resolve_source(config["snowflake_pairwise_summary"])
    bq_path = resolve_source(config["bigquery_pairwise_summary"])
    rs_super_path = resolve_source(config["redshift_super_summary"])
    rs_typed_path = resolve_source(config["redshift_typed_summary"])
    sf = json.loads(sf_path.read_text(encoding="utf-8"))
    bq = json.loads(bq_path.read_text(encoding="utf-8"))
    rs_super = json.loads(rs_super_path.read_text(encoding="utf-8"))
    rs_typed = json.loads(rs_typed_path.read_text(encoding="utf-8"))
    snowflake_cost_contract = sf.get("snowflake_normalized_query_cost_contract")

    # Use the accepted Snowflake-matched ClickHouse row as the canonical global
    # baseline. Pairwise windows remain explicit in the output provenance.
    ch = accepted_row(sf, "ClickHouse")
    rows = [
        {
            "label": "ClickHouse",
            "runtime_sec": float(ch["runtime_sec"]),
            "full_path_cost_usd": float(ch["total_cost"]),
            "color": manifest["providers"]["clickhouse"]["color"],
            "source_pair": "ClickHouse vs Snowflake matched window",
        },
        {
            "label": "Snowflake",
            "runtime_sec": float(accepted_row(sf, "Snowflake")["runtime_sec"]),
            "full_path_cost_usd": float(accepted_row(sf, "Snowflake")["total_cost"]),
            "color": manifest["providers"]["snowflake"]["color"],
            "source_pair": "ClickHouse vs Snowflake matched window",
        },
        {
            "label": "BigQuery · Capacity",
            "runtime_sec": float(accepted_row(bq, "BigQuery · Capacity")["runtime_sec"]),
            "full_path_cost_usd": float(accepted_row(bq, "BigQuery · Capacity")["total_cost"]),
            "color": manifest["providers"]["bigquery"]["color"],
            "source_pair": "ClickHouse vs BigQuery matched window",
        },
        {
            "label": "BigQuery · On-demand",
            "runtime_sec": float(accepted_row(bq, "BigQuery · On-demand")["runtime_sec"]),
            "full_path_cost_usd": float(accepted_row(bq, "BigQuery · On-demand")["total_cost"]),
            "color": manifest["providers"]["bigquery"]["color"],
            "source_pair": "ClickHouse vs BigQuery matched window",
        },
        {
            "label": "Redshift · SUPER",
            "runtime_sec": float(accepted_row(rs_super, "Redshift · SUPER")["runtime_sec"]),
            "full_path_cost_usd": float(accepted_row(rs_super, "Redshift · SUPER")["total_cost"]),
            "color": manifest["providers"]["redshift_super"]["color"],
            "source_pair": "ClickHouse vs Redshift SUPER matched window",
        },
        {
            "label": "Redshift · Typed",
            "runtime_sec": float(accepted_row(rs_typed, "Redshift · Typed")["runtime_sec"]),
            "full_path_cost_usd": float(accepted_row(rs_typed, "Redshift · Typed")["total_cost"]),
            "color": manifest["providers"]["redshift_typed"]["color"],
            "source_pair": "ClickHouse vs Redshift typed matched window",
        },
    ]
    required_labels = validate_required_labels(
        manifest,
        "full_path_cost_vs_query_runtime",
        (row["label"] for row in rows),
    )

    basename, figure_size, layout = resolve_layout(
        args.basename,
        args.wide,
        (9.5, 7.2),
        dpi=args.dpi,
    )
    fig, axis = plt.subplots(
        figsize=figure_size, dpi=args.dpi if args.wide else None
    )
    plot_background = configure_figure(fig, wide=args.wide)
    axis.set_facecolor(plot_background)
    axis.set_xscale("log")
    axis.set_yscale("log")
    marker_size = 420 if args.wide else 270
    annotation_fontsize = 16 if args.wide else 11.5
    tick_fontsize = 14 if args.wide else 10
    label_fontsize = 16 if args.wide else 11

    offsets = {
        "ClickHouse": (-12, 12, "right", "bottom"),
        "Snowflake": (12, 10, "left", "bottom"),
        "BigQuery · Capacity": (12, 13, "left", "bottom"),
        "BigQuery · On-demand": (12, -13, "left", "top"),
        "Redshift · SUPER": (-12, 12, "right", "bottom"),
        "Redshift · Typed": (-12, -12, "right", "top"),
    }
    for row in rows:
        axis.scatter(
            row["runtime_sec"], row["full_path_cost_usd"], s=marker_size,
            color=row["color"], edgecolor=plot_background, linewidth=1.5, zorder=5,
        )
        dx, dy, horizontal, vertical = offsets[row["label"]]
        axis.annotate(
            row["label"],
            (row["runtime_sec"], row["full_path_cost_usd"]),
            xytext=(dx, dy), textcoords="offset points",
            ha=horizontal, va=vertical, color=row["color"], fontsize=annotation_fontsize,
            fontweight="bold", zorder=6,
        )

    runtimes = [row["runtime_sec"] for row in rows]
    costs = [row["full_path_cost_usd"] for row in rows]
    axis.set_xlim(max(runtimes) * 1.65, min(runtimes) / 1.7)
    axis.set_ylim(max(costs) * 1.55, min(costs) / 1.55)
    axis.xaxis.set_major_locator(LogLocator(base=10))
    axis.yaxis.set_major_locator(LogLocator(base=10))
    axis.xaxis.set_major_formatter(FuncFormatter(seconds))
    axis.yaxis.set_major_formatter(FuncFormatter(dollars))
    axis.grid(True, which="major", color=GRID, linewidth=.8, alpha=.75)
    axis.grid(True, which="minor", color=GRID, linewidth=.35, alpha=.28)
    axis.tick_params(colors=MUTED, labelsize=tick_fontsize)
    axis.spines[:].set_color(GRID)
    axis.set_xlabel("Accumulated query runtime  ·  slower ←     → faster", color=MUTED, fontsize=label_fontsize, labelpad=14)
    axis.set_ylabel(
        "Full-path cost\n(fresh-data path + queries)  ·  lower cost ↑",
        color=MUTED,
        fontsize=label_fontsize,
        labelpad=16,
    )
    if snowflake_cost_contract and args.query_cost_display == "attribution":
        fig.text(
            .99,
            .018,
            "Snowflake normalized query-cost proxy: jobs >5s priced at Gen2 Small for full elapsed time",
            ha="right",
            color=MUTED,
            fontsize=10.5 if args.wide else 7.8,
        )
    if args.wide:
        # Use essentially the full slide width for the quadrant itself.  The
        # prior wide layout enlarged the canvas while leaving a smaller,
        # centered plotting island.
        fig.subplots_adjust(
            left=.115,
            right=.950,
            bottom=.16 if args.query_cost_display == "attribution" else .13,
            top=.735,
        )
    else:
        fig.tight_layout(
            rect=(0, .055 if args.query_cost_display == "attribution" else .01, 1, 1)
        )

    output = args.output_dir.expanduser().resolve()
    png, svg = save_figure(fig, output, basename, args.dpi, wide=args.wide)
    plt.close(fig)
    csv_path = output / f"{basename}_data.csv"
    summary_path = output / f"{basename}_summary.json"
    write_csv(csv_path, [
        {key: value for key, value in row.items() if key != "color"}
        for row in rows
    ])
    write_json(summary_path, {
        "schema_version": 1,
        "chart": "global_full_path_cost_vs_query_runtime",
        "layout": layout,
        "presentation": {
            "query_cost_display": args.query_cost_display,
            "cost_attribution_visible_on_chart": (
                bool(snowflake_cost_contract)
                and args.query_cost_display == "attribution"
            ),
        },
        "contract": {
            "x": "Accumulated query runtime in seconds; logarithmic and inverted so faster is right",
            "y": "Fresh-data-path cost plus query cost in USD; logarithmic and inverted so lower cost is up",
            "alignment": "The requested global view deliberately accepts the distinct pairwise matched query windows",
            "clickhouse_point": "Uses the accepted Snowflake-matched ClickHouse row, the longer active-ingestion query window",
            "marks": "Dots and labels only",
            "snowflake_normalized_query_cost": snowflake_cost_contract,
            "required_labels": required_labels,
        },
        "manifest": {"path": str(manifest_path), "sha256": sha256(manifest_path)},
        "sources": {
            "snowflake_pairwise_summary": {"path": str(sf_path), "sha256": sha256(sf_path)},
            "bigquery_pairwise_summary": {"path": str(bq_path), "sha256": sha256(bq_path)},
            "redshift_super_summary": {"path": str(rs_super_path), "sha256": sha256(rs_super_path)},
            "redshift_typed_summary": {"path": str(rs_typed_path), "sha256": sha256(rs_typed_path)},
        },
        "rows": rows,
        "outputs": {"png": str(png), "svg": str(svg), "csv": str(csv_path)},
    })
    for path in (png, svg, csv_path, summary_path):
        print(f"Written: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
