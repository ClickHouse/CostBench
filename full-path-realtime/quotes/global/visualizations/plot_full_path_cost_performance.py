#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Render the accepted pairwise-normalized full-path scores in one chart."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

from _common import (
    MUTED, WHITE, configure_figure, load_manifest, resolve_layout,
    resolve_source, save_figure, sha256, validate_required_labels, write_csv,
    write_json,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path(__file__).with_name("manifest.json"))
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--basename", default="full_path_cost_performance_all_systems")
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


def row(payload: dict[str, Any], label: str) -> dict[str, Any]:
    try:
        return next(item for item in payload["rows"] if item["label"] == label)
    except StopIteration as error:
        raise ValueError(f"missing accepted cost row {label!r}") from error


def rounded_bar(
    axis: Any,
    *,
    position: float,
    width: float,
    height: float,
    x_limit: float,
    row_count: int,
    color: str,
) -> None:
    """Draw a rounded horizontal bar in axes-relative coordinates."""
    width_fraction = width / x_limit
    height_fraction = height / row_count
    y_fraction = (position + 0.5) / row_count
    rounding = min(height_fraction * 0.12, width_fraction / 2)
    axis.add_patch(
        FancyBboxPatch(
            (0.0, y_fraction - height_fraction / 2),
            width_fraction,
            height_fraction,
            transform=axis.transAxes,
            boxstyle=f"round,pad=0,rounding_size={rounding}",
            facecolor=color,
            edgecolor="none",
            linewidth=0,
            clip_on=True,
            zorder=2,
        )
    )


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
    sf_ch = row(sf, "ClickHouse")
    bq_ch = row(bq, "ClickHouse")
    rows = [
        {
            "label": "ClickHouse",
            "relative": 1.0,
            "color": manifest["providers"]["clickhouse"]["color"],
            "source_pair": "pairwise baselines",
            "accepted_values": {"snowflake_pair": sf_ch, "bigquery_pair": bq_ch},
        },
        {
            "label": "BigQuery · Capacity",
            "relative": float(row(bq, "BigQuery · Capacity")["relative_to_clickhouse"]),
            "color": manifest["providers"]["bigquery"]["color"],
            "source_pair": "ClickHouse vs BigQuery matched window",
            "accepted_values": row(bq, "BigQuery · Capacity"),
        },
        {
            "label": "BigQuery · On-demand",
            "relative": float(row(bq, "BigQuery · On-demand")["relative_to_clickhouse"]),
            "color": manifest["providers"]["bigquery"]["color"],
            "source_pair": "ClickHouse vs BigQuery matched window",
            "accepted_values": row(bq, "BigQuery · On-demand"),
        },
        {
            "label": "Snowflake",
            "relative": float(row(sf, "Snowflake")["relative_to_clickhouse"]),
            "color": manifest["providers"]["snowflake"]["color"],
            "source_pair": "ClickHouse vs Snowflake matched window",
            "accepted_values": row(sf, "Snowflake"),
        },
        {
            "label": "Redshift · SUPER",
            "relative": float(row(rs_super, "Redshift · SUPER")["relative_to_clickhouse"]),
            "color": manifest["providers"]["redshift_super"]["color"],
            "source_pair": "ClickHouse vs Redshift SUPER matched window",
            "accepted_values": row(rs_super, "Redshift · SUPER"),
        },
        {
            "label": "Redshift · Typed",
            "relative": float(row(rs_typed, "Redshift · Typed")["relative_to_clickhouse"]),
            "color": manifest["providers"]["redshift_typed"]["color"],
            "source_pair": "ClickHouse vs Redshift typed matched window",
            "accepted_values": row(rs_typed, "Redshift · Typed"),
        },
    ]
    required_labels = validate_required_labels(
        manifest, "full_path_cost_performance", (row["label"] for row in rows)
    )
    widths = [math.log10(float(item["relative"])) for item in rows]
    maximum_width = max(widths)
    labels = [item["label"] for item in rows]
    colors = [item["color"] for item in rows]

    basename, figure_size, layout = resolve_layout(
        args.basename,
        args.wide,
        (11.5, 6.2),
        dpi=args.dpi,
    )
    fig, axis = plt.subplots(
        figsize=figure_size, dpi=args.dpi if args.wide else None
    )
    plot_background = configure_figure(fig, wide=args.wide)
    axis.set_facecolor(plot_background)
    positions = list(reversed(range(len(rows))))
    bar_height = .72 if args.wide else .58
    value_fontsize = 19 if args.wide else 13
    system_fontsize = 20 if args.wide else 14
    x_limit = maximum_width * 1.22
    axis.set_xlim(0, x_limit)
    axis.set_ylim(-0.5, len(rows) - 0.5)
    axis.set_title(
        "Full-path cost-performance score = (fresh-data path cost + query cost) × total query runtime\nLOG SCALE · LOWER IS BETTER",
        color=WHITE,
        fontsize=15 if args.wide else 10.5,
        pad=18,
    )
    for position, width, color in zip(positions, widths, colors, strict=True):
        if width:
            rounded_bar(
                axis,
                position=position,
                width=width,
                height=bar_height,
                x_limit=x_limit,
                row_count=len(rows),
                color=color,
            )
        else:
            axis.scatter(0, position, s=155 if args.wide else 95, color=color, zorder=4)
    for position, item, width in zip(positions, rows, widths, strict=True):
        value = "best" if item["relative"] == 1 else f"{item['relative']:,.0f}× worse"
        axis.text(min(width + maximum_width * .025, maximum_width * 1.08), position, value,
                  va="center", ha="left", color=WHITE, fontsize=value_fontsize, fontweight="bold")
    axis.set_yticks(positions, labels)
    for tick, color in zip(axis.get_yticklabels(), colors, strict=True):
        tick.set_color(color)
        tick.set_fontsize(system_fontsize)
        tick.set_fontweight("bold")
    axis.set_xticks([])
    axis.tick_params(axis="y", length=0, pad=12)
    axis.spines[:].set_visible(False)
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
        # Keep the horizontal comparison fully inside the staged content area.
        fig.subplots_adjust(
            left=.250,
            right=.950,
            bottom=.13 if args.query_cost_display == "attribution" else .08,
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
        {"label": item["label"], "relative_to_pairwise_clickhouse": item["relative"], "source_pair": item["source_pair"]}
        for item in rows
    ])
    write_json(summary_path, {
        "schema_version": 1,
        "chart": "global_full_path_cost_performance",
        "layout": layout,
        "presentation": {
            "query_cost_display": args.query_cost_display,
            "cost_attribution_visible_on_chart": (
                bool(snowflake_cost_contract)
                and args.query_cost_display == "attribution"
            ),
        },
        "contract": {
            "formula": "Accepted pairwise score = (fresh-data-path cost + query cost) × accumulated query runtime",
            "normalization": "Each non-ClickHouse row reuses relative_to_clickhouse from its own accepted pairwise summary",
            "warning": "Snowflake, BigQuery, and Redshift use their own pairwise matched active-ingestion windows. The global chart does not claim a cross-provider iteration join.",
            "clickhouse_display": "Single 1× baseline; pairwise ClickHouse absolute scores remain separately preserved",
            "snowflake_normalized_query_cost": snowflake_cost_contract,
            "required_labels": required_labels,
            "score_scale": "log10 relative to ClickHouse",
            "baseline_bar_width": 0,
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
