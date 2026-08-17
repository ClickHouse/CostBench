#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Plot complete-ingest fresh-data-path cost for ClickHouse and Snowflake."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt

from _common import (
    CLICKHOUSE,
    MUTED,
    SNOWFLAKE,
    SNOWFLAKE_DARK,
    configure_figure,
    money,
    read_json,
    resolve_layout,
    rounded_bar,
    save_figure,
    sha256,
    tier_cost,
    validate_snowflake_credit_cost,
    write_json,
)


def args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clickhouse-ingest-cost", type=Path, required=True)
    parser.add_argument("--snowflake-snowpipe-cost", type=Path, required=True)
    parser.add_argument("--snowflake-mv-refresh-cost", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--tier", default="enterprise")
    parser.add_argument(
        "--basename", default="ingest_fresh_path_cost_clickhouse_vs_snowflake"
    )
    parser.add_argument(
        "--wide",
        action="store_true",
        help="Render the Keynote-native 5156x2900 staged variant and suffix outputs with _wide.",
    )
    parser.add_argument("--dpi", type=int, default=300)
    return parser.parse_args()


def main() -> int:
    a = args()
    paths = {
        name: value.expanduser().resolve()
        for name, value in {
            "clickhouse": a.clickhouse_ingest_cost,
            "snowpipe": a.snowflake_snowpipe_cost,
            "mv_refresh": a.snowflake_mv_refresh_cost,
        }.items()
    }
    values = {name: read_json(path) for name, path in paths.items()}
    ch = float(
        tier_cost(values["clickhouse"], a.tier, context=str(paths["clickhouse"]))[
            "total_compute_cost_usd"
        ]
    )
    snowpipe = validate_snowflake_credit_cost(
        values["snowpipe"], a.tier, context=str(paths["snowpipe"])
    )
    mv = validate_snowflake_credit_cost(
        values["mv_refresh"], a.tier, context=str(paths["mv_refresh"])
    )
    sf = snowpipe + mv
    basename, figure_size, layout = resolve_layout(
        a.basename, a.wide, (12, 6.2), dpi=a.dpi
    )
    fig, axis = plt.subplots(
        figsize=figure_size, dpi=a.dpi if a.wide else None
    )
    plot_background = configure_figure(fig, wide=a.wide)
    axis.set_facecolor(plot_background)
    axis.set_xlim(0, 1)
    axis.set_ylim(0, 1)
    axis.axis("off")
    left, full, height = 0.025, (0.82 if a.wide else 0.78), 0.105
    value_gap = 0.018
    system_size = 34 if a.wide else 28
    total_size = 31 if a.wide else 25
    detail_size = 19 if a.wide else 15
    rows = (("ClickHouse", CLICKHOUSE, 0.67, ch), ("Snowflake", SNOWFLAKE, 0.25, sf))
    for label, color, y, total in rows:
        axis.text(
            left,
            y + 0.16,
            label,
            color=color,
            fontsize=system_size,
            fontweight="bold",
            va="center",
        )
        width = full * total / sf
        axis.text(
            left + width + value_gap,
            y + height / 2,
            money(total),
            color="white",
            fontsize=total_size,
            fontweight="bold",
            ha="left",
            va="center",
        )
    rounded_bar(axis, left, rows[0][2], full * ch / sf, height, CLICKHOUSE)
    sf_bar = rounded_bar(axis, left, rows[1][2], full, height, SNOWFLAKE_DARK)
    ingest_width = full * snowpipe / sf
    segment = plt.Rectangle(
        (left, rows[1][2]), ingest_width, height, facecolor=SNOWFLAKE, edgecolor="none"
    )
    segment.set_clip_path(sf_bar)
    axis.add_patch(segment)
    axis.text(
        left,
        rows[0][2] - 0.07,
        f"Bundled write service · ingest, sorting, merges & incremental MV · {money(ch)}",
        color=MUTED,
        fontsize=detail_size,
        va="top",
    )
    detail = f"Snowpipe Streaming {money(snowpipe)} · serverless MV refresh {money(mv)}".replace(
        "$", r"\$"
    )
    axis.text(
        left, rows[1][2] - 0.07, detail, color=MUTED, fontsize=detail_size, va="top"
    )
    if a.wide:
        fig.subplots_adjust(left=0.055, right=0.945, bottom=0.070, top=0.755)
    else:
        fig.subplots_adjust(left=0.008, right=0.997, bottom=0.02, top=0.995)
    output = a.output_dir.expanduser().resolve()
    png, svg = save_figure(fig, output, basename, a.dpi, wide=a.wide)
    plt.close(fig)
    summary = output / f"{basename}_summary.json"
    write_json(
        summary,
        {
            "schema_version": 1,
            "chart": "complete_ingest_fresh_data_path_cost",
            "layout": layout,
            "scope": "complete ingestion; storage and read-query costs excluded",
            "tier": a.tier,
            "presentation": {
                "total_label_placement": "directly_after_bar_end",
                "maximum_bar_width_axis_fraction": full,
            },
            "costs": {
                "clickhouse": {"bundled_write_service_usd": ch, "total_usd": ch},
                "snowflake": {
                    "snowpipe_streaming_usd": snowpipe,
                    "serverless_mv_refresh_usd": mv,
                    "total_usd": sf,
                    "ingest_warehouse_used": False,
                },
            },
            "sources": {
                name: {"path": str(path), "sha256": sha256(path)}
                for name, path in paths.items()
            },
            "outputs": {"png": str(png), "svg": str(svg)},
        },
    )
    for path in (png, svg, summary):
        print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
