#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Render one accepted ClickHouse versus Redshift full-path score."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

from _layout import CLICKHOUSE, REDSHIFT, WHITE, configure_figure, load_json, resolve_layout, save_figure, write_json


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--basename", required=True)
    parser.add_argument("--wide", action="store_true")
    parser.add_argument("--dpi", type=int, default=300)
    args = parser.parse_args()
    source = args.summary.expanduser().resolve()
    payload = load_json(source)
    rows = payload["rows"]
    if len(rows) != 2 or rows[0]["label"] != "ClickHouse":
        raise ValueError("expected two accepted rows with ClickHouse first")
    relative = [float(row["relative_to_clickhouse"]) for row in rows]
    maximum = max(relative)
    widths = [math.log10(value) for value in relative]
    maximum_width = max(widths)
    colors = [CLICKHOUSE, REDSHIFT]

    basename, size, layout = resolve_layout(args.basename, args.wide, (11, 4.8), args.dpi)
    figure, axis = plt.subplots(figsize=size, dpi=args.dpi if args.wide else None)
    background = configure_figure(figure, wide=args.wide)
    axis.set_facecolor(background)
    axis.set_xlim(-maximum_width * .025, maximum_width * 1.22)
    axis.set_ylim(-.55, 1.55)
    axis.set_xticks([])
    axis.set_title(
        "Full-path cost-performance score = (fresh-data path cost + query cost) × total query runtime\nLOG SCALE · LOWER IS BETTER",
        color=WHITE,
        fontsize=15 if args.wide else 10.5,
        pad=18,
    )
    axis.spines[:].set_visible(False)
    axis.tick_params(axis="y", length=0, pad=12)
    positions = [1, 0]
    axis.set_yticks(positions, [row["label"] for row in rows])
    for tick, color in zip(axis.get_yticklabels(), colors, strict=True):
        tick.set_color(color)
        tick.set_fontsize(20 if args.wide else 14)
        tick.set_fontweight("bold")
    for position, width, color, value in zip(positions, widths, colors, relative, strict=True):
        if width:
            axis.add_patch(
                FancyBboxPatch(
                    (0, position - .31), width, .62,
                    boxstyle="round,pad=0,rounding_size=.08", facecolor=color, edgecolor="none",
                )
            )
        else:
            axis.scatter(0, position, s=155 if args.wide else 95, color=color, zorder=4)
        label = "best" if value == 1 else f"{value:,.0f}× worse"
        axis.text(width + maximum_width * .025, position, label, va="center", color=WHITE,
                  fontsize=19 if args.wide else 13, fontweight="bold")
    if args.wide:
        figure.subplots_adjust(left=.23, right=.94, bottom=.13, top=.71)
    else:
        figure.tight_layout()
    output = args.output_dir.expanduser().resolve()
    png, svg = save_figure(figure, output, basename, args.dpi, wide=args.wide)
    plt.close(figure)
    summary_path = output / f"{basename}_summary.json"
    write_json(summary_path, {
        "schema_version": 1,
        "chart": "full_path_cost_performance_clickhouse_vs_redshift",
        "layout": layout,
        "source": str(source),
        "rows": rows,
        "query_cost_display": "total",
        "score_scale": "log10 relative to ClickHouse",
        "baseline_bar_width": 0,
        "outputs": {"png": str(png), "svg": str(svg)},
    })
    for path in (png, svg, summary_path):
        print(f"Written: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
