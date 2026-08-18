#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Render ClickHouse versus Redshift complete-ingest fresh-data-path cost."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

from _layout import (
    CLICKHOUSE, MUTED, REDSHIFT, WHITE, configure_figure, load_json, money,
    resolve_layout, save_figure, tier_cost, write_json,
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256(path.read_bytes())
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clickhouse", type=Path, required=True)
    parser.add_argument("--redshift", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--basename", default="fresh_data_path_cost_clickhouse_vs_redshift")
    parser.add_argument("--wide", action="store_true")
    parser.add_argument("--dpi", type=int, default=300)
    args = parser.parse_args()
    ch_path = args.clickhouse.expanduser().resolve()
    rs_path = args.redshift.expanduser().resolve()
    ch = load_json(ch_path)
    rs = load_json(rs_path)
    ch_total = tier_cost(ch, "Enterprise")
    rs_total = float(rs["total_cost_usd"])
    writer = float(rs["components"]["writer_workgroup"]["cost_usd"])
    msk = float(rs["components"]["msk"]["total_cost_usd"])
    if abs(rs_total - writer - msk) > 1e-7:
        raise ValueError("Redshift fresh-path components do not sum to total")
    if rs["shared_path_contract"]["shared_between_read_variants"] is not True:
        raise ValueError("Redshift fresh path must be shared between read variants")

    basename, size, layout = resolve_layout(args.basename, args.wide, (11.5, 5.6), args.dpi)
    figure, axis = plt.subplots(figsize=size, dpi=args.dpi if args.wide else None)
    background = configure_figure(figure, wide=args.wide)
    axis.set_facecolor(background)
    axis.set_xlim(0, max(ch_total, rs_total) * 1.18)
    axis.set_ylim(-.65, 1.65)
    axis.axis("off")
    positions = (1, 0)
    values = (ch_total, rs_total)
    colors = (CLICKHOUSE, REDSHIFT)
    labels = ("ClickHouse", "Redshift Serverless")
    height = .55
    for position, value, color, label in zip(positions, values, colors, labels, strict=True):
        axis.add_patch(
            FancyBboxPatch(
                (0, position - height / 2), value, height,
                boxstyle="round,pad=0,rounding_size=.09", facecolor=color, edgecolor="none",
            )
        )
        axis.text(-max(values) * .025, position, label, ha="right", va="center", color=color,
                  fontsize=21 if args.wide else 15, fontweight="bold")
        axis.text(value + max(values) * .018, position, money(value), ha="left", va="center",
                  color=WHITE, fontsize=20 if args.wide else 14, fontweight="bold")
    axis.text(
        rs_total, -.44, f"Writer {money(writer)} · MSK {money(msk)}",
        ha="right", va="top", color=MUTED, fontsize=13 if args.wide else 9.5,
        parse_math=False,
    )
    if args.wide:
        figure.subplots_adjust(left=.22, right=.94, bottom=.15, top=.71)
    else:
        figure.tight_layout()
    output = args.output_dir.expanduser().resolve()
    png, svg = save_figure(figure, output, basename, args.dpi, wide=args.wide)
    plt.close(figure)
    summary = {
        "schema_version": 1,
        "chart": "complete_ingest_fresh_data_path_cost_clickhouse_vs_redshift",
        "layout": layout,
        "costs": {
            "clickhouse": {"total_usd": ch_total},
            "redshift": {"writer_usd": writer, "msk_usd": msk, "total_usd": rs_total},
        },
        "contract": {
            "redshift_path_shared_between_variants": True,
            "client_cross_az_included": False,
            "redshift_managed_storage_included": False,
        },
        "sources": {
            "clickhouse": {"path": str(ch_path), "sha256": sha256(ch_path)},
            "redshift": {"path": str(rs_path), "sha256": sha256(rs_path)},
        },
        "outputs": {"png": str(png), "svg": str(svg)},
    }
    summary_path = output / f"{basename}_summary.json"
    write_json(summary_path, summary)
    for path in (png, svg, summary_path):
        print(f"Written: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
