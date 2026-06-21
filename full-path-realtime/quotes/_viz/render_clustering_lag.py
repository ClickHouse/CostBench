#!/usr/bin/env python3
"""
Quotes benchmark — Snowflake Automatic-Clustering lag over time (STOCKHOUSE_2 re-run).

Two stacked panels sharing an elapsed-time x-axis:
  top    — average clustering DEPTH of the raw table and MV over time
           (SYSTEM$CLUSTERING_INFORMATION, sampled by ops/clustering_lag.sh). Depth ~1 is
           ideal; higher = more overlapping partitions per key = worse pruning / read
           amplification. Under sustained ~1M EPS it ramps up monotonically — Automatic
           Clustering can't reduce it while ingest keeps re-disordering the (sym,t) key.
  bottom — Automatic-Clustering CREDITS per hour (ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY).
           A steady ~0.7 cr/hr during ingest, then a large catch-up spike once ingest stops.

This isn't a freshness lag in seconds — it's a layout-quality (read-amplification) signal.

  python3 render_clustering_lag.py _test/clustering_lag_snowflake.jsonl \
      --credits _test/clustering_credits_snowflake.csv --stop-hours 30.6 \
      --out _out/clustering_lag.png

Style matches the other renderers (dark theme, Inter fallback).
"""
import sys
import csv
import json
import argparse
from datetime import datetime

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as _fm
from matplotlib.ticker import FuncFormatter, NullFormatter

matplotlib.rcParams["font.family"] = (
    "Inter" if any(f.name == "Inter" for f in _fm.fontManager.ttflist) else "DejaVu Sans"
)
matplotlib.rcParams["axes.titleweight"] = "bold"

BACKGROUND_COLOR = "#2B2B2B"
GRID_COLOR = "#4A4A4A"
RAW_COLOR = "#29B5E8"   # Snowflake cyan — raw QUOTES
MV_COLOR = "#2EC4B6"    # teal — QUOTES_DAILY MV
STOP_COLOR = "#FF4B3A"  # ingest-stop marker


def parse_iso(s):
    return datetime.fromisoformat(str(s).replace("Z", "+00:00"))


def load_depth(path):
    """[(elapsed_h, raw_depth, mv_depth), ...] from the clustering_lag JSONL."""
    rows = [json.loads(l) for l in open(path) if l.strip()]
    t0 = parse_iso(rows[0]["polled_at"])
    out = []
    for r in rows:
        h = (parse_iso(r["polled_at"]) - t0).total_seconds() / 3600.0
        out.append((h, r.get("raw_avg_depth"), r.get("mv_avg_depth")))
    return out, t0


def load_credits(path, t0):
    """{table: [(elapsed_h_of_hour_start, credits), ...]} from the AC-history CSV."""
    by = {}
    for r in csv.DictReader(open(path)):
        t = datetime.strptime(r["HR"], "%Y-%m-%d %H:%M:%S.%f %z")
        h = (t - t0).total_seconds() / 3600.0
        by.setdefault(r["TABLE_NAME"], []).append((h, float(r["CREDITS"] or 0)))
    for v in by.values():
        v.sort()
    return by


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("depth", help="clustering_lag JSONL (polled_at + raw/mv_avg_depth).")
    ap.add_argument("--credits", help="AUTOMATIC_CLUSTERING_HISTORY CSV (TABLE_NAME,HR,CREDITS,...).")
    ap.add_argument("--stop-hours", type=float, default=None,
                    help="Elapsed hours at which ingest was stopped (draws a marker).")
    ap.add_argument("--credits-scale", choices=["log", "linear"], default="log",
                    help="Y-axis scale for the credits/hr panel (default log). Linear makes the "
                         "post-stop catch-up spike dominate; steady bars look tiny.")
    ap.add_argument("--depth-scale", choices=["log", "linear"], default="log",
                    help="Y-axis scale for the depth panel (default log).")
    ap.add_argument("--raw-only", action="store_true",
                    help="Plot only the raw QUOTES table (drop the MV series from both panels) "
                         "— useful with linear depth, where the MV's small values hug zero.")
    ap.add_argument("-o", "--out", help="Output PNG.")
    ap.add_argument("--dpi", type=int, default=300)
    ap.add_argument("--title", default=None)
    ap.add_argument("--no-title", action="store_true")
    args = ap.parse_args()

    depth, t0 = load_depth(args.depth)
    credits = load_credits(args.credits, t0) if args.credits else {}

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 7), sharex=True,
                                   gridspec_kw={"height_ratios": [2, 1]})
    fig.patch.set_facecolor(BACKGROUND_COLOR)
    for ax in (ax1, ax2):
        ax.set_facecolor(BACKGROUND_COLOR)
        ax.grid(True, which="major", color=GRID_COLOR, lw=0.6, alpha=0.7, zorder=0)
        for side in ("right", "top"):
            ax.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            ax.spines[side].set_color("white")
        ax.tick_params(colors="white", labelsize=10)

    # --- top: clustering depth ---
    series_defs = [(1, RAW_COLOR, "raw QUOTES (sym, t)")]
    if not args.raw_only:
        series_defs.append((2, MV_COLOR, "MV QUOTES_DAILY (sym, day)"))
    for key, color, label in series_defs:
        # log can't show <=0; linear can include the t=0 origin
        pts = [(d[0], d[key]) for d in depth if d[key] is not None
               and (args.depth_scale == "linear" or d[key] > 0)]
        if pts:
            ax1.plot([p[0] for p in pts], [p[1] for p in pts], lw=2.2, color=color, label=label)
    ax1.set_yscale(args.depth_scale)
    ax1.yaxis.set_major_formatter(FuncFormatter(lambda y, _p: f"{y:g}"))
    ax1.yaxis.set_minor_formatter(NullFormatter())
    ax1.set_ylabel(f"avg clustering depth ({args.depth_scale})\n↓ lower is better (1 = ideal)",
                   color="white", fontsize=11)
    ax1.legend(loc="upper left", facecolor=BACKGROUND_COLOR, edgecolor=GRID_COLOR,
               labelcolor="white", fontsize=10, framealpha=0.9)

    # --- bottom: AC credits per hour (log y; bars per hour) ---
    cmap = {"QUOTES": (RAW_COLOR, "raw QUOTES"), "QUOTES_DAILY": (MV_COLOR, "MV QUOTES_DAILY")}
    for tbl, series in credits.items():
        if args.raw_only and tbl != "QUOTES":
            continue
        color, label = cmap.get(tbl, ("#FFFFFF", tbl))
        xs = [h + 0.5 for h, _ in series]   # center bar in its hour
        ys = [c for _, c in series]
        ax2.bar(xs, ys, width=0.9, color=color, edgecolor="black", linewidth=0.3,
                label=label, zorder=3)
    if credits:
        ax2.set_yscale(args.credits_scale)
        ax2.yaxis.set_major_formatter(FuncFormatter(lambda y, _p: f"{y:g}"))
        ax2.legend(loc="upper left", facecolor=BACKGROUND_COLOR, edgecolor=GRID_COLOR,
                   labelcolor="white", fontsize=9.5, framealpha=0.9)
    ax2.set_ylabel(f"Auto-Clustering\ncredits / hr ({args.credits_scale})",
                   color="white", fontsize=11)
    ax2.set_xlabel("Elapsed ingest time (hours)", color="white", fontsize=11)
    ax2.set_xlim(left=0)

    # --- ingest-stop marker on both panels ---
    if args.stop_hours is not None:
        for ax in (ax1, ax2):
            ax.axvline(args.stop_hours, color=STOP_COLOR, lw=1.4, ls="--", zorder=2)
        ax1.text(args.stop_hours, ax1.get_ylim()[1], " ingest stop", color=STOP_COLOR,
                 fontsize=9, va="top", ha="left")

    if not args.no_title:
        fig.suptitle(args.title or "Snowflake Automatic-Clustering lag under ~1M EPS ingest",
                     color="white", fontsize=14, fontweight="bold", y=0.98)
    fig.tight_layout(rect=(0, 0, 1, 0.96 if not args.no_title else 1.0))

    if args.out:
        fig.savefig(args.out, dpi=args.dpi, facecolor=fig.get_facecolor())
        print(f"wrote {args.out}", file=sys.stderr)
    else:
        plt.show()


if __name__ == "__main__":
    main()
