#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9", "numpy==2.4.6"]
# ///
"""
Interactive-warehouse fallback latency composition (stacked bar).

Story: a dashboard-vs-MV query is issued to the INTERACTIVE warehouse; it exceeds the 5s
interactive statement timeout, is cancelled, and FALLS BACK to a STANDARD warehouse where it
actually completes. The user-experienced latency is therefore the 5s wasted on the interactive
timeout PLUS the standard-warehouse execution time — this chart stacks the two so the dead 5s is
visible rather than hidden behind a "1s" number.

  python3 render_fallback.py --standard _test/dash_mv_std_snowflake.jsonl \
      --interactive-success _test/dash_mv_iv_snowflake.jsonl \
      --timeout 5 --agg median --out _out/t2/fallback_latency.png

Style matches render_query_latency.py.
"""
import sys
import json
import glob
import argparse
import statistics as st

import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

import matplotlib.font_manager as _fm
matplotlib.rcParams["font.family"] = (
    "Inter" if any(f.name == "Inter" for f in _fm.fontManager.ttflist) else "DejaVu Sans"
)
matplotlib.rcParams["font.sans-serif"] = ["Inter", "DejaVu Sans"]
matplotlib.rcParams["axes.titleweight"] = "bold"

BACKGROUND_COLOR = "#2B2B2B"
GRID_COLOR = "#4A4A4A"
C_TIMEOUT = "#B34A4A"   # muted red — wasted time on the interactive timeout
C_STD     = "#29B5E8"   # Snowflake cyan — actual work on the standard (fallback) warehouse
C_IV_OK   = "#A259FF"   # Snowflake-IT purple — the interactive success case (when it completes)


def fmt_secs(y, _pos=None):
    if y <= 0:
        return "0"
    return f"{y:g}s" if y >= 1 else f"{y*1000:g}ms"


def lbl(v):
    return f"{v:.2f}s" if v >= 1 else f"{v*1000:.0f}ms"


def pooled(pat, keep_timeouts, timeout):
    """Pool per-query latencies across a glob/comma list. keep_timeouts: map 'timeout'->timeout
    (include failures) or drop them (successful-only)."""
    files = [f for part in pat.split(",") for f in glob.glob(part.strip())]
    vals = []
    for path in sorted(set(files)):
        for r in (json.loads(l) for l in open(path) if l.strip()):
            for x in r.get("result", []):
                if not x:
                    continue
                v = x[0]
                if v == "timeout":
                    if keep_timeouts:
                        vals.append(timeout)
                elif v is not None:
                    vals.append(v)
    return vals


def agg_of(vals, agg):
    if not vals:
        return 0.0
    if agg == "median":
        return st.median(vals)
    if agg == "mean":
        return st.mean(vals)
    return float(np.percentile(vals, float(agg[1:])))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--standard", required=True, help="dashboard-vs-MV JSONL run on the standard (fallback) wh")
    ap.add_argument("--interactive-success", help="dashboard-vs-MV JSONL run on the interactive wh (for the 'when it completes' context bar)")
    ap.add_argument("--timeout", type=float, default=5.0, help="interactive statement timeout, seconds")
    ap.add_argument("--agg", choices=["median", "mean", "p90", "p95", "p99"], default="median")
    ap.add_argument("-o", "--out")
    ap.add_argument("--dpi", type=int, default=300)
    ap.add_argument("--title", default=None)
    args = ap.parse_args()

    std_exec = agg_of(pooled(args.standard, keep_timeouts=False, timeout=args.timeout), args.agg)

    bars = []   # (label, [(segment_value, color, seg_label)], total_label)
    if args.interactive_success:
        iv_ok = agg_of(pooled(args.interactive_success, keep_timeouts=False, timeout=args.timeout), args.agg)
        bars.append(("Interactive WH\n(query completes)", [(iv_ok, C_IV_OK, None)], lbl(iv_ok)))
    bars.append(("Interactive WH times out\n→ fallback to Standard WH",
                 [(args.timeout, C_TIMEOUT, f"{args.timeout:g}s interactive timeout"),
                  (std_exec, C_STD, f"MV dashboard on standard: {lbl(std_exec)}")],
                 lbl(args.timeout + std_exec)))

    fig, ax = plt.subplots(figsize=(2.9 * len(bars) + 3.5, 6))
    fig.patch.set_facecolor(BACKGROUND_COLOR)
    ax.set_facecolor(BACKGROUND_COLOR)

    seen = {}
    for i, (label, segs, total) in enumerate(bars):
        bottom = 0.0
        for val, color, seg_label in segs:
            ax.bar(i, val, bottom=bottom, width=0.55, color=color,
                   edgecolor="black", linewidth=0.5, zorder=3,
                   label=seg_label if seg_label and seg_label not in seen else None)
            if seg_label:
                seen[seg_label] = True
            if val > 0:  # segment value label, centered in the segment
                ax.text(i, bottom + val / 2, lbl(val), ha="center", va="center",
                        color="white", fontsize=9, fontweight="bold", zorder=4)
            bottom += val
        ax.text(i, bottom * 1.02, "total " + total, ha="center", va="bottom",
                color="white", fontsize=11, fontweight="bold", zorder=4)

    ax.set_xticks(range(len(bars)))
    ax.set_xticklabels([b[0] for b in bars], color="white", fontsize=11)
    ax.yaxis.set_major_formatter(FuncFormatter(fmt_secs))
    ax.tick_params(colors="white", labelsize=10)
    ax.set_ylabel(f"End-to-end query latency ({args.agg})\n↓ lower is better", color="white", fontsize=11)
    ax.grid(True, axis="y", which="major", color=GRID_COLOR, lw=0.6, alpha=0.7, zorder=0)
    for side in ("right", "top"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color("white")
    top = max(sum(v for v, _, _ in segs) for _, segs, _ in bars)
    ax.set_ylim(top=top * 1.18)

    ax.legend(loc="upper left", facecolor=BACKGROUND_COLOR, edgecolor=GRID_COLOR,
              labelcolor="white", fontsize=9.5, framealpha=0.9)
    ax.set_title(args.title or "Dashboard-MV latency: interactive timeout + standard-warehouse fallback",
                 color="white", fontsize=13, pad=16)

    fig.tight_layout()
    if args.out:
        fig.savefig(args.out, dpi=args.dpi, facecolor=fig.get_facecolor())
        print(f"wrote {args.out}", file=sys.stderr)
    else:
        plt.show()


if __name__ == "__main__":
    main()
