#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib"]
# ///
"""
Quotes benchmark — cumulative cost over time, split by component.

One panel per system (small multiples), stacked cumulative compute cost ($) vs elapsed hours
from the benchmark start. Layers: Ingest (bottom) → Clustering → MV refresh. The ingest layer
plateaus once its billed window ends, while clustering and MV refresh keep climbing — that tail
is the extra cost of getting the data query-ready as clustering/refresh lag behind the load.
ClickHouse sorts + rolls up at ingest, so it has only the ingest layer (no tail).

Reads the normalized `_test/cost_timeline_<vendor>.json` produced by build_cost_timeline.py.

  python3 render_cost_over_time.py _test/cost_timeline_*.json --out _out/cost_over_time.png

Style matches render_ingest_cost.py / ../_viz2.
"""
import sys
import json
import glob
import argparse

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

import matplotlib.font_manager as _fm
# Use Inter when installed, else fall back to DejaVu Sans (avoids noisy
# "findfont: Font family 'Inter' not found" warnings on every text element).
matplotlib.rcParams["font.family"] = (
    "Inter" if any(f.name == "Inter" for f in _fm.fontManager.ttflist) else "DejaVu Sans"
)
matplotlib.rcParams["font.sans-serif"] = ["Inter", "DejaVu Sans"]
matplotlib.rcParams["axes.titleweight"] = "bold"

VENDOR_ORDER = ["ClickHouse", "Snowflake", "Databricks", "Redshift", "BigQuery"]
# component colours — match render_ingest_cost.py
COMP_COLOR = {"Ingest": "#4E79A7", "Clustering": "#F28E2B", "MV refresh": "#59A14F"}
COMP_ORDER = ["Ingest", "Clustering", "MV refresh"]
# a system that does all prep at ingest (ClickHouse) draws its single layer in ClickHouse yellow
EVERYTHING_COLOR = "#FDFF88"
BACKGROUND_COLOR = "#2B2B2B"
GRID_COLOR = "#4A4A4A"


def cum_ingest(comp, xs):
    rate = comp["total_usd"] / comp["hours"]
    return [rate * min(x, comp["hours"]) for x in xs]


def cum_events(comp, xs):
    ev = comp["events"]
    out, acc, i = [], 0.0, 0
    for x in xs:
        while i < len(ev) and ev[i][0] <= x:
            acc += ev[i][1]
            i += 1
        out.append(acc)
    return out


def vendor_key(name):
    return VENDOR_ORDER.index(name) if name in VENDOR_ORDER else 99


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("files", nargs="+", help="cost_timeline_<vendor>.json file(s).")
    ap.add_argument("-o", "--out", help="Output PNG.")
    ap.add_argument("--dpi", type=int, default=300)
    ap.add_argument("--title", default=None)
    ap.add_argument("--no-title", action="store_true")
    args = ap.parse_args()

    timelines = []
    for path in sorted(set(f for p in args.files for f in glob.glob(p))):
        timelines.append(json.load(open(path)))
    if not timelines:
        sys.exit("No cost_timeline_*.json found.")
    timelines.sort(key=lambda t: vendor_key(t["system"]))

    xmax = max(t.get("max_elapsed_h", 27) for t in timelines)
    # global y max = largest stacked total across vendors
    def total(t):
        c = t["components"]
        s = c["Ingest"]["total_usd"]
        for n in ("Clustering", "MV refresh"):
            if n in c:
                s += sum(u for _, u in c[n]["events"])
        return s
    ymax = max(total(t) for t in timelines)

    n = len(timelines)
    fig, axes = plt.subplots(1, n, figsize=(4.7 * n, 5.2), squeeze=False, sharey=True)
    axes = axes[0]
    fig.patch.set_facecolor(BACKGROUND_COLOR)

    grid = [i * xmax / 400 for i in range(401)]   # smooth shared x grid

    present_comps = []
    had_everything = False
    for ax, t in zip(axes, timelines):
        ax.set_facecolor(BACKGROUND_COLOR)
        comps = t["components"]
        # a system with only the ingest layer does ALL prep at ingest -> "everything", in yellow
        everything_only = not any(k in comps for k in ("Clustering", "MV refresh"))
        had_everything = had_everything or everything_only
        # cumulative per component on the shared grid, in stack order
        layers = []
        for name in COMP_ORDER:
            if name not in comps:
                continue
            c = comps[name]
            cum = cum_ingest(c, grid) if c["type"] == "flat" else cum_events(c, grid)
            layers.append((name, cum))
            if name not in present_comps:
                present_comps.append(name)
        bottom = [0.0] * len(grid)
        for name, cum in layers:
            top = [b + v for b, v in zip(bottom, cum)]
            color = EVERYTHING_COLOR if (name == "Ingest" and everything_only) else COMP_COLOR[name]
            ax.fill_between(grid, bottom, top, color=color, zorder=3, linewidth=0)
            bottom = top
        grand = bottom[-1]
        # ingest-window marker (billed) — the plateau of the ingest layer
        ih = comps["Ingest"]["hours"]
        ax.axvline(ih, color="white", lw=0.8, ls=(0, (4, 3)), alpha=0.5, zorder=4)
        ax.text(ih, ymax * 1.02, f"ingest billed {ih:g}h", color="white", fontsize=7.5,
                ha="center", va="bottom", alpha=0.7)
        ax.set_title(f"{t['system']}  ·  ${grand:,.0f}", color="white", fontsize=12, pad=16)
        ax.set_xlim(0, xmax)
        ax.set_ylim(0, ymax * 1.08)
        ax.set_xlabel("Elapsed time since ingest start (h)", color="white", fontsize=10)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _p: f"${y:,.0f}"))
        ax.tick_params(colors="white", labelsize=9)
        ax.grid(True, axis="y", color=GRID_COLOR, lw=0.6, alpha=0.6, zorder=0)
        for side in ("right", "top"):
            ax.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            ax.spines[side].set_color("white")
    axes[0].set_ylabel("Cumulative compute cost (USD)\n↓ lower is better",
                       color="white", fontsize=11)

    leg = [(COMP_COLOR[c], c) for c in present_comps]
    if had_everything:
        leg.append((EVERYTHING_COLOR, "Everything (ingest + sort + MV)"))
    handles = [plt.Rectangle((0, 0), 1, 1, color=col) for col, _ in leg]
    fig.legend(handles, [lbl for _, lbl in leg], loc="upper center", ncol=len(leg),
               facecolor=BACKGROUND_COLOR, edgecolor=GRID_COLOR, labelcolor="white",
               fontsize=10, framealpha=0.9, bbox_to_anchor=(0.5, 0.965))

    if not args.no_title:
        fig.suptitle(args.title or "Cumulative cost over time, by component",
                     color="white", fontsize=14, fontweight="bold", y=0.995)
    fig.tight_layout(rect=(0, 0, 1, 0.90))

    if args.out:
        fig.savefig(args.out, dpi=args.dpi, facecolor=fig.get_facecolor())
        print(f"wrote {args.out}", file=sys.stderr)
    else:
        plt.show()


if __name__ == "__main__":
    main()
