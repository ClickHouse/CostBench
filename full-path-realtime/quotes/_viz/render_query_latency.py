#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9", "numpy==2.4.6"]
# ///
"""
Quotes benchmark — query-latency summary (grouped bars).

One representative server-side query latency per system, per workload (dashboard / drilldown),
as grouped bars on a log y-axis. The representative value is the median of all individual
query latencies in that workload over the run (robust to the full-scan tail; use --agg mean
to switch). This is the headline "how fast" comparison; cost is a separate chart.

  python3 render_query_latency.py \
      --workload "Dashboard=_test/dashboard_*.jsonl" \
      --workload "Drilldown=_test/drilldown_*.jsonl" \
      --out _out/query_latency.png

Style matches render_latency.py / ../_viz2.
"""
import sys
import json
import glob
import argparse
import statistics as st

import numpy as np

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, NullFormatter

import matplotlib.font_manager as _fm
# Use Inter when installed, else fall back to DejaVu Sans (avoids noisy
# "findfont: Font family 'Inter' not found" warnings on every text element).
matplotlib.rcParams["font.family"] = (
    "Inter" if any(f.name == "Inter" for f in _fm.fontManager.ttflist) else "DejaVu Sans"
)
matplotlib.rcParams["font.sans-serif"] = ["Inter", "DejaVu Sans"]
matplotlib.rcParams["axes.titleweight"] = "bold"

VENDOR_COLOR = {
    "ClickHouse":   "#FDFF88",
    "Redshift":     "#FFB30A",
    "Databricks":   "#FF4B3A",
    "Snowflake IT": "#A259FF",  # interactive-tables variant (purple, distinct from MV cyan)
    "Snowflake":    "#29B5E8",
    "BigQuery":     "#4285F4",
}
VENDOR_ORDER = ["ClickHouse", "Snowflake", "Snowflake IT", "Databricks", "Redshift", "BigQuery"]
BACKGROUND_COLOR = "#2B2B2B"
GRID_COLOR = "#4A4A4A"


def vendor_of(system: str) -> str:
    s = (system or "").lower()
    # longest name first so "Snowflake IT" wins over the "Snowflake" substring
    for name in sorted(VENDOR_COLOR, key=len, reverse=True):
        if name.lower() in s:
            return name
    return system or "unknown"


def tier_of(rec):
    machine = str(rec.get("machine") or "").strip()
    cs = rec.get("cluster_size")
    if machine and isinstance(cs, int):
        return f"{machine} ×{cs}"
    return machine or str(cs or "").strip()


def fmt_secs(y, _pos=None):
    if y <= 0:
        return "0"
    if y >= 1:
        return f"{y:g}s"
    return f"{y*1000:g}ms"


def bar_label(v):
    if v >= 1:
        return f"{v:.2f}s"
    return f"{v*1000:.0f}ms"


def workload_stats(glob_pat, agg):
    """{vendor: (value, tier)} for one workload, value = agg of pooled per-query latencies.

    glob_pat may be a single glob or a comma-separated list of globs/paths (so callers can
    select specific vendors, e.g. 'dashboard_clickhouse.jsonl,dashboard_snowflake.jsonl').
    """
    files = []
    for part in glob_pat.split(","):
        files.extend(glob.glob(part.strip()))
    out = {}
    for path in sorted(set(files)):
        rows = [json.loads(l) for l in open(path) if l.strip()]
        if not rows:
            continue
        v = vendor_of(rows[0].get("system", ""))
        # "timeout" (interactive 5s cap) -> 5.0s (lower bound) so it counts in the stat
        pooled = [(5.0 if x[0] == "timeout" else x[0]) for r in rows for x in r.get("result", [])
                  if x and x[0] is not None]
        if not pooled:
            continue
        if agg == "median":
            val = st.median(pooled)
        elif agg == "mean":
            val = st.mean(pooled)
        else:  # pXX percentile, e.g. p99
            val = float(np.percentile(pooled, float(agg[1:])))
        out[v] = (val, tier_of(rows[0]))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workload", action="append", required=True, metavar="LABEL=GLOB",
                    help="Repeatable. e.g. --workload 'Dashboard=_test/dashboard_*.jsonl'")
    ap.add_argument("--agg", choices=["median", "mean", "p90", "p95", "p99"],
                    default="median")
    ap.add_argument("-o", "--out", help="Output PNG.")
    ap.add_argument("--dpi", type=int, default=300)
    ap.add_argument("--title", default=None)
    ap.add_argument("--no-title", action="store_true")
    args = ap.parse_args()

    workloads = []
    for spec in args.workload:
        if "=" not in spec:
            sys.exit(f"--workload must be LABEL=GLOB, got: {spec}")
        label, pat = spec.split("=", 1)
        workloads.append((label, workload_stats(pat, args.agg)))

    # systems present, in stable order
    present = set().union(*[set(s) for _, s in workloads])
    systems = [v for v in VENDOR_ORDER if v in present] + \
              [v for v in present if v not in VENDOR_ORDER]
    tiers = {}
    for _, stats in workloads:
        for v, (_, tier) in stats.items():
            tiers.setdefault(v, tier)

    fig, ax = plt.subplots(figsize=(2.6 * len(workloads) + 4, 5.5))
    fig.patch.set_facecolor(BACKGROUND_COLOR)
    ax.set_facecolor(BACKGROUND_COLOR)

    n = len(systems)
    group_w = 0.8
    bar_w = group_w / n
    handles_for_legend = {}

    for gi, (label, stats) in enumerate(workloads):
        for si, v in enumerate(systems):
            if v not in stats:
                continue
            val = stats[v][0]
            x = gi + (si - (n - 1) / 2) * bar_w
            b = ax.bar(x, val, width=bar_w * 0.92, color=VENDOR_COLOR.get(v, "#FFF"),
                       edgecolor="black", linewidth=0.5, zorder=3)
            handles_for_legend.setdefault(v, b)
            ax.text(x, val * 1.06, bar_label(val), ha="center", va="bottom",
                    color="white", fontsize=8.5, zorder=4)

    ax.set_yscale("log")
    ax.set_xticks(range(len(workloads)))
    ax.set_xticklabels([w[0] for w in workloads], color="white", fontsize=12)
    ax.yaxis.set_major_formatter(FuncFormatter(fmt_secs))
    ax.yaxis.set_minor_formatter(NullFormatter())
    ax.tick_params(colors="white", labelsize=10)
    ax.set_ylabel(f"{args.agg.title()} query latency (log)\n↓ lower is better",
                  color="white", fontsize=11)
    ax.grid(True, axis="y", which="major", color=GRID_COLOR, lw=0.6, alpha=0.7, zorder=0)
    for side in ("right", "top"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color("white")
    # headroom for the top labels on a log axis
    top = max(val for _, s in workloads for (val, _) in s.values())
    ax.set_ylim(top=top * 1.6)

    # legend as a horizontal row above the plot, so it never collides with tall bars/labels
    legend_labels = [f"{v} · {tiers[v]}" if tiers.get(v) else v for v in systems]
    leg_h = [handles_for_legend[v] for v in systems if v in handles_for_legend]
    leg_l = [lbl for v, lbl in zip(systems, legend_labels) if v in handles_for_legend]
    ax.legend(leg_h, leg_l, loc="lower center", bbox_to_anchor=(0.5, 1.005),
              ncol=len(leg_h), facecolor=BACKGROUND_COLOR, edgecolor=GRID_COLOR,
              labelcolor="white", fontsize=9.5, framealpha=0.9)

    if not args.no_title:
        ax.set_title(args.title or "Query latency by workload", color="white",
                     fontsize=14, pad=34)

    fig.tight_layout()
    if args.out:
        fig.savefig(args.out, dpi=args.dpi, facecolor=fig.get_facecolor())
        print(f"wrote {args.out}", file=sys.stderr)
    else:
        plt.show()


if __name__ == "__main__":
    main()
