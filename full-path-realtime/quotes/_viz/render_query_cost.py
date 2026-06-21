#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""
Quotes benchmark — query cost summary (grouped bars).

Total compute cost to run each query workload (dashboard / drilldown) over the benchmark,
one bar per system, on a log y-axis. Cost comes from the per-system cost JSONs
(`<vendor>/costs/{dashboard,drilldown}.json`, staged into `_test/cost_*.json`), which carry
`total_compute_cost_usd` per pricing tier. `--tier` selects the tier (default "enterprise");
a system without that tier falls back to its only/cheapest entry, and each bar is labelled with
the tier actually used.

  python3 render_query_cost.py \
      --workload "Dashboard (vs MV)=_test/cost_dashboard_*.json" \
      --workload "Drilldown (vs raw)=_test/cost_drilldown_*.json" \
      --tier enterprise --out _out/query_cost.png

Style matches render_query_latency.py / ../_viz2.
"""
import sys
import json
import glob
import argparse

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
    "ClickHouse": "#FDFF88",
    "Redshift":   "#FFB30A",
    "Databricks": "#FF4B3A",
    "Snowflake":  "#29B5E8",
    "BigQuery":   "#4285F4",
}
VENDOR_ORDER = ["ClickHouse", "Snowflake", "Databricks", "Redshift", "BigQuery"]
BACKGROUND_COLOR = "#2B2B2B"
GRID_COLOR = "#4A4A4A"


def vendor_of(system: str) -> str:
    s = (system or "").lower()
    for name in VENDOR_COLOR:
        if name.lower() in s:
            return name
    return system or "unknown"


def hw_tier(d):
    """Hardware tier label for the legend."""
    machine = str(d.get("machine") or d.get("warehouse_size") or "").strip()
    cs = d.get("cluster_size")
    if machine and isinstance(cs, int):
        return f"{machine} ×{cs}"
    return machine


def pick_cost(d, tier):
    """(usd, tier_name) — the cost entry whose tier matches `tier` (case-insensitive
    substring); else the cheapest entry."""
    costs = d.get("costs", [])
    if not costs:
        return None
    for c in costs:
        if tier.lower() in str(c.get("tier", "")).lower():
            return c["total_compute_cost_usd"], c.get("tier")
    c = min(costs, key=lambda c: c["total_compute_cost_usd"])
    return c["total_compute_cost_usd"], c.get("tier")


def money(v):
    if v >= 1:
        return f"${v:,.2f}"
    if v >= 0.01:
        return f"${v:.3f}"
    return f"${v:.4f}"


def fmt_dollars(y, _pos=None):
    if y >= 1:
        return f"${y:g}"
    if y >= 0.01:
        return f"${y:.2f}"
    return f"${y:g}"


def workload_costs(glob_pat, tier):
    """{vendor: (usd, pricing_tier, hw_tier)} for one workload."""
    files = []
    for part in glob_pat.split(","):
        files.extend(glob.glob(part.strip()))
    out = {}
    for path in sorted(set(files)):
        d = json.load(open(path))
        v = vendor_of(d.get("system", ""))
        picked = pick_cost(d, tier)
        if picked is None:
            continue
        out[v] = (picked[0], picked[1], hw_tier(d))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workload", action="append", required=True, metavar="LABEL=GLOB",
                    help="Repeatable. e.g. --workload 'Dashboard=_test/cost_dashboard_*.json'")
    ap.add_argument("--tier", default="enterprise",
                    help="Pricing tier to use (case-insensitive substring; default enterprise). "
                         "Systems lacking it fall back to their cheapest tier.")
    ap.add_argument("-o", "--out", help="Output PNG.")
    ap.add_argument("--dpi", type=int, default=300)
    ap.add_argument("--title", default=None)
    ap.add_argument("--no-title", action="store_true")
    ap.add_argument("--yscale", choices=["log", "linear"], default="log",
                    help="Y (cost) axis scale. Linear exaggerates the magnitude gap.")
    args = ap.parse_args()

    workloads = []
    for spec in args.workload:
        if "=" not in spec:
            sys.exit(f"--workload must be LABEL=GLOB, got: {spec}")
        label, pat = spec.split("=", 1)
        workloads.append((label, workload_costs(pat, args.tier)))

    present = set().union(*[set(s) for _, s in workloads]) if workloads else set()
    systems = [v for v in VENDOR_ORDER if v in present] + \
              [v for v in present if v not in VENDOR_ORDER]
    hw = {}
    for _, stats in workloads:
        for v, (_, _, h) in stats.items():
            hw.setdefault(v, h)

    fig, ax = plt.subplots(figsize=(2.6 * len(workloads) + 4, 5.5))
    fig.patch.set_facecolor(BACKGROUND_COLOR)
    ax.set_facecolor(BACKGROUND_COLOR)

    n = len(systems)
    bar_w = 0.8 / n
    handles = {}
    top = max(c for _, s in workloads for (c, _, _) in s.values())

    for gi, (label, stats) in enumerate(workloads):
        for si, v in enumerate(systems):
            if v not in stats:
                continue
            usd, ptier, _ = stats[v]
            x = gi + (si - (n - 1) / 2) * bar_w
            b = ax.bar(x, usd, width=bar_w * 0.92, color=VENDOR_COLOR.get(v, "#FFF"),
                       edgecolor="black", linewidth=0.5, zorder=3)
            handles.setdefault(v, b)
            ax.text(x, usd * 1.08, f"{money(usd)}\n{ptier}", ha="center", va="bottom",
                    color="white", fontsize=8, linespacing=1.25, zorder=4)

    ax.set_yscale(args.yscale)
    ax.set_xticks(range(len(workloads)))
    ax.set_xticklabels([w[0] for w in workloads], color="white", fontsize=12)
    ax.yaxis.set_major_formatter(FuncFormatter(fmt_dollars))
    ax.yaxis.set_minor_formatter(NullFormatter())
    ax.tick_params(colors="white", labelsize=10)
    ax.set_ylabel(f"Compute cost for the workload (USD, {args.yscale})\n↓ lower is better",
                  color="white", fontsize=11)
    ax.grid(True, axis="y", which="major", color=GRID_COLOR, lw=0.6, alpha=0.7, zorder=0)
    for side in ("right", "top"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color("white")
    # headroom for the 2-line labels (more on log, which compresses the top)
    ax.set_ylim(top=top * (3.0 if args.yscale == "log" else 1.22))

    leg_h = [handles[v] for v in systems if v in handles]
    leg_l = [f"{v} · {hw[v]}" if hw.get(v) else v for v in systems if v in handles]
    ax.legend(leg_h, leg_l, loc="lower center", bbox_to_anchor=(0.5, 1.005),
              ncol=len(leg_h), facecolor=BACKGROUND_COLOR, edgecolor=GRID_COLOR,
              labelcolor="white", fontsize=9.5, framealpha=0.9)

    if not args.no_title:
        ax.set_title(args.title or "Query compute cost by workload",
                     color="white", fontsize=14, pad=34)

    fig.tight_layout()
    if args.out:
        fig.savefig(args.out, dpi=args.dpi, facecolor=fig.get_facecolor())
        print(f"wrote {args.out}", file=sys.stderr)
    else:
        plt.show()


if __name__ == "__main__":
    main()
