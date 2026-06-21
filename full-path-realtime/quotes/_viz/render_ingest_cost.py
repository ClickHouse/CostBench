#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""
Quotes benchmark — ingest cost composition (stacked bars).

Total compute cost to ingest ~100B rows with the rollup attached, broken into components,
one stacked bar per system. ClickHouse does its sort and rollup *at ingest*, so it has a
single component (ingest) and no separate clustering or MV-refresh cost. Snowflake and
Databricks bill separately for automatic clustering (raw table + MV) and MV refresh on top of
ingest — so their bars stack those components.

Linear y-axis (stacked sums must read additively). Component cost comes from the per-system
cost JSONs (`<vendor>/costs/{ingest,clustering_raw_table,clustering_mv_table,mv_refresh}.json`,
staged into `_test/cost_<component>_<vendor>.json`); each carries a per-tier cost under
`total_compute_cost_usd` or `total_cost_usd`. `--tier` selects the pricing tier (default
enterprise); a system lacking it falls back to its only/cheapest entry.

  python3 render_ingest_cost.py \
      --component "Ingest=_test/cost_ingest_*.json" \
      --component "Clustering (raw)=_test/cost_clustering_raw_*.json" \
      --component "Clustering (MV)=_test/cost_clustering_mv_*.json" \
      --component "MV refresh=_test/cost_mv_refresh_*.json" \
      --tier enterprise --out _out/ingest_cost.png

Style matches render_query_cost.py / ../_viz2.
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

VENDOR_COLOR = {
    "ClickHouse": "#FDFF88", "Redshift": "#FFB30A", "Databricks": "#FF4B3A",
    "Snowflake": "#29B5E8", "BigQuery": "#4285F4",
}
VENDOR_ORDER = ["ClickHouse", "Snowflake", "Databricks", "Redshift", "BigQuery"]
# component colours by name — identical to render_cost_over_time.py
COMP_COLOR = {"Ingest": "#4E79A7", "Clustering": "#F28E2B", "MV refresh": "#59A14F"}
_EXTRA = ["#B07AA1", "#76B7B2", "#EDC948"]  # fallback for any other component label
# A system that does all prep at ingest (ClickHouse) gets a single bar in ClickHouse yellow,
# labelled "Everything", instead of sharing the blue Ingest colour of the others.
EVERYTHING_COLOR = "#FDFF88"


def comp_color(label, idx):
    return COMP_COLOR.get(label, _EXTRA[idx % len(_EXTRA)])
BACKGROUND_COLOR = "#2B2B2B"
GRID_COLOR = "#4A4A4A"


def vendor_of(system: str) -> str:
    s = (system or "").lower()
    for name in VENDOR_COLOR:
        if name.lower() in s:
            return name
    return system or "unknown"


def hw_tier(d):
    """Cluster config / warehouse size for the x-axis label. Snowflake/Databricks expose a
    warehouse_size; ClickHouse exposes nodes × mem_gib_per_node."""
    m = d.get("machine") or d.get("warehouse_size")
    if m:
        return str(m).strip()
    if d.get("nodes") and d.get("mem_gib_per_node"):
        return f"{int(d['nodes'])} × {int(d['mem_gib_per_node'])} GiB"
    return ""


def pick_cost(d, tier):
    """(usd, tier_name) for the matching pricing tier (case-insensitive substring); else the
    cheapest. Handles both `total_compute_cost_usd` (ingest) and `total_cost_usd` (clustering /
    refresh)."""
    costs = d.get("costs", [])
    if not costs:
        return None

    def usd(c):
        return c.get("total_compute_cost_usd", c.get("total_cost_usd", 0.0))

    for c in costs:
        if tier.lower() in str(c.get("tier", "")).lower():
            return usd(c), c.get("tier")
    c = min(costs, key=usd)
    return usd(c), c.get("tier")


def money(v):
    if v >= 1:
        return f"${v:,.0f}"
    if v >= 0.01:
        return f"${v:.2f}"
    return f"${v:.4f}"


def component_costs(glob_pat, tier):
    """{vendor: (usd, pricing_tier, hw_tier)} for one component across systems."""
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
        if v in out:  # multiple files for one vendor (e.g. clustering raw + MV) -> sum cost
            out[v] = (out[v][0] + picked[0], out[v][1], out[v][2])
        else:
            out[v] = (picked[0], picked[1], hw_tier(d))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--component", action="append", required=True, metavar="LABEL=GLOB",
                    help="Repeatable, stacked bottom→top. e.g. "
                         "--component 'Ingest=_test/cost_ingest_*.json'")
    ap.add_argument("--tier", default="enterprise",
                    help="Pricing tier (case-insensitive substring; default enterprise). "
                         "Systems lacking it fall back to their cheapest tier.")
    ap.add_argument("-o", "--out", help="Output PNG.")
    ap.add_argument("--dpi", type=int, default=300)
    ap.add_argument("--title", default=None)
    ap.add_argument("--no-title", action="store_true")
    args = ap.parse_args()

    components = []  # [(label, {vendor: (usd, ptier, hw)})]
    for spec in args.component:
        if "=" not in spec:
            sys.exit(f"--component must be LABEL=GLOB, got: {spec}")
        label, pat = spec.split("=", 1)
        components.append((label, component_costs(pat, args.tier)))

    present = set().union(*[set(c) for _, c in components]) if components else set()
    systems = [v for v in VENDOR_ORDER if v in present] + \
              [v for v in present if v not in VENDOR_ORDER]
    if not systems:
        sys.exit("No cost data found for any system.")

    hw = {}
    ptier_used = {}
    for _, comp in components:
        for v, (_, pt, h) in comp.items():
            hw.setdefault(v, h)
            ptier_used.setdefault(v, pt)

    fig, ax = plt.subplots(figsize=(1.7 * len(systems) + 3.5, 6))
    fig.patch.set_facecolor(BACKGROUND_COLOR)
    ax.set_facecolor(BACKGROUND_COLOR)

    # Vendors that only have the "Ingest" component (no separate clustering / MV refresh) do
    # ALL the prep at ingest — their single bar is "everything", drawn in ClickHouse yellow.
    has_extra = set()
    for label, comp in components:
        if label != "Ingest":
            has_extra |= set(comp)
    everything = [v for v in systems if v not in has_extra]

    xs = range(len(systems))
    bottoms = {v: 0.0 for v in systems}
    totals = {v: 0.0 for v in systems}
    thresh = 0.04 * max(1.0, max(sum(c.get(s, (0.0,))[0] for _, c in components)
                                 for s in systems))

    for ci, (label, comp) in enumerate(components):
        heights = [comp.get(v, (0.0,))[0] for v in systems]
        # the Ingest segment of an "everything" system is ClickHouse yellow, not the blue
        colors = [EVERYTHING_COLOR if (label == "Ingest" and v in everything)
                  else comp_color(label, ci) for v in systems]
        ax.bar(list(xs), heights, width=0.62, bottom=[bottoms[v] for v in systems],
               color=colors, edgecolor=BACKGROUND_COLOR, linewidth=0.8, zorder=3)
        for v, h in zip(systems, heights):
            if h > 0:
                totals[v] += h
                if h >= thresh:
                    on_yellow = label == "Ingest" and v in everything
                    ax.text(systems.index(v), bottoms[v] + h / 2, money(h), ha="center",
                            va="center", color="#1A1A1A" if on_yellow else "white",
                            fontsize=8, zorder=4)
            bottoms[v] += h

    # total (+ pricing tier) above each bar. The "everything" system shows just the total —
    # its single-bar meaning is carried by the yellow colour + legend entry.
    top = max(totals.values())
    for v in systems:
        txt = money(totals[v]) if v in everything else f"{money(totals[v])}\n{ptier_used.get(v, '')}"
        ax.text(systems.index(v), totals[v] + top * 0.015, txt, ha="center", va="bottom",
                color="white", fontsize=9.5, fontweight="bold", linespacing=1.3, zorder=4)

    ax.set_xticks(list(xs))
    ax.set_xticklabels([f"{v}\n{hw.get(v, '')}" for v in systems], color="white", fontsize=11)
    ax.set_ylim(top=top * 1.18)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _p: f"${y:,.0f}"))
    ax.tick_params(colors="white", labelsize=10)
    ax.set_ylabel("Compute cost to ingest ~100B rows (USD)\n↓ lower is better",
                  color="white", fontsize=11)
    ax.grid(True, axis="y", color=GRID_COLOR, lw=0.6, alpha=0.7, zorder=0)
    for side in ("right", "top"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color("white")

    # legend: component swatches in their colours, plus the ClickHouse-yellow "Everything" entry
    items = [(comp_color(label, ci), label) for ci, (label, _) in enumerate(components)]
    if everything:
        items.append((EVERYTHING_COLOR, "Everything (ingest + sort + MV)"))
    handles = [plt.Rectangle((0, 0), 1, 1, color=c) for c, _ in items]
    ax.legend(handles, [lbl for _, lbl in items],
              loc="upper left", facecolor=BACKGROUND_COLOR, edgecolor=GRID_COLOR,
              labelcolor="white", fontsize=9.5, framealpha=0.9, title="Cost component",
              title_fontproperties={"weight": "bold"})
    ax.get_legend().get_title().set_color("white")

    if not args.no_title:
        ax.set_title(args.title or "Ingest cost composition", color="white", fontsize=14, pad=12)

    fig.tight_layout()
    if args.out:
        fig.savefig(args.out, dpi=args.dpi, facecolor=fig.get_facecolor())
        print(f"wrote {args.out}", file=sys.stderr)
    else:
        plt.show()


if __name__ == "__main__":
    main()
