#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""
Quotes benchmark — storage size comparison (raw table vs MV) across systems.

Two panels of bars: the raw base table and the materialized view, one bar per system, with
the size, row count, and monthly storage cost labelled. Values are current/active compressed
on-disk size (the apples-to-apples footprint; excludes Snowflake time-travel + fail-safe and
Databricks time-travel — see the note in storage.json).

T1 Snowflake raw bars stack standard + interactive table sizes when `details` is present.
Use `--split-snowflake` to show ClickHouse, Standard, and Interactive as separate bars.

  python3 render_storage.py _test/storage.json --out _out/storage.png

Compare T0 (standard MV) vs T1 (interactive tables) in one figure:

  python3 render_storage.py --compare \
      T0=../snowflake/results/t0/storage.json \
      T1=../snowflake/results/t1/storage.json \
      --out _out/storage_t0_t1.png

Style matches render_latency.py / ../_viz2.
"""
import sys
import json
import argparse

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from matplotlib.patches import Patch

import matplotlib.font_manager as _fm
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
TB = 1_000_000_000_000  # decimal TB — matches vendor pricing units
DEFAULT_STORAGE_USD_PER_TB = {
    "ClickHouse": 25.30,
    "Snowflake": 23.0,
    "Databricks": 23.0,
}
# Snowflake T1 raw stack — standard table (bottom) + interactive table (top).
STACK_SEGMENT = {
    "standard":    ("Standard table", "#1A8FB8"),
    "interactive": ("Interactive table", "#29B5E8"),
}
STACK_ORDER = ["standard", "interactive"]


def vendor_of(system: str) -> str:
    s = (system or "").lower()
    for name in VENDOR_COLOR:
        if name.lower() in s:
            return name
    return system or "unknown"


def human_bytes(b, _pos=None):
    for div, suf in ((2**40, "TiB"), (2**30, "GiB"), (2**20, "MiB"), (2**10, "KiB")):
        if b >= div:
            v = b / div
            return f"{v:.0f} {suf}" if v >= 100 else f"{v:.1f} {suf}"
    return f"{b:.0f} B"


def human_count(n):
    for div, suf in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if n >= div:
            return f"{n/div:.1f}{suf}"
    return f"{n:.0f}"


def fmt_usd(v):
    if v >= 100:
        return f"${v:,.0f}/mo"
    if v >= 1:
        return f"${v:.2f}/mo"
    return f"${v:.3f}/mo"


def storage_cost_usd(b, vendor, pricing):
    rate = pricing.get(vendor)
    if rate is None:
        return None
    return b / TB * rate


def order_key(system):
    v = vendor_of(system)
    return VENDOR_ORDER.index(v) if v in VENDOR_ORDER else 99


def filter_rows(rows):
    def _has_bytes(r):
        b = r.get("bytes")
        return isinstance(b, (int, float)) and not isinstance(b, bool) and b > 0
    dropped = sorted({r.get("system", "?") for r in rows if not _has_bytes(r)})
    kept = [r for r in rows if _has_bytes(r)]
    return kept, dropped


def load_tier(path, vendors=None):
    data = json.load(open(path))
    raw, dropped_raw = filter_rows(data.get("raw", []))
    mv, dropped_mv = filter_rows(data.get("mv", []))
    if vendors:
        keep = {v.lower() for v in vendors}
        raw = [r for r in raw if vendor_of(r["system"]).lower() in keep]
        mv = [r for r in mv if vendor_of(r["system"]).lower() in keep]
    dropped = sorted(set(dropped_raw) | set(dropped_mv))
    return {"raw": raw, "mv": mv, "note": data.get("note"), "dropped": dropped}


def stack_segments(row):
    details = row.get("details") or {}
    segs = []
    for key in STACK_ORDER:
        seg = details.get(key)
        if not seg:
            continue
        b = seg.get("bytes")
        if isinstance(b, (int, float)) and not isinstance(b, bool) and b > 0:
            segs.append((key, seg))
    return segs


def total_label(row, pricing):
    v = vendor_of(row["system"])
    label = human_bytes(row["bytes"])
    if row.get("rows"):
        label += f"\n{human_count(row['rows'])} rows"
    cost = storage_cost_usd(row["bytes"], v, pricing)
    if cost is not None:
        label += f"\n{fmt_usd(cost)}"
    return label


# Panel copy — T1 is asymmetric: SF raw = standard + IT tables; right panel = CH MV vs SF IT aggregate.
TIER_PANELS = {
    ("T0", "raw"): ("T0 — Raw table", None),
    ("T0", "mv"):  ("T0 — Materialized view", None),
    ("T1", "raw"): (
        "T1 — Raw tables",
        ["ClickHouse", "Snowflake\n(std + IT)"],
    ),
    ("T1", "raw", "split"): (
        "T1 — Raw tables (ClickHouse vs std / IT)",
        None,
    ),
    ("T1", "mv"): (
        "T1 — Rollup (CH MV vs SF interactive aggregate)",
        ["ClickHouse\n(MV)", "Snowflake\n(IT aggregate)"],
    ),
}


def expand_snowflake_raw(rows):
    """Split Snowflake `details` into side-by-side standard + interactive bars."""
    out = []
    for r in rows:
        segs = stack_segments(r)
        if segs and vendor_of(r["system"]) == "Snowflake":
            for seg_key, seg in segs:
                out.append({
                    "system": "Snowflake",
                    "bytes": seg["bytes"],
                    "rows": seg.get("rows"),
                    "_segment": seg_key,
                    "_sort": STACK_ORDER.index(seg_key) + 1,
                })
        else:
            out.append({**r, "_sort": 0 if vendor_of(r["system"]) == "ClickHouse" else 99})
    return sorted(out, key=lambda r: r.get("_sort", order_key(r["system"])))


def bar_color(row):
    seg = row.get("_segment")
    if seg in STACK_SEGMENT:
        return STACK_SEGMENT[seg][1]
    return VENDOR_COLOR.get(vendor_of(row["system"]), "#FFF")


def xtick_for_row(row):
    seg = row.get("_segment")
    if seg == "standard":
        return "Standard"
    if seg == "interactive":
        return "Interactive"
    return vendor_of(row["system"])


def draw_panel(ax, rows, title, pricing, xtick_labels=None, split_snowflake=False):
    if split_snowflake:
        rows = expand_snowflake_raw(rows)
        xtick_labels = [xtick_for_row(r) for r in rows]
    else:
        rows = sorted(rows, key=lambda r: order_key(r["system"]))
    if not rows:
        ax.set_visible(False)
        return

    xs = list(range(len(rows)))
    top = max(r["bytes"] for r in rows)
    has_stack = not split_snowflake and any(stack_segments(r) for r in rows)

    for x, r in zip(xs, rows):
        segs = [] if split_snowflake else stack_segments(r)
        v = vendor_of(r["system"])
        if segs:
            bottom = 0.0
            for seg_key, seg in segs:
                b = seg["bytes"]
                _, color = STACK_SEGMENT[seg_key]
                ax.bar(x, b, width=0.62, bottom=bottom, color=color,
                       edgecolor="black", linewidth=0.5, zorder=3)
                if b >= top * 0.07:
                    ax.text(x, bottom + b / 2, STACK_SEGMENT[seg_key][0],
                            ha="center", va="center", color="white",
                            fontsize=7.5, zorder=4)
                bottom += b
        else:
            ax.bar(x, r["bytes"], width=0.62, color=bar_color(r),
                   edgecolor="black", linewidth=0.5, zorder=3)

        ax.text(x, r["bytes"] + top * 0.02, total_label(r, pricing),
                ha="center", va="bottom", color="white", fontsize=9, zorder=4)

    ax.set_xticks(xs)
    if xtick_labels is None:
        xtick_labels = [vendor_of(r["system"]) for r in rows]
    ax.set_xticklabels(xtick_labels, color="white", fontsize=9.5)
    ax.set_facecolor(BACKGROUND_COLOR)
    ax.set_title(title, color="white", fontsize=12, pad=8)
    ax.yaxis.set_major_formatter(FuncFormatter(human_bytes))
    ax.tick_params(colors="white", labelsize=9)
    ax.set_ylim(top=top * (1.32 if has_stack else 1.28))
    ax.grid(True, axis="y", which="major", color=GRID_COLOR, lw=0.6, alpha=0.7, zorder=0)
    for side in ("right", "top"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color("white")

    if has_stack:
        handles = [Patch(facecolor=STACK_SEGMENT[k][1], edgecolor="black", linewidth=0.5,
                         label=STACK_SEGMENT[k][0]) for k in STACK_ORDER]
        ax.legend(handles=handles, loc="upper left", facecolor=BACKGROUND_COLOR,
                  edgecolor=GRID_COLOR, labelcolor="white", fontsize=8.5, framealpha=0.9)


def pricing_note(pricing, vendors=None):
    parts = []
    for vendor in VENDOR_ORDER:
        if vendor in pricing and (vendors is None or vendor in vendors):
            parts.append(f"{vendor} ${pricing[vendor]:.2f}/TB")
    return "  ·  ".join(parts) if parts else ""


def tier_vendors(tier):
    out = set()
    for key in ("raw", "mv"):
        for r in tier.get(key, []):
            out.add(vendor_of(r["system"]))
    return out


HEADER_HEIGHT_IN = 0.58
TITLE_OFFSET_IN = 0.10
SUBTITLE_OFFSET_IN = 0.30


def finish_figure(fig, main, pricing, no_title, vendors=None):
    if no_title:
        fig.tight_layout()
        return fig
    h = fig.get_size_inches()[1]
    rect_top = 1.0 - HEADER_HEIGHT_IN / h
    fig.suptitle(main, color="white", fontsize=15, fontweight="bold",
                 y=1.0 - TITLE_OFFSET_IN / h)
    note = pricing_note(pricing, vendors)
    if note:
        fig.text(0.5, 1.0 - SUBTITLE_OFFSET_IN / h, f"{note} per month",
                 ha="center", va="top", color="#BBBBBB", fontsize=10)
    fig.tight_layout(rect=(0, 0, 1, rect_top))
    return fig


def panel_config(tier_label, key, split_snowflake=False):
    if split_snowflake and tier_label == "T1" and key == "raw":
        return TIER_PANELS.get((tier_label, key, "split"), (f"{tier_label} — {key}", None))
    return TIER_PANELS.get((tier_label, key), (f"{tier_label} — {key}", None))


def panel_width(rows):
    return 13 if len(rows) >= 3 else 12


def render_single(tier, pricing, title, no_title, tier_label=None, split_snowflake=False):
    raw, mv = tier["raw"], tier["mv"]
    if not raw and not mv:
        sys.exit("storage.json has no 'raw' or 'mv' entries with bytes.")

    raw_plot = expand_snowflake_raw(raw) if split_snowflake else raw
    w = max(panel_width(raw_plot), panel_width(mv))
    fig, axes = plt.subplots(1, 2, figsize=(w, 5.6))
    fig.patch.set_facecolor(BACKGROUND_COLOR)
    raw_title, raw_xticks = panel_config(tier_label, "raw", split_snowflake) if tier_label else ("Raw table", None)
    mv_title, mv_xticks = panel_config(tier_label, "mv") if tier_label else ("Materialized view", None)
    draw_panel(axes[0], raw, raw_title, pricing, raw_xticks, split_snowflake=split_snowflake)
    draw_panel(axes[1], mv, mv_title, pricing, mv_xticks)
    axes[0].set_ylabel("On-disk size, compressed\n↓ smaller is better",
                       color="white", fontsize=11)

    main = title or "Storage size — raw table vs MV (active, compressed)"
    finish_figure(fig, main, pricing, no_title, vendors=tier_vendors(tier))
    return fig


def render_compare(tiers, pricing, title, no_title):
    n = len(tiers)
    max_cols = max(max(len(t["raw"]), len(t["mv"])) for _, t in tiers)
    w = 13 if max_cols >= 3 else 12
    fig, axes = plt.subplots(n, 2, figsize=(w, 5.0 * n), squeeze=False)
    fig.patch.set_facecolor(BACKGROUND_COLOR)

    for i, (label, tier) in enumerate(tiers):
        for j, key in enumerate(("raw", "mv")):
            panel_title, xtick_labels = panel_config(label, key)
            draw_panel(axes[i, j], tier[key], panel_title, pricing, xtick_labels)
        if i == 0:
            axes[i, 0].set_ylabel("On-disk size, compressed\n↓ smaller is better",
                                  color="white", fontsize=11)
        else:
            axes[i, 0].set_ylabel("")

    vendors = set()
    for _, tier in tiers:
        vendors |= tier_vendors(tier)
    main = title or "Storage size — ClickHouse vs Snowflake vs Databricks (T0 vs T1)"
    finish_figure(fig, main, pricing, no_title, vendors=vendors)
    return fig


def parse_compare(spec):
    if "=" not in spec:
        sys.exit(f"--compare expects LABEL=PATH, got: {spec!r}")
    label, path = spec.split("=", 1)
    label = label.strip()
    path = path.strip()
    if not label:
        sys.exit(f"--compare label is empty: {spec!r}")
    return label, path


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("data", nargs="?", help="storage.json with {'raw':[...], 'mv':[...]}.")
    ap.add_argument("--compare", nargs="+", metavar="LABEL=PATH",
                    help="Tier comparison, one or more LABEL=PATH pairs "
                         "(e.g. --compare T0=t0/storage.json T1=t1/storage.json).")
    ap.add_argument("-o", "--out", help="Output PNG.")
    ap.add_argument("--dpi", type=int, default=300)
    ap.add_argument("--title", default=None)
    ap.add_argument("--no-title", action="store_true")
    ap.add_argument("--tier", choices=["T0", "T1", "T2"], default=None,
                    help="Benchmark tier — adjusts panel titles (e.g. T1 raw = std+IT, rollup = CH MV vs SF IT).")
    ap.add_argument("--vendors", nargs="+", metavar="V",
                    help="Include only these vendors (e.g. clickhouse snowflake).")
    ap.add_argument("--split-snowflake", action="store_true",
                    help="T1 raw panel: side-by-side ClickHouse, Standard, Interactive bars.")
    ap.add_argument("--clickhouse-usd-per-tb", type=float, default=25.30)
    ap.add_argument("--snowflake-usd-per-tb", type=float, default=23.0)
    ap.add_argument("--databricks-usd-per-tb", type=float, default=23.0)
    args = ap.parse_args()

    pricing = dict(DEFAULT_STORAGE_USD_PER_TB)
    pricing["ClickHouse"] = args.clickhouse_usd_per_tb
    pricing["Snowflake"] = args.snowflake_usd_per_tb
    pricing["Databricks"] = args.databricks_usd_per_tb

    if args.compare:
        tiers = []
        for spec in args.compare:
            label, path = parse_compare(spec)
            tier = load_tier(path, args.vendors)
            if tier["dropped"]:
                print(f"{label}: skipping placeholder/empty entr"
                      f"{'y' if len(tier['dropped'])==1 else 'ies'} "
                      f"(no bytes yet): {', '.join(tier['dropped'])}", file=sys.stderr)
            if not tier["raw"] and not tier["mv"]:
                sys.exit(f"{label}: no storage entries with bytes in {path}")
            tiers.append((label, tier))
        fig = render_compare(tiers, pricing, args.title, args.no_title)
    elif args.data:
        tier = load_tier(args.data, args.vendors)
        if tier["dropped"]:
            print(f"storage: skipping placeholder/empty entr"
                  f"{'y' if len(tier['dropped'])==1 else 'ies'} "
                  f"(no bytes yet): {', '.join(tier['dropped'])}", file=sys.stderr)
        fig = render_single(tier, pricing, args.title, args.no_title,
                            tier_label=args.tier, split_snowflake=args.split_snowflake)
    else:
        ap.error("provide a storage.json path or --compare LABEL=PATH")

    if args.out:
        fig.savefig(args.out, dpi=args.dpi, facecolor=fig.get_facecolor())
        print(f"wrote {args.out}", file=sys.stderr)
    else:
        plt.show()


if __name__ == "__main__":
    main()
