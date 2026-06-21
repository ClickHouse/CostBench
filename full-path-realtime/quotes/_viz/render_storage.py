#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""
Quotes benchmark — storage size comparison (raw table vs MV) across systems.

Two panels of bars: the raw base table and the materialized view, one bar per system, with
the size and total row count labelled. Values are current/active compressed on-disk size (the
apples-to-apples footprint; excludes Snowflake time-travel + fail-safe and Databricks
time-travel — see the note in _test/storage.json).

  python3 render_storage.py _test/storage.json --out _out/storage.png

Style matches render_latency.py / ../_viz2.
"""
import sys
import json
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


def order_key(system):
    v = vendor_of(system)
    return VENDOR_ORDER.index(v) if v in VENDOR_ORDER else 99


def draw_panel(ax, rows, title):
    rows = sorted(rows, key=lambda r: order_key(r["system"]))
    xs = range(len(rows))
    top = max(r["bytes"] for r in rows)
    for x, r in zip(xs, rows):
        v = vendor_of(r["system"])
        ax.bar(x, r["bytes"], width=0.62, color=VENDOR_COLOR.get(v, "#FFF"),
               edgecolor="black", linewidth=0.5, zorder=3)
        label = human_bytes(r["bytes"])
        if r.get("rows"):
            label += f"\n{human_count(r['rows'])} rows"
        ax.text(x, r["bytes"] + top * 0.02, label, ha="center", va="bottom",
                color="white", fontsize=9, zorder=4)
    ax.set_xticks(list(xs))
    ax.set_xticklabels([vendor_of(r["system"]) for r in rows],
                       color="white", fontsize=9.5)
    ax.set_facecolor(BACKGROUND_COLOR)
    ax.set_title(title, color="white", fontsize=12, pad=8)
    ax.yaxis.set_major_formatter(FuncFormatter(human_bytes))
    ax.tick_params(colors="white", labelsize=9)
    ax.set_ylim(top=top * 1.22)
    ax.grid(True, axis="y", which="major", color=GRID_COLOR, lw=0.6, alpha=0.7, zorder=0)
    for side in ("right", "top"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color("white")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("data", help="storage.json with {'raw':[...], 'mv':[...]}.")
    ap.add_argument("-o", "--out", help="Output PNG.")
    ap.add_argument("--dpi", type=int, default=300)
    ap.add_argument("--title", default=None)
    ap.add_argument("--no-title", action="store_true")
    args = ap.parse_args()

    data = json.load(open(args.data))
    raw, mv = data.get("raw", []), data.get("mv", [])
    # Drop placeholder/empty entries (bytes null or <= 0) so a not-yet-filled system
    # (e.g. a ClickHouse placeholder awaiting manual numbers) doesn't crash the panel.
    def _has_bytes(r):
        b = r.get("bytes")
        return isinstance(b, (int, float)) and not isinstance(b, bool) and b > 0
    dropped = sorted({r.get("system", "?") for arr in (raw, mv) for r in arr if not _has_bytes(r)})
    raw = [r for r in raw if _has_bytes(r)]
    mv = [r for r in mv if _has_bytes(r)]
    if dropped:
        print(f"storage: skipping placeholder/empty entr{'y' if len(dropped)==1 else 'ies'} "
              f"(no bytes yet): {', '.join(dropped)}", file=sys.stderr)
    if not raw and not mv:
        sys.exit("storage.json has no 'raw' or 'mv' entries with bytes.")

    fig, axes = plt.subplots(1, 2, figsize=(12, 5.2))
    fig.patch.set_facecolor(BACKGROUND_COLOR)
    draw_panel(axes[0], raw, "Raw table")
    draw_panel(axes[1], mv, "Materialized view")
    axes[0].set_ylabel("On-disk size, compressed\n↓ smaller is better",
                       color="white", fontsize=11)

    if not args.no_title:
        fig.suptitle(args.title or "Storage size — raw table vs MV (active, compressed)",
                     color="white", fontsize=15, fontweight="bold", y=0.99)

    fig.tight_layout(rect=(0, 0, 1, 0.95 if not args.no_title else 1.0))
    if args.out:
        fig.savefig(args.out, dpi=args.dpi, facecolor=fig.get_facecolor())
        print(f"wrote {args.out}", file=sys.stderr)
    else:
        plt.show()


if __name__ == "__main__":
    main()
