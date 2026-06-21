#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""
Quotes ingest benchmark — query latency vs data volume.

Each system writes a JSONL time series (one record per runner iteration) as the
table grows under sustained ~1M EPS ingest. This renders **query latency (y, log)
vs raw row count (x, log)**, one line per system, faceted one subplot per query.

Input: one or more JSONL files, each a single system's time series (dashboard or
drilldown). Records follow RUNNERS_SPEC.md:
  {"raw_rows": int, "mv_rows": int, "system": str, "machine"/"cluster_size": str,
   "result": [[sec_or_null], ...]}  # one inner list per query, in queries-file order

Usage:
  python3 render_latency.py _test/dashboard_*.jsonl --out _out/dashboard.png \
      --title "Dashboard query latency vs volume" \
      --query-labels "Single-symbol summary;Watchlist summary;Top movers;Daily activity"

Style matches ../_viz2 (dark theme, Inter, vendor color map).
"""
import sys
import json
import math
import argparse

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, NullFormatter

# ---- style (consistent with ../_viz2/render.py) ----
import matplotlib.font_manager as _fm
# Use Inter when installed, else fall back to DejaVu Sans (avoids noisy
# "findfont: Font family 'Inter' not found" warnings on every text element).
matplotlib.rcParams["font.family"] = (
    "Inter" if any(f.name == "Inter" for f in _fm.fontManager.ttflist) else "DejaVu Sans"
)
matplotlib.rcParams["font.sans-serif"] = ["Inter", "DejaVu Sans"]
matplotlib.rcParams["axes.titleweight"] = "bold"

VENDOR_COLOR = {
    "ClickHouse":   "#FDFF88",  # ClickHouse yellow
    "Redshift":     "#FFB30A",  # AWS orange
    "Databricks":   "#FF4B3A",  # Databricks red
    "Snowflake IT": "#A259FF",  # Snowflake interactive-tables variant (purple, distinct from MV)
    "Snowflake":    "#29B5E8",  # Snowflake (MV/warehouse) cyan
    "BigQuery":     "#4285F4",  # Google blue
}
BACKGROUND_COLOR = "#2B2B2B"
GRID_COLOR = "#4A4A4A"
TIMEOUT_SEC = 5.0   # interactive-warehouse query cap; "timeout" results plot here (lower bound)

# stable plot order so colors/legend are deterministic
VENDOR_ORDER = ["ClickHouse", "Snowflake", "Snowflake IT", "Databricks", "Redshift", "BigQuery"]


def vendor_of(system: str) -> str:
    s = (system or "").lower()
    # longest name first so "Snowflake IT" wins over the "Snowflake" substring
    for name in sorted(VENDOR_COLOR, key=len, reverse=True):
        if name.lower() in s:
            return name
    return system or "unknown"


def tier_of(rec: dict) -> str:
    """Human warehouse/cluster tier for the legend. An integer cluster_size (e.g.
    ClickHouse replica count) is appended as '×N'; string tiers (Snowflake '2.7',
    Databricks 'X-Small') are used as-is."""
    machine = str(rec.get("machine") or "").strip()
    cs = rec.get("cluster_size")
    if machine and isinstance(cs, int):
        return f"{machine} ×{cs}"
    return machine or str(cs or "").strip()


def load_series(path):
    """Return {vendor: {"tier": str, "points": [(raw_rows, [lat,...]), ...]}}."""
    by_vendor = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            v = vendor_of(rec.get("system", ""))
            entry = by_vendor.setdefault(v, {"tier": tier_of(rec), "points": []})
            if not entry["tier"]:
                entry["tier"] = tier_of(rec)
            entry["points"].append((rec.get("raw_rows", 0) or 0, rec.get("result", [])))
    return by_vendor


def human_rows(x, _pos=None):
    if x <= 0:
        return "0"
    for div, suf in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if x >= div:
            val = x / div
            return f"{val:g}{suf}"
    return f"{x:g}"


def human_secs(y, _pos=None):
    if y >= 1:
        return f"{y:g}s"
    return f"{y*1000:g}ms"


def grid_dims(n):
    return {1: (1, 1), 2: (1, 2), 3: (1, 3), 4: (2, 2)}.get(
        n, (math.ceil(n / 3), 3)
    )


def rolling_median(ys, window):
    """Centered rolling median; edges shrink the window. Pure python (no pandas)."""
    n = len(ys)
    half = window // 2
    out = []
    for i in range(n):
        lo, hi = max(0, i - half), min(n, i + half + 1)
        out.append(sorted(ys[lo:hi])[(hi - lo) // 2])
    return out


def clip_to_window(pts, lo, hi):
    """`pts` sorted by x. Return the points inside [lo, hi] with interpolated endpoints exactly
    at lo and hi, so every series starts and ends at the same x (the shared window edges).
    lo/hi are within each series' extent by construction (lo = max of per-series mins,
    hi = min of per-series maxes), so the endpoints are real interpolations, not extrapolations."""
    def interp(x):
        if x <= pts[0][0]:
            return pts[0][1]
        if x >= pts[-1][0]:
            return pts[-1][1]
        for i in range(1, len(pts)):
            x0, y0 = pts[i - 1]
            x1, y1 = pts[i]
            if x0 <= x <= x1:
                return y0 if x1 == x0 else y0 + (y1 - y0) * (x - x0) / (x1 - x0)
        return pts[-1][1]
    return [(lo, interp(lo))] + [(x, y) for x, y in pts if lo < x < hi] + [(hi, interp(hi))]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("files", nargs="+", help="One JSONL per system (dashboard or drilldown).")
    ap.add_argument("-o", "--out", help="Output PNG (omit to show interactively).")
    ap.add_argument("--dpi", type=int, default=300)
    ap.add_argument("--title", default=None, help="Figure title.")
    ap.add_argument("--no-title", action="store_true")
    ap.add_argument("--x", choices=["raw_rows", "mv_rows"], default="raw_rows",
                    help="(reserved) x-axis volume metric; raw_rows by default.")
    ap.add_argument("--query-labels", default=None,
                    help="';'-separated subplot titles, in queries-file order.")
    ap.add_argument("--min-rows", type=float, default=1.0,
                    help="Drop points below this row count (log x can't show 0).")
    ap.add_argument("--smooth", type=int, default=0, metavar="W",
                    help="Rolling-median window (odd, e.g. 7). Draws raw faint + a bold "
                         "trend line. 0 = raw lines only.")
    ap.add_argument("--no-raw", action="store_true",
                    help="With --smooth, draw only the rolling-median trend (hide the faint "
                         "raw line).")
    ap.add_argument("--full-range", action="store_true",
                    help="Plot each system's full row-count range. Default trims the x-axis "
                         "to the volume window where ALL systems have data.")
    ap.add_argument("--xscale", choices=["log", "linear"], default="log",
                    help="X (row-count) axis scale. Default log (volume grows multiplicatively).")
    ap.add_argument("--yscale", choices=["log", "linear"], default="log",
                    help="Y (latency) axis scale. Default log.")
    args = ap.parse_args()

    # vendor -> {tier, points}, merged across all input files
    series = {}
    nq = 0
    for path in args.files:
        for v, entry in load_series(path).items():
            tgt = series.setdefault(v, {"tier": entry["tier"], "points": []})
            tgt["points"].extend(entry["points"])
            if not tgt["tier"]:
                tgt["tier"] = entry["tier"]
            for _, res in entry["points"]:
                nq = max(nq, len(res))
    if not series or nq == 0:
        sys.exit("No usable records / no query results found in input.")

    labels = (args.query_labels.split(";") if args.query_labels
              else [f"Query {i+1}" for i in range(nq)])
    labels += [f"Query {i+1}" for i in range(len(labels), nq)]

    # Common x-range: the row-count window where EVERY system has data, so a system
    # that logged from a much lower volume doesn't leave the others with empty space.
    common = None
    if not args.full_range and len(series) > 1:
        extents = []
        for e in series.values():
            rr = [raw for raw, _ in e["points"] if raw >= args.min_rows]
            if rr:
                extents.append((min(rr), max(rr)))
        if len(extents) == len(series):
            lo, hi = max(e[0] for e in extents), min(e[1] for e in extents)
            if lo < hi:
                common = (lo, hi)
            else:
                print("warning: systems do not overlap in row count; showing full range",
                      file=sys.stderr)

    rows, cols = grid_dims(nq)
    fig_w = max(6.2 * cols, 9.5)  # floor so long titles aren't clipped on a single panel
    fig, axes = plt.subplots(rows, cols, figsize=(fig_w, 4.6 * rows), squeeze=False)
    fig.patch.set_facecolor(BACKGROUND_COLOR)

    ordered = [v for v in VENDOR_ORDER if v in series] + \
              [v for v in series if v not in VENDOR_ORDER]

    legend_handles, legend_labels = [], []

    for qi in range(nq):
        ax = axes[qi // cols][qi % cols]
        ax.set_facecolor(BACKGROUND_COLOR)
        for v in ordered:
            color = VENDOR_COLOR.get(v, "#FFFFFF")
            pts = []
            for raw, res in series[v]["points"]:
                if raw < args.min_rows or qi >= len(res):
                    continue
                lat = res[qi]
                if isinstance(lat, list):  # spec stores each try as [val]; flatten
                    lat = lat[0] if lat and lat[0] is not None else None
                if lat is None:
                    continue
                if lat == "timeout":       # interactive 5s cap — plot at the wall (lower bound)
                    lat = TIMEOUT_SEC
                pts.append((raw, float(lat)))
            if not pts:
                continue
            pts.sort()
            # anchor every line to the shared window edges so all vendors start/end together
            if common:
                pts = clip_to_window(pts, common[0], common[1])
            xs = [p[0] for p in pts]
            ys = [p[1] for p in pts]
            if args.smooth and args.smooth > 1 and len(ys) >= 3:
                # raw faint in the background (unless --no-raw), bold median trend on top
                if not args.no_raw:
                    ax.plot(xs, ys, lw=0.7, color=color, alpha=0.22, zorder=2)
                trend = rolling_median(ys, args.smooth)
                (line,) = ax.plot(xs, trend, lw=2.2, color=color, zorder=3)
            else:
                (line,) = ax.plot(xs, ys, marker="o", ms=3.5, lw=1.8,
                                  color=color, mec="black", mew=0.4, zorder=3)
            tier = series[v]["tier"]
            lbl = f"{v} · {tier}" if tier else v
            if lbl not in legend_labels:
                legend_handles.append(line)
                legend_labels.append(lbl)

        ax.set_xscale(args.xscale)
        ax.set_yscale(args.yscale)
        if common:
            ax.set_xlim(common[0] * 0.97, common[1] * 1.03)  # small padding
        ax.set_title(labels[qi], color="white", fontsize=12, pad=8)
        ax.xaxis.set_major_formatter(FuncFormatter(human_rows))
        ax.yaxis.set_major_formatter(FuncFormatter(human_secs))
        ax.yaxis.set_minor_formatter(NullFormatter())
        ax.xaxis.set_minor_formatter(NullFormatter())
        ax.tick_params(colors="white", labelsize=9)
        ax.grid(True, which="major", color=GRID_COLOR, lw=0.6, alpha=0.7, zorder=0)
        for side in ("right", "top"):
            ax.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            ax.spines[side].set_color("white")
        # axis labels only on outer edges to reduce clutter
        if qi // cols == rows - 1:
            ax.set_xlabel(f"Raw rows ({args.xscale})", color="white", fontsize=10)
        if qi % cols == 0:
            ax.set_ylabel(f"Query latency ({args.yscale})\n↓ lower is better",
                          color="white", fontsize=10)

    # hide any unused axes (e.g. 5 queries in a 2x3 grid)
    for k in range(nq, rows * cols):
        axes[k // cols][k % cols].set_visible(False)

    if not args.no_title:
        fig.suptitle(args.title or "Query latency vs data volume",
                     color="white", fontsize=14, fontweight="bold", y=0.995)

    # horizontal legend row just under the title — never overlaps the panels
    if legend_handles:
        fig.legend(legend_handles, legend_labels, loc="upper center",
                   bbox_to_anchor=(0.5, 0.95 if not args.no_title else 0.99),
                   ncol=len(legend_handles),
                   facecolor=BACKGROUND_COLOR, edgecolor=GRID_COLOR,
                   labelcolor="white", fontsize=10, framealpha=0.9)

    top = 0.90 if not args.no_title else 0.93
    fig.tight_layout(rect=(0, 0, 1, top))

    if args.out:
        fig.savefig(args.out, dpi=args.dpi, facecolor=fig.get_facecolor())
        print(f"wrote {args.out}", file=sys.stderr)
    else:
        plt.show()


if __name__ == "__main__":
    main()
