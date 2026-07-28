#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""
Dashboard end-to-end latency: interactive-warehouse timeout + standard-warehouse fallback.

Combines the dashboard-vs-MV run on the INTERACTIVE warehouse with the same run on the STANDARD
warehouse, matched by iteration number. For each (iteration, query):
  - interactive query EXECUTED  -> plot its latency,         PURPLE dot  (ran on interactive wh)
  - interactive query TIMED OUT -> take the SAME iteration from the standard file, add the 5s
                                   interactive timeout,        BLUE dot  (fell back to standard)
So the y value is the real user-experienced latency, and the colour says which path it took.
Faceted one subplot per query vs data volume — shows exactly where the dashboard stops fitting
the interactive 5s budget and starts paying the fallback cost.

  python3 render_dashboard_fallback.py \
      --interactive _test/dash_mv_iv_snowflake.jsonl \
      --standard    _test/dash_mv_std_snowflake.jsonl \
      --timeout 5 --query-labels "Single-symbol summary;Watchlist summary;Top movers;Daily activity" \
      --out _out/t2/t2_dashboard_fallback.png

Style matches render_latency.py.
"""
import sys
import json
import math
import argparse

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, NullFormatter
from matplotlib.lines import Line2D

import matplotlib.font_manager as _fm
matplotlib.rcParams["font.family"] = (
    "Inter" if any(f.name == "Inter" for f in _fm.fontManager.ttflist) else "DejaVu Sans"
)
matplotlib.rcParams["font.sans-serif"] = ["Inter", "DejaVu Sans"]
matplotlib.rcParams["axes.titleweight"] = "bold"

BACKGROUND_COLOR = "#2B2B2B"
GRID_COLOR = "#4A4A4A"
C_INTERACTIVE = "#A259FF"   # light purple — query ran on the INTERACTIVE warehouse.
                            # Was #FFA6D2 pink, which sat at OKLab dE 4.7 from the blue
                            # fallback series under protanopia (floor is 8) — the two
                            # series were indistinguishable for protan viewers.
C_FALLBACK    = "#29B5E8"   # blue — timed out on interactive, ran on the standard wh (+5s)
C_CLICKHOUSE  = "#FDFF88"   # yellow — ClickHouse comparison line
C_LINE        = "#8A8F98"   # faint connector


def load_records(path):
    rows = []
    for l in open(path):
        if not l.strip():
            continue
        r = json.loads(l)
        rows.append((r.get("iteration"), r.get("raw_rows", 0) or 0, r.get("result", [])))
    return rows


def cell(res, q):
    """One query's latency from a result row: float, the string 'timeout', or None."""
    if q >= len(res):
        return None
    v = res[q]
    if isinstance(v, list):
        v = v[0] if v else None
    return v


def human_rows(x, _pos=None):
    if x <= 0:
        return "0"
    for div, suf in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if x >= div:
            return f"{x/div:g}{suf}"
    return f"{x:g}"


def human_secs(y, _pos=None):
    if y <= 0:
        return "0"
    return f"{y:g}s" if y >= 1 else f"{y*1000:g}ms"


def grid_dims(n):
    return {1: (1, 1), 2: (1, 2), 3: (1, 3), 4: (2, 2)}.get(n, (math.ceil(n / 3), 3))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--interactive", required=True, help="dashboard-vs-MV JSONL run on the interactive wh")
    ap.add_argument("--standard", required=True, help="dashboard-vs-MV JSONL run on the standard wh")
    ap.add_argument("--clickhouse", default=None, help="ClickHouse dashboard JSONL to overlay as a comparison line")
    ap.add_argument("--timeout", type=float, default=5.0, help="interactive statement timeout added on fallback")
    ap.add_argument("--query-labels", default=None, help="';'-separated subplot titles, in query order")
    ap.add_argument("--min-rows", type=float, default=1.0)
    ap.add_argument("--max-rows", type=float, default=float("inf"), metavar="N",
                    help="Drop points above this row count, e.g. 100e9.")
    ap.add_argument("--xscale", choices=["log", "linear"], default="log")
    ap.add_argument("--yscale", choices=["log", "linear"], default="log")
    ap.add_argument("--no-connect", action="store_true", help="don't draw the faint connector line")
    ap.add_argument("--drop-outliers", action="store_true",
                    help="Exclude points above the Tukey upper fence (Q3 + 1.5*IQR), computed "
                         "per subplot. The count, the threshold and the true maximum are "
                         "annotated on the subplot — the excursions are removed from the plot, "
                         "not from the record.")
    ap.add_argument("--ymax-percentile", type=float, default=None, metavar="P",
                    help="clip the y-axis to this percentile per subplot (e.g. 99). Points above "
                         "it are NOT dropped: they are pinned to the top edge as carets and the "
                         "count plus the true maximum are annotated. Use on linear axes, where a "
                         "single compile-time outlier (up to 874s here) flattens everything else.")
    ap.add_argument("-o", "--out")
    ap.add_argument("--dpi", type=int, default=300)
    ap.add_argument("--title", default=None)
    args = ap.parse_args()

    iv = load_records(args.interactive)
    std = {it: res for it, _, res in load_records(args.standard)}  # by iteration number
    ch = load_records(args.clickhouse) if args.clickhouse else []
    nq = max((len(res) for _, _, res in iv), default=0)
    if nq == 0:
        sys.exit("no query results in interactive file")

    labels = (args.query_labels.split(";") if args.query_labels else [f"Query {i+1}" for i in range(nq)])
    labels += [f"Query {i+1}" for i in range(len(labels), nq)]

    # per query: build (x, y, path) where path in {"iv","fb"}; count unresolved timeouts
    per_q = [[] for _ in range(nq)]
    unresolved = 0
    for q in range(nq):
        for it, raw, res in iv:
            if raw < args.min_rows or raw > args.max_rows:
                continue
            v = cell(res, q)
            if v is None:
                continue
            if v == "timeout":
                sres = std.get(it)
                sv = cell(sres, q) if sres is not None else None
                if isinstance(sv, (int, float)):
                    per_q[q].append((raw, float(sv) + args.timeout, "fb"))
                else:
                    unresolved += 1        # timed out on interactive AND no standard match
            elif isinstance(v, (int, float)):
                per_q[q].append((raw, float(v), "iv"))

    rows, cols = grid_dims(nq)
    fig_w = max(6.2 * cols, 9.5)
    fig, axes = plt.subplots(rows, cols, figsize=(fig_w, 4.6 * rows), squeeze=False)
    fig.patch.set_facecolor(BACKGROUND_COLOR)

    for q in range(nq):
        ax = axes[q // cols][q % cols]
        ax.set_facecolor(BACKGROUND_COLOR)
        pts = sorted(per_q[q])
        n_dropped, fence, dropped_max = 0, None, None
        if pts and args.drop_outliers and len(pts) >= 8:
            ys_all = sorted(p[1] for p in pts)
            n = len(ys_all)
            q1, q3 = ys_all[n // 4], ys_all[3 * n // 4]
            fence = q3 + 1.5 * (q3 - q1)
            outliers = [p for p in pts if p[1] > fence]
            if outliers:
                n_dropped = len(outliers)
                dropped_max = max(p[1] for p in outliers)
                pts = [p for p in pts if p[1] <= fence]
        clip_at, n_over, true_max = None, 0, None
        if pts and args.ymax_percentile is not None:
            ordered = sorted(p[1] for p in pts)
            k = min(len(ordered) - 1, int(round(args.ymax_percentile / 100.0 * (len(ordered) - 1))))
            clip_at = ordered[k]
            true_max = ordered[-1]
            n_over = sum(1 for p in pts if p[1] > clip_at)
        if pts:
            xs = [p[0] for p in pts]
            ys = [p[1] for p in pts]
            cs = [C_INTERACTIVE if p[2] == "iv" else C_FALLBACK for p in pts]
            if not args.no_connect:
                ax.plot(xs, ys, lw=0.8, color=C_LINE, alpha=0.5, zorder=2)
            if clip_at is None:
                ax.scatter(xs, ys, c=cs, s=14, edgecolor="black", linewidth=0.3, zorder=3)
            else:
                inside = [(x, y, c) for x, y, c in zip(xs, ys, cs) if y <= clip_at]
                above = [(x, c) for x, y, c in zip(xs, ys, cs) if y > clip_at]
                if inside:
                    ax.scatter([p[0] for p in inside], [p[1] for p in inside],
                               c=[p[2] for p in inside], s=14, edgecolor="black",
                               linewidth=0.3, zorder=3)
                # Pinned carets, so a clipped point is still visibly present at its x.
                if above:
                    ax.scatter([p[0] for p in above], [clip_at] * len(above),
                               c=[p[1] for p in above], s=44, marker="^",
                               edgecolor="white", linewidth=0.6, zorder=5, clip_on=False)
        # ClickHouse comparison line
        chp = sorted((raw, cell(res, q)) for _, raw, res in ch
                     if args.min_rows <= raw <= args.max_rows
                     and isinstance(cell(res, q), (int, float)))
        if chp:
            ax.plot([p[0] for p in chp], [p[1] for p in chp], lw=2.0,
                    color=C_CLICKHOUSE, zorder=4)
        ax.set_xscale(args.xscale)
        ax.set_yscale(args.yscale)
        if n_dropped:
            # Never a silent cap: state how many points were removed and how far out they went.
            ax.text(0.015, 0.97,
                    f"{n_dropped} outliers excluded (> {human_secs(round(fence, 1))}, "
                    f"max {human_secs(round(dropped_max, 1))})",
                    transform=ax.transAxes, ha="left", va="top", fontsize=8.5,
                    color="white", zorder=6,
                    bbox=dict(boxstyle="round,pad=0.3", facecolor=BACKGROUND_COLOR,
                              edgecolor=GRID_COLOR, alpha=0.9))
        if clip_at is not None:
            ax.set_ylim(0 if args.yscale == "linear" else None, clip_at * 1.04)
            if n_over:
                # Never a silent cap: say how many points are off-scale and how far.
                # Just below the caret row, not on it and not at the bottom: the carets own the
                # top edge in this mode and the ClickHouse baseline owns y=0.
                ax.text(0.015, 0.90,
                        f"▲ {n_over} above {human_secs(clip_at)} (max {human_secs(true_max)})",
                        transform=ax.transAxes, ha="left", va="top", fontsize=8.5,
                        color="white", zorder=6,
                        bbox=dict(boxstyle="round,pad=0.3", facecolor=BACKGROUND_COLOR,
                                  edgecolor=GRID_COLOR, alpha=0.9))
        ax.set_title(labels[q], color="white", fontsize=12, pad=8)
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
        if q // cols == rows - 1:
            ax.set_xlabel(f"Raw rows ({args.xscale})", color="white", fontsize=10)
        if q % cols == 0:
            ax.set_ylabel(f"End-to-end latency ({args.yscale})\n↓ lower is better", color="white", fontsize=10)

    for k in range(nq, rows * cols):
        axes[k // cols][k % cols].set_visible(False)

    fig.suptitle(args.title or "Dashboard latency: interactive-wh timeout + standard fallback",
                 color="white", fontsize=14, fontweight="bold", y=0.995)
    handles = [
        Line2D([0], [0], marker="o", color="none", markerfacecolor=C_INTERACTIVE, markeredgecolor="black",
               markersize=8, label="ran on interactive wh"),
        Line2D([0], [0], marker="o", color="none", markerfacecolor=C_FALLBACK, markeredgecolor="black",
               markersize=8, label=f"timed out → fallback to standard wh (+{args.timeout:g}s)"),
    ]
    if ch:
        handles.append(Line2D([0], [0], color=C_CLICKHOUSE, lw=2.5, label="ClickHouse"))
    fig.legend(handles=handles, loc="upper center", bbox_to_anchor=(0.5, 0.95), ncol=len(handles),
               facecolor=BACKGROUND_COLOR, edgecolor=GRID_COLOR, labelcolor="white",
               fontsize=10, framealpha=0.9)
    fig.tight_layout(rect=(0, 0, 1, 0.90))

    if unresolved:
        print(f"note: {unresolved} interactive timeouts had no matching standard iteration (skipped)",
              file=sys.stderr)
    if args.out:
        fig.savefig(args.out, dpi=args.dpi, facecolor=fig.get_facecolor())
        print(f"wrote {args.out}", file=sys.stderr)
    else:
        plt.show()


if __name__ == "__main__":
    main()
