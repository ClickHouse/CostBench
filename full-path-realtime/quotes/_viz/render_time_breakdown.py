#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""
Query latency broken into COMPILATION vs EXECUTION, stacked area, linear axes.

Answers "where did the time actually go" for a single system, which a latency line
cannot show. On a streaming-fed Snowflake interactive MV, compilation ran ~10x
execution — the runners originally recorded EXECUTION_TIME only and so reported a
latency ~12x lower than the real elapsed time.

Input: one JSONL from a backfilled runner file (see snowflake/t2/backfill_timings.py),
carrying three same-shape arrays per record:
  {"raw_rows": int, "system": str, "machine"/"cluster_size": str,
   "result":           [[sec_or_"timeout"], ...],   # TOTAL_ELAPSED_TIME
   "compilation_time": [[sec_or_null], ...],
   "execution_time":   [[sec_or_null], ...]}

Two bands: compilation and execution. Note these do NOT sum to elapsed — there is also
queueing, and on the interactive warehouse a further 1.5-4s that Snowflake attributes to
neither phase. That remainder is computed and reported in the subtitle as an excluded
percentage rather than drawn, so the chart never implies the stack is the whole latency.
On the MV-std arm it is <0.1%, so the envelope still tracks the latency line charts.

Usage:
  python3 render_time_breakdown.py _test/dash_mv_std_snowflake.jsonl \
      --out _out/t2/t2_dash_mvstd_breakdown_linear.png --smooth 7 \
      --query-labels "Single-symbol summary;Watchlist summary;Top movers;Daily activity" \
      --title "Dashboard on the interactive MV (standard wh): compile vs execute"

Style matches render_latency.py (dark theme, Inter, same 3% x-padding and the same
rolling-median smoothing, so this chart's envelope coincides with the latency line
charts' curve). Palette: teal (compilation) / orange (execution)
— see the palette note above the BANDS table for the measured checks.
"""
import sys
import json
import math
import argparse

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.font_manager as _fm
import matplotlib.patheffects as pe
from matplotlib.ticker import FuncFormatter

matplotlib.rcParams["font.family"] = (
    "Inter" if any(f.name == "Inter" for f in _fm.fontManager.ttflist) else "DejaVu Sans"
)
matplotlib.rcParams["font.sans-serif"] = ["Inter", "DejaVu Sans"]
matplotlib.rcParams["axes.titleweight"] = "bold"

BACKGROUND_COLOR = "#2B2B2B"
GRID_COLOR = "#4A4A4A"
INK_PRIMARY = "#FFFFFF"
INK_SECONDARY = "#C3C2B7"

# Compilation = deep teal, execution = orange. Teal is unused anywhere else in _viz, and is
# deliberately NOT from the blue/purple/yellow families, which now carry meaning across the
# chart set (standard warehouse / interactive warehouse / ClickHouse). Checked on the #2B2B2B
# surface: OKLab L 0.624 (in the 0.48-0.67 band), chroma 0.108, contrast 4.16:1, and against
# the adjacent orange band dE 26.8 normal / 15.4 protan / 14.0 deutan / 35.5 tritan — all
# clear of the 8 floor. A green would have read better against the other charts but collapses
# to dE ~9 against orange under deuteranopia, which is the one adjacency that matters here.
BANDS = [
    ("compilation_time", "Compilation", "#0E9C90"),
    ("execution_time",   "Execution",   "#d95926"),
]
GAP_LW = 2.0        # 2px surface gap between stacked fills
LABEL_MIN_FRAC = 0.10   # only direct-label a band thicker than this share of the y-range


def human_rows(x, _pos=None):
    if x <= 0:
        return "0"
    for div, suf in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if x >= div:
            return f"{x / div:g}{suf}"
    return f"{x:g}"


def human_secs(y, _pos=None):
    if y >= 1:
        return f"{y:g}s"
    return f"{y * 1000:g}ms"


def grid_dims(n):
    return {1: (1, 1), 2: (1, 2), 3: (1, 3), 4: (2, 2)}.get(n, (math.ceil(n / 3), 3))


def rolling_median(ys, window):
    """Centered rolling median; edges shrink the window. Identical to render_latency.py's,
    so this chart and the latency line charts smooth with the same estimator."""
    n, half, out = len(ys), window // 2, []
    for i in range(n):
        lo, hi = max(0, i - half), min(n, i + half + 1)
        out.append(sorted(ys[lo:hi])[(hi - lo) // 2])
    return out


def smooth_bands(compile_s, exec_s, other_s, window):
    """Smooth the CUMULATIVE boundaries, then difference them to get the bands.

    Smoothing each band separately would need a mean (the median is not additive), and a
    mean does not suppress outliers — it spreads them, so this chart's total would sit far
    above the rolling-median curve render_latency.py draws for the same data. Smoothing the
    three cumulative curves instead gives a top boundary that IS rolling_median(total),
    exactly the latency line charts' series. Bands stay non-negative because the median is
    an order statistic: cum1 <= cum2 <= cum3 pointwise implies the same after smoothing."""
    cum1 = list(compile_s)
    cum2 = [a + b for a, b in zip(compile_s, exec_s)]
    cum3 = [a + b + c for a, b, c in zip(compile_s, exec_s, other_s)]
    b1, b2, b3 = (rolling_median(c, window) for c in (cum1, cum2, cum3))
    return [b1,
            [y - x for x, y in zip(b1, b2)],
            [y - x for x, y in zip(b2, b3)]]


def tier_of(rec):
    machine = str(rec.get("machine") or "").strip()
    cs = rec.get("cluster_size")
    if machine and isinstance(cs, int):
        return f"{machine} x{cs}"
    return machine or str(cs or "").strip()


def load(path, max_rows=float("inf")):
    """-> (system, tier, n_queries, per_query[{x, compile, exec, other}], dropped)."""
    recs = []
    with open(path) as f:
        for line in f:
            if line.strip():
                recs.append(json.loads(line))
    if not recs:
        sys.exit(f"No records in {path}")
    for key in ("compilation_time", "execution_time"):
        if key not in recs[0]:
            sys.exit(f"{path} has no '{key}' — this needs a backfilled results file "
                     f"(see snowflake/t2/backfill_timings.py).")

    n_q = len(recs[0]["result"])
    series = [{"x": [], "compile": [], "exec": [], "other": []} for _ in range(n_q)]
    dropped = 0
    for rec in recs:
        x = rec.get("raw_rows", 0) or 0
        if x > max_rows:
            continue
        for q in range(n_q):
            total = rec["result"][q][0]
            comp = rec["compilation_time"][q][0]
            ex = rec["execution_time"][q][0]
            # A timed-out query is censored at the warehouse cap, not measured: it has no
            # total to decompose. Drop it and report the count rather than drawing a
            # 5s bar that looks like a latency.
            if not isinstance(total, (int, float)) or comp is None:
                dropped += 1
                continue
            comp = max(0.0, comp)
            ex = max(0.0, ex or 0.0)
            s = series[q]
            s["x"].append(x)
            s["compile"].append(comp)
            s["exec"].append(ex)
            s["other"].append(max(0.0, total - comp - ex))
    for s in series:
        order = sorted(range(len(s["x"])), key=lambda i: s["x"][i])
        for k in ("x", "compile", "exec", "other"):
            s[k] = [s[k][i] for i in order]
    return recs[0].get("system", "unknown"), tier_of(recs[0]), n_q, series, dropped


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("jsonl")
    ap.add_argument("--out", required=True)
    ap.add_argument("--title", default="Query latency breakdown: compilation vs execution")
    ap.add_argument("--query-labels", default="")
    ap.add_argument("--max-rows", type=float, default=float("inf"), metavar="N",
                    help="Drop iterations above this row count, e.g. 100e9.")
    ap.add_argument("--smooth", type=int, default=7,
                    help="centered rolling-median window in iterations (1 = raw)")
    ap.add_argument("--share", action="store_true",
                    help="plot each band as a share of total (0-100%%) instead of seconds")
    args = ap.parse_args()

    system, tier, n_q, series, dropped = load(args.jsonl, args.max_rows)
    labels = [s.strip() for s in args.query_labels.split(";") if s.strip()]
    labels += [f"Query {i + 1}" for i in range(len(labels), n_q)]

    rows, cols = grid_dims(n_q)
    fig, axes = plt.subplots(rows, cols, figsize=(7.2 * cols, 5.0 * rows), squeeze=False)
    fig.patch.set_facecolor(BACKGROUND_COLOR)

    med_share = []
    # Median share per band across every query, used to annotate the legend. A thin band
    # cannot carry a direct label without the clipping anti-patterns.md warns about, so the
    # legend carries its magnitude instead.
    band_shares = {name: [] for _, name, _c in BANDS}
    residual = []       # the queue/overhead time the two plotted bands do NOT cover
    for q in range(n_q):
        s = series[q]
        tot = [a + b + c for a, b, c in zip(s["compile"], s["exec"], s["other"])]
        for (key, name, _c), vals in zip(BANDS, (s["compile"], s["exec"], s["other"])):
            band_shares[name].extend(v / t for v, t in zip(vals, tot) if t > 0)
        residual.extend(v / t for v, t in zip(s["other"], tot) if t > 0)
    band_label = {}
    for _k, name, _c in BANDS:
        vs = sorted(band_shares[name])
        pct = 100 * vs[len(vs) // 2] if vs else 0.0
        band_label[name] = f"{name} · {pct:.0f}%" if pct >= 0.5 else f"{name} · <1%"

    for q in range(n_q):
        ax = axes[q // cols][q % cols]
        ax.set_facecolor(BACKGROUND_COLOR)
        s = series[q]
        if not s["x"]:
            ax.text(0.5, 0.5, "no data", ha="center", va="center",
                    color=INK_SECONDARY, transform=ax.transAxes)
            continue

        bands = smooth_bands(s["compile"], s["exec"], s["other"], max(1, args.smooth))
        if args.share:
            tot = [max(1e-9, a + b + c) for a, b, c in zip(*bands)]
            bands = [[100.0 * v / t for v, t in zip(b, tot)] for b in bands]

        # median compile share over the un-smoothed points, for the caption
        tot_raw = [a + b + c for a, b, c in zip(s["compile"], s["exec"], s["other"])]
        shares = sorted(c / t for c, t in zip(s["compile"], tot_raw) if t > 0)
        if shares:
            med_share.append(shares[len(shares) // 2])

        lower = [0.0] * len(s["x"])
        for (_, name, color), vals in zip(BANDS, bands):
            upper = [lo + v for lo, v in zip(lower, vals)]
            ax.fill_between(s["x"], lower, upper, facecolor=color, linewidth=0,
                            label=band_label[name] if q == 0 else None, zorder=2)
            # 2px surface gap so touching fills never bleed into one another
            ax.plot(s["x"], upper, color=BACKGROUND_COLOR, lw=GAP_LW,
                    solid_capstyle="butt", zorder=3)
            lower = upper

        # Same 3% x-padding render_latency.py applies, so this chart's right edge lines up
        # with the latency line charts' instead of running flush to the spine.
        ax.set_xlim(min(s["x"]) * 0.97, max(s["x"]) * 1.03)
        top = max(lower) * 1.08 if not args.share else 100.0
        ax.set_ylim(0, top)

        # Direct labels, so identity is never colour-alone — but only where the band is
        # thick enough to hold text without the clipping the anti-pattern list warns about.
        lower = [0.0] * len(s["x"])
        for (_, name, _c), vals in zip(BANDS, bands):
            upper = [lo + v for lo, v in zip(lower, vals)]
            # Pick the thickest point, but only from the interior: in --share mode the bands
            # are near-constant so a plain argmax lands on the last sample and the centered
            # label overflows the right spine.
            lo_i, hi_i = int(0.12 * len(vals)), max(1, int(0.88 * len(vals)))
            i = max(range(lo_i, hi_i), key=lambda j: vals[j])
            if vals[i] >= LABEL_MIN_FRAC * top:
                ax.text(s["x"][i], (lower[i] + upper[i]) / 2, name.split(" (")[0],
                        ha="center", va="center", fontsize=10, fontweight="bold",
                        color=INK_PRIMARY, zorder=5,
                        path_effects=[pe.withStroke(linewidth=2.5, foreground="#141414")])
            lower = upper

        ax.set_title(labels[q], color=INK_PRIMARY, fontsize=13, pad=10)
        ax.xaxis.set_major_formatter(FuncFormatter(human_rows))
        ax.yaxis.set_major_formatter(FuncFormatter(
            (lambda y, _p: f"{y:g}%") if args.share else human_secs))
        ax.grid(True, which="major", color=GRID_COLOR, lw=0.6, alpha=0.7, zorder=0)
        ax.set_axisbelow(True)
        for spine in ax.spines.values():
            spine.set_color(GRID_COLOR)
        ax.tick_params(colors=INK_SECONDARY, labelsize=10)
        if q // cols == rows - 1:
            ax.set_xlabel("Raw rows (linear)", color=INK_SECONDARY, fontsize=11)
        if q % cols == 0:
            ax.set_ylabel("Share of latency" if args.share
                          else "Query latency (linear)\n↓ lower is better",
                          color=INK_SECONDARY, fontsize=11)

    for k in range(n_q, rows * cols):
        axes[k // cols][k % cols].axis("off")

    fig.suptitle(args.title, color=INK_PRIMARY, fontsize=17, fontweight="bold", y=0.985)
    sub = f"{system}"
    if tier:
        sub += f" · {tier}"
    if med_share:
        sub += (f"  ·  compilation is {100 * sum(med_share) / len(med_share):.0f}% "
                f"of median latency")
    # Compilation + execution is not the whole of elapsed time. Say so rather than letting
    # the stack imply it, now that the remainder is no longer drawn as its own band.
    if residual:
        r = 100 * sum(residual) / len(residual)
        sub += f"  ·  excludes queue/overhead ({'<0.1' if r < 0.1 else f'{r:.1f}'}% of elapsed)"
    if dropped:
        sub += f"  ·  {dropped} timed-out queries excluded (censored at the cap, not measured)"
    fig.text(0.5, 0.945, sub, ha="center", color=INK_SECONDARY, fontsize=11)

    handles, lbls = axes[0][0].get_legend_handles_labels()
    leg = fig.legend(handles, lbls, loc="upper center", bbox_to_anchor=(0.5, 0.93),
                     ncol=len(BANDS), frameon=True, facecolor=BACKGROUND_COLOR,
                     edgecolor=GRID_COLOR, fontsize=11)
    for t in leg.get_texts():
        t.set_color(INK_PRIMARY)

    fig.tight_layout(rect=(0, 0, 1, 0.905))
    fig.savefig(args.out, dpi=200, facecolor=BACKGROUND_COLOR)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
