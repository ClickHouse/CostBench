#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""
Quotes ingest benchmark — materialized-view freshness lag over time.

Snowflake's MV is maintained by a serverless background service that falls behind
under sustained ~1M EPS ingest; `mv_latency.sh` polls SHOW MATERIALIZED VIEWS and
records `behind_by` (e.g. "14m28s"). ClickHouse's incremental MV is updated
synchronously on every INSERT, so it is always in sync — a flat 0s baseline.

Input: one or more Snowflake mv_latency JSONL files (SHOW MV dumps with `polled_at`
+ `behind_by`). Vendor is inferred from the filename (default Snowflake). A
ClickHouse 0s baseline is drawn across the same time span (disable with --no-baseline).

  python3 render_mv_lag.py _test/mv_latency_snowflake.jsonl --out _out/mv_lag.png --smooth 9

Style matches render_latency.py / ../../_viz2.
"""
import sys
import re
import csv
import json
import argparse
from datetime import datetime

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
    # Interactive-table refresh lag (one line per IT in the refresh-history CSV).
    # Purple/pink so they read as a distinct family from the cyan MV — lets us later
    # drop a Snowflake MV line onto the same axes without a colour clash.
    "Snowflake IT (aggregate)": "#A259FF",
    "Snowflake IT (raw)":       "#FF8AD8",
}

# Map the NAME column of an INTERACTIVE_TABLE_REFRESH_HISTORY dump to a series label.
IT_LABELS = {
    "QUOTES_DAILY_IT": "Snowflake IT (aggregate)",  # (sym,day) rollup, 1-min target lag
    "QUOTES_IT":       "Snowflake IT (raw)",         # raw copy,        10-min target lag
}
BACKGROUND_COLOR = "#2B2B2B"
GRID_COLOR = "#4A4A4A"
VOLUME_COLOR = "#B0B6BD"  # muted grey for the data-volume line (right axis)

# How each system maintains the rollup — shown in the legend.
MV_KIND = {
    "ClickHouse": "incremental MV",     # synchronous on every INSERT -> always 0s
    "Snowflake":  "serverless MV",       # background service; behind_by sampled every 60s
    "Databricks": "refresh-triggered MV", # periodic refresh; staleness ~= refresh interval
    "Snowflake IT (aggregate)": "1-min target lag",
    "Snowflake IT (raw)":       "10-min target lag",
}

# Configured TARGET_LAG per series, in MINUTES. Drawn as a dashed reference line so the chart
# shows the SLA the system promised and how far the measured staleness diverges from it.
TARGET_LAG_MIN = {
    "Snowflake IT (aggregate)": 1.0,    # 1-min target — violated under ~1M EPS (drifts to ~7 min)
    "Snowflake IT (raw)":       10.0,   # 10-min target — held (refresh lands ~2.5 min, well inside)
}

_DUR = re.compile(r"(\d+)\s*([hms])")


def vendor_from_name(path: str) -> str:
    # Match the FILENAME only, not the full path — the repo lives under a directory named
    # "Clickhouse", so matching the whole path would tag every file as ClickHouse.
    s = path.lower().replace("\\", "/").rsplit("/", 1)[-1]
    for name in VENDOR_COLOR:
        if name.lower() in s:
            return name
    return "Snowflake"


def parse_behind(s):
    """'14m28s' / '3m6s' / '0s' / '1h2m3s' -> seconds (float). None if unparseable."""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    parts = _DUR.findall(s)
    if not parts:
        try:
            return float(s)
        except ValueError:
            return None
    mult = {"h": 3600, "m": 60, "s": 1}
    return float(sum(int(n) * mult[u] for n, u in parts))


def parse_ts(s):
    return datetime.fromisoformat(str(s).replace("Z", "+00:00"))


def human_rows(x, _pos=None):
    if x <= 0:
        return "0"
    for div, suf in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if x >= div:
            return f"{x/div:g}{suf}"
    return f"{x:g}"


def load_volume(path):
    """(datetime, raw_rows) series from a runner JSONL (dashboard/drilldown), sorted."""
    pts = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            ts = rec.get("iteration_started_at") or rec.get("polled_at")
            rr = rec.get("raw_rows")
            if ts and rr is not None:
                pts.append((parse_ts(ts), rr))
    pts.sort()
    return pts


def interp_rows(vol, t):
    """Linear-interpolate raw_rows at time t from a sorted (datetime, rows) series."""
    if not vol:
        return None
    if t <= vol[0][0]:
        return vol[0][1]
    if t >= vol[-1][0]:
        return vol[-1][1]
    lo, hi = 0, len(vol) - 1
    while hi - lo > 1:                       # binary search for the bracketing pair
        mid = (lo + hi) // 2
        if vol[mid][0] <= t:
            lo = mid
        else:
            hi = mid
    (t0, r0), (t1, r1) = vol[lo], vol[hi]
    span = (t1 - t0).total_seconds() or 1.0
    return r0 + (r1 - r0) * (t - t0).total_seconds() / span


def load_lag(path):
    """Return [(datetime, lag_seconds), ...] from a lag file.

    Two shapes are supported:
      - JSONL  : Snowflake mv_latency dump  -> `polled_at` + `behind_by` ('14m28s').
      - CSV    : Databricks refresh log     -> `completed_at` + `seconds_since_prev_refresh`
                 (the gap between refreshes = worst-case staleness for that cycle).
    """
    pts = []
    if path.lower().endswith(".csv"):
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                ts = row.get("polled_at") or row.get("completed_at")
                bb = row.get("behind_by")
                if bb not in (None, "", "null"):
                    lag = parse_behind(bb)
                else:
                    sec = row.get("seconds_since_prev_refresh")
                    lag = None if sec in (None, "", "null") else float(sec)
                if ts and lag is not None:
                    pts.append((parse_ts(ts), lag))
    else:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                lag = parse_behind(rec.get("behind_by"))
                ts = rec.get("polled_at")
                if ts and lag is not None:
                    pts.append((parse_ts(ts), lag))
    pts.sort()
    return pts


def parse_it_ts(s):
    """Refresh-history timestamps look like '2026-06-16 02:37:40.431 -0700' (space before the
    tz offset). strptime with %z handles the offset directly and is version-independent."""
    return datetime.strptime(str(s).strip(), "%Y-%m-%d %H:%M:%S.%f %z")


def is_it_refresh(path):
    """True if `path` is an INTERACTIVE_TABLE_REFRESH_HISTORY CSV (has the staleness column)."""
    if not path.lower().endswith(".csv"):
        return False
    with open(path, newline="") as f:
        header = f.readline()
    return "STALENESS_AT_DONE_SEC" in header.upper()


def load_it_refresh(path):
    """IT refresh-history CSV -> {series_label: [(datetime, staleness_seconds), ...]}.

    One file holds every IT (QUOTES_DAILY_IT, QUOTES_IT, ...), so it yields multiple series
    keyed by table name. We plot SUCCEEDED refreshes only — SKIPPED rows are no-op cycles
    (nothing new to refresh) and carry no meaningful staleness. `STALENESS_AT_DONE_SEC` is the
    freshness gap at the instant the refresh completes = the IT's analogue of MV `behind_by`.
    """
    out = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            if (row.get("STATE") or "").upper() != "SUCCEEDED":
                continue
            name = row.get("NAME")
            sec = row.get("STALENESS_AT_DONE_SEC")
            ts = row.get("REFRESH_END_TIME")
            if not (name and ts) or sec in (None, "", "null"):
                continue
            label = IT_LABELS.get(name, f"Snowflake IT ({name})")
            out.setdefault(label, []).append((parse_it_ts(ts), float(sec)))
    for k in out:
        out[k].sort()
    return out


def rolling_median(ys, window):
    n, half, out = len(ys), window // 2, []
    for i in range(n):
        lo, hi = max(0, i - half), min(n, i + half + 1)
        out.append(sorted(ys[lo:hi])[(hi - lo) // 2])
    return out


def rolling_mean(ys, window):
    """Centered moving average — rides through the refresh sawtooth more smoothly than the
    median (which snaps between the low/high of each cycle)."""
    n, half, out = len(ys), window // 2, []
    for i in range(n):
        lo, hi = max(0, i - half), min(n, i + half + 1)
        out.append(sum(ys[lo:hi]) / (hi - lo))
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("files", nargs="+", help="Snowflake mv_latency JSONL file(s).")
    ap.add_argument("-o", "--out", help="Output PNG.")
    ap.add_argument("--dpi", type=int, default=300)
    ap.add_argument("--title", default=None)
    ap.add_argument("--no-title", action="store_true")
    ap.add_argument("--ylabel", default="MV lag behind base table (minutes)\n↓ lower is fresher",
                    help="Left y-axis label (e.g. 'IT freshness lag behind base table (minutes)').")
    ap.add_argument("--target", action="append", default=[], metavar="LABEL=MIN",
                    help="Override/add a target-lag reference line (minutes) for a series, e.g. "
                         "'Snowflake IT (aggregate)=1'. Defaults cover the interactive tables.")
    ap.add_argument("--no-targets", action="store_true",
                    help="Don't draw the target-lag reference lines.")
    ap.add_argument("--smooth", type=int, default=0, metavar="W",
                    help="Rolling-median window (odd). Raw faint + bold trend. 0 = raw only.")
    ap.add_argument("--no-raw", action="store_true",
                    help="With --smooth, draw only the rolling-median trend (hide the faint "
                         "raw line).")
    ap.add_argument("--no-baseline", action="store_true",
                    help="Don't draw the ClickHouse always-in-sync 0s baseline.")
    ap.add_argument("--mv-kind", default=None,
                    help="Override the MV-kind label shown in the legend (e.g. 'interactive MV').")
    ap.add_argument("--smooth-mode", choices=["median", "mean"], default="median",
                    help="Smoothing statistic for --smooth: 'median' (default) or 'mean' "
                         "(moving average — smoother through the refresh sawtooth).")
    ap.add_argument("--volume-from", metavar="FILE",
                    help="Runner JSONL (e.g. dashboard_snowflake.jsonl) with "
                         "iteration_started_at + raw_rows. If set, the x-axis becomes base-"
                         "table row count (interpolated at each poll) instead of elapsed time.")
    ap.add_argument("--xscale", choices=["log", "linear"], default="linear",
                    help="X-axis scale when --volume-from is used (default linear).")
    ap.add_argument("--volume-line", metavar="FILE",
                    help="Runner JSONL with iteration_started_at + raw_rows. Keeps the x-axis "
                         "as elapsed time and overlays growing data volume as a line on a "
                         "second (right) y-axis. Ignored if --volume-from is set.")
    ap.add_argument("--xmax", type=float, default=None,
                    help="Clip the x-axis to this max (in x-axis units: hours for elapsed-time "
                         "mode, rows for --volume-from). e.g. --xmax 24 for a 24h window.")
    args = ap.parse_args()

    targets = {} if args.no_targets else dict(TARGET_LAG_MIN)
    for spec in args.target:
        if "=" not in spec:
            sys.exit(f"--target must be LABEL=MIN, got: {spec}")
        lbl, mn = spec.rsplit("=", 1)
        targets[lbl] = float(mn)

    vol = load_volume(args.volume_from) if args.volume_from else None
    vol_line = (load_volume(args.volume_line)
                if (args.volume_line and not args.volume_from) else None)

    # series-label -> [(datetime, lag_seconds), ...]. Most files map to one vendor; an
    # INTERACTIVE_TABLE_REFRESH_HISTORY CSV fans out into one series per IT (by table name).
    raw = {}
    for path in args.files:
        if is_it_refresh(path):
            for label, pts in load_it_refresh(path).items():
                raw.setdefault(label, []).extend(pts)
        else:
            v = vendor_from_name(path)
            raw.setdefault(v, []).extend(load_lag(path))
    raw = {v: sorted(pts) for v, pts in raw.items() if pts}
    if not raw:
        sys.exit("No usable lag records found.")

    # Elapsed time is measured per system from ITS OWN ingest start, so runs that began at
    # different wall-clock times still line up at "t hours into ingest".
    t0_by_vendor = {v: pts[0][0] for v, pts in raw.items()}

    by_rows = vol is not None
    series = {}
    xmax = 0.0
    for v, pts in raw.items():
        if by_rows:
            xs = [interp_rows(vol, t) for t, _ in pts]
        else:
            xs = [(t - t0_by_vendor[v]).total_seconds() / 3600.0 for t, _ in pts]
        ys = [lag for _, lag in pts]
        # drop points where x is unresolved or beyond the requested --xmax cap
        xy = [(x, y) for x, y in zip(xs, ys)
              if x is not None and (args.xmax is None or x <= args.xmax)]
        if not xy:
            continue
        series[v] = ([p[0] for p in xy], [p[1] for p in xy])
        xmax = max(xmax, max(series[v][0]))
    if args.xmax is not None:
        xmax = args.xmax

    fig, ax = plt.subplots(figsize=(11, 5))
    fig.patch.set_facecolor(BACKGROUND_COLOR)
    ax.set_facecolor(BACKGROUND_COLOR)

    handles, labels = [], []

    # ClickHouse always-in-sync baseline at 0s across the full span
    if not args.no_baseline and "ClickHouse" not in series:
        x0 = 1.0 if (by_rows and args.xscale == "log") else 0.0
        (line,) = ax.plot([x0, xmax], [0, 0], lw=2.2, color=VENDOR_COLOR["ClickHouse"],
                          zorder=3)
        handles.append(line)
        labels.append(f"ClickHouse · {MV_KIND['ClickHouse']} (always 0s)")

    order = ["Snowflake IT (aggregate)", "Snowflake IT (raw)",
             "Snowflake", "Databricks", "Redshift", "BigQuery", "ClickHouse"]
    ymax_data = 0.0
    for v in sorted(series, key=lambda x: (order.index(x) if x in order else 99)):
        xs, ys = series[v]
        color = VENDOR_COLOR.get(v, "#FFFFFF")
        # y in minutes for readability
        ym = [y / 60.0 for y in ys]
        if args.smooth and args.smooth > 1 and len(ym) >= 3:
            if not args.no_raw:
                ax.plot(xs, ym, lw=0.7, color=color, alpha=0.22, zorder=2)
            plot_y = (rolling_mean if args.smooth_mode == "mean" else rolling_median)(ym, args.smooth)
        else:
            plot_y = ym
        (line,) = ax.plot(xs, plot_y, lw=2.2 if args.smooth else 1.6, color=color, zorder=3)
        handles.append(line)
        ymax_data = max(ymax_data, max(plot_y))
        peak = max(plot_y)   # from the plotted (smoothed) series so the legend peak matches the line

        tgt = targets.get(v)
        if tgt is not None:
            # Dashed target-lag reference line + shading of the region where measured staleness
            # diverges ABOVE the target (an SLA breach). The shaded area = "how much we divert".
            ax.plot([0, xmax], [tgt, tgt], lw=1.3, ls=(0, (6, 3)), color=color, alpha=0.55, zorder=2)
            ax.fill_between(xs, tgt, plot_y, where=[y > tgt for y in plot_y], interpolate=True,
                            color=color, alpha=0.13, zorder=1)
            ax.text(xmax * 0.992, tgt, f" {tgt:g}-min target", color=color, alpha=0.8,
                    fontsize=8.5, ha="right", va="bottom", zorder=4)
            if peak > tgt * 1.2:                     # breaching the target
                status = f"peak {peak:.0f} min — {peak/tgt:.0f}× over {tgt:g}-min target"
            else:                                    # comfortably inside the target
                status = f"peak {peak:.1f} min — within {tgt:g}-min target"
            labels.append(f"{v} · {status}")
        else:
            kind = args.mv_kind or MV_KIND.get(v, "MV")
            peak_txt = f"{peak:.1f} min" if peak < 10 else f"{peak:.0f} min"
            labels.append(f"{v} · {kind} (peak {peak_txt} behind)")

    # Headroom so the highest target line (e.g. 10-min raw) and its annotation stay on-canvas.
    ytop = max(ymax_data, max(targets.values(), default=0.0)) * 1.15 or 1.0
    ax.set_ylim(bottom=0, top=ytop)
    if by_rows:
        ax.set_xscale(args.xscale)
        ax.set_xlim((1.0 if args.xscale == "log" else 0.0), xmax)
        ax.xaxis.set_major_formatter(FuncFormatter(human_rows))
        ax.set_xlabel("Base-table row count", color="white", fontsize=11)
    else:
        ax.set_xlim(0, xmax)
        ax.set_xlabel("Elapsed ingest time (hours)", color="white", fontsize=11)
    ax.set_ylabel(args.ylabel, color="white", fontsize=11)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _p: f"{y:g}m"))
    ax.tick_params(colors="white", labelsize=10)
    ax.grid(True, which="major", color=GRID_COLOR, lw=0.6, alpha=0.7, zorder=0)
    for side in ("right", "top"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color("white")

    # data-volume line on a second (right) y-axis, sharing the elapsed-time x. Elapsed is
    # measured from the volume file's own system start (matching that system's lag series).
    if vol_line and not by_rows:
        vol_v = vendor_from_name(args.volume_line)
        base_t0 = t0_by_vendor.get(vol_v, vol_line[0][0])
        ax2 = ax.twinx()
        vxy = [((t - base_t0).total_seconds() / 3600.0, r) for t, r in vol_line]
        vxy = [(x, r) for x, r in vxy if x >= 0 and (args.xmax is None or x <= args.xmax)]
        vx = [x for x, _ in vxy]
        vy = [r for _, r in vxy]
        (vline,) = ax2.plot(vx, vy, color=VOLUME_COLOR, lw=2.0, ls=(0, (5, 2)), zorder=2)
        ax2.set_ylim(bottom=0)
        ax2.set_xlim(0, xmax)
        ax2.set_ylabel("Base-table rows ingested", color=VOLUME_COLOR, fontsize=11)
        ax2.yaxis.set_major_formatter(FuncFormatter(human_rows))
        ax2.tick_params(axis="y", colors=VOLUME_COLOR, labelsize=10)
        ax2.spines["top"].set_visible(False)
        ax2.spines["left"].set_visible(False)
        ax2.spines["right"].set_color(VOLUME_COLOR)
        # keep the lag lines visually on top of the volume line
        ax.set_zorder(ax2.get_zorder() + 1)
        ax.patch.set_visible(False)
        handles.append(vline)
        labels.append("Rows ingested (right axis)")

    ax.legend(handles, labels, loc="upper left", facecolor=BACKGROUND_COLOR,
              edgecolor=GRID_COLOR, labelcolor="white", fontsize=10, framealpha=0.9)

    if not args.no_title:
        ax.set_title(args.title or "Materialized-view freshness lag under ~1M EPS ingest",
                     color="white", fontsize=14, pad=12)

    fig.tight_layout()
    if args.out:
        fig.savefig(args.out, dpi=args.dpi, facecolor=fig.get_facecolor())
        print(f"wrote {args.out}", file=sys.stderr)
    else:
        plt.show()


if __name__ == "__main__":
    main()
