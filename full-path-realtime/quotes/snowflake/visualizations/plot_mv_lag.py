#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Plot Snowflake persisted-MV refresh lag against interpolated base-table rows."""

from __future__ import annotations

import argparse
import bisect
import math
import re
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from _common import (
    CLICKHOUSE,
    GRID,
    SNOWFLAKE,
    configure_figure,
    human_rows,
    iter_jsonl,
    resolve_layout,
    save_figure,
    sha256,
    write_csv,
    write_json,
)


@dataclass(frozen=True)
class Point:
    source_line: int
    observed_at: str
    base_table_rows: int
    lag_seconds: float
    refresh_watermark: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--freshness", type=Path, required=True)
    parser.add_argument("--dashboard", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--basename", default="mv_lag_clickhouse_vs_snowflake")
    parser.add_argument(
        "--display-smooth-window",
        type=int,
        default=1,
        help="Optional centered rolling-mean window applied only to the rendered line",
    )
    parser.add_argument(
        "--curve-interpolation",
        choices=("none", "pchip"),
        default="none",
        help="Optional shape-preserving interpolation applied only to the rendered line",
    )
    parser.add_argument(
        "--curve-points",
        type=int,
        default=1600,
        help="Number of rendered curve points when --curve-interpolation=pchip",
    )
    parser.add_argument("--dpi", type=int, default=300)
    parser.add_argument("--wide", action="store_true", help="Render the Keynote-native 5156x2900 staged variant and suffix outputs with _wide.")
    return parser.parse_args()


def timestamp(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def lag_seconds(value: str) -> float:
    matches = re.findall(r"(\d+(?:\.\d+)?)([dhms])", value)
    if not matches:
        raise ValueError(f"cannot parse Snowflake behind_by={value!r}")
    return sum(float(number) * {"d": 86400, "h": 3600, "m": 60, "s": 1}[unit] for number, unit in matches)


def dashboard_timeline(path: Path) -> tuple[list[tuple[float, int]], dict[str, Any]]:
    values: list[tuple[float, int, int]] = []
    for line, record in iter_jsonl(path):
        rows = int(record.get("raw_rows") or 0)
        if rows > 0:
            values.append((timestamp(str(record["iteration_started_at"])), rows, line))
    values.sort()
    if len(values) < 2:
        raise ValueError("need at least two dashboard observations")
    final_rows = max(item[1] for item in values)
    endpoint = next(item for item in values if item[1] >= final_rows)
    active = [(when, rows) for when, rows, _ in values if when <= endpoint[0]]
    return active, {
        "final_rows": final_rows,
        "active_endpoint_timestamp": datetime.fromtimestamp(endpoint[0]).astimezone().isoformat(),
        "active_endpoint_source_line": endpoint[2],
        "dashboard_records": len(values),
    }


def interpolate(timeline: list[tuple[float, int]], when: float) -> int:
    times = [item[0] for item in timeline]
    if when <= times[0]: return timeline[0][1]
    if when >= times[-1]: return timeline[-1][1]
    right = bisect.bisect_right(times, when)
    t0, r0 = timeline[right - 1]
    t1, r1 = timeline[right]
    return round(r0 + (r1 - r0) * ((when - t0) / (t1 - t0)))


def load_points(path: Path, timeline: list[tuple[float, int]]) -> tuple[list[Point], dict[str, int]]:
    points: list[Point] = []
    read = invalid = after = 0
    end_time = timeline[-1][0]
    for line, record in iter_jsonl(path):
        read += 1
        if int(record.get("rows") or 0) <= 0 or str(record.get("refreshed_on", "")).startswith("1969-"):
            invalid += 1
            continue
        when = timestamp(str(record["polled_at"]))
        if when > end_time:
            after += 1
            continue
        points.append(Point(line, str(record["polled_at"]), interpolate(timeline, when), lag_seconds(str(record["behind_by"])), str(record["refreshed_on"])))
    if len(points) < 3:
        raise ValueError("not enough active freshness observations")
    return points, {"records_read": read, "initial_invalid_excluded": invalid, "post_ingestion_excluded": after}


def complete_cycle_maxima(points: list[Point]) -> list[Point]:
    cycles: list[list[Point]] = []
    current: list[Point] = []
    watermark = ""
    for point in points:
        if current and point.refresh_watermark != watermark:
            cycles.append(current); current = []
        current.append(point); watermark = point.refresh_watermark
    if current: cycles.append(current)
    if len(cycles) < 3: raise ValueError("need at least three refresh cycles")
    return [max(cycle, key=lambda p: p.lag_seconds) for cycle in cycles[1:-1]]


def rolling_mean(values: list[float], window: int) -> list[float]:
    half = window // 2
    return [
        statistics.fmean(values[max(0, index - half):min(len(values), index + half + 1)])
        for index in range(len(values))
    ]


def validate_odd_window(value: int, name: str) -> None:
    if value < 1 or value % 2 == 0:
        raise ValueError(f"{name} must be a positive odd integer")


def unique_mean_series(xs: list[int], ys: list[float]) -> tuple[list[float], list[float]]:
    """Collapse duplicate x coordinates without changing their local mean."""
    grouped: list[tuple[int, list[float]]] = []
    for x, y in zip(xs, ys, strict=True):
        if grouped and grouped[-1][0] == x:
            grouped[-1][1].append(y)
        else:
            grouped.append((x, [y]))
    return [float(x) for x, _ in grouped], [statistics.fmean(values) for _, values in grouped]


def pchip_slopes(xs: list[float], ys: list[float]) -> list[float]:
    """Fritsch-Carlson/PCHIP slopes: C1 smooth and shape preserving."""
    count = len(xs)
    if count < 2:
        raise ValueError("PCHIP needs at least two unique x coordinates")
    widths = [xs[index + 1] - xs[index] for index in range(count - 1)]
    if any(width <= 0 for width in widths):
        raise ValueError("PCHIP x coordinates must be strictly increasing")
    secants = [(ys[index + 1] - ys[index]) / widths[index] for index in range(count - 1)]
    if count == 2:
        return [secants[0], secants[0]]

    slopes = [0.0] * count
    for index in range(1, count - 1):
        left = secants[index - 1]
        right = secants[index]
        if left == 0 or right == 0 or left * right < 0:
            slopes[index] = 0.0
            continue
        w1 = 2 * widths[index] + widths[index - 1]
        w2 = widths[index] + 2 * widths[index - 1]
        slopes[index] = (w1 + w2) / (w1 / left + w2 / right)

    def endpoint(width0: float, width1: float, delta0: float, delta1: float) -> float:
        slope = ((2 * width0 + width1) * delta0 - width0 * delta1) / (width0 + width1)
        if slope * delta0 <= 0:
            return 0.0
        if delta0 * delta1 < 0 and abs(slope) > abs(3 * delta0):
            return 3 * delta0
        return slope

    slopes[0] = endpoint(widths[0], widths[1], secants[0], secants[1])
    slopes[-1] = endpoint(widths[-1], widths[-2], secants[-1], secants[-2])
    return slopes


def pchip_curve(xs: list[float], ys: list[float], points: int) -> tuple[list[float], list[float]]:
    if points < 2:
        raise ValueError("--curve-points must be at least 2")
    slopes = pchip_slopes(xs, ys)
    render_xs = [xs[0] + (xs[-1] - xs[0]) * index / (points - 1) for index in range(points)]
    render_ys: list[float] = []
    segment = 0
    for x in render_xs:
        while segment < len(xs) - 2 and x > xs[segment + 1]:
            segment += 1
        width = xs[segment + 1] - xs[segment]
        t = (x - xs[segment]) / width
        h00 = 2 * t**3 - 3 * t**2 + 1
        h10 = t**3 - 2 * t**2 + t
        h01 = -2 * t**3 + 3 * t**2
        h11 = t**3 - t**2
        render_ys.append(
            h00 * ys[segment]
            + h10 * width * slopes[segment]
            + h01 * ys[segment + 1]
            + h11 * width * slopes[segment + 1]
        )
    return render_xs, render_ys


def main() -> int:
    args = parse_args()
    validate_odd_window(args.display_smooth_window, "--display-smooth-window")
    freshness = args.freshness.expanduser().resolve()
    dashboard = args.dashboard.expanduser().resolve()
    output = args.output_dir.expanduser().resolve()
    timeline, timeline_meta = dashboard_timeline(dashboard)
    active, load_meta = load_points(freshness, timeline)
    plotted = active
    final_rows = timeline_meta["final_rows"]
    window = 61
    ys = rolling_mean([p.lag_seconds / 60 for p in plotted], window)
    average = statistics.fmean(ys)
    maximum = max(ys)
    display_ys = rolling_mean(ys, args.display_smooth_window)
    source_xs = [p.base_table_rows for p in plotted]
    unique_xs, unique_ys = unique_mean_series(source_xs, display_ys)
    if args.curve_interpolation == "pchip":
        render_xs, render_ys = pchip_curve(unique_xs, unique_ys, args.curve_points)
    else:
        render_xs, render_ys = unique_xs, unique_ys

    basename, figure_size, layout = resolve_layout(
        args.basename, args.wide, (11, 5), dpi=args.dpi
    )
    fig, axis = plt.subplots(
        figsize=figure_size, dpi=args.dpi if args.wide else None
    )
    plot_background = configure_figure(fig, wide=args.wide)
    axis.set_facecolor(plot_background)
    line_width = 3.2 if args.wide else 2.4
    label_size = 16 if args.wide else 12
    tick_size = 14 if args.wide else 10.5
    legend_size = 14 if args.wide else 10.5
    ch, = axis.plot([0, final_rows], [0, 0], color=CLICKHOUSE, linewidth=line_width, zorder=4)
    sf, = axis.plot(render_xs, render_ys, color=SNOWFLAKE, linewidth=line_width, zorder=3)
    axis.set_xlim(0, final_rows); axis.set_ylim(0, max(2, math.ceil(maximum * 1.15)))
    axis.xaxis.set_major_formatter(FuncFormatter(human_rows))
    axis.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:g}m"))
    axis.set_xlabel("Base-table row count", color="white", fontsize=label_size)
    axis.set_ylabel("Persisted MV refresh lag (minutes)\n↓ lower is fresher", color="white", fontsize=label_size)
    axis.tick_params(colors="white", labelsize=tick_size); axis.grid(True, color=GRID, linewidth=.8 if args.wide else .65, alpha=.7)
    axis.spines[["top", "right"]].set_visible(False); axis.spines[["left", "bottom"]].set_color("white")
    axis.legend([ch, sf], ["ClickHouse · incremental MV (always 0s)", f"Snowflake · serverless MV refresh (avg {average:.1f}m · max {maximum:.1f}m)"],
                loc="upper left", facecolor=plot_background, edgecolor=GRID, labelcolor="white", fontsize=legend_size, framealpha=.9)
    if args.wide:
        fig.subplots_adjust(left=.080, right=.965, bottom=.120, top=.745)
    else:
        fig.tight_layout(pad=.8)
    png, svg = save_figure(
        fig, output, basename, args.dpi, wide=args.wide
    ); plt.close(fig)
    csv_path = output / f"{basename}_data.csv"
    write_csv(
        csv_path,
        ["source_line", "observed_at", "base_table_rows", "lag_seconds", "lag_minutes", "trend_lag_minutes", "display_lag_minutes", "refresh_watermark"],
        (
            {
                **asdict(point),
                "lag_minutes": point.lag_seconds / 60,
                "trend_lag_minutes": ys[index],
                "display_lag_minutes": display_ys[index],
            }
            for index, point in enumerate(plotted)
        ),
    )
    curve_csv = output / f"{basename}_curve_data.csv"
    write_csv(
        curve_csv,
        ["base_table_rows", "rendered_lag_minutes"],
        ({"base_table_rows": round(x), "rendered_lag_minutes": y} for x, y in zip(render_xs, render_ys, strict=True)),
    )
    summary = output / f"{basename}_summary.json"
    write_json(summary, {
        "schema_version": 1, "chart": "clickhouse_vs_snowflake_mv_lag", "layout": layout,
        "selection": {**timeline_meta, **load_meta, "active_source_samples": len(active), "plotted_samples": len(plotted), "alignment": "linear interpolation of dashboard raw_rows at each freshness poll timestamp"},
        "series": {"clickhouse": {"lag_seconds": 0, "source": "benchmark-semantic baseline"}, "snowflake": {"source": "behind_by", "average_plotted_lag_minutes": average, "maximum_plotted_lag_minutes": maximum}},
        "rendering": {
            "aggregation": "centered rolling mean of measured one-minute freshness polls",
            "window_observations": window,
            "display_only_smoothing": {
                "method": "centered rolling mean",
                "window_observations": args.display_smooth_window,
            },
            "curve_interpolation": {
                "method": args.curve_interpolation,
                "shape_preserving": args.curve_interpolation == "pchip",
                "source_unique_points": len(unique_xs),
                "rendered_points": len(render_xs),
            },
            "legend_statistics_source": "61-sample trend before display-only smoothing and interpolation",
            "raw_line_visible": False,
        },
        "sources": {"freshness": {"path": str(freshness), "sha256": sha256(freshness)}, "dashboard": {"path": str(dashboard), "sha256": sha256(dashboard)}},
        "outputs": {"png": str(png), "svg": str(svg), "csv": str(csv_path), "curve_csv": str(curve_csv)},
    })
    for path in (png, svg, csv_path, curve_csv, summary): print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
