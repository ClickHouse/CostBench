#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Plot BigQuery MV refresh lag against base-table rows during active ingestion.

The BigQuery freshness monitor records the acknowledged base-table row count
and MV refresh-watermark lag in every JSONL observation.  The plotted active
window ends at—and includes—the first observation at the complete ingest row
count.  Later observations at the unchanged final count are excluded.

ClickHouse incremental-MV lag is represented as a synthetic 0-second baseline;
no ClickHouse measurement file is required.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib
import matplotlib.font_manager as font_manager
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from _layout import configure_figure, resolve_layout, save_figure, write_json


GRID_COLOR = "#4A4A4A"
CLICKHOUSE_COLOR = "#FDFF88"
BIGQUERY_COLOR = "#4285F4"

matplotlib.rcParams["font.family"] = (
    "Inter"
    if any(font.name == "Inter" for font in font_manager.fontManager.ttflist)
    else "DejaVu Sans"
)
matplotlib.rcParams["font.sans-serif"] = ["Inter", "DejaVu Sans"]


@dataclass(frozen=True)
class Point:
    source_line: int
    iteration: int
    observed_at: str
    base_table_rows: int
    lag_seconds: float
    refresh_watermark: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--freshness", type=Path, required=True)
    parser.add_argument("--ingest-summary", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--basename",
        default="mv_lag_clickhouse_vs_bigquery",
        help="Output filename stem.",
    )
    parser.add_argument("--dpi", type=int, default=300)
    parser.add_argument(
        "--aggregation",
        choices=("refresh-cycle-max", "rolling-mean", "raw"),
        default="refresh-cycle-max",
        help=(
            "Visible BigQuery series. Default refresh-cycle-max plots one "
            "measured maximum per refresh watermark cycle."
        ),
    )
    parser.add_argument(
        "--smooth-window",
        type=int,
        default=31,
        help=(
            "Centered rolling-mean window used by --aggregation rolling-mean; "
            "default 31 observations."
        ),
    )
    parser.add_argument(
        "--title",
        help="Optional chart title. By default the chart has no title.",
    )
    parser.add_argument("--wide", action="store_true", help="Render the Keynote-native 5156x2900 staged variant and suffix outputs with _wide.")
    return parser.parse_args()


def centered_rolling_mean(values: list[float], window: int) -> list[float]:
    if window <= 1:
        return list(values)
    half = window // 2
    smoothed: list[float] = []
    for index in range(len(values)):
        start = max(0, index - half)
        end = min(len(values), index + half + 1)
        smoothed.append(sum(values[start:end]) / (end - start))
    return smoothed


def refresh_cycle_maxima(points: list[Point]) -> list[Point]:
    """Choose one measured maximum from each fully observed watermark cycle.

    The first cycle began before monitoring and the last continues beyond the
    active-ingestion endpoint.  Dropping those two boundary cycles avoids
    presenting partial-cycle values as cycle maxima.
    """
    cycles: list[list[Point]] = []
    current: list[Point] = []
    current_watermark: str | None = None
    for point in points:
        if current and point.refresh_watermark != current_watermark:
            cycles.append(current)
            current = []
        current.append(point)
        current_watermark = point.refresh_watermark
    if current:
        cycles.append(current)
    if len(cycles) < 3:
        raise ValueError("need at least three refresh-watermark cycles")
    complete_cycles = cycles[1:-1]
    return [
        max(cycle, key=lambda point: point.lag_seconds)
        for cycle in complete_cycles
    ]


def read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object in {path}")
    return value


def load_points(path: Path) -> tuple[list[Point], int]:
    points: list[Point] = []
    total_records = 0
    with path.open(encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            total_records += 1
            record = json.loads(line)
            rows = record.get("ingest_acknowledged_rows")
            lag = record.get("watermark_lag_sec")
            if rows is None or lag is None:
                raise ValueError(
                    f"missing ingest_acknowledged_rows or watermark_lag_sec "
                    f"at {path}:{line_number}"
                )
            points.append(
                Point(
                    source_line=line_number,
                    iteration=int(record["iteration"]),
                    observed_at=str(record["observed_at"]),
                    base_table_rows=int(rows),
                    lag_seconds=float(lag),
                    refresh_watermark=record.get("refresh_watermark"),
                )
            )
    if not points:
        raise ValueError(f"no freshness observations found in {path}")
    return points, total_records


def human_rows(value: float, _position: float | None = None) -> str:
    if value <= 0:
        return "0"
    for divisor, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if value >= divisor:
            return f"{value / divisor:g}{suffix}"
    return f"{value:g}"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_csv(
    path: Path,
    points: list[Point],
    plotted_lag_minutes: list[float],
    endpoint: Point,
) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "source_line",
                "iteration",
                "observed_at",
                "base_table_rows",
                "lag_seconds",
                "lag_minutes",
                "plotted_lag_minutes",
                "refresh_watermark",
                "phase",
            ]
        )
        for point, plotted_value in zip(points, plotted_lag_minutes):
            writer.writerow(
                [
                    point.source_line,
                    point.iteration,
                    point.observed_at,
                    point.base_table_rows,
                    f"{point.lag_seconds:.6f}",
                    f"{point.lag_seconds / 60.0:.9f}",
                    f"{plotted_value:.9f}",
                    point.refresh_watermark or "",
                    "active_endpoint" if point is endpoint else "active",
                ]
            )


def point_json(point: Point) -> dict[str, Any]:
    return {
        "source_line": point.source_line,
        "iteration": point.iteration,
        "observed_at": point.observed_at,
        "base_table_rows": point.base_table_rows,
        "lag_seconds": point.lag_seconds,
        "lag_minutes": point.lag_seconds / 60.0,
        "refresh_watermark": point.refresh_watermark,
    }


def main() -> int:
    args = parse_args()
    if args.smooth_window < 1:
        raise ValueError("--smooth-window must be at least 1")
    freshness_path = args.freshness.expanduser().resolve()
    ingest_summary_path = args.ingest_summary.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    ingest_summary = read_json(ingest_summary_path)
    final_rows = int(ingest_summary["acknowledged_rows"])
    if final_rows <= 0:
        raise ValueError("ingest summary has no positive acknowledged_rows")

    all_points, total_records = load_points(freshness_path)
    endpoint_index = next(
        (
            index
            for index, point in enumerate(all_points)
            if point.base_table_rows >= final_rows
        ),
        None,
    )
    if endpoint_index is None:
        raise ValueError(
            "freshness file never observes the complete ingest row count; "
            "cannot establish the active-ingestion endpoint"
        )

    active_points = all_points[: endpoint_index + 1]
    endpoint = active_points[-1]
    peak = max(active_points, key=lambda point: point.lag_seconds)
    run_ids = {
        json.loads(line)["run_id"]
        for line in freshness_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }
    if run_ids != {ingest_summary.get("run_id")}:
        raise ValueError(
            f"run identity mismatch: freshness={sorted(run_ids)!r}, "
            f"ingest_summary={ingest_summary.get('run_id')!r}"
        )

    if args.aggregation == "refresh-cycle-max":
        plotted_points = refresh_cycle_maxima(active_points)
        ys = [point.lag_seconds / 60.0 for point in plotted_points]
        legend_suffix = "maximum lag before each refresh"
        rendering = {
            "method": (
                "one measured maximum per fully observed refresh-watermark "
                "cycle"
            ),
            "boundary_cycles_excluded": 2,
            "boundary_cycle_reason": (
                "first cycle began before monitoring; last cycle continued "
                "past the active-ingestion endpoint"
            ),
            "raw_line_visible": False,
        }
    elif args.aggregation == "rolling-mean":
        plotted_points = active_points
        raw_ys = [point.lag_seconds / 60.0 for point in plotted_points]
        ys = centered_rolling_mean(raw_ys, args.smooth_window)
        legend_suffix = f"{args.smooth_window}-min rolling mean"
        rendering = {
            "method": "centered rolling mean",
            "window_observations": args.smooth_window,
            "nominal_observation_interval_seconds": 60,
            "raw_line_visible": False,
        }
    else:
        plotted_points = active_points
        ys = [point.lag_seconds / 60.0 for point in plotted_points]
        legend_suffix = "raw 1-min samples"
        rendering = {"method": "raw observations", "raw_line_visible": True}
    xs = [point.base_table_rows for point in plotted_points]
    plotted_average_minutes = sum(ys) / len(ys)
    plotted_maximum_index = max(range(len(ys)), key=ys.__getitem__)
    plotted_maximum = plotted_points[plotted_maximum_index]
    plotted_maximum_minutes = ys[plotted_maximum_index]
    peak_minutes = peak.lag_seconds / 60.0

    basename, figure_size, layout = resolve_layout(
        args.basename, args.wide, (11, 5), dpi=args.dpi
    )
    line_width = 3.2 if args.wide else 2.2
    tick_fontsize = 13 if args.wide else 10
    label_fontsize = 15 if args.wide else 11
    legend_fontsize = 13.5 if args.wide else 10
    title_fontsize = 20 if args.wide else 14
    fig, axis = plt.subplots(
        figsize=figure_size, dpi=args.dpi if args.wide else None
    )
    plot_background = configure_figure(fig, wide=args.wide)
    axis.set_facecolor(plot_background)

    (clickhouse_line,) = axis.plot(
        [0, final_rows],
        [0, 0],
        color=CLICKHOUSE_COLOR,
        linewidth=line_width,
        zorder=4,
    )
    (bigquery_line,) = axis.plot(
        xs,
        ys,
        color=BIGQUERY_COLOR,
        linewidth=line_width,
        zorder=3,
    )

    axis.set_xlim(0, final_rows)
    # Scale to the visible rolling-mean series, not the hidden raw startup
    # outlier. The unsmoothed peak remains preserved in the provenance summary.
    y_top = max(2.0, math.ceil(max(ys) * 1.15))
    axis.set_ylim(0, y_top)
    axis.xaxis.set_major_formatter(FuncFormatter(human_rows))
    axis.yaxis.set_major_formatter(FuncFormatter(lambda value, _position: f"{value:g}m"))
    axis.set_xlabel("Base-table row count", color="white", fontsize=label_fontsize)
    axis.set_ylabel(
        "Persisted MV refresh lag (minutes)\n↓ lower is fresher",
        color="white",
        fontsize=label_fontsize,
    )
    axis.tick_params(colors="white", labelsize=tick_fontsize)
    axis.grid(True, which="major", color=GRID_COLOR, linewidth=0.6, alpha=0.7)
    axis.spines["top"].set_visible(False)
    axis.spines["right"].set_visible(False)
    axis.spines["left"].set_color("white")
    axis.spines["bottom"].set_color("white")
    axis.legend(
        [clickhouse_line, bigquery_line],
        [
            "ClickHouse · incremental MV (always 0s)",
            (
                "BigQuery · automatic MV refresh "
                f"(avg {plotted_average_minutes:.1f}m · "
                f"max {plotted_maximum_minutes:.1f}m)"
            ),
        ],
        loc="upper left",
        facecolor=plot_background,
        edgecolor=GRID_COLOR,
        labelcolor="white",
        fontsize=legend_fontsize,
        framealpha=0.9,
    )
    if args.title:
        axis.set_title(args.title, color="white", fontsize=title_fontsize, pad=12)

    if args.wide:
        fig.subplots_adjust(
            left=0.080,
            right=0.965,
            bottom=0.120,
            top=0.710 if args.title else 0.745,
        )
    else:
        fig.tight_layout()
    png_path = output_dir / f"{basename}.png"
    svg_path = output_dir / f"{basename}.svg"
    csv_path = output_dir / f"{basename}_data.csv"
    summary_path = output_dir / f"{basename}_summary.json"
    png_path, svg_path = save_figure(
        fig, output_dir, basename, args.dpi, wide=args.wide
    )
    plt.close(fig)

    write_csv(csv_path, plotted_points, ys, endpoint)
    summary = {
        "schema_version": 1,
        "chart": "clickhouse_vs_bigquery_mv_lag",
        "layout": layout,
        "run_id": ingest_summary["run_id"],
        "source": {
            "freshness_jsonl": str(freshness_path),
            "freshness_sha256": sha256(freshness_path),
            "ingest_summary_json": str(ingest_summary_path),
            "ingest_summary_sha256": sha256(ingest_summary_path),
        },
        "selection": {
            "definition": (
                "active observations through and including the first observation "
                "at the complete ingest row count"
            ),
            "final_ingest_rows": final_rows,
            "source_records": total_records,
            "active_records_included": len(active_points),
            "post_ingestion_records_excluded": total_records - len(active_points),
            "active_endpoint": point_json(endpoint),
        },
        "series": {
            "clickhouse": {
                "source": "synthetic benchmark-semantic baseline",
                "lag_seconds": 0,
            },
            "bigquery": {
                "source": "watermark_lag_sec",
                "active_source_samples": len(active_points),
                "plotted_samples": len(plotted_points),
                "plotted_average_lag_seconds": plotted_average_minutes * 60.0,
                "plotted_average_lag_minutes": plotted_average_minutes,
                "plotted_maximum": point_json(plotted_maximum),
                "peak": point_json(peak),
            },
        },
        "rendering": {
            "aggregation": rendering,
            "x_axis": "ingest_acknowledged_rows",
            "y_axis": "watermark_lag_sec / 60",
            "png": str(png_path),
            "svg": str(svg_path),
            "plotted_data_csv": str(csv_path),
        },
    }
    write_json(summary_path, summary)

    print(f"Wrote {png_path}")
    print(f"Wrote {svg_path}")
    print(f"Wrote {csv_path}")
    print(f"Wrote {summary_path}")
    print(
        f"Active samples: {len(active_points):,}; "
        f"excluded post-ingestion: {total_records - len(active_points):,}; "
        f"BigQuery peak: {peak.lag_seconds:.6f}s ({peak_minutes:.6f}m)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
