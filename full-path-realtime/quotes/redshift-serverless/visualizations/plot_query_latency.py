#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Plot any two benchmark runner JSONL latency series by observed row count."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
from matplotlib import patches
from matplotlib.ticker import FuncFormatter

from _layout import (
    CLICKHOUSE, GRID, MUTED, REDSHIFT, WHITE, configure_figure, duration,
    load_json, money, ratio_text, resolve_layout, save_figure, tier_cost,
    write_json,
)


WORKLOADS = {
    "aggregate": ("Single-symbol summary", "Watchlist summary", "Top movers", "Daily activity"),
    "drilldown": ("Hourly OHLCV bars", "Risk & liquidity (B7)"),
}


@dataclass(frozen=True)
class Point:
    series: str
    label: str
    source_line: int
    iteration: int
    observed_at: str
    raw_rows: int
    query_number: int
    query_name: str
    latency_sec: float


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def scalar(value: Any, context: str) -> float:
    if not isinstance(value, list) or len(value) != 1:
        raise ValueError(f"expected one trial at {context}; got {value!r}")
    item = value[0]
    if isinstance(item, bool) or not isinstance(item, (int, float)):
        raise ValueError(f"missing/non-numeric latency at {context}: {item!r}")
    result = float(item)
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"invalid latency at {context}: {result}")
    return result


def load_points(
    path: Path,
    series: str,
    label: str,
    query_names: tuple[str, ...],
    max_rows: int,
) -> tuple[list[Point], dict[str, int]]:
    points: list[Point] = []
    read = selected = zero = above = 0
    with path.open(encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, 1):
            if not raw.strip():
                continue
            read += 1
            record = json.loads(raw)
            rows = int(record.get("raw_rows") or 0)
            if rows <= 0:
                zero += 1
                continue
            if rows > max_rows:
                above += 1
                continue
            results = record.get("result")
            if not isinstance(results, list) or len(results) != len(query_names):
                raise ValueError(f"expected {len(query_names)} query results at {path}:{line_number}")
            selected += 1
            for index, name in enumerate(query_names):
                points.append(
                    Point(
                        series, label, line_number, int(record["iteration"]),
                        str(record.get("iteration_started_at") or record.get("scheduled_start_at") or ""),
                        rows, index + 1, name,
                        scalar(results[index], f"{path}:{line_number}:q{index + 1}"),
                    )
                )
    if not selected:
        raise ValueError(f"no selected observations in {path}")
    return points, {"read": read, "selected": selected, "zero_excluded": zero, "above_cap_excluded": above}


def human_rows(value: float, _position: float | None = None) -> str:
    if value <= 0:
        return "0"
    for divisor, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M")):
        if value >= divisor:
            return f"{value / divisor:g}{suffix}"
    return f"{value:g}"


def human_seconds(value: float, _position: float | None = None) -> str:
    if math.isclose(value, 0, abs_tol=1e-12):
        return "0s"
    return f"{value * 1000:g}ms" if value < 1 else f"{value:g}s"


def draw_strip(
    figure: Any,
    left: dict[str, Any],
    right: dict[str, Any],
    left_label: str,
    right_label: str,
    left_color: str,
    right_color: str,
    *,
    left_tier: str,
    right_tier: str,
    wide: bool,
) -> dict[str, float]:
    for field in ("iterations_included", "queries_per_iteration"):
        if int(left[field]) != int(right[field]):
            raise ValueError(f"matched summary mismatch for {field}")
    left_runtime = float(left["total_runtime_seconds"])
    right_runtime = float(right["total_runtime_seconds"])
    left_cost = tier_cost(left, left_tier)
    right_cost = tier_cost(right, right_tier)
    runtime_ratio = right_runtime / left_runtime
    cost_ratio = right_cost / left_cost
    x, y, width, height = (.10, .020, .80, .112) if wide else (.06, .014, .88, .125)
    figure.add_artist(
        patches.FancyBboxPatch(
            (x, y), width, height, transform=figure.transFigure,
            boxstyle="round,pad=0.006,rounding_size=0.008",
            facecolor="#202020", edgecolor=GRID, linewidth=1, zorder=2,
        )
    )
    xs = [x + width * value for value in (.035, .25, .51, .76)]
    headers = ("SYSTEM", "ACCUMULATED RUNTIME", "ACCUMULATED QUERY COST", "COMPARISON")
    header_y = y + height - .027
    first_y, second_y = y + .052, y + .019
    header_size = 9.4 if wide else 8.2
    value_size = 11 if wide else 9.5
    for position, header in zip(xs, headers, strict=True):
        figure.text(position, header_y, header, color=MUTED, fontsize=header_size, fontweight="bold")
    rows = (
        (first_y, left_color, left_label, duration(left_runtime), money(left_cost), "baseline"),
        (
            second_y, right_color, right_label, duration(right_runtime), money(right_cost),
            f"{ratio_text(runtime_ratio, 'runtime')} · {ratio_text(cost_ratio, 'cost')}",
        ),
    )
    for row_y, color, *values in rows:
        for position, value in zip(xs, values, strict=True):
            figure.text(position, row_y, value, color=color, fontsize=value_size, fontweight="bold")
    return {"runtime_ratio": runtime_ratio, "cost_ratio": cost_ratio}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload", choices=tuple(WORKLOADS), required=True)
    parser.add_argument("--left", type=Path, required=True)
    parser.add_argument("--right", type=Path, required=True)
    parser.add_argument("--left-label", default="ClickHouse Cloud")
    parser.add_argument("--right-label", default="Redshift Serverless")
    parser.add_argument("--left-color", default=CLICKHOUSE)
    parser.add_argument("--right-color", default=REDSHIFT)
    parser.add_argument("--left-cost", type=Path)
    parser.add_argument("--right-cost", type=Path)
    parser.add_argument("--left-tier", default="Enterprise")
    parser.add_argument("--right-tier", default="Standard")
    parser.add_argument("--max-rows", type=int, default=100_000_000_000)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--basename", required=True)
    parser.add_argument("--wide", action="store_true")
    parser.add_argument("--dpi", type=int, default=300)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    query_names = WORKLOADS[args.workload]
    left_path = args.left.expanduser().resolve()
    right_path = args.right.expanduser().resolve()
    output = args.output_dir.expanduser().resolve()
    left_points, left_meta = load_points(left_path, "left", args.left_label, query_names, args.max_rows)
    right_points, right_meta = load_points(right_path, "right", args.right_label, query_names, args.max_rows)
    points = left_points + right_points

    rows = math.ceil(len(query_names) / 2)
    basename, size, layout = resolve_layout(args.basename, args.wide, (12, 7.4 if rows == 2 else 4.2), args.dpi)
    figure, axes = plt.subplots(rows, 2, figsize=size, dpi=args.dpi if args.wide else None, sharex=True, squeeze=False)
    background = configure_figure(figure, wide=args.wide)
    handles: list[Any] = []
    labels: list[str] = []
    reports: dict[str, Any] = {}
    for number, query_name in enumerate(query_names, 1):
        axis = axes[(number - 1) // 2][(number - 1) % 2]
        axis.set_facecolor(background)
        panel_max = 0.0
        for series, label, color in (
            ("left", args.left_label, args.left_color),
            ("right", args.right_label, args.right_color),
        ):
            selected = sorted(
                (point for point in points if point.series == series and point.query_number == number),
                key=lambda point: (point.raw_rows, point.iteration),
            )
            values = [point.latency_sec for point in selected]
            panel_max = max(panel_max, max(values))
            line, = axis.plot(
                [point.raw_rows for point in selected], values,
                color=color, linewidth=3 if args.wide else 2.3, zorder=3,
            )
            if label not in labels:
                handles.append(line)
                labels.append(label)
            reports[f"{series}_q{number}"] = {
                "observations": len(values),
                "median_sec": statistics.median(values),
                "maximum_sec": max(values),
            }
        axis.set_xlim(0, args.max_rows)
        axis.set_ylim(0, max(1, math.ceil(panel_max * 1.12 * 2) / 2))
        axis.set_title(query_name, color=WHITE, fontsize=17 if args.wide else 12, pad=10)
        axis.xaxis.set_major_formatter(FuncFormatter(human_rows))
        axis.yaxis.set_major_formatter(FuncFormatter(human_seconds))
        axis.tick_params(colors=WHITE, labelsize=13 if args.wide else 9)
        axis.grid(True, color=GRID, linewidth=.65, alpha=.7)
        axis.spines[["top", "right"]].set_visible(False)
        axis.spines[["left", "bottom"]].set_color(WHITE)
        if (number - 1) // 2 == rows - 1:
            axis.set_xlabel("Base-table row count", color=WHITE, fontsize=15 if args.wide else 10)
        if number % 2:
            axis.set_ylabel("Query latency (seconds)\n↓ lower is better", color=WHITE, fontsize=15 if args.wide else 10)
    figure.legend(
        handles, labels, loc="upper center", ncol=2, facecolor=background,
        edgecolor=GRID, labelcolor=WHITE, fontsize=13.5 if args.wide else 10,
        bbox_to_anchor=(.5, .777 if args.wide else 1.005), framealpha=.9,
    )
    ratios = None
    left_cost_path = right_cost_path = None
    if bool(args.left_cost) != bool(args.right_cost):
        raise ValueError("--left-cost and --right-cost must be supplied together")
    if args.left_cost and args.right_cost:
        left_cost_path = args.left_cost.expanduser().resolve()
        right_cost_path = args.right_cost.expanduser().resolve()
        ratios = draw_strip(
            figure, load_json(left_cost_path), load_json(right_cost_path),
            args.left_label.replace(" Cloud", ""), args.right_label,
            args.left_color, args.right_color,
            left_tier=args.left_tier, right_tier=args.right_tier, wide=args.wide,
        )
    if args.wide:
        figure.subplots_adjust(
            left=.09, right=.965, bottom=.225 if ratios else .095,
            top=.69, wspace=.105, hspace=.25,
        )
    else:
        figure.tight_layout(rect=(0, .19 if ratios else 0, 1, .93))
    png, svg = save_figure(figure, output, basename, args.dpi, wide=args.wide)
    plt.close(figure)

    csv_path = output / f"{basename}_data.csv"
    summary_path = output / f"{basename}_summary.json"
    output.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(asdict(points[0])))
        writer.writeheader()
        writer.writerows(asdict(point) for point in points)
    summary = {
        "schema_version": 1,
        "chart": f"{args.workload}_query_latency_pairwise",
        "layout": layout,
        "selection": {"max_rows_inclusive": args.max_rows, "smoothing": None, "outliers_excluded": 0},
        "sources": {
            "left": {"path": str(left_path), "sha256": sha256(left_path), "load": left_meta},
            "right": {"path": str(right_path), "sha256": sha256(right_path), "load": right_meta},
        },
        "reports": reports,
        "matched_totals_ratios": ratios,
        "outputs": {"png": str(png), "svg": str(svg), "csv": str(csv_path)},
    }
    if left_cost_path and right_cost_path:
        summary["sources"].update(
            left_cost={"path": str(left_cost_path), "sha256": sha256(left_cost_path)},
            right_cost={"path": str(right_cost_path), "sha256": sha256(right_cost_path)},
        )
    write_json(summary_path, summary)
    for path in (png, svg, csv_path, summary_path):
        print(f"Written: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
