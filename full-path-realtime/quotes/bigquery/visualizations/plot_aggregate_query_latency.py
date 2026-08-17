#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Plot ClickHouse and BigQuery aggregate-query latency versus raw rows.

The input files are the fixed-rate dashboard-runner JSONL outputs.  Each
system is plotted at its own observed raw-row counts on a shared linear x-axis;
records are never joined by iteration number.  The default publication window
ends at 100 billion rows and excludes post-ingestion observations beyond it.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib
import matplotlib.font_manager as font_manager
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from _layout import (
    BIGQUERY_COLOR,
    CLICKHOUSE_COLOR,
    GRID_COLOR,
    configure_figure,
    draw_matched_query_totals_strip,
    read_json,
    resolve_layout,
    save_figure,
    validate_bigquery_query_cost,
    validate_clickhouse_query_cost,
    validate_matched_query_totals,
    write_json,
)


QUERY_NAMES = (
    "Single-symbol summary",
    "Watchlist summary",
    "Top movers",
    "Daily activity",
)
DEFAULT_BASENAME = "aggregate_query_latency_clickhouse_vs_bigquery"
CHART_KEY = "clickhouse_vs_bigquery_aggregate_query_latency"
CLICKHOUSE_QUERY_FILE = "queries_mv.sql"
BIGQUERY_QUERY_FILE = "queries_mv.sql"

matplotlib.rcParams["font.family"] = (
    "Inter"
    if any(font.name == "Inter" for font in font_manager.fontManager.ttflist)
    else "DejaVu Sans"
)
matplotlib.rcParams["font.sans-serif"] = ["Inter", "DejaVu Sans"]
matplotlib.rcParams["axes.titleweight"] = "bold"


@dataclass(frozen=True)
class Point:
    system_key: str
    system_label: str
    source_line: int
    iteration: int
    observed_at: str
    raw_rows: int
    query_number: int
    query_name: str
    latency_sec: float
    runtime_source: str


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    bigquery_root = script_dir.parent
    quotes_root = bigquery_root.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clickhouse", type=Path, required=True)
    parser.add_argument("--bigquery", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--clickhouse-queries",
        type=Path,
        default=quotes_root / "clickhouse-cloud" / CLICKHOUSE_QUERY_FILE,
    )
    parser.add_argument(
        "--bigquery-queries",
        type=Path,
        default=bigquery_root / BIGQUERY_QUERY_FILE,
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=100_000_000_000,
        help="Inclusive raw-row chart cap; default 100 billion.",
    )
    parser.add_argument(
        "--basename",
        default=DEFAULT_BASENAME,
    )
    parser.add_argument("--dpi", type=int, default=300)
    parser.add_argument("--title", help="Optional figure title.")
    parser.add_argument(
        "--summary-strip",
        action="store_true",
        help="Add the matched accumulated-runtime/query-cost evidence strip.",
    )
    parser.add_argument("--clickhouse-cost-summary", type=Path)
    parser.add_argument("--bigquery-cost-summary", type=Path)
    parser.add_argument("--tier", default="Enterprise")
    parser.add_argument("--wide", action="store_true", help="Render the Keynote-native 5156x2900 staged variant and suffix outputs with _wide.")
    return parser.parse_args()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def scalar_result(value: Any, *, context: str) -> float:
    if not isinstance(value, list) or len(value) != 1:
        raise ValueError(f"expected one trial at {context}; got {value!r}")
    scalar = value[0]
    if scalar is None or isinstance(scalar, bool) or not isinstance(scalar, (int, float)):
        raise ValueError(f"missing/non-numeric latency at {context}: {scalar!r}")
    latency = float(scalar)
    if latency < 0:
        raise ValueError(f"negative latency at {context}: {latency}")
    return latency


def load_points(path: Path, system_key: str, max_rows: int) -> tuple[list[Point], dict[str, Any]]:
    points: list[Point] = []
    records_read = 0
    records_selected = 0
    records_above_cap = 0
    systems: set[str] = set()
    versions: set[str] = set()
    machines: set[str] = set()
    cluster_sizes: set[str] = set()
    run_ids: set[str] = set()

    with path.open(encoding="utf-8") as handle:
        for source_line, raw_line in enumerate(handle, start=1):
            if not raw_line.strip():
                continue
            records_read += 1
            record = json.loads(raw_line)
            systems.add(str(record.get("system") or ""))
            versions.add(str(record.get("version") or ""))
            machines.add(str(record.get("machine") or ""))
            cluster_sizes.add(str(record.get("cluster_size") or ""))
            if record.get("run_id") is not None:
                run_ids.add(str(record["run_id"]))

            raw_rows = int(record.get("raw_rows") or 0)
            if raw_rows <= 0:
                continue
            if raw_rows > max_rows:
                records_above_cap += 1
                continue

            results = record.get("result")
            if not isinstance(results, list) or len(results) != len(QUERY_NAMES):
                raise ValueError(
                    f"expected {len(QUERY_NAMES)} query results at "
                    f"{path}:{source_line}; got {results!r}"
                )
            jobs = record.get("query_jobs")
            if system_key == "bigquery":
                if not isinstance(jobs, list) or len(jobs) != len(QUERY_NAMES):
                    raise ValueError(f"missing BigQuery job evidence at {path}:{source_line}")
                for job in jobs:
                    if job.get("error") is not None:
                        raise ValueError(
                            f"BigQuery query error at {path}:{source_line}: {job['error']}"
                        )
                    if job.get("cache_hit") is not False:
                        raise ValueError(
                            f"BigQuery cache was not explicitly false at {path}:{source_line}"
                        )

            observed_at = str(
                record.get("iteration_started_at")
                or record.get("scheduled_start_at")
                or ""
            )
            records_selected += 1
            for query_index, query_name in enumerate(QUERY_NAMES):
                context = f"{path}:{source_line}:q{query_index + 1}"
                latency = scalar_result(results[query_index], context=context)
                runtime_source = "clickhouse-client --time"
                if system_key == "bigquery":
                    job = jobs[query_index]
                    job_runtime = job.get("runtime_sec")
                    if job_runtime is None or not math.isclose(
                        latency, float(job_runtime), rel_tol=0, abs_tol=1e-9
                    ):
                        raise ValueError(
                            f"result/job runtime mismatch at {context}: "
                            f"result={latency}, job={job_runtime}"
                        )
                    runtime_source = str(job.get("runtime_source") or "unknown")
                points.append(
                    Point(
                        system_key=system_key,
                        system_label=(
                            "ClickHouse Cloud" if system_key == "clickhouse" else "BigQuery"
                        ),
                        source_line=source_line,
                        iteration=int(record["iteration"]),
                        observed_at=observed_at,
                        raw_rows=raw_rows,
                        query_number=query_index + 1,
                        query_name=query_name,
                        latency_sec=latency,
                        runtime_source=runtime_source,
                    )
                )

    if records_selected == 0:
        raise ValueError(f"no records in (0, {max_rows:,}] at {path}")
    return points, {
        "records_read": records_read,
        "records_selected": records_selected,
        "zero_row_records_excluded": records_read - records_selected - records_above_cap,
        "records_above_cap_excluded": records_above_cap,
        "systems": sorted(systems),
        "versions": sorted(versions),
        "machines": sorted(machines),
        "cluster_sizes": sorted(cluster_sizes),
        "run_ids": sorted(run_ids),
    }


def human_rows(value: float, _position: float | None = None) -> str:
    if value <= 0:
        return "0"
    for divisor, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if value >= divisor:
            return f"{value / divisor:g}{suffix}"
    return f"{value:g}"


def human_seconds(value: float, _position: float | None = None) -> str:
    if math.isclose(value, 0.0, abs_tol=1e-12):
        return "0s"
    if value < 1:
        return f"{value * 1000:g}ms"
    return f"{value:g}s"


def points_for(points: list[Point], system_key: str, query_number: int) -> list[Point]:
    return sorted(
        (
            point
            for point in points
            if point.system_key == system_key and point.query_number == query_number
        ),
        key=lambda point: (point.raw_rows, point.iteration),
    )


def series_summary(series: list[Point]) -> dict[str, Any]:
    values = [point.latency_sec for point in series]
    ordered = sorted(values)
    p95_index = max(0, math.ceil(0.95 * len(ordered)) - 1)
    max_point = max(series, key=lambda point: point.latency_sec)
    return {
        "observations": len(series),
        "first_raw_rows": series[0].raw_rows,
        "last_raw_rows": series[-1].raw_rows,
        "average_latency_sec": statistics.fmean(values),
        "median_latency_sec": statistics.median(values),
        "p95_latency_sec": ordered[p95_index],
        "maximum": {
            "latency_sec": max_point.latency_sec,
            "source_line": max_point.source_line,
            "iteration": max_point.iteration,
            "observed_at": max_point.observed_at,
            "raw_rows": max_point.raw_rows,
        },
    }


def write_csv(path: Path, points: list[Point]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "system",
                "source_line",
                "iteration",
                "observed_at",
                "raw_rows",
                "query_number",
                "query_name",
                "latency_sec",
                "runtime_source",
            ]
        )
        for point in sorted(
            points,
            key=lambda item: (item.query_number, item.system_key, item.raw_rows, item.iteration),
        ):
            writer.writerow(
                [
                    point.system_label,
                    point.source_line,
                    point.iteration,
                    point.observed_at,
                    point.raw_rows,
                    point.query_number,
                    point.query_name,
                    f"{point.latency_sec:.9f}",
                    point.runtime_source,
                ]
            )


def main() -> int:
    args = parse_args()
    if args.max_rows <= 0:
        raise ValueError("--max-rows must be positive")
    clickhouse_path = args.clickhouse.expanduser().resolve()
    bigquery_path = args.bigquery.expanduser().resolve()
    clickhouse_queries = args.clickhouse_queries.expanduser().resolve()
    bigquery_queries = args.bigquery_queries.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.summary_strip and not (
        args.clickhouse_cost_summary and args.bigquery_cost_summary
    ):
        raise ValueError(
            "--summary-strip requires --clickhouse-cost-summary and "
            "--bigquery-cost-summary"
        )

    clickhouse_points, clickhouse_meta = load_points(
        clickhouse_path, "clickhouse", args.max_rows
    )
    bigquery_points, bigquery_meta = load_points(
        bigquery_path, "bigquery", args.max_rows
    )
    all_points = clickhouse_points + bigquery_points

    column_count = 2
    row_count = math.ceil(len(QUERY_NAMES) / column_count)
    figure_height = 7.4 if row_count == 2 else 4.1
    basename, figure_size, layout = resolve_layout(
        args.basename, args.wide, (11, figure_height), dpi=args.dpi
    )
    fig, axes = plt.subplots(
        row_count,
        column_count,
        figsize=figure_size,
        dpi=args.dpi if args.wide else None,
        sharex=True,
        squeeze=False,
    )
    plot_background = configure_figure(fig, wide=args.wide)
    legend_handles: list[Any] = []
    legend_labels: list[str] = []

    for query_number, query_name in enumerate(QUERY_NAMES, start=1):
        row_index = (query_number - 1) // column_count
        axis = axes[row_index][(query_number - 1) % column_count]
        axis.set_facecolor(plot_background)
        visible_max = 0.0
        for system_key, color, label in (
            ("clickhouse", CLICKHOUSE_COLOR, "ClickHouse Cloud"),
            ("bigquery", BIGQUERY_COLOR, "BigQuery"),
        ):
            series = points_for(all_points, system_key, query_number)
            xs = [point.raw_rows for point in series]
            ys = [point.latency_sec for point in series]
            visible_max = max(visible_max, max(ys))
            (line,) = axis.plot(
                xs,
                ys,
                color=color,
                linewidth=3.0 if args.wide else 2.35,
                zorder=3,
            )
            if label not in legend_labels:
                legend_handles.append(line)
                legend_labels.append(label)

        y_top = max(1.0, math.ceil(visible_max * 1.12 * 2) / 2)
        axis.set_xlim(0, args.max_rows)
        axis.set_ylim(0, y_top)
        axis.set_title(
            query_name,
            color="white",
            fontsize=17 if args.wide else 12,
            pad=12 if args.wide else 8,
        )
        axis.xaxis.set_major_formatter(FuncFormatter(human_rows))
        axis.yaxis.set_major_formatter(FuncFormatter(human_seconds))
        axis.tick_params(colors="white", labelsize=13 if args.wide else 9)
        axis.grid(
            True,
            which="major",
            color=GRID_COLOR,
            linewidth=0.75 if args.wide else 0.6,
            alpha=0.7,
        )
        axis.spines["top"].set_visible(False)
        axis.spines["right"].set_visible(False)
        axis.spines["left"].set_color("white")
        axis.spines["bottom"].set_color("white")
        if row_index == row_count - 1:
            axis.set_xlabel(
                "Base-table row count",
                color="white",
                fontsize=15 if args.wide else 10,
            )
        if query_number % 2 == 1:
            axis.set_ylabel(
                "Query latency (seconds)\n↓ lower is better",
                color="white",
                fontsize=15 if args.wide else 10,
            )

    if args.title:
        fig.suptitle(
            args.title,
            color="white",
            fontsize=20 if args.wide else 14,
            fontweight="bold",
            y=0.99,
        )
    fig.legend(
        legend_handles,
        legend_labels,
        loc="upper center",
        bbox_to_anchor=(
            0.5,
            (0.750 if args.title else 0.777) if args.wide
            else (0.95 if args.title else 0.995),
        ),
        ncol=2,
        facecolor=plot_background,
        edgecolor=GRID_COLOR,
        labelcolor="white",
        fontsize=13.5 if args.wide else 10,
        framealpha=0.9,
    )

    matched_totals: dict[str, Any] | None = None
    clickhouse_cost_path: Path | None = None
    bigquery_cost_path: Path | None = None
    if args.summary_strip:
        clickhouse_cost_path = args.clickhouse_cost_summary.expanduser().resolve()
        bigquery_cost_path = args.bigquery_cost_summary.expanduser().resolve()
        clickhouse_totals = validate_clickhouse_query_cost(
            read_json(clickhouse_cost_path), tier=args.tier
        )
        bigquery_totals = validate_bigquery_query_cost(
            read_json(bigquery_cost_path), tier=args.tier
        )
        validate_matched_query_totals(clickhouse_totals, bigquery_totals)
        ratios = draw_matched_query_totals_strip(
            fig, clickhouse_totals, bigquery_totals, wide=args.wide
        )
        matched_totals = {
            "tier": args.tier,
            "clickhouse": clickhouse_totals,
            "bigquery": bigquery_totals,
            "ratios": ratios,
        }

    if args.wide:
        fig.subplots_adjust(
            left=0.095,
            right=0.965,
            bottom=0.230 if args.summary_strip else 0.095,
            top=0.655 if args.title else 0.690,
            wspace=0.105,
            hspace=0.250,
        )
    else:
        fig.tight_layout(
            rect=(
                0,
                0.210 if args.summary_strip else 0,
                1,
                0.88 if args.title else 0.905,
            )
        )

    png_path = output_dir / f"{basename}.png"
    svg_path = output_dir / f"{basename}.svg"
    csv_path = output_dir / f"{basename}_data.csv"
    summary_path = output_dir / f"{basename}_summary.json"
    png_path, svg_path = save_figure(
        fig, output_dir, basename, args.dpi, wide=args.wide
    )
    plt.close(fig)
    write_csv(csv_path, all_points)

    summary: dict[str, Any] = {
        "schema_version": 1,
        "chart": CHART_KEY,
        "layout": layout,
        "selection": {
            "definition": "records with 0 < raw_rows <= max_rows",
            "max_rows_inclusive": args.max_rows,
            "alignment": (
                "each system plotted at its own observed raw-row counts; "
                "no iteration join or interpolation"
            ),
            "smoothing": None,
        },
        "sources": {
            "clickhouse_jsonl": str(clickhouse_path),
            "clickhouse_jsonl_sha256": sha256(clickhouse_path),
            "clickhouse_queries_sql": str(clickhouse_queries),
            "clickhouse_queries_sql_sha256": sha256(clickhouse_queries),
            "bigquery_jsonl": str(bigquery_path),
            "bigquery_jsonl_sha256": sha256(bigquery_path),
            "bigquery_queries_sql": str(bigquery_queries),
            "bigquery_queries_sql_sha256": sha256(bigquery_queries),
        },
        "input_metadata": {
            "clickhouse": clickhouse_meta,
            "bigquery": bigquery_meta,
        },
        "query_order": [
            {"query_number": index, "query_name": name}
            for index, name in enumerate(QUERY_NAMES, start=1)
        ],
        "series": {},
        "rendering": {
            "x_axis": "raw_rows (linear)",
            "y_axis": "result[query][0] seconds (linear)",
            "png": str(png_path),
            "svg": str(svg_path),
            "plotted_data_csv": str(csv_path),
        },
    }
    if clickhouse_cost_path and bigquery_cost_path:
        summary["sources"].update(
            {
                "clickhouse_cost_summary": str(clickhouse_cost_path),
                "clickhouse_cost_summary_sha256": sha256(clickhouse_cost_path),
                "bigquery_cost_summary": str(bigquery_cost_path),
                "bigquery_cost_summary_sha256": sha256(bigquery_cost_path),
            }
        )
    if matched_totals:
        summary["matched_active_ingestion_totals"] = matched_totals
    for system_key in ("clickhouse", "bigquery"):
        summary["series"][system_key] = {
            str(query_number): series_summary(
                points_for(all_points, system_key, query_number)
            )
            for query_number in range(1, len(QUERY_NAMES) + 1)
        }
    write_json(summary_path, summary)

    for path in (png_path, svg_path, csv_path, summary_path):
        print(f"Wrote {path}")
    print(
        "Selected iterations: "
        f"ClickHouse={clickhouse_meta['records_selected']:,}, "
        f"BigQuery={bigquery_meta['records_selected']:,}; "
        f"row cap={args.max_rows:,}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
