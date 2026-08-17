#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Plot Snowflake compilation and execution time versus base-table rows."""

from __future__ import annotations

import argparse
import math
import statistics
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.ticker import FuncFormatter

from _common import (
    GRID,
    annotate_outlier_stats,
    configure_figure,
    human_rows,
    human_seconds,
    iter_jsonl,
    resolve_layout,
    save_figure,
    scalar_trial,
    sha256,
    write_csv,
    write_json,
)


DEFAULT_QUERY_NAMES = (
    "Single-symbol summary",
    "Watchlist summary",
    "Top movers",
    "Daily activity",
)
DEFAULT_BASENAME = "aggregate_query_time_breakdown_snowflake"
CHART_KEY = "snowflake_query_compilation_execution_breakdown"
# Match the architecture visual: compilation runs in the orange serverless
# compilation path, while execution runs in Snowflake's blue warehouse path.
COMPILATION = "#FFAA1D"
EXECUTION = "#29B5E8"


@dataclass(frozen=True)
class PhasePoint:
    source_line: int
    iteration: int
    observed_at: str
    raw_rows: int
    query_number: int
    query_name: str
    latency_sec: float
    compilation_sec: float
    execution_sec: float
    residual_sec: float


@dataclass(frozen=True)
class DisplayPoint:
    source_line: int
    raw_rows: int
    compilation_sec: float
    execution_sec: float


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--compilation-legend-label",
        default="Compilation",
        help="Legend label for provider-reported compilation_time.",
    )
    parser.add_argument(
        "--execution-legend-label",
        default="Execution",
        help="Legend label for provider-reported execution_time.",
    )
    parser.add_argument(
        "--queries",
        type=Path,
        default=root / "snowflake/t2/queries_mv_imv.sql",
    )
    parser.add_argument(
        "--query-label",
        action="append",
        dest="query_labels",
        help="Panel label, repeated once per query; defaults to dashboard labels.",
    )
    parser.add_argument("--max-rows", type=int, default=100_000_000_000)
    parser.add_argument(
        "--drop-outliers",
        action="store_true",
        help=(
            "Exclude whole observations whose end-to-end result exceeds the "
            "per-query Tukey upper fence (Q3 + 1.5*IQR). Excluded observations "
            "remain in the CSV and summary."
        ),
    )
    parser.add_argument(
        "--smooth-window",
        type=int,
        default=1,
        help=(
            "Centered rolling-median display window; applied to cumulative phase "
            "boundaries so the displayed stack remains additive."
        ),
    )
    parser.add_argument(
        "--annotate-outliers",
        action="store_true",
        help=(
            "Add a compact exclusion-count, Tukey-fence, and maximum-excluded-"
            "latency overlay only to panels with exclusions; requires "
            "--drop-outliers."
        ),
    )
    parser.add_argument("--basename", default=DEFAULT_BASENAME)
    parser.add_argument("--wide", action="store_true", help="Render the Keynote-native 5156x2900 staged variant and suffix outputs with _wide.")
    parser.add_argument("--dpi", type=int, default=300)
    return parser.parse_args()


def query_count(path: Path, max_rows: int) -> int:
    for line, record in iter_jsonl(path):
        raw_rows = int(record.get("raw_rows") or 0)
        if not 0 < raw_rows <= max_rows:
            continue
        results = record.get("result")
        if not isinstance(results, list) or not results:
            raise ValueError(f"missing query results at {path}:{line}")
        return len(results)
    raise ValueError(f"no records in active row window at {path}")


def labels(args: argparse.Namespace, count: int) -> tuple[str, ...]:
    if args.query_labels:
        if len(args.query_labels) != count:
            raise ValueError(
                f"expected {count} --query-label values; got {len(args.query_labels)}"
            )
        return tuple(args.query_labels)
    if count == len(DEFAULT_QUERY_NAMES):
        return DEFAULT_QUERY_NAMES
    return tuple(f"Query {number}" for number in range(1, count + 1))


def load(
    path: Path,
    query_names: tuple[str, ...],
    max_rows: int,
) -> tuple[list[PhasePoint], dict[str, Any]]:
    points: list[PhasePoint] = []
    read = selected = above = zero = 0
    systems: set[str] = set()
    machines: set[str] = set()
    for line, record in iter_jsonl(path):
        read += 1
        systems.add(str(record.get("system") or ""))
        machines.add(str(record.get("machine") or ""))
        raw_rows = int(record.get("raw_rows") or 0)
        if raw_rows <= 0:
            zero += 1
            continue
        if raw_rows > max_rows:
            above += 1
            continue
        results = record.get("result")
        compilation = record.get("compilation_time")
        execution = record.get("execution_time")
        for field, value in (
            ("result", results),
            ("compilation_time", compilation),
            ("execution_time", execution),
        ):
            if not isinstance(value, list) or len(value) != len(query_names):
                raise ValueError(
                    f"expected {len(query_names)} {field} values at {path}:{line}"
                )
        selected += 1
        for index, query_name in enumerate(query_names):
            context = f"{path}:{line}:q{index + 1}"
            latency = scalar_trial(results[index], context=f"{context}:result")
            comp = scalar_trial(
                compilation[index], context=f"{context}:compilation_time"
            )
            exe = scalar_trial(
                execution[index], context=f"{context}:execution_time"
            )
            points.append(
                PhasePoint(
                    source_line=line,
                    iteration=int(record["iteration"]),
                    observed_at=str(record.get("iteration_started_at") or ""),
                    raw_rows=raw_rows,
                    query_number=index + 1,
                    query_name=query_name,
                    latency_sec=latency,
                    compilation_sec=comp,
                    execution_sec=exe,
                    residual_sec=latency - comp - exe,
                )
            )
    if not selected:
        raise ValueError(f"no records in active row window at {path}")
    return points, {
        "records_read": read,
        "records_selected": selected,
        "zero_row_records_excluded": zero,
        "records_above_cap_excluded": above,
        "systems": sorted(systems),
        "machines": sorted(machines),
    }


def query_series(points: list[PhasePoint], query: int) -> list[PhasePoint]:
    return sorted(
        (point for point in points if point.query_number == query),
        key=lambda point: (point.raw_rows, point.iteration),
    )


def numeric_stats(values: list[float]) -> dict[str, float | int]:
    ordered = sorted(values)
    return {
        "observations": len(values),
        "average_sec": statistics.fmean(values),
        "median_sec": statistics.median(values),
        "p95_sec": ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)],
        "minimum_sec": min(values),
        "maximum_sec": max(values),
    }


def point_stats(values: list[PhasePoint]) -> dict[str, Any]:
    return {
        "observations": len(values),
        "end_to_end_latency": numeric_stats([point.latency_sec for point in values]),
        "compilation": numeric_stats([point.compilation_sec for point in values]),
        "execution": numeric_stats([point.execution_sec for point in values]),
        "residual_end_to_end_minus_phases": numeric_stats(
            [point.residual_sec for point in values]
        ),
        "maximum_end_to_end_point": asdict(
            max(values, key=lambda point: point.latency_sec)
        ),
    }


def tukey_upper_filter(
    values: list[PhasePoint],
) -> tuple[list[PhasePoint], dict[str, Any]]:
    """Use the exact per-query upper-fence rule used by the latency renderer."""
    if len(values) < 8:
        return values, {
            "method": "Tukey upper fence on end-to-end result (Q3 + 1.5*IQR)",
            "applied": False,
            "reason": "fewer than 8 observations",
            "excluded_observations": 0,
        }
    ordered = sorted(point.latency_sec for point in values)
    count = len(ordered)
    q1 = ordered[count // 4]
    q3 = ordered[3 * count // 4]
    fence = q3 + 1.5 * (q3 - q1)
    excluded = [point for point in values if point.latency_sec > fence]
    kept = [point for point in values if point.latency_sec <= fence]
    return kept, {
        "method": "Tukey upper fence on end-to-end result (Q3 + 1.5*IQR)",
        "applied": True,
        "q1_latency_sec": q1,
        "q3_latency_sec": q3,
        "upper_fence_sec": fence,
        "excluded_observations": len(excluded),
        "excluded_max_latency_sec": (
            max(point.latency_sec for point in excluded) if excluded else None
        ),
        "excluded_points": [asdict(point) for point in excluded],
    }


def rolling_phase_median(
    values: list[PhasePoint], window: int
) -> list[DisplayPoint]:
    if window < 1 or window % 2 == 0:
        raise ValueError("--smooth-window must be a positive odd integer")
    half = window // 2
    displayed: list[DisplayPoint] = []
    for index, point in enumerate(values):
        lower = max(0, index - half)
        upper = min(len(values), index + half + 1)
        candidates = values[lower:upper]
        compilation = statistics.median(
            candidate.compilation_sec for candidate in candidates
        )
        phase_total = statistics.median(
            candidate.compilation_sec + candidate.execution_sec
            for candidate in candidates
        )
        displayed.append(
            DisplayPoint(
                source_line=point.source_line,
                raw_rows=point.raw_rows,
                compilation_sec=compilation,
                execution_sec=max(0.0, phase_total - compilation),
            )
        )
    return displayed


def main() -> int:
    args = parse_args()
    if args.annotate_outliers and not args.drop_outliers:
        raise ValueError("--annotate-outliers requires --drop-outliers")
    input_path = args.input.expanduser().resolve()
    queries_path = args.queries.expanduser().resolve()
    output = args.output_dir.expanduser().resolve()
    count = query_count(input_path, args.max_rows)
    query_names = labels(args, count)
    points, input_metadata = load(input_path, query_names, args.max_rows)

    rows = math.ceil(count / 2)
    basename, figure_size, layout = resolve_layout(
        args.basename,
        args.wide,
        (11, 7.4 if rows == 2 else 4.1),
        dpi=args.dpi,
    )
    fig, axes = plt.subplots(
        rows,
        2,
        figsize=figure_size,
        dpi=args.dpi if args.wide else None,
        sharex=True,
        squeeze=False,
    )
    plot_background = configure_figure(fig, wide=args.wide)
    excluded_ids: set[tuple[int, int]] = set()
    displayed_by_id: dict[tuple[int, int], DisplayPoint] = {}
    query_summaries: dict[str, Any] = {}

    for number, name in enumerate(query_names, start=1):
        axis = axes[(number - 1) // 2][(number - 1) % 2]
        axis.set_facecolor(plot_background)
        observed = query_series(points, number)
        if args.drop_outliers:
            included, outlier_report = tukey_upper_filter(observed)
        else:
            included = observed
            outlier_report = {
                "method": "Tukey upper fence on end-to-end result (Q3 + 1.5*IQR)",
                "applied": False,
                "reason": "--drop-outliers not requested",
                "excluded_observations": 0,
            }
        if not included:
            raise ValueError(f"outlier filter removed every q{number} observation")
        included_ids = {(point.source_line, point.query_number) for point in included}
        excluded_ids.update(
            (point.source_line, point.query_number)
            for point in observed
            if (point.source_line, point.query_number) not in included_ids
        )
        displayed = rolling_phase_median(included, args.smooth_window)
        for item in displayed:
            displayed_by_id[(item.source_line, number)] = item

        xs = [point.raw_rows for point in displayed]
        compilation = [point.compilation_sec for point in displayed]
        phase_total = [
            point.compilation_sec + point.execution_sec for point in displayed
        ]
        axis.fill_between(
            xs,
            0,
            compilation,
            color=COMPILATION,
            alpha=0.94,
            linewidth=0,
            zorder=2,
        )
        axis.fill_between(
            xs,
            compilation,
            phase_total,
            color=EXECUTION,
            alpha=0.96,
            linewidth=0,
            zorder=3,
        )
        axis.plot(xs, compilation, color=plot_background, linewidth=0.65, zorder=4)
        axis.plot(xs, phase_total, color=EXECUTION, linewidth=1.0, zorder=5)
        axis.set_xlim(0, args.max_rows)
        axis.set_ylim(0, max(1.0, math.ceil(max(phase_total) * 1.12 * 2) / 2))
        title_size = 17 if args.wide else 13
        tick_size = 13 if args.wide else 10
        label_size = 15 if args.wide else 11
        axis.set_title(name, color="white", fontsize=title_size, pad=10)
        axis.xaxis.set_major_formatter(FuncFormatter(human_rows))
        axis.yaxis.set_major_formatter(FuncFormatter(human_seconds))
        axis.tick_params(colors="white", labelsize=tick_size)
        axis.grid(True, color=GRID, linewidth=0.6, alpha=0.7, zorder=1)
        axis.spines[["top", "right"]].set_visible(False)
        axis.spines[["left", "bottom"]].set_color("white")
        if (number - 1) // 2 == rows - 1:
            axis.set_xlabel("Base-table row count", color="white", fontsize=label_size)
        if number % 2:
            axis.set_ylabel(
                "Provider phase time (seconds)\n↓ lower is better",
                color="white",
                fontsize=label_size,
            )
        if (
            args.annotate_outliers
            and int(outlier_report.get("excluded_observations") or 0) > 0
        ):
            annotate_outlier_stats(
                axis,
                outlier_report,
                len(observed),
                fontsize=10.5 if args.wide else 8.0,
            )
        query_summaries[str(number)] = {
            "query_name": name,
            "observed": point_stats(observed),
            "included_before_smoothing": point_stats(included),
            "displayed": {
                "observations": len(displayed),
                "compilation": numeric_stats(compilation),
                "execution": numeric_stats(
                    [point.execution_sec for point in displayed]
                ),
                "phase_total": numeric_stats(phase_total),
            },
            "outlier_filter": outlier_report,
        }

    for index in range(count, rows * 2):
        axes[index // 2][index % 2].set_visible(False)

    fig.legend(
        handles=[
            Patch(
                facecolor=COMPILATION,
                label=args.compilation_legend_label,
            ),
            Patch(
                facecolor=EXECUTION,
                label=args.execution_legend_label,
            ),
        ],
        loc="upper center",
        bbox_to_anchor=(0.5, 0.777 if args.wide else 0.995),
        ncol=2,
        facecolor=plot_background,
        edgecolor=GRID,
        labelcolor="white",
        fontsize=13.5 if args.wide else 10.5,
        framealpha=0.9,
    )
    if args.wide:
        fig.subplots_adjust(
            left=.090,
            right=.965,
            bottom=.095,
            top=.690,
            wspace=.105,
            hspace=.250,
        )
    else:
        fig.tight_layout(rect=(.008, .01, .995, .905))
    png, svg = save_figure(fig, output, basename, args.dpi, wide=args.wide)
    plt.close(fig)

    csv_path = output / f"{basename}_data.csv"
    csv_rows: list[dict[str, Any]] = []
    for point in sorted(points, key=lambda item: (item.query_number, item.raw_rows)):
        row = asdict(point)
        point_id = (point.source_line, point.query_number)
        displayed = displayed_by_id.get(point_id)
        row["included_in_plot"] = point_id not in excluded_ids
        row["exclusion_reason"] = (
            "above per-query Tukey upper fence on end-to-end result"
            if point_id in excluded_ids
            else ""
        )
        row["display_compilation_sec"] = (
            displayed.compilation_sec if displayed is not None else None
        )
        row["display_execution_sec"] = (
            displayed.execution_sec if displayed is not None else None
        )
        csv_rows.append(row)
    output.mkdir(parents=True, exist_ok=True)
    write_csv(csv_path, list(csv_rows[0].keys()), csv_rows)

    summary_path = output / f"{basename}_summary.json"
    write_json(
        summary_path,
        {
            "schema_version": 1,
            "chart": CHART_KEY,
            "layout": layout,
            "presentation": {
                "compilation_legend_label": args.compilation_legend_label,
                "execution_legend_label": args.execution_legend_label,
            },
            "selection": {
                "max_rows_inclusive": args.max_rows,
                "x_axis": "each observation at its recorded raw_rows",
                "outlier_policy": (
                    "per query: exclude whole observation when end-to-end result > Q3 + 1.5*IQR"
                    if args.drop_outliers
                    else "none"
                ),
                "outliers_preserved_in_csv_and_summary": True,
                "outlier_annotation_enabled": args.annotate_outliers,
                "outlier_annotations_rendered": sum(
                    1
                    for query in query_summaries.values()
                    if args.annotate_outliers
                    and int(
                        query["outlier_filter"].get("excluded_observations") or 0
                    )
                    > 0
                ),
                "smoothing": (
                    {
                        "method": "centered rolling median of cumulative phase boundaries",
                        "window_observations": args.smooth_window,
                        "edges": "shrinking window",
                    }
                    if args.smooth_window > 1
                    else None
                ),
            },
            "timing_contract": {
                "stack": "provider-reported compilation_time plus execution_time",
                "outlier_metric": "runner result (end-to-end query latency)",
                "residual": "result - compilation_time - execution_time; retained in CSV and summary, not plotted",
                "interpretation": "the stack is provider phase telemetry; the end-to-end result remains the latency contract and normalized cost is not allocated from these phases",
            },
            "colors": {
                "compilation": COMPILATION,
                "execution": EXECUTION,
            },
            "sources": {
                "input": {"path": str(input_path), "sha256": sha256(input_path)},
                "queries": {"path": str(queries_path), "sha256": sha256(queries_path)},
            },
            "input_metadata": input_metadata,
            "queries": query_summaries,
            "outputs": {"png": str(png), "svg": str(svg), "csv": str(csv_path)},
        },
    )
    for path in (png, svg, csv_path, summary_path):
        print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
