#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Compare Snowflake dashboard latency across data paths and warehouses."""

from __future__ import annotations

import argparse
import math
import statistics
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from _common import (
    GRID,
    WHITE,
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


QUERY_NAMES = (
    "Single-symbol summary",
    "Watchlist summary",
    "Top movers",
    "Daily activity",
)
DEFAULT_BASENAME = "dashboard_path_latency_snowflake"
CHART_KEY = "snowflake_dashboard_data_path_and_warehouse_latency"


@dataclass(frozen=True)
class Condition:
    key: str
    label: str
    short_label: str
    total_label: str
    total_short_label: str
    expected_machine: str
    data_path: str
    warehouse: str
    color: str
    linestyle: str


CONDITIONS = (
    Condition(
        key="imv_interactive",
        label="Interactive MV · Interactive + Gen2 fallback",
        short_label="IMV · Interactive/fallback",
        total_label="Interactive MV",
        total_short_label="IMV",
        expected_machine="Interactive Small",
        data_path="Interactive Materialized View",
        warehouse="Interactive Small",
        color="#29B5E8",
        linestyle="-",
    ),
    Condition(
        key="imv_gen2",
        label="Interactive MV · Gen2 Small",
        short_label="IMV · Gen2",
        total_label="Interactive MV · Gen2 Small",
        total_short_label="IMV · Gen2",
        expected_machine="Gen2 Small",
        data_path="Interactive Materialized View",
        warehouse="Gen2 Small",
        color="#FF9D4D",
        linestyle="--",
    ),
    Condition(
        key="raw_interactive",
        label="Raw Interactive Table · Interactive + Gen2 fallback",
        short_label="Raw IT · Interactive/fallback",
        total_label="Raw Interactive Table",
        total_short_label="Raw IT",
        expected_machine="Interactive Small",
        data_path="Raw Interactive Table",
        warehouse="Interactive Small",
        color="#C792EA",
        linestyle="-.",
    ),
)


@dataclass(frozen=True)
class Point:
    condition: str
    source_line: int
    iteration: int
    observed_at: str
    raw_rows: int
    mv_rows: int
    query_number: int
    query_name: str
    latency_sec: float
    compilation_sec: float
    execution_sec: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--imv-interactive", type=Path, required=True)
    parser.add_argument("--imv-gen2", type=Path, required=True)
    parser.add_argument("--raw-interactive", type=Path, required=True)
    parser.add_argument("--mv-queries", type=Path, required=True)
    parser.add_argument("--raw-queries", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-rows", type=int, default=100_000_000_000)
    parser.add_argument(
        "--drop-outliers",
        action="store_true",
        help=(
            "Apply a separate per-condition, per-query Tukey upper fence. "
            "Excluded measurements remain in the CSV and summary."
        ),
    )
    parser.add_argument(
        "--annotate-outliers",
        action="store_true",
        help=(
            "Annotate only panels and conditions with actual exclusions; "
            "requires --drop-outliers."
        ),
    )
    parser.add_argument(
        "--smooth-window",
        type=int,
        default=7,
        help="Centered rolling-median display window; 1 disables smoothing.",
    )
    parser.add_argument("--basename", default=DEFAULT_BASENAME)
    parser.add_argument(
        "--query-cost-display",
        choices=("total", "attribution"),
        default="total",
        help=(
            "Use clean data-path labels, or labels that also disclose the "
            "Interactive/Gen2 fallback attribution. Provenance is unchanged."
        ),
    )
    parser.add_argument("--wide", action="store_true")
    parser.add_argument("--dpi", type=int, default=300)
    return parser.parse_args()


def load(
    path: Path,
    condition: Condition,
    max_rows: int,
) -> tuple[list[Point], dict[str, Any]]:
    points: list[Point] = []
    records_read = records_selected = zero_rows = above_cap = 0
    systems: set[str] = set()
    machines: set[str] = set()
    cluster_sizes: set[str] = set()

    for line_number, record in iter_jsonl(path):
        records_read += 1
        systems.add(str(record.get("system") or ""))
        machines.add(str(record.get("machine") or ""))
        cluster_sizes.add(str(record.get("cluster_size") or ""))
        raw_rows = int(record.get("raw_rows") or 0)
        if raw_rows <= 0:
            zero_rows += 1
            continue
        if raw_rows > max_rows:
            above_cap += 1
            continue

        results = record.get("result")
        compilation = record.get("compilation_time")
        execution = record.get("execution_time")
        for field, values in (
            ("result", results),
            ("compilation_time", compilation),
            ("execution_time", execution),
        ):
            if not isinstance(values, list) or len(values) != len(QUERY_NAMES):
                raise ValueError(
                    f"expected {len(QUERY_NAMES)} {field} entries at "
                    f"{path}:{line_number}"
                )

        records_selected += 1
        for query_index, query_name in enumerate(QUERY_NAMES):
            context = f"{path}:{line_number}:q{query_index + 1}"
            points.append(
                Point(
                    condition=condition.key,
                    source_line=line_number,
                    iteration=int(record["iteration"]),
                    observed_at=str(record.get("iteration_started_at") or ""),
                    raw_rows=raw_rows,
                    mv_rows=int(record.get("mv_rows") or 0),
                    query_number=query_index + 1,
                    query_name=query_name,
                    latency_sec=scalar_trial(
                        results[query_index], context=f"{context}:result"
                    ),
                    compilation_sec=scalar_trial(
                        compilation[query_index],
                        context=f"{context}:compilation",
                    ),
                    execution_sec=scalar_trial(
                        execution[query_index], context=f"{context}:execution"
                    ),
                )
            )

    if not records_selected:
        raise ValueError(f"no observations within the selected row window at {path}")
    if machines != {condition.expected_machine}:
        raise ValueError(
            f"{condition.key}: expected machine {condition.expected_machine!r}; "
            f"found {sorted(machines)!r}"
        )
    return points, {
        "records_read": records_read,
        "records_selected": records_selected,
        "zero_row_records_excluded": zero_rows,
        "records_above_cap_excluded": above_cap,
        "systems": sorted(systems),
        "machines": sorted(machines),
        "cluster_sizes": sorted(cluster_sizes),
        "minimum_selected_raw_rows": min(point.raw_rows for point in points),
        "maximum_selected_raw_rows": max(point.raw_rows for point in points),
    }


def query_series(points: list[Point], condition: str, query: int) -> list[Point]:
    return sorted(
        (
            point
            for point in points
            if point.condition == condition and point.query_number == query
        ),
        key=lambda point: (point.raw_rows, point.iteration),
    )


def point_stats(points: list[Point]) -> dict[str, Any]:
    latencies = [point.latency_sec for point in points]
    ordered = sorted(latencies)
    maximum = max(points, key=lambda point: point.latency_sec)
    return {
        "observations": len(points),
        "average_latency_sec": statistics.fmean(latencies),
        "median_latency_sec": statistics.median(latencies),
        "p95_latency_sec": ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)],
        "maximum": asdict(maximum),
    }


def tukey_upper_filter(
    points: list[Point],
) -> tuple[list[Point], dict[str, Any]]:
    if len(points) < 8:
        return points, {
            "method": "Tukey upper fence (Q3 + 1.5*IQR)",
            "applied": False,
            "reason": "fewer than 8 observations",
            "excluded_observations": 0,
        }
    ordered = sorted(point.latency_sec for point in points)
    count = len(ordered)
    q1 = ordered[count // 4]
    q3 = ordered[3 * count // 4]
    fence = q3 + 1.5 * (q3 - q1)
    excluded = [point for point in points if point.latency_sec > fence]
    accepted = [point for point in points if point.latency_sec <= fence]
    return accepted, {
        "method": "Tukey upper fence (Q3 + 1.5*IQR)",
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


def rolling_median(points: list[Point], window: int) -> list[Point]:
    if window <= 1:
        return points
    if window % 2 == 0:
        raise ValueError("--smooth-window must be odd")
    half = window // 2
    result: list[Point] = []
    for index, point in enumerate(points):
        lower = max(0, index - half)
        upper = min(len(points), index + half + 1)
        result.append(
            replace(
                point,
                latency_sec=statistics.median(
                    candidate.latency_sec for candidate in points[lower:upper]
                ),
            )
        )
    return result


def annotate_exclusions(
    axis: Any,
    reports: dict[str, dict[str, Any]],
    observed_counts: dict[str, int],
    *,
    fontsize: float,
    query_cost_display: str,
) -> bool:
    lines: list[str] = []
    for condition in CONDITIONS:
        report = reports[condition.key]
        excluded = int(report.get("excluded_observations") or 0)
        if excluded == 0:
            continue
        fence = float(report["upper_fence_sec"])
        lines.append(
            f"{(condition.short_label if query_cost_display == 'attribution' else condition.total_short_label)}: "
            f"{excluded}/{observed_counts[condition.key]} "
            f"> {fence:,.1f}s"
        )
    if not lines:
        return False
    axis.text(
        0.018,
        0.975,
        "Display filtering (Tukey)\n" + "\n".join(lines),
        transform=axis.transAxes,
        ha="left",
        va="top",
        color=WHITE,
        fontsize=fontsize,
        linespacing=1.25,
        zorder=20,
        bbox={
            "boxstyle": "round,pad=0.38",
            "facecolor": "#222222",
            "edgecolor": GRID,
            "linewidth": 0.75,
            "alpha": 0.91,
        },
    )
    return True


def main() -> int:
    args = parse_args()
    if args.annotate_outliers and not args.drop_outliers:
        raise ValueError("--annotate-outliers requires --drop-outliers")
    if args.smooth_window < 1 or args.smooth_window % 2 == 0:
        raise ValueError("--smooth-window must be a positive odd integer")

    paths = {
        "imv_interactive": args.imv_interactive.expanduser().resolve(),
        "imv_gen2": args.imv_gen2.expanduser().resolve(),
        "raw_interactive": args.raw_interactive.expanduser().resolve(),
        "mv_queries": args.mv_queries.expanduser().resolve(),
        "raw_queries": args.raw_queries.expanduser().resolve(),
    }
    output_dir = args.output_dir.expanduser().resolve()

    all_points: list[Point] = []
    input_metadata: dict[str, Any] = {}
    for condition in CONDITIONS:
        points, metadata = load(paths[condition.key], condition, args.max_rows)
        all_points.extend(points)
        input_metadata[condition.key] = metadata

    basename, figure_size, layout = resolve_layout(
        args.basename,
        args.wide,
        (12.4, 7.8),
        dpi=args.dpi,
    )
    fig, axes = plt.subplots(
        2,
        2,
        figsize=figure_size,
        dpi=args.dpi if args.wide else None,
        sharex=True,
        squeeze=False,
    )
    plot_background = configure_figure(fig, wide=args.wide)

    plotted: dict[tuple[str, int], list[Point]] = {}
    accepted: dict[tuple[str, int], list[Point]] = {}
    reports: dict[tuple[str, int], dict[str, Any]] = {}
    excluded_ids: set[tuple[str, int, int]] = set()
    display_latency: dict[tuple[str, int, int], float] = {}
    legend_handles: dict[str, Any] = {}
    annotations_rendered = 0

    for query_number, query_name in enumerate(QUERY_NAMES, start=1):
        axis = axes[(query_number - 1) // 2][(query_number - 1) % 2]
        axis.set_facecolor(plot_background)
        panel_maximum = 0.0
        panel_reports: dict[str, dict[str, Any]] = {}
        observed_counts: dict[str, int] = {}

        for condition in CONDITIONS:
            observed = query_series(all_points, condition.key, query_number)
            observed_counts[condition.key] = len(observed)
            if args.drop_outliers:
                filtered, report = tukey_upper_filter(observed)
            else:
                filtered = observed
                report = {
                    "method": "Tukey upper fence (Q3 + 1.5*IQR)",
                    "applied": False,
                    "reason": "--drop-outliers not requested",
                    "excluded_observations": 0,
                }
            if not filtered:
                raise ValueError(
                    f"outlier filter removed every {condition.key} q{query_number} point"
                )
            trend = rolling_median(filtered, args.smooth_window)
            accepted[(condition.key, query_number)] = filtered
            plotted[(condition.key, query_number)] = trend
            reports[(condition.key, query_number)] = report
            panel_reports[condition.key] = report

            accepted_ids = {
                (point.condition, point.source_line, point.query_number)
                for point in filtered
            }
            excluded_ids.update(
                (point.condition, point.source_line, point.query_number)
                for point in observed
                if (point.condition, point.source_line, point.query_number)
                not in accepted_ids
            )
            for point in trend:
                display_latency[
                    (point.condition, point.source_line, point.query_number)
                ] = point.latency_sec

            panel_maximum = max(
                panel_maximum, max(point.latency_sec for point in trend)
            )
            line, = axis.plot(
                [point.raw_rows for point in trend],
                [point.latency_sec for point in trend],
                color=condition.color,
                linestyle=condition.linestyle,
                linewidth=3.1 if args.wide else 2.4,
                zorder=3,
            )
            legend_handles.setdefault(condition.key, line)

        axis.set_xlim(0, args.max_rows)
        axis.set_ylim(0, max(1.0, math.ceil(panel_maximum * 1.12 * 2) / 2))
        axis.set_title(
            query_name,
            color=WHITE,
            fontsize=17 if args.wide else 13,
            pad=10,
        )
        axis.xaxis.set_major_formatter(FuncFormatter(human_rows))
        axis.yaxis.set_major_formatter(FuncFormatter(human_seconds))
        axis.tick_params(colors=WHITE, labelsize=13 if args.wide else 10)
        axis.grid(
            True,
            color=GRID,
            linewidth=0.8 if args.wide else 0.65,
            alpha=0.7,
        )
        axis.spines[["top", "right"]].set_visible(False)
        axis.spines[["left", "bottom"]].set_color(WHITE)
        if (query_number - 1) // 2 == 1:
            axis.set_xlabel(
                "Base-table row count",
                color=WHITE,
                fontsize=15 if args.wide else 11,
            )
        if query_number % 2:
            axis.set_ylabel(
                "End-to-end query latency\n↓ lower is better",
                color=WHITE,
                fontsize=15 if args.wide else 11,
            )
        if args.annotate_outliers and annotate_exclusions(
            axis,
            panel_reports,
            observed_counts,
            fontsize=9.5 if args.wide else 7.4,
            query_cost_display=args.query_cost_display,
        ):
            annotations_rendered += 1

    legend_labels: list[str] = []
    for condition in CONDITIONS:
        total_excluded = sum(
            int(reports[(condition.key, query)].get("excluded_observations") or 0)
            for query in range(1, len(QUERY_NAMES) + 1)
        )
        suffix = " · Tukey-filtered" if total_excluded else ""
        display_label = (
            condition.label
            if args.query_cost_display == "attribution"
            else condition.total_label
        )
        legend_labels.append(display_label + suffix)
    fig.legend(
        [legend_handles[condition.key] for condition in CONDITIONS],
        legend_labels,
        loc="upper center",
        bbox_to_anchor=(0.5, 0.777 if args.wide else 0.995),
        ncol=3,
        facecolor=plot_background,
        edgecolor=GRID,
        labelcolor=WHITE,
        fontsize=12.0 if args.wide else 8.8,
        framealpha=0.9,
        columnspacing=1.6,
        handlelength=3.2,
    )
    if args.wide:
        fig.subplots_adjust(
            left=0.090,
            right=0.965,
            bottom=0.095,
            top=0.690,
            wspace=0.105,
            hspace=0.250,
        )
    else:
        fig.tight_layout(rect=(0.008, 0.01, 0.995, 0.91))

    png, svg = save_figure(
        fig, output_dir, basename, args.dpi, wide=args.wide
    )
    plt.close(fig)

    csv_path = output_dir / f"{basename}_data.csv"
    csv_rows: list[dict[str, Any]] = []
    for point in sorted(
        all_points,
        key=lambda item: (
            item.query_number,
            item.condition,
            item.raw_rows,
            item.iteration,
        ),
    ):
        condition = next(item for item in CONDITIONS if item.key == point.condition)
        point_id = (point.condition, point.source_line, point.query_number)
        row = asdict(point)
        row.update(
            {
                "legend_label": (
                    condition.label
                    if args.query_cost_display == "attribution"
                    else condition.total_label
                ),
                "attribution_legend_label": condition.label,
                "data_path": condition.data_path,
                "warehouse": condition.warehouse,
                "display_latency_sec": display_latency.get(point_id, ""),
                "included_in_plot": point_id not in excluded_ids,
                "exclusion_reason": (
                    "above per-condition, per-query Tukey upper fence"
                    if point_id in excluded_ids
                    else ""
                ),
            }
        )
        csv_rows.append(row)
    write_csv(csv_path, list(csv_rows[0].keys()), csv_rows)

    summary_path = output_dir / f"{basename}_summary.json"
    write_json(
        summary_path,
        {
            "schema_version": 1,
            "chart": CHART_KEY,
            "layout": layout,
            "presentation": {
                "query_cost_display": args.query_cost_display,
                "cost_attribution_visible_on_chart": (
                    args.query_cost_display == "attribution"
                ),
            },
            "comparison_contract": {
                "questions": "the same four dashboard questions",
                "data_paths": {
                    "imv": "read the maintained Interactive Materialized View",
                    "raw": "compute equivalent results directly from the raw Interactive Table",
                },
                "latency_metric": "runner result: end-to-end latency including compilation and execution",
                "interactive_fallback_disclosure": "For cost attribution, Interactive query jobs with elapsed time >5s are treated as fallback-priced at Gen2 Small; this latency chart does not classify or recolor individual points.",
                "alignment": "each condition at its own observed raw_rows; no iteration join or interpolation",
                "row_cap_inclusive": args.max_rows,
            },
            "selection": {
                "zero_row_baseline": "excluded",
                "smoothing": (
                    {
                        "method": "centered rolling median",
                        "window_observations": args.smooth_window,
                        "edges": "shrinking window",
                    }
                    if args.smooth_window > 1
                    else None
                ),
                "outlier_policy": (
                    "per condition and query: exclude latency > Q3 + 1.5*IQR"
                    if args.drop_outliers
                    else "none"
                ),
                "outliers_preserved_in_csv_and_summary": True,
                "outlier_annotation_enabled": args.annotate_outliers,
                "outlier_annotations_rendered": annotations_rendered,
            },
            "sources": {
                key: {"path": str(path), "sha256": sha256(path)}
                for key, path in paths.items()
            },
            "input_metadata": input_metadata,
            "conditions": {
                condition.key: {
                    "label": (
                        condition.label
                        if args.query_cost_display == "attribution"
                        else condition.total_label
                    ),
                    "attribution_label": condition.label,
                    "data_path": condition.data_path,
                    "warehouse": condition.warehouse,
                    "color": condition.color,
                    "linestyle": condition.linestyle,
                    "queries": {
                        str(query): {
                            "observed": point_stats(
                                query_series(all_points, condition.key, query)
                            ),
                            "accepted_after_filter": point_stats(
                                accepted[(condition.key, query)]
                            ),
                            "display_trend": point_stats(
                                plotted[(condition.key, query)]
                            ),
                            "outlier_filter": reports[(condition.key, query)],
                        }
                        for query in range(1, len(QUERY_NAMES) + 1)
                    },
                }
                for condition in CONDITIONS
            },
            "outputs": {
                "png": str(png),
                "svg": str(svg),
                "csv": str(csv_path),
            },
        },
    )
    for path in (png, svg, csv_path, summary_path):
        print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
