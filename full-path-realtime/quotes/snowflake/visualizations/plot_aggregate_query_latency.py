#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Plot ClickHouse and Snowflake aggregate-query latency versus raw rows."""

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
    CLICKHOUSE, GRID, SNOWFLAKE, annotate_outlier_stats,
    configure_figure, draw_matched_query_totals_strip, human_rows,
    human_seconds, iter_jsonl, read_json, resolve_layout, save_figure,
    scalar_trial, sha256,
    validate_clickhouse_query_cost, validate_matched_query_totals,
    validate_snowflake_query_cost, write_csv, write_json,
)


QUERY_NAMES = ("Single-symbol summary", "Watchlist summary", "Top movers", "Daily activity")
DEFAULT_BASENAME = "aggregate_query_latency_clickhouse_vs_snowflake"
CHART_KEY = "clickhouse_vs_snowflake_aggregate_query_latency"


@dataclass(frozen=True)
class Point:
    system: str
    source_line: int
    iteration: int
    observed_at: str
    raw_rows: int
    query_number: int
    query_name: str
    latency_sec: float
    compilation_sec: float | None
    execution_sec: float | None
    runtime_source: str


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clickhouse", type=Path, required=True)
    parser.add_argument("--snowflake", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--clickhouse-legend-label",
        default="ClickHouse Cloud",
        help="Legend label; does not change the internal system identity.",
    )
    parser.add_argument(
        "--snowflake-legend-label",
        default="Snowflake",
        help="Legend label; does not change the internal system identity.",
    )
    parser.add_argument("--clickhouse-queries", type=Path, default=root / "clickhouse-cloud/queries_mv.sql")
    parser.add_argument("--snowflake-queries", type=Path, default=root / "snowflake/t2/queries_mv_imv.sql")
    parser.add_argument("--max-rows", type=int, default=100_000_000_000)
    parser.add_argument(
        "--drop-outliers",
        action="store_true",
        help=(
            "Exclude Snowflake points above the per-query Tukey upper fence "
            "(Q3 + 1.5*IQR). Excluded observations remain in the CSV and summary."
        ),
    )
    parser.add_argument(
        "--smooth-window",
        type=int,
        default=1,
        help="Centered rolling-median display window; 1 disables smoothing.",
    )
    parser.add_argument(
        "--annotate-outliers",
        action="store_true",
        help=(
            "Add a compact Snowflake exclusion-count, Tukey-fence, and maximum-"
            "excluded-latency overlay only to panels with exclusions; requires "
            "--drop-outliers."
        ),
    )
    parser.add_argument("--basename", default=DEFAULT_BASENAME)
    parser.add_argument(
        "--summary-strip",
        action="store_true",
        help="Add matched active-ingestion accumulated runtime and query cost below the latency panels.",
    )
    parser.add_argument("--clickhouse-cost-summary", type=Path)
    parser.add_argument("--snowflake-cost-summary", type=Path)
    parser.add_argument("--snowflake-pricing", type=Path)
    parser.add_argument(
        "--snowflake-fallback-pricing",
        type=Path,
        help="Required when the Snowflake cost summary contains fallback components.",
    )
    parser.add_argument(
        "--query-cost-display",
        choices=("total", "attribution"),
        default="total",
        help=(
            "Show only total query cost, or also render the Snowflake "
            "fallback-attribution note. Validation and JSON provenance are "
            "identical in both modes."
        ),
    )
    parser.add_argument("--tier", default="enterprise")
    parser.add_argument(
        "--wide",
        action="store_true",
        help=(
            "Render an exact 5156x2900 Keynote variant with a transparent "
            "560px header-safe area and subtle chart stage; suffix outputs "
            "with _wide."
        ),
    )
    parser.add_argument(
        "--outlier-legend-disclosure",
        choices=("suffix", "panel-only"),
        default="suffix",
        help=(
            "Either append the outlier disclosure to the Snowflake legend or "
            "rely on per-panel annotations."
        ),
    )
    parser.add_argument("--dpi", type=int, default=300)
    return parser.parse_args()


def load(path: Path, system: str, max_rows: int) -> tuple[list[Point], dict[str, Any]]:
    points: list[Point] = []
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
        if not isinstance(results, list) or len(results) != len(QUERY_NAMES):
            raise ValueError(f"expected {len(QUERY_NAMES)} results at {path}:{line}")
        compilation = record.get("compilation_time")
        execution = record.get("execution_time")
        if system == "Snowflake":
            if not isinstance(compilation, list) or len(compilation) != len(QUERY_NAMES):
                raise ValueError(f"missing Snowflake compilation telemetry at {path}:{line}")
            if not isinstance(execution, list) or len(execution) != len(QUERY_NAMES):
                raise ValueError(f"missing Snowflake execution telemetry at {path}:{line}")
        selected += 1
        for index, query_name in enumerate(QUERY_NAMES):
            context = f"{path}:{line}:q{index + 1}"
            comp = scalar_trial(compilation[index], context=f"{context}:compilation") if system == "Snowflake" else None
            exe = scalar_trial(execution[index], context=f"{context}:execution") if system == "Snowflake" else None
            points.append(Point(
                system=system,
                source_line=line,
                iteration=int(record["iteration"]),
                observed_at=str(record.get("iteration_started_at") or ""),
                raw_rows=raw_rows,
                query_number=index + 1,
                query_name=query_name,
                latency_sec=scalar_trial(results[index], context=context),
                compilation_sec=comp,
                execution_sec=exe,
                runtime_source=("runner result (end-to-end); compilation/execution retained" if system == "Snowflake" else "clickhouse-client --time"),
            ))
    if not selected:
        raise ValueError(f"no records in active row window at {path}")
    return points, {
        "records_read": read, "records_selected": selected,
        "zero_row_records_excluded": zero, "records_above_cap_excluded": above,
        "systems": sorted(systems), "machines": sorted(machines),
    }


def series(points: list[Point], system: str, query: int) -> list[Point]:
    return sorted((p for p in points if p.system == system and p.query_number == query), key=lambda p: (p.raw_rows, p.iteration))


def stats(values: list[Point]) -> dict[str, Any]:
    ys = [p.latency_sec for p in values]
    ordered = sorted(ys)
    maximum = max(values, key=lambda p: p.latency_sec)
    return {
        "observations": len(values), "average_latency_sec": statistics.fmean(ys),
        "median_latency_sec": statistics.median(ys),
        "p95_latency_sec": ordered[max(0, math.ceil(len(ordered) * .95) - 1)],
        "maximum": asdict(maximum),
    }


def tukey_upper_filter(values: list[Point]) -> tuple[list[Point], dict[str, Any]]:
    """Apply the deprecated renderer's upper-outlier rule without losing evidence."""
    if len(values) < 8:
        return values, {
            "method": "Tukey upper fence (Q3 + 1.5*IQR)",
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


def rolling_median(values: list[Point], window: int) -> list[Point]:
    if window <= 1:
        return values
    if window % 2 == 0:
        raise ValueError("--smooth-window must be odd")
    half = window // 2
    trend: list[Point] = []
    for index, point in enumerate(values):
        lower = max(0, index - half)
        upper = min(len(values), index + half + 1)
        latency = statistics.median(
            candidate.latency_sec for candidate in values[lower:upper]
        )
        trend.append(replace(point, latency_sec=latency))
    return trend


def main() -> int:
    args = parse_args()
    if args.annotate_outliers and not args.drop_outliers:
        raise ValueError("--annotate-outliers requires --drop-outliers")
    summary_inputs = {
        "clickhouse_cost_summary": args.clickhouse_cost_summary,
        "snowflake_cost_summary": args.snowflake_cost_summary,
        "snowflake_pricing": args.snowflake_pricing,
    }
    if args.summary_strip and any(value is None for value in summary_inputs.values()):
        missing = [name for name, value in summary_inputs.items() if value is None]
        raise ValueError(f"--summary-strip requires: {', '.join(missing)}")
    paths = {name: value.expanduser().resolve() for name, value in {
        "clickhouse": args.clickhouse, "snowflake": args.snowflake,
        "clickhouse_queries": args.clickhouse_queries, "snowflake_queries": args.snowflake_queries,
    }.items()}
    paths.update({
        name: value.expanduser().resolve()
        for name, value in summary_inputs.items()
        if value is not None
    })
    if args.snowflake_fallback_pricing is not None:
        paths["snowflake_fallback_pricing"] = (
            args.snowflake_fallback_pricing.expanduser().resolve()
        )
    output = args.output_dir.expanduser().resolve()
    ch, ch_meta = load(paths["clickhouse"], "ClickHouse Cloud", args.max_rows)
    sf, sf_meta = load(paths["snowflake"], "Snowflake", args.max_rows)
    all_points = ch + sf

    rows = math.ceil(len(QUERY_NAMES) / 2)
    default_height = (8.1 if rows == 2 else 4.8) if args.summary_strip else (7.4 if rows == 2 else 4.1)
    basename, figure_size, layout = resolve_layout(
        args.basename,
        args.wide,
        (11, default_height),
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
    handles: list[Any] = []
    labels: list[str] = []
    plotted: dict[tuple[str, int], list[Point]] = {}
    outlier_reports: dict[tuple[str, int], dict[str, Any]] = {}
    excluded_ids: set[tuple[str, int, int]] = set()
    for number, name in enumerate(QUERY_NAMES, start=1):
        axis = axes[(number - 1) // 2][(number - 1) % 2]
        axis.set_facecolor(plot_background)
        maximum = 0.0
        for system, color in (("ClickHouse Cloud", CLICKHOUSE), ("Snowflake", SNOWFLAKE)):
            observed = series(all_points, system, number)
            if args.drop_outliers and system == "Snowflake":
                values, report = tukey_upper_filter(observed)
            else:
                values = observed
                report = {
                    "method": "Tukey upper fence (Q3 + 1.5*IQR)",
                    "applied": False,
                    "reason": (
                        "filter is Snowflake-only"
                        if args.drop_outliers
                        else "--drop-outliers not requested"
                    ),
                    "excluded_observations": 0,
                }
            if not values:
                raise ValueError(f"outlier filter removed every {system} q{number} point")
            values = rolling_median(values, args.smooth_window)
            plotted[(system, number)] = values
            outlier_reports[(system, number)] = report
            plotted_ids = {(point.system, point.source_line, point.query_number) for point in values}
            excluded_ids.update(
                (point.system, point.source_line, point.query_number)
                for point in observed
                if (point.system, point.source_line, point.query_number) not in plotted_ids
            )
            ys = [p.latency_sec for p in values]
            maximum = max(maximum, max(ys))
            line, = axis.plot(
                [p.raw_rows for p in values],
                ys,
                color=color,
                linewidth=3.0 if args.wide else 2.35,
                zorder=3,
            )
            label = (
                args.clickhouse_legend_label
                if system == "ClickHouse Cloud"
                else args.snowflake_legend_label
            )
            if label not in labels:
                handles.append(line); labels.append(label)
        axis.set_xlim(0, args.max_rows)
        axis.set_ylim(0, max(1.0, math.ceil(maximum * 1.12 * 2) / 2))
        title_size = 17 if args.wide else 13
        tick_size = 13 if args.wide else 10
        label_size = 15 if args.wide else 11
        axis.set_title(name, color="white", fontsize=title_size, pad=10)
        axis.xaxis.set_major_formatter(FuncFormatter(human_rows))
        axis.yaxis.set_major_formatter(FuncFormatter(human_seconds))
        axis.tick_params(colors="white", labelsize=tick_size)
        axis.grid(True, color=GRID, linewidth=.8 if args.wide else .65, alpha=.7)
        axis.spines[["top", "right"]].set_visible(False)
        axis.spines[["left", "bottom"]].set_color("white")
        if (number - 1) // 2 == rows - 1:
            axis.set_xlabel("Base-table row count", color="white", fontsize=label_size)
        if number % 2:
            axis.set_ylabel("Query latency (seconds)\n↓ lower is better", color="white", fontsize=label_size)
        snowflake_report = outlier_reports[("Snowflake", number)]
        if (
            args.annotate_outliers
            and int(snowflake_report.get("excluded_observations") or 0) > 0
        ):
            observed = series(all_points, "Snowflake", number)
            annotate_outlier_stats(
                axis,
                snowflake_report,
                len(observed),
                fontsize=10.5 if args.wide else 8.0,
            )
    snowflake_exclusions = sum(
        int(outlier_reports[("Snowflake", number)].get("excluded_observations") or 0)
        for number in range(1, len(QUERY_NAMES) + 1)
    )
    if (
        snowflake_exclusions
        and args.outlier_legend_disclosure == "panel-only"
        and not args.annotate_outliers
    ):
        raise ValueError(
            "--outlier-legend-disclosure panel-only requires "
            "--annotate-outliers when exclusions are present"
        )
    if (
        snowflake_exclusions
        and args.outlier_legend_disclosure == "suffix"
        and args.snowflake_legend_label in labels
    ):
        labels[labels.index(args.snowflake_legend_label)] = (
            f"{args.snowflake_legend_label} · Tukey outliers excluded"
        )
    legend_y = 0.777 if args.wide else 0.995
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(.5, legend_y), ncol=2,
               facecolor=plot_background, edgecolor=GRID, labelcolor="white",
               fontsize=13.5 if args.wide else 10.5, framealpha=.9)
    matched_totals: dict[str, Any] | None = None
    if args.summary_strip:
        clickhouse_totals = validate_clickhouse_query_cost(
            read_json(paths["clickhouse_cost_summary"]),
            args.tier,
            context=str(paths["clickhouse_cost_summary"]),
        )
        snowflake_totals = validate_snowflake_query_cost(
            read_json(paths["snowflake_cost_summary"]),
            read_json(paths["snowflake_pricing"]),
            args.tier,
            context=str(paths["snowflake_cost_summary"]),
            fallback_pricing=(
                read_json(paths["snowflake_fallback_pricing"])
                if "snowflake_fallback_pricing" in paths
                else None
            ),
        )
        validate_matched_query_totals(
            clickhouse_totals,
            snowflake_totals,
            context=CHART_KEY,
        )
        ratios = draw_matched_query_totals_strip(
            fig,
            clickhouse_totals,
            snowflake_totals,
            query_cost_display=args.query_cost_display,
            wide=args.wide,
        )
        matched_totals = {
            "scope": "matched active-ingestion query executions for this workload only",
            "pricing_tier": args.tier,
            "clickhouse": clickhouse_totals,
            "snowflake": snowflake_totals,
            **ratios,
        }
    if args.wide:
        fig.subplots_adjust(
            left=0.095,
            right=0.965,
            bottom=0.230 if args.summary_strip else 0.095,
            top=0.690,
            wspace=0.105,
            hspace=0.250,
        )
    else:
        fig.tight_layout(rect=(0.008, .210 if args.summary_strip else .01, .995, .905))
    png, svg = save_figure(
        fig,
        output,
        basename,
        args.dpi,
        wide=args.wide,
    )
    plt.close(fig)
    csv_path = output / f"{basename}_data.csv"
    csv_rows = []
    for point in sorted(all_points, key=lambda p: (p.query_number, p.system, p.raw_rows)):
        row = asdict(point)
        point_id = (point.system, point.source_line, point.query_number)
        row["included_in_plot"] = point_id not in excluded_ids
        row["exclusion_reason"] = (
            "above per-query Tukey upper fence" if point_id in excluded_ids else ""
        )
        csv_rows.append(row)
    write_csv(csv_path, list(csv_rows[0].keys()), csv_rows)
    summary_path = output / f"{basename}_summary.json"
    write_json(summary_path, {
        "schema_version": 1, "chart": CHART_KEY, "layout": layout,
        "presentation": {
            "clickhouse_legend_label": args.clickhouse_legend_label,
            "snowflake_legend_label": args.snowflake_legend_label,
            "outlier_legend_disclosure": args.outlier_legend_disclosure,
            "query_cost_display": args.query_cost_display,
            "cost_attribution_visible_on_chart": (
                args.summary_strip
                and args.query_cost_display == "attribution"
                and bool(
                    matched_totals
                    and matched_totals["snowflake"].get(
                        "fallback_priced_query_jobs"
                    )
                )
            ),
        },
        "selection": {
            "max_rows_inclusive": args.max_rows,
            "alignment": "each system at its own observed raw rows; no iteration matching or interpolation",
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
                "Snowflake only, per query: exclude latency > Q3 + 1.5*IQR"
                if args.drop_outliers
                else "none"
            ),
            "outliers_preserved_in_csv_and_summary": True,
            "outlier_annotation_enabled": args.annotate_outliers,
            "outlier_annotations_rendered": sum(
                1
                for number in range(1, len(QUERY_NAMES) + 1)
                if args.annotate_outliers
                and int(
                    outlier_reports[("Snowflake", number)].get(
                        "excluded_observations"
                    )
                    or 0
                )
                > 0
            ),
            "matched_runtime_cost_summary_strip": args.summary_strip,
        },
        "sources": {key: {"path": str(path), "sha256": sha256(path)} for key, path in paths.items()},
        "input_metadata": {"clickhouse": ch_meta, "snowflake": sf_meta},
        "series": {
            system: {
                str(q): {
                    "observed": stats(series(all_points, system, q)),
                    "plotted": stats(plotted[(system, q)]),
                    "outlier_filter": outlier_reports[(system, q)],
                }
                for q in range(1, len(QUERY_NAMES) + 1)
            }
            for system in ("ClickHouse Cloud", "Snowflake")
        },
        "snowflake_timing_contract": "result is plotted as end-to-end latency; compilation_time and execution_time are retained as supporting telemetry",
        "matched_active_ingestion_totals": matched_totals,
        "outputs": {"png": str(png), "svg": str(svg), "csv": str(csv_path)},
    })
    for path in (png, svg, csv_path, summary_path): print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
