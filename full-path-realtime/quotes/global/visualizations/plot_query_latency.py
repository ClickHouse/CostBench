#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Render provider-neutral aggregate or drill-down latency charts."""

from __future__ import annotations

import argparse
import math
import statistics
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
from _common import (
    GRID,
    configure_figure,
    human_rows,
    human_seconds,
    iter_jsonl,
    load_manifest,
    resolve_layout,
    resolve_source,
    rolling_median,
    save_figure,
    scalar_trial,
    sha256,
    validate_required_labels,
    write_csv,
    write_json,
)
from matplotlib.ticker import FuncFormatter

WORKLOADS = {
    "aggregate": {
        "query_names": ("Single-symbol summary", "Watchlist summary", "Top movers", "Daily activity"),
        "smooth_window": 7,
        "basename": "aggregate_query_latency_all_systems",
    },
    "drilldown": {
        "query_names": ("Hourly OHLCV bars", "Risk & liquidity (B7)"),
        "smooth_window": 5,
        "basename": "drilldown_query_latency_all_systems",
    },
}


@dataclass(frozen=True)
class Point:
    provider: str
    label: str
    source_line: int
    iteration: int
    observed_at: str
    raw_rows: int
    query_number: int
    query_name: str
    latency_sec: float
    runtime_source: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload", choices=tuple(WORKLOADS), required=True)
    parser.add_argument("--manifest", type=Path, default=Path(__file__).with_name("manifest.json"))
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-rows", type=int)
    parser.add_argument("--smooth-window", type=int)
    parser.add_argument("--basename")
    parser.add_argument(
        "--wide",
        action="store_true",
        help="Render the Keynote-native 5156x2900 staged variant and append _wide to the basename.",
    )
    parser.add_argument("--dpi", type=int, default=300)
    return parser.parse_args()


def load_points(
    path: Path,
    provider: str,
    label: str,
    query_names: tuple[str, ...],
    max_rows: int,
) -> tuple[list[Point], dict[str, Any]]:
    points: list[Point] = []
    read = selected = zero = above = 0
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
        if not isinstance(results, list) or len(results) != len(query_names):
            raise ValueError(f"expected {len(query_names)} results at {path}:{line}")
        jobs = record.get("query_jobs")
        if provider == "bigquery":
            if not isinstance(jobs, list) or len(jobs) != len(query_names):
                raise ValueError(f"missing BigQuery job evidence at {path}:{line}")
            for job in jobs:
                if job.get("error") is not None or job.get("cache_hit") is not False:
                    raise ValueError(f"invalid BigQuery query evidence at {path}:{line}")
        selected += 1
        for index, query_name in enumerate(query_names):
            latency = scalar_trial(results[index], f"{path}:{line}:q{index + 1}")
            source = "clickhouse-client --time"
            if provider == "snowflake":
                source = "runner result (end-to-end)"
            elif provider == "bigquery":
                job = jobs[index]
                if not math.isclose(latency, float(job["runtime_sec"]), abs_tol=1e-9):
                    raise ValueError(f"BigQuery result/job runtime mismatch at {path}:{line}:q{index + 1}")
                source = str(job.get("runtime_source") or "unknown")
            points.append(Point(
                provider, label, line, int(record["iteration"]),
                str(record.get("iteration_started_at") or record.get("scheduled_start_at") or ""),
                raw_rows, index + 1, query_name, latency, source,
            ))
    if not selected:
        raise ValueError(f"no records in (0, {max_rows:,}] at {path}")
    return points, {
        "records_read": read,
        "records_selected": selected,
        "zero_row_records_excluded": zero,
        "records_above_cap_excluded": above,
        "systems": sorted(systems),
        "machines": sorted(machines),
    }


def tukey_upper(values: list[Point]) -> tuple[list[Point], dict[str, Any]]:
    ordered = sorted(point.latency_sec for point in values)
    if len(ordered) < 8:
        return values, {"applied": False, "reason": "fewer than 8 observations", "excluded": 0}
    count = len(ordered)
    q1 = ordered[count // 4]
    q3 = ordered[3 * count // 4]
    fence = q3 + 1.5 * (q3 - q1)
    excluded = [point for point in values if point.latency_sec > fence]
    return [point for point in values if point.latency_sec <= fence], {
        "applied": True,
        "method": "Tukey upper fence (Q3 + 1.5*IQR)",
        "q1_sec": q1,
        "q3_sec": q3,
        "upper_fence_sec": fence,
        "excluded": len(excluded),
        "excluded_max_sec": max((point.latency_sec for point in excluded), default=None),
        "excluded_points": [asdict(point) for point in excluded],
    }


def latency_stats(points: list[Point]) -> dict[str, Any]:
    values = [point.latency_sec for point in points]
    ordered = sorted(values)
    return {
        "observations": len(values),
        "average_sec": statistics.fmean(values),
        "median_sec": statistics.median(values),
        "p95_sec": ordered[max(0, math.ceil(len(ordered) * .95) - 1)],
        "maximum_sec": max(values),
    }


def main() -> int:
    args = parse_args()
    manifest, manifest_path = load_manifest(args.manifest)
    workload = WORKLOADS[args.workload]
    query_names = workload["query_names"]
    max_rows = args.max_rows or int(manifest["row_cap"])
    smooth_window = args.smooth_window or int(workload["smooth_window"])
    basename = args.basename or str(workload["basename"])
    output = args.output_dir.expanduser().resolve()

    all_points: list[Point] = []
    source_meta: dict[str, Any] = {}
    source_paths: dict[str, Path] = {}
    query_paths: dict[str, Path] = {}
    active_providers = {
        provider: config
        for provider, config in manifest["providers"].items()
        if args.workload in config and f"{args.workload}_queries" in config
    }
    required_labels = validate_required_labels(
        manifest,
        f"{args.workload}_query_latency",
        (config["label"] for config in active_providers.values()),
    )
    for provider, config in active_providers.items():
        path = resolve_source(config[args.workload])
        query_path = resolve_source(config[f"{args.workload}_queries"])
        source_paths[provider] = path
        query_paths[provider] = query_path
        points, metadata = load_points(path, provider, config["label"], query_names, max_rows)
        all_points.extend(points)
        source_meta[provider] = metadata

    rows = math.ceil(len(query_names) / 2)
    basename, figure_size, layout = resolve_layout(
        basename,
        args.wide,
        (12, 7.6 if rows == 2 else 4.3),
        dpi=args.dpi,
    )
    line_width = 3.0 if args.wide else 2.2
    title_fontsize = 17 if args.wide else 12
    tick_fontsize = 13 if args.wide else 9
    label_fontsize = 15 if args.wide else 10
    legend_fontsize = 13.5 if args.wide else 10
    fig, axes = plt.subplots(
        rows,
        2,
        figsize=figure_size,
        dpi=args.dpi if args.wide else None,
        sharex=True,
        squeeze=False,
    )
    plot_background = configure_figure(fig, wide=args.wide)
    legend_handles: list[Any] = []
    legend_labels: list[str] = []
    reports: dict[str, Any] = {}
    csv_rows: list[dict[str, Any]] = []

    for number, query_name in enumerate(query_names, 1):
        axis = axes[(number - 1) // 2][(number - 1) % 2]
        axis.set_facecolor(plot_background)
        panel_max = 0.0
        for provider, config in active_providers.items():
            observed = sorted(
                (p for p in all_points if p.provider == provider and p.query_number == number),
                key=lambda p: (p.raw_rows, p.iteration),
            )
            use_filter = (
                args.workload == "aggregate"
                and config.get("aggregate_outlier_policy") == "tukey_upper"
            )
            plotted, report = tukey_upper(observed) if use_filter else (
                observed,
                {"applied": False, "reason": "no configured filter", "excluded": 0},
            )
            if not plotted:
                raise ValueError(f"no plotted {provider} points for q{number}")
            smoothed = rolling_median([point.latency_sec for point in plotted], smooth_window)
            panel_max = max(panel_max, max(smoothed))
            line, = axis.plot(
                [point.raw_rows for point in plotted], smoothed,
                color=config["color"], linewidth=line_width, zorder=3,
            )
            legend = config["label"]
            if legend not in legend_labels:
                legend_handles.append(line)
                legend_labels.append(legend)
            plotted_ids = {point.source_line for point in plotted}
            for point in observed:
                row = asdict(point)
                row["included_in_plot"] = point.source_line in plotted_ids
                csv_rows.append(row)
            reports[f"{provider}_q{number}"] = {
                "raw_stats": latency_stats(observed),
                "plotted_stats_before_display_smoothing": latency_stats(plotted),
                "outlier_policy": report,
            }
        axis.set_xlim(0, max_rows)
        axis.set_ylim(0, max(1.0, math.ceil(panel_max * 1.12 * 2) / 2))
        axis.set_title(query_name, color="white", fontsize=title_fontsize, pad=10)
        axis.xaxis.set_major_formatter(FuncFormatter(human_rows))
        axis.yaxis.set_major_formatter(FuncFormatter(human_seconds))
        axis.tick_params(colors="white", labelsize=tick_fontsize)
        axis.grid(True, color=GRID, linewidth=.6, alpha=.7)
        axis.spines[["top", "right"]].set_visible(False)
        axis.spines[["left", "bottom"]].set_color("white")
        if (number - 1) // 2 == rows - 1:
            axis.set_xlabel("Base-table row count", color="white", fontsize=label_fontsize)
        if number % 2:
            axis.set_ylabel("Query latency (seconds)\n↓ lower is better", color="white", fontsize=label_fontsize)

    for index in range(len(query_names), rows * 2):
        axes[index // 2][index % 2].set_visible(False)
    snowflake_exclusions = sum(
        int(report["outlier_policy"].get("excluded") or 0)
        for key, report in reports.items()
        if key.startswith("snowflake_q")
    )
    snowflake_label = manifest["providers"].get("snowflake", {}).get("label")
    if snowflake_exclusions and snowflake_label in legend_labels:
        legend_labels[legend_labels.index(snowflake_label)] = (
            f"{snowflake_label} · outliers excluded"
        )
    fig.legend(
        legend_handles, legend_labels, loc="upper center", ncol=min(5, len(legend_labels)),
        facecolor=plot_background, edgecolor=GRID, labelcolor="white", fontsize=legend_fontsize,
        bbox_to_anchor=(.5, .777 if args.wide else 1.005), framealpha=.9,
    )
    if args.wide:
        fig.subplots_adjust(
            left=.090, right=.965, bottom=.095, top=.690,
            wspace=.105, hspace=.250,
        )
    else:
        fig.tight_layout(rect=(0, 0, 1, .93))
    png, svg = save_figure(fig, output, basename, args.dpi, wide=args.wide)
    plt.close(fig)

    csv_path = output / f"{basename}_data.csv"
    summary_path = output / f"{basename}_summary.json"
    write_csv(csv_path, csv_rows)
    write_json(summary_path, {
        "schema_version": 1,
        "chart": f"global_{args.workload}_query_latency",
        "layout": layout,
        "contract": {
            "row_cap": max_rows,
            "x_axis": "Each provider's own observed raw-row count; no iteration joins",
            "display_smoothing": f"centered rolling median, {smooth_window} observations, provider-local",
            "outliers": "Snowflake aggregate only: accepted Tukey upper-fence policy from its pairwise chart; no Redshift points are excluded",
            "required_labels": required_labels,
        },
        "manifest": {"path": str(manifest_path), "sha256": sha256(manifest_path)},
        "sources": {
            provider: {
                "runner_jsonl": str(source_paths[provider]),
                "runner_sha256": sha256(source_paths[provider]),
                "queries": str(query_paths[provider]),
                "queries_sha256": sha256(query_paths[provider]),
                "load": source_meta[provider],
            }
            for provider in source_paths
        },
        "reports": reports,
        "outputs": {"png": str(png), "svg": str(svg), "csv": str(csv_path)},
    })
    print(f"Written: {png}")
    print(f"Written: {svg}")
    print(f"Written: {csv_path}")
    print(f"Written: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
