#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Render persisted materialized-view refresh lag across all systems."""

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
    GRID, configure_figure, human_rows, iter_jsonl, load_manifest, pchip_curve,
    resolve_layout, resolve_source, save_figure, sha256, time_rolling_mean,
    timestamp, unique_mean_series, validate_required_labels, write_csv, write_json,
)


@dataclass(frozen=True)
class LagPoint:
    provider: str
    source_line: int
    observed_at: str
    base_table_rows: int
    lag_seconds: float
    refresh_watermark: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path(__file__).with_name("manifest.json"))
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--trend-window-minutes", type=float, default=61)
    parser.add_argument("--display-window-minutes", type=float, default=11)
    parser.add_argument("--curve-points", type=int, default=2200)
    parser.add_argument("--basename", default="mv_lag_all_systems")
    parser.add_argument(
        "--wide",
        action="store_true",
        help="Render the Keynote-native 5156x2900 staged variant and append _wide to the basename.",
    )
    parser.add_argument("--dpi", type=int, default=300)
    return parser.parse_args()


def parse_lag(value: str) -> float:
    matches = re.findall(r"(\d+(?:\.\d+)?)([dhms])", value)
    if not matches:
        raise ValueError(f"cannot parse behind_by={value!r}")
    factors = {"d": 86400, "h": 3600, "m": 60, "s": 1}
    return sum(float(number) * factors[unit] for number, unit in matches)


def snowflake_timeline(path: Path) -> tuple[list[tuple[float, int]], dict[str, Any]]:
    values: list[tuple[float, int, int]] = []
    for line, record in iter_jsonl(path):
        rows = int(record.get("raw_rows") or 0)
        if rows > 0:
            values.append((timestamp(str(record["iteration_started_at"])), rows, line))
    values.sort()
    final_rows = max(item[1] for item in values)
    endpoint = next(item for item in values if item[1] >= final_rows)
    active = [(when, rows) for when, rows, _ in values if when <= endpoint[0]]
    return active, {
        "final_rows": final_rows,
        "active_endpoint": datetime.fromtimestamp(endpoint[0]).astimezone().isoformat(),
        "active_endpoint_source_line": endpoint[2],
    }


def interpolate_rows(timeline: list[tuple[float, int]], when: float) -> int:
    times = [item[0] for item in timeline]
    if when <= times[0]:
        return timeline[0][1]
    if when >= times[-1]:
        return timeline[-1][1]
    right = bisect.bisect_right(times, when)
    t0, r0 = timeline[right - 1]
    t1, r1 = timeline[right]
    return round(r0 + (r1 - r0) * ((when - t0) / (t1 - t0)))


def load_snowflake(freshness: Path, dashboard: Path) -> tuple[list[LagPoint], dict[str, Any]]:
    timeline, timeline_meta = snowflake_timeline(dashboard)
    points: list[LagPoint] = []
    read = invalid = after = 0
    endpoint = timeline[-1][0]
    for line, record in iter_jsonl(freshness):
        read += 1
        if int(record.get("rows") or 0) <= 0 or str(record.get("refreshed_on") or "").startswith("1969-"):
            invalid += 1
            continue
        observed = str(record["polled_at"])
        when = timestamp(observed)
        if when > endpoint:
            after += 1
            continue
        points.append(LagPoint(
            "snowflake", line, observed, interpolate_rows(timeline, when),
            parse_lag(str(record["behind_by"])), str(record["refreshed_on"]),
        ))
    return points, {
        **timeline_meta,
        "records_read": read,
        "initial_invalid_excluded": invalid,
        "post_ingestion_excluded": after,
        "records_selected": len(points),
    }


def load_bigquery(freshness: Path, ingest_summary: Path) -> tuple[list[LagPoint], dict[str, Any]]:
    summary = __import__("json").loads(ingest_summary.read_text(encoding="utf-8"))
    final_rows = int(summary["acknowledged_rows"])
    points: list[LagPoint] = []
    read = after = invalid = 0
    reached_endpoint = False
    endpoint_line: int | None = None
    for line, record in iter_jsonl(freshness):
        read += 1
        if reached_endpoint:
            after += 1
            continue
        rows = int(record.get("ingest_acknowledged_rows") or 0)
        lag = record.get("watermark_lag_sec")
        if rows <= 0 or lag is None:
            invalid += 1
            continue
        points.append(LagPoint(
            "bigquery", line, str(record["observed_at"]), rows, float(lag),
            str(record.get("refresh_watermark") or record.get("last_refresh_time") or ""),
        ))
        if rows >= final_rows:
            reached_endpoint = True
            endpoint_line = line
    if not reached_endpoint:
        raise ValueError("BigQuery freshness evidence never reaches the ingest endpoint")
    return points, {
        "final_rows": final_rows,
        "active_endpoint_source_line": endpoint_line,
        "records_read": read,
        "initial_invalid_excluded": invalid,
        "post_ingestion_excluded": after,
        "records_selected": len(points),
    }


def load_redshift(
    lag_path: Path,
    refresh_path: Path,
    final_rows: int,
) -> tuple[list[LagPoint], dict[str, Any]]:
    lag_records = [(line, record) for line, record in iter_jsonl(lag_path)]
    stale_initial = False
    if (
        len(lag_records) >= 2
        and int(lag_records[0][1].get("raw_rows") or 0)
        > int(lag_records[1][1].get("raw_rows") or 0)
    ):
        lag_records = lag_records[1:]
        stale_initial = True
    endpoint_index = next(
        (
            index
            for index, (_, record) in enumerate(lag_records)
            if int(record.get("raw_rows") or 0) >= final_rows
        ),
        None,
    )
    if endpoint_index is None:
        raise ValueError("Redshift lag evidence never reaches the ingest endpoint")
    active_lag = lag_records[: endpoint_index + 1]
    timeline = [
        (timestamp(str(record["ts"])), int(record.get("raw_rows") or 0))
        for _, record in active_lag
    ]
    endpoint_time = timeline[-1][0]
    points: list[LagPoint] = []
    read = invalid = after = 0
    for line, record in iter_jsonl(refresh_path):
        if record.get("target_mv") != "quotes_daily" or record.get("status") != "ok":
            continue
        read += 1
        # The accepted T2 journal predates the monitor fix that stopped
        # double-counting streaming lag in end_to_end_freshness_s.
        # child_freshness_s already spans the Kafka watermark through the
        # persisted quotes_daily MV and is therefore the correct pre-aggregate
        # freshness value.
        lag = record.get("child_freshness_s")
        if lag is None:
            invalid += 1
            continue
        observed = str(record["finished_at"])
        when = timestamp(observed)
        if when > endpoint_time:
            after += 1
            continue
        points.append(
            LagPoint(
                "redshift",
                line,
                observed,
                interpolate_rows(timeline, when),
                float(lag),
                observed,
            )
        )
    if not points:
        raise ValueError("no active Redshift dashboard freshness observations")
    return points, {
        "final_rows": final_rows,
        "active_endpoint_source_line": active_lag[-1][0],
        "active_endpoint": active_lag[-1][1]["ts"],
        "records_read": read,
        "initial_invalid_excluded": invalid,
        "initial_nonmonotonic_lag_sample_excluded": stale_initial,
        "post_ingestion_excluded": after,
        "records_selected": len(points),
    }


def render_series(points: list[LagPoint], trend_minutes: float, display_minutes: float, curve_points: int) -> dict[str, Any]:
    ordered = sorted(points, key=lambda p: timestamp(p.observed_at))
    times = [timestamp(point.observed_at) for point in ordered]
    raw_minutes = [point.lag_seconds / 60 for point in ordered]
    trend = time_rolling_mean(times, raw_minutes, trend_minutes * 60)
    display = time_rolling_mean(times, trend, display_minutes * 60)
    source_xs = [point.base_table_rows for point in ordered]
    unique_xs, unique_ys = unique_mean_series(source_xs, display)
    curve_xs, curve_ys = pchip_curve(unique_xs, unique_ys, curve_points)
    return {
        "points": ordered,
        "raw_minutes": raw_minutes,
        "trend_minutes": trend,
        "display_minutes": display,
        "curve_xs": curve_xs,
        "curve_ys": curve_ys,
        "average_trend_minutes": statistics.fmean(trend),
        "maximum_trend_minutes": max(trend),
        "raw_maximum_minutes": max(raw_minutes),
    }


def main() -> int:
    args = parse_args()
    manifest, manifest_path = load_manifest(args.manifest)
    providers = manifest["providers"]
    required_labels = validate_required_labels(
        manifest,
        "mv_lag",
        (
            providers["clickhouse"]["label"],
            providers["snowflake"]["label"],
            providers["bigquery"]["label"],
            providers["redshift"]["label"],
        ),
    )
    sf_freshness = resolve_source(providers["snowflake"]["freshness"])
    sf_dashboard = resolve_source(providers["snowflake"]["freshness_dashboard"])
    bq_freshness = resolve_source(providers["bigquery"]["freshness"])
    bq_summary = resolve_source(providers["bigquery"]["ingest_summary"])
    rs_lag = resolve_source(providers["redshift"]["freshness_lag"])
    rs_refresh = resolve_source(providers["redshift"]["freshness_refresh"])
    sf_points, sf_meta = load_snowflake(sf_freshness, sf_dashboard)
    bq_points, bq_meta = load_bigquery(bq_freshness, bq_summary)
    rs_points, rs_meta = load_redshift(
        rs_lag,
        rs_refresh,
        int(providers["redshift"]["freshness_final_rows"]),
    )
    series = {
        "snowflake": render_series(sf_points, args.trend_window_minutes, args.display_window_minutes, args.curve_points),
        "bigquery": render_series(bq_points, args.trend_window_minutes, args.display_window_minutes, args.curve_points),
        "redshift": render_series(rs_points, args.trend_window_minutes, args.display_window_minutes, args.curve_points),
    }
    final_rows = max(sf_meta["final_rows"], bq_meta["final_rows"], rs_meta["final_rows"])

    basename, figure_size, layout = resolve_layout(
        args.basename,
        args.wide,
        (12, 5.4),
        dpi=args.dpi,
    )
    fig, axis = plt.subplots(
        figsize=figure_size, dpi=args.dpi if args.wide else None
    )
    plot_background = configure_figure(fig, wide=args.wide)
    axis.set_facecolor(plot_background)
    line_width = 3.2 if args.wide else 2.3
    label_fontsize = 15 if args.wide else 11
    tick_fontsize = 13 if args.wide else 10
    legend_fontsize = 13.5 if args.wide else 9.5
    ch, = axis.plot([0, final_rows], [0, 0], color=providers["clickhouse"]["color"], linewidth=line_width, zorder=4)
    handles = [ch]
    labels = [f"{providers['clickhouse']['label']} · incremental MV (always 0s)"]
    for provider in ("snowflake", "bigquery", "redshift"):
        item = series[provider]
        line, = axis.plot(item["curve_xs"], item["curve_ys"], color=providers[provider]["color"], linewidth=line_width, zorder=3)
        handles.append(line)
        labels.append(
            f"{providers[provider]['label']} · persisted MV refresh "
            f"(avg {item['average_trend_minutes']:.1f}m · max {item['maximum_trend_minutes']:.1f}m)"
        )
    maximum = max(max(item["curve_ys"]) for item in series.values())
    axis.set_xlim(0, final_rows)
    axis.set_ylim(0, max(2, math.ceil(maximum * 1.14)))
    axis.xaxis.set_major_formatter(FuncFormatter(human_rows))
    axis.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:g}m"))
    axis.set_xlabel("Base-table row count", color="white", fontsize=label_fontsize)
    axis.set_ylabel("Persisted MV refresh lag (minutes)\n↓ lower is fresher", color="white", fontsize=label_fontsize)
    axis.tick_params(colors="white", labelsize=tick_fontsize)
    axis.grid(True, color=GRID, linewidth=.6, alpha=.7)
    axis.spines[["top", "right"]].set_visible(False)
    axis.spines[["left", "bottom"]].set_color("white")
    axis.legend(handles, labels, loc="upper left", facecolor=plot_background, edgecolor=GRID, labelcolor="white", fontsize=legend_fontsize, framealpha=.9)
    if args.wide:
        fig.subplots_adjust(left=.080, right=.965, bottom=.120, top=.745)
    else:
        fig.tight_layout()
    output = args.output_dir.expanduser().resolve()
    png, svg = save_figure(fig, output, basename, args.dpi, wide=args.wide)
    plt.close(fig)

    data_rows: list[dict[str, Any]] = []
    curve_rows: list[dict[str, Any]] = []
    for provider, item in series.items():
        for point, raw, trend, display in zip(
            item["points"], item["raw_minutes"], item["trend_minutes"], item["display_minutes"], strict=True,
        ):
            row = asdict(point)
            row.update(raw_lag_minutes=raw, trend_lag_minutes=trend, display_lag_minutes=display)
            data_rows.append(row)
        curve_rows.extend(
            {"provider": provider, "base_table_rows": x, "display_lag_minutes": y}
            for x, y in zip(item["curve_xs"], item["curve_ys"], strict=True)
        )
    data_path = output / f"{basename}_data.csv"
    curve_path = output / f"{basename}_curve.csv"
    summary_path = output / f"{basename}_summary.json"
    write_csv(data_path, data_rows)
    write_csv(curve_path, curve_rows)
    write_json(summary_path, {
        "schema_version": 1,
        "chart": "global_persisted_mv_refresh_lag",
        "layout": layout,
        "contract": {
            "metric": "Persisted MV refresh lag, not query-answer freshness",
            "clickhouse": "0 by ingest-time incremental MV design",
            "bigquery_query_semantics": "Default direct MV queries can reconcile unrefreshed base-table changes at query time; that behavior is not represented by this persisted-refresh-lag line",
            "redshift": "Freshness of the persisted quotes_daily pre-aggregate MV; child_freshness_s already spans the Kafka event watermark through the MV",
            "trend": f"centered time rolling mean, {args.trend_window_minutes:g} minutes",
            "display_smoothing": f"centered time rolling mean, {args.display_window_minutes:g} minutes",
            "interpolation": "PCHIP; display only; legend avg/max use the pre-display trend",
            "required_labels": required_labels,
        },
        "manifest": {"path": str(manifest_path), "sha256": sha256(manifest_path)},
        "sources": {
            "snowflake_freshness": {"path": str(sf_freshness), "sha256": sha256(sf_freshness)},
            "snowflake_dashboard": {"path": str(sf_dashboard), "sha256": sha256(sf_dashboard)},
            "bigquery_freshness": {"path": str(bq_freshness), "sha256": sha256(bq_freshness)},
            "bigquery_ingest_summary": {"path": str(bq_summary), "sha256": sha256(bq_summary)},
            "redshift_lag": {"path": str(rs_lag), "sha256": sha256(rs_lag)},
            "redshift_refresh": {"path": str(rs_refresh), "sha256": sha256(rs_refresh)},
        },
        "load": {"snowflake": sf_meta, "bigquery": bq_meta, "redshift": rs_meta},
        "statistics": {
            provider: {
                "average_trend_minutes": item["average_trend_minutes"],
                "maximum_trend_minutes": item["maximum_trend_minutes"],
                "raw_maximum_minutes": item["raw_maximum_minutes"],
            }
            for provider, item in series.items()
        },
        "outputs": {"png": str(png), "svg": str(svg), "data_csv": str(data_path), "curve_csv": str(curve_path)},
    })
    for path in (png, svg, data_path, curve_path, summary_path):
        print(f"Written: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
