#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Render Redshift persisted pre-aggregate MV freshness during active ingestion."""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import math
import statistics
from datetime import datetime
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from _layout import GRID, REDSHIFT, WHITE, configure_figure, resolve_layout, save_figure, write_json


FINAL_ROWS = 113_219_565_734


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def epoch(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def human_rows(value: float, _position: float | None = None) -> str:
    if value <= 0:
        return "0"
    return f"{value / 1e9:g}B" if value >= 1e9 else f"{value / 1e6:g}M"


def human_seconds(value: float, _position: float | None = None) -> str:
    return f"{value / 60:g}m" if value >= 120 else f"{value:g}s"


def interpolate(timeline: list[tuple[float, int]], when: float) -> int:
    times = [item[0] for item in timeline]
    if when <= times[0]:
        return timeline[0][1]
    if when >= times[-1]:
        return timeline[-1][1]
    right = bisect.bisect_right(times, when)
    t0, r0 = timeline[right - 1]
    t1, r1 = timeline[right]
    return round(r0 + (r1 - r0) * ((when - t0) / (t1 - t0)))


def rolling_median(values: list[float], window: int = 5) -> list[float]:
    half = window // 2
    return [
        statistics.median(values[max(0, index - half): min(len(values), index + half + 1)])
        for index in range(len(values))
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lag", type=Path, required=True)
    parser.add_argument("--refresh", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--basename", default="redshift_freshness")
    parser.add_argument("--wide", action="store_true")
    parser.add_argument("--dpi", type=int, default=300)
    args = parser.parse_args()
    lag_path = args.lag.expanduser().resolve()
    refresh_path = args.refresh.expanduser().resolve()

    lag_records: list[dict[str, Any]] = [json.loads(line) for line in lag_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    stale_initial = False
    if len(lag_records) >= 2 and int(lag_records[0].get("raw_rows") or 0) > int(lag_records[1].get("raw_rows") or 0):
        lag_records = lag_records[1:]
        stale_initial = True
    endpoint_index = next(
        (index for index, record in enumerate(lag_records) if int(record.get("raw_rows") or 0) >= FINAL_ROWS),
        None,
    )
    if endpoint_index is None:
        raise ValueError("lag evidence never reaches the completed dataset")
    active_lag = lag_records[: endpoint_index + 1]
    timeline = [(epoch(str(record["ts"])), int(record["raw_rows"])) for record in active_lag]
    endpoint_time = timeline[-1][0]

    daily: list[tuple[int, float, str]] = []
    post_ingestion_excluded = 0
    for raw in refresh_path.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        record = json.loads(raw)
        if record.get("target_mv") != "quotes_daily" or record.get("status") != "ok":
            continue
        # This run predates the monitor fix that stopped adding streaming lag a
        # second time to end_to_end_freshness_s. child_freshness_s is already
        # measured from the Kafka event watermark to the persisted aggregate
        # MV, so it is the correct pre-aggregation freshness value.
        freshness = record.get("child_freshness_s")
        if freshness is None:
            continue
        observed = str(record["finished_at"])
        when = epoch(observed)
        if when > endpoint_time:
            post_ingestion_excluded += 1
            continue
        daily.append((interpolate(timeline, when), float(freshness), observed))
    if not daily:
        raise ValueError("missing active pre-aggregate MV freshness observations")

    basename, size, layout = resolve_layout(args.basename, args.wide, (12, 4.8), args.dpi)
    figure, axis = plt.subplots(figsize=size, dpi=args.dpi if args.wide else None)
    background = configure_figure(figure, wide=args.wide)
    axis.set_facecolor(background)
    ys = [item[1] for item in daily]
    display = rolling_median(ys)
    axis.plot([item[0] for item in daily], display, color=REDSHIFT, linewidth=3 if args.wide else 2.3)
    axis.set_xlim(0, FINAL_ROWS)
    axis.set_ylim(0, max(10, math.ceil(max(display) * 1.15 / 10) * 10))
    axis.set_title("Pre-aggregate MV freshness", color=WHITE, fontsize=17 if args.wide else 12, pad=10)
    axis.xaxis.set_major_formatter(FuncFormatter(human_rows))
    axis.yaxis.set_major_formatter(FuncFormatter(human_seconds))
    axis.tick_params(colors=WHITE, labelsize=13 if args.wide else 9)
    axis.grid(True, color=GRID, linewidth=.65, alpha=.7)
    axis.spines[["top", "right"]].set_visible(False)
    axis.spines[["left", "bottom"]].set_color(WHITE)
    axis.set_xlabel("Base-table row count", color=WHITE, fontsize=15 if args.wide else 10)
    axis.set_ylabel("Persisted MV refresh lag\n↓ lower is fresher", color=WHITE, fontsize=15 if args.wide else 10)
    ordered = sorted(ys)
    stats = {
        "Pre-aggregate MV": {
            "observations": len(ys),
            "median_seconds": statistics.median(ys),
            "p95_seconds": ordered[max(0, math.ceil(.95 * len(ordered)) - 1)],
            "maximum_seconds": max(ys),
        }
    }
    if args.wide:
        figure.subplots_adjust(left=.085, right=.965, bottom=.12, top=.73)
    else:
        figure.tight_layout()
    output = args.output_dir.expanduser().resolve()
    png, svg = save_figure(figure, output, basename, args.dpi, wide=args.wide)
    plt.close(figure)
    summary_path = output / f"{basename}_summary.json"
    write_json(summary_path, {
        "schema_version": 1,
        "chart": "redshift_preaggregate_mv_freshness",
        "layout": layout,
        "active_endpoint": active_lag[-1]["ts"],
        "final_rows": FINAL_ROWS,
        "contract": {
            "object": "quotes_daily persisted pre-aggregate MV",
            "source": "quotes_streamed streaming MV",
            "metric": "Kafka event watermark to persisted pre-aggregate MV",
            "plotted_field": "child_freshness_s",
            "lag_file_use": "row-count timeline and active-ingestion endpoint only; streaming lag is not plotted",
        },
        "selection": {
            "first_nonmonotonic_lag_sample_excluded": stale_initial,
            "post_ingestion_daily_refreshes_excluded": post_ingestion_excluded,
            "display_smoothing": "centered 5-observation rolling median",
        },
        "statistics": stats,
        "sources": {
            "lag": {"path": str(lag_path), "sha256": sha256(lag_path)},
            "refresh": {"path": str(refresh_path), "sha256": sha256(refresh_path)},
        },
        "outputs": {"png": str(png), "svg": str(svg)},
    })
    for path in (png, svg, summary_path):
        print(f"Written: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
