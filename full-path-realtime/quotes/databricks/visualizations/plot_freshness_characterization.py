#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Plot metadata-derived Databricks MV freshness against committed raw rows."""

from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

BACKGROUND = "#2B2B2B"
GRID = "#4A4A4A"
MUTED = "#A0A0A0"
WHITE = "#FFFFFF"
RED = "#FF3621"
YELLOW = "#FDFF62"
BLUE = "#5DADE2"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ingest-metrics", type=Path, required=True)
    parser.add_argument("--ingest-summary", type=Path, required=True)
    parser.add_argument("--freshness", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--dpi", type=int, default=220)
    return parser.parse_args()


def timestamp(value: str) -> float:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def iso(value: float) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not rows or not all(isinstance(row, dict) for row in rows):
        raise ValueError(f"expected nonempty JSON objects: {path}")
    return rows


def update_id(event: Mapping[str, Any]) -> str | None:
    origin = event.get("origin")
    value = origin.get("update_id") if isinstance(origin, Mapping) else None
    return str(value) if value else None


def chosen_maintenance(event: Mapping[str, Any]) -> str | None:
    details = event.get("details")
    planning = (
        details.get("planning_information") if isinstance(details, Mapping) else None
    )
    techniques = (
        planning.get("technique_information")
        if isinstance(planning, Mapping)
        else None
    )
    if not isinstance(techniques, list):
        return None
    for technique in techniques:
        if isinstance(technique, Mapping) and technique.get("is_chosen") is True:
            value = technique.get("maintenance_type")
            return str(value) if value else None
    return None


def source_rows(event: Mapping[str, Any]) -> int | None:
    details = event.get("details")
    planning = (
        details.get("planning_information") if isinstance(details, Mapping) else None
    )
    sources = (
        planning.get("source_table_information")
        if isinstance(planning, Mapping)
        else None
    )
    if not isinstance(sources, list) or len(sources) != 1:
        return None
    source = sources[0]
    value = source.get("num_rows") if isinstance(source, Mapping) else None
    return int(value) if value is not None else None


def completed_updates(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    plans = {
        identifier: event
        for event in events
        if event.get("event_type") == "planning_information"
        and (identifier := update_id(event))
    }
    updates: list[dict[str, Any]] = []
    for event in events:
        if event.get("event_type") != "update_progress":
            continue
        details = event.get("details")
        progress = (
            details.get("update_progress") if isinstance(details, Mapping) else None
        )
        if not isinstance(progress, Mapping) or progress.get("state") != "COMPLETED":
            continue
        identifier = update_id(event)
        plan = plans.get(identifier or "")
        if plan is None:
            continue
        rows = source_rows(plan)
        maintenance = chosen_maintenance(plan)
        if rows is None or maintenance is None:
            continue
        updates.append(
            {
                "update_id": identifier,
                "completed_at": str(event["timestamp"]),
                "completed_ts": timestamp(str(event["timestamp"])),
                "planned_at": str(plan["timestamp"]),
                "planned_ts": timestamp(str(plan["timestamp"])),
                "source_rows": rows,
                "maintenance_type": maintenance,
                "incremental": maintenance == "MAINTENANCE_TYPE_GROUP_AGGREGATE",
                "no_op": maintenance == "MAINTENANCE_TYPE_NO_OP",
            }
        )
    return sorted(updates, key=lambda item: item["completed_ts"])


def interpolate_y(points: list[tuple[float, int]], when: float) -> int:
    times = [point[0] for point in points]
    if when <= times[0]:
        return points[0][1]
    if when >= times[-1]:
        return points[-1][1]
    right = bisect.bisect_right(times, when)
    t0, y0 = points[right - 1]
    t1, y1 = points[right]
    return round(y0 + (y1 - y0) * ((when - t0) / (t1 - t0)))


def interpolate_x(points: list[tuple[float, int]], rows: int) -> float:
    counts = [point[1] for point in points]
    if rows <= counts[0]:
        return points[0][0]
    if rows >= counts[-1]:
        return points[-1][0]
    right = bisect.bisect_left(counts, rows)
    t0, y0 = points[right - 1]
    t1, y1 = points[right]
    return t0 + (t1 - t0) * ((rows - y0) / (y1 - y0))


def main() -> int:
    args = parse_args()
    metrics_path = args.ingest_metrics.expanduser().resolve()
    summary_path = args.ingest_summary.expanduser().resolve()
    freshness_path = args.freshness.expanduser().resolve()
    output = args.output_dir.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)

    metrics = load_jsonl(metrics_path)
    summary = load_json(summary_path)
    freshness = load_jsonl(freshness_path)[-1]
    events = freshness["event_log"]["bounded_rows"]
    updates = completed_updates(events)
    if not updates or not any(update["incremental"] for update in updates):
        raise ValueError("freshness evidence has no completed incremental updates")

    start_ts = timestamp(str(summary["started_at"]))
    final_rows = int(summary["provider_committed_rows"])
    final_commit_ts = timestamp(
        str(freshness["zerobus_ingest"]["latest_commit_time"])
    )
    timeline = [(start_ts, 0)]
    timeline.extend(
        (timestamp(str(row["updated_at"])), int(row["provider_committed_rows"]))
        for row in metrics
    )
    timeline.append((final_commit_ts, final_rows))
    timeline.sort()

    # Collapse duplicate timestamps and enforce monotonic committed progress.
    collapsed: list[tuple[float, int]] = []
    for when, rows in timeline:
        rows = max(rows, collapsed[-1][1] if collapsed else 0)
        if collapsed and when == collapsed[-1][0]:
            collapsed[-1] = (when, max(rows, collapsed[-1][1]))
        else:
            collapsed.append((when, rows))
    timeline = collapsed

    sample_times = {point[0] for point in timeline}
    for update in updates:
        sample_times.add(update["completed_ts"] - 0.001)
        sample_times.add(update["completed_ts"])
    sample_times = sorted(sample_times)

    points: list[dict[str, Any]] = []
    for when in sample_times:
        raw_rows = interpolate_y(timeline, when)
        completed = [
            update for update in updates if update["completed_ts"] <= when
        ]
        latest = completed[-1] if completed else None
        watermark = int(latest["source_rows"]) if latest else 0
        row_lag = max(0, raw_rows - watermark)
        if row_lag == 0:
            lag_seconds = 0.0
        else:
            watermark_ts = (
                interpolate_x(timeline, watermark) if watermark else start_ts
            )
            lag_seconds = max(0.0, when - watermark_ts)
        points.append(
            {
                "observed_at": iso(when),
                "raw_committed_rows": raw_rows,
                "mv_source_rows": watermark,
                "rows_behind": row_lag,
                "metadata_lag_seconds": lag_seconds,
                "latest_completed_update": latest["update_id"] if latest else None,
                "latest_maintenance_type": (
                    latest["maintenance_type"] if latest else None
                ),
            }
        )

    active_points = [
        point
        for point in points
        if timestamp(point["observed_at"]) <= final_commit_ts
    ]
    final_incremental = next(
        (
            update
            for update in updates
            if update["incremental"] and update["completed_ts"] >= final_commit_ts
        ),
        None,
    )
    confirming_noop = next(
        (
            update
            for update in updates
            if update["no_op"]
            and final_incremental is not None
            and update["completed_ts"] > final_incremental["completed_ts"]
        ),
        None,
    )
    final_drain = (
        final_incremental["completed_ts"] - final_commit_ts
        if final_incremental
        else None
    )
    proof_bound = (
        confirming_noop["completed_ts"] - final_commit_ts
        if confirming_noop
        else None
    )

    x = [point["raw_committed_rows"] / 1_000_000 for point in points]
    row_lag = [point["rows_behind"] / 1_000_000 for point in points]
    lag_seconds = [point["metadata_lag_seconds"] for point in points]

    plt.rcParams["font.family"] = "DejaVu Sans"
    figure, axes = plt.subplots(
        2,
        1,
        figsize=(12.8, 8.4),
        sharex=True,
        gridspec_kw={"height_ratios": [1, 1]},
    )
    figure.patch.set_facecolor(BACKGROUND)
    for axis in axes:
        axis.set_facecolor(BACKGROUND)
        axis.grid(True, color=GRID, linewidth=0.7, alpha=0.65)
        axis.tick_params(colors=WHITE)
        for spine in axis.spines.values():
            spine.set_color(GRID)

    axes[0].plot(x, row_lag, color=RED, linewidth=2.2)
    axes[0].fill_between(x, row_lag, color=RED, alpha=0.18)
    axes[0].set_ylabel("Rows behind (millions)", color=WHITE)

    axes[1].plot(x, lag_seconds, color=BLUE, linewidth=2.2)
    axes[1].fill_between(x, lag_seconds, color=BLUE, alpha=0.14)
    axes[1].axhline(60, color=YELLOW, linestyle="--", linewidth=1.2)
    axes[1].text(
        5,
        64,
        "1-minute trigger spacing (not an SLA)",
        color=YELLOW,
        fontsize=9,
    )
    axes[1].set_ylabel("Metadata-derived lag (minutes)", color=WHITE)
    axes[1].set_xlabel("Provider-committed raw rows (millions)", color=WHITE)

    incremental_number = 0
    for update in updates:
        completed_rows = interpolate_y(timeline, update["completed_ts"]) / 1_000_000
        if update["incremental"]:
            incremental_number += 1
            label = (
                f"Incremental {incremental_number}\n"
                f"MV snapshot {update['source_rows'] / 1_000_000:.1f}M"
            )
            color = YELLOW
            offset = (
                (-8, 48, "right")
                if completed_rows > final_rows / 1_000_000 * 0.95
                else (0, 12 + 18 * (incremental_number % 2), "center")
            )
        else:
            label = "No-op confirmation"
            color = MUTED
            offset = (8, 14, "left")
        for axis in axes:
            axis.axvline(
                completed_rows,
                color=color,
                linestyle=":",
                linewidth=1.0,
                alpha=0.85,
            )
        axes[0].annotate(
            label,
            xy=(completed_rows, 0),
            xytext=(offset[0], offset[1]),
            textcoords="offset points",
            color=color,
            fontsize=8,
            ha=offset[2],
            va="bottom",
        )

    axes[0].yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:.0f}M"))
    axes[1].yaxis.set_major_formatter(
        FuncFormatter(lambda value, _: f"{value / 60:.1f}m")
    )
    axes[1].xaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:.0f}M"))

    title = "Databricks MV freshness during sustained 1M-EPS ingestion"
    subtitle = (
        "Metadata-only reconstruction · MV watermark from "
        "planning_information.source_table_information.num_rows"
    )
    figure.suptitle(title, color=WHITE, fontsize=17, fontweight="bold", y=0.985)
    figure.text(0.5, 0.945, subtitle, color=MUTED, fontsize=10, ha="center")
    note = (
        f"600M rows · {float(summary['eps']['average_provider_committed_rows_per_sec']):,.0f} "
        f"average EPS · peak observed lag "
        f"{max(point['metadata_lag_seconds'] for point in points) / 60:.2f}m · "
        f"final-drain bound {final_drain:.1f}s"
        if final_drain is not None
        else "Final-drain bound unavailable"
    )
    figure.text(0.5, 0.915, note, color=WHITE, fontsize=10, ha="center")
    figure.text(
        0.01,
        0.012,
        "Lag is reconstructed from provider metadata; it is not event-time lag. "
        "A later no-op proves complete catch-up.",
        color=MUTED,
        fontsize=8.5,
    )
    figure.tight_layout(rect=(0.02, 0.04, 0.99, 0.90))

    png = output / "databricks_mv_freshness_vs_rows.png"
    svg = output / "databricks_mv_freshness_vs_rows.svg"
    figure.savefig(png, dpi=args.dpi, facecolor=BACKGROUND, bbox_inches="tight")
    figure.savefig(svg, facecolor=BACKGROUND, bbox_inches="tight")
    plt.close(figure)

    csv_path = output / "databricks_mv_freshness_vs_rows.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(points[0]))
        writer.writeheader()
        writer.writerows(points)

    summary_output = {
        "schema_version": 1,
        "status": "preliminary_characterization",
        "chart": "databricks_mv_freshness_vs_committed_rows",
        "method": {
            "raw_progress": "producer provider-acknowledged row timeline",
            "mv_watermark": (
                "event_log planning_information.source_table_information.num_rows "
                "paired with completed update_id"
            ),
            "lag_seconds": (
                "elapsed time since the committed-row timeline reached the latest "
                "completed MV source-row watermark; zero when row lag is zero"
            ),
            "limitation": (
                "provider event log exposes source row count but no source Delta "
                "version; this is a metadata-derived bound, not per-row event-time lag"
            ),
        },
        "run_id": summary["run_id"],
        "provider_committed_rows": final_rows,
        "average_provider_committed_eps": float(
            summary["eps"]["average_provider_committed_rows_per_sec"]
        ),
        "completed_updates": updates,
        "incremental_update_count": sum(
            1 for update in updates if update["incremental"]
        ),
        "no_op_update_count": sum(1 for update in updates if update["no_op"]),
        "maximum_active_rows_behind": max(
            point["rows_behind"] for point in active_points
        ),
        "maximum_active_metadata_lag_seconds": max(
            point["metadata_lag_seconds"] for point in active_points
        ),
        "maximum_observed_metadata_lag_seconds": max(
            point["metadata_lag_seconds"] for point in points
        ),
        "final_raw_commit_at": iso(final_commit_ts),
        "final_drain_upper_bound_seconds": final_drain,
        "subsequent_no_op_proof_bound_seconds": proof_bound,
        "sources": {
            "ingest_metrics": {
                "path": str(metrics_path),
                "sha256": sha256(metrics_path),
            },
            "ingest_summary": {
                "path": str(summary_path),
                "sha256": sha256(summary_path),
            },
            "freshness": {
                "path": str(freshness_path),
                "sha256": sha256(freshness_path),
            },
        },
        "artifacts": {
            "png": str(png),
            "svg": str(svg),
            "csv": str(csv_path),
        },
    }
    summary_json = output / "databricks_mv_freshness_vs_rows_summary.json"
    summary_json.write_text(
        json.dumps(summary_output, indent=2) + "\n", encoding="utf-8"
    )
    print(png)
    print(svg)
    print(csv_path)
    print(summary_json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
