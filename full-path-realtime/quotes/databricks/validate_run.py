#!/usr/bin/env python3
"""Validate a Databricks full-path benchmark run from offline evidence only."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

EXPECTED_ROWS = 113_219_565_734
ALIGNED_FIELDS = (
    "result",
    "compilation_time",
    "execution_time",
    "queue_time",
    "result_fetch_time",
    "client_wall_time",
    "statement_ids",
    "cache_hit",
    "read_io_cache_percent",
    "result_row_count",
    "result_hash",
)


def parse_utc_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("timestamp is empty")
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("timestamp is not finite")
        if abs(number) >= 100_000_000_000:
            number /= 1000
        parsed = datetime.fromtimestamp(number, timezone.utc)
    else:
        raise ValueError("timestamp is not an ISO-8601 value")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp has no timezone")
    if parsed.utcoffset() != timedelta(0):
        raise ValueError("timestamp is not UTC")
    return parsed.astimezone(timezone.utc)


def iso_utc(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _integer(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if str(parsed) == str(value).strip() or isinstance(value, int) else None


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _nested(value: Any, *path: str) -> Any:
    current = value
    for part in path:
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def _read_json(path: Path, label: str) -> tuple[dict[str, Any], list[str]]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, Mapping):
            raise ValueError("top-level value is not an object")
        return dict(value), []
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return {}, [f"{label}: {type(exc).__name__}: {exc}"]


def _read_jsonl(path: Path, label: str) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        return [], [f"{label}: {type(exc).__name__}: {exc}"]
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
            if not isinstance(value, Mapping):
                raise ValueError("top-level value is not an object")
            rows.append(dict(value))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"{label}:{line_number}: {type(exc).__name__}: {exc}")
    return rows, errors


def atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


class Gates:
    def __init__(self) -> None:
        self.values: list[dict[str, Any]] = []

    def add(
        self,
        gate_id: str,
        passed: bool,
        evidence: Mapping[str, Any],
        reasons: Sequence[str],
    ) -> None:
        self.values.append(
            {
                "id": gate_id,
                "passed": bool(passed),
                "evidence": dict(evidence),
                "reasons": list(reasons),
            }
        )


def _gate_source_manifest(
    gates: Gates,
    manifest: Mapping[str, Any],
    errors: Sequence[str],
    expected_rows: int,
) -> None:
    reasons = list(errors)
    files = _list(manifest.get("files"))
    selected_paths = [
        str(item.get("path"))
        for item in files
        if isinstance(item, Mapping) and item.get("path") is not None
    ]
    excluded = [str(value) for value in _list(manifest.get("excluded_files"))]
    tasks = [
        item for item in _list(manifest.get("tasks")) if isinstance(item, Mapping)
    ]
    task_rows = [
        _integer(
            item.get("task_rows")
            if item.get("task_rows") is not None
            else item.get("row_count")
            if item.get("row_count") is not None
            else item.get("rows")
        )
        for item in tasks
    ]
    file_rows = [
        _integer(item.get("selected_rows"))
        for item in files
        if isinstance(item, Mapping)
    ]
    task_ordinals = [
        _integer(
            item.get("canonical_ordinal")
            if item.get("canonical_ordinal") is not None
            else item.get("task_ordinal")
            if item.get("task_ordinal") is not None
            else item.get("ordinal")
        )
        for item in tasks
    ]
    file_ordinals = [
        _integer(item.get("file_ordinal"))
        for item in files
        if isinstance(item, Mapping)
    ]
    checks = {
        "total_rows_exact": _integer(manifest.get("total_rows")) == expected_rows,
        "quotes_0_flag_false": manifest.get("quotes_0_parquet_included") is False,
        "quotes_0_not_selected": all(
            Path(value).name != "quotes_0.parquet" for value in selected_paths
        ),
        "manifest_sha256_present": isinstance(manifest.get("manifest_sha256"), str)
        and len(str(manifest.get("manifest_sha256"))) == 64,
        "tasks_present": bool(tasks),
        "file_count_consistent": _integer(
            manifest.get("selected_file_count")
        ) == len(files),
        "task_count_consistent": _integer(
            manifest.get("selected_row_group_count")
        ) == len(tasks),
        "canonical_task_order": task_ordinals == list(range(len(tasks))),
        "canonical_file_order": (
            all(value is not None and value >= 0 for value in file_ordinals)
            and file_ordinals == sorted(file_ordinals)
            and len(set(file_ordinals)) == len(file_ordinals)
        ),
        "task_rows_sum_exact": (
            bool(task_rows)
            and all(value is not None and value >= 0 for value in task_rows)
            and sum(value for value in task_rows if value is not None) == expected_rows
        ),
        "file_rows_sum_exact": (
            bool(file_rows)
            and all(value is not None and value >= 0 for value in file_rows)
            and sum(value for value in file_rows if value is not None) == expected_rows
        ),
    }
    reasons.extend(name for name, passed in checks.items() if not passed)
    gates.add(
        "source_manifest",
        not reasons,
        {
            "expected_rows": expected_rows,
            "actual_rows": manifest.get("total_rows"),
            "excluded_files": excluded,
            "checks": checks,
        },
        reasons,
    )


def _gate_producer(
    gates: Gates,
    progress: Mapping[str, Any],
    manifest: Mapping[str, Any],
    errors: Sequence[str],
    expected_rows: int,
) -> None:
    reasons = list(errors)
    source = _mapping(progress.get("source"))
    stream_status = _mapping(progress.get("stream_status"))
    workers = _integer(_nested(progress, "config", "workers"))
    task_count = _integer(manifest.get("selected_row_group_count"))
    completed = _list(progress.get("completed_task_ordinals"))
    completed_ints = [_integer(value) for value in completed]
    completed_ranges = _list(progress.get("completed_task_ranges"))
    compact_complete = (
        task_count is not None
        and task_count > 0
        and completed_ranges == [[0, task_count - 1]]
    )
    stream_checks = [
        isinstance(status, Mapping)
        and status.get("close_succeeded") is True
        and status.get("unacked_inspection_succeeded") is True
        and _integer(status.get("unacked_batches")) == 0
        for status in stream_status.values()
    ]
    checks = {
        "provider_committed_exact": _integer(
            progress.get("provider_committed_rows")
        )
        == expected_rows,
        "logical_raw_exact": _integer(progress.get("logical_raw_rows"))
        == expected_rows,
        "submitted_exact": _integer(progress.get("submitted_rows")) == expected_rows,
        "finished": progress.get("finished") is True,
        "not_running": progress.get("running") is False,
        "clean_checkpoint": progress.get("clean_checkpoint") is True,
        "not_ambiguous": progress.get("ambiguous") is False,
        "no_terminal_error": progress.get("terminal_error") in (None, ""),
        "no_errors": progress.get("errors") == [],
        "no_pending_batches": _integer(progress.get("pending_batches")) == 0,
        "no_pending_rows": _integer(progress.get("pending_rows")) == 0,
        "no_partial_tasks": progress.get("partial_task_rows") == {},
        "fresh_table_baseline": _integer(progress.get("baseline_table_rows")) == 0
        and progress.get("baseline_table_rows_unknown") is False,
        "manifest_hash_matches": source.get("manifest_sha256")
        == manifest.get("manifest_sha256"),
        "source_total_matches": _integer(source.get("total_rows")) == expected_rows,
        "all_tasks_completed": (
            task_count is not None
            and _integer(progress.get("completed_tasks")) == task_count
            and (
                completed_ints == list(range(task_count))
                or compact_complete
            )
        ),
        "all_streams_clean": (
            workers is not None
            and workers > 0
            and len(stream_status) == workers
            and all(stream_checks)
        ),
    }
    reasons.extend(name for name, passed in checks.items() if not passed)
    gates.add(
        "producer_completion",
        not reasons,
        {
            "provider_committed_rows": progress.get("provider_committed_rows"),
            "completed_tasks": progress.get("completed_tasks"),
            "expected_tasks": task_count,
            "stream_count": len(stream_status),
            "checks": checks,
        },
        reasons,
    )


def _metric_interval_evidence(
    rows: Sequence[Mapping[str, Any]],
    eps_min: float,
    eps_max: float,
) -> tuple[list[dict[str, Any]], list[str]]:
    evidence: list[dict[str, Any]] = []
    reasons: list[str] = []
    previous_rows = 0
    previous_elapsed = 0.0
    previous_seen = False
    for index, row in enumerate(rows):
        explicit_warmup = (
            row.get("warmup") is True
            or row.get("warm_up") is True
            or row.get("is_warmup") is True
            or _nested(row, "eps", "warmup") is True
        )
        if index == 0 and explicit_warmup:
            evidence.append(
                {"index": index, "ignored": True, "reason": "explicit first warm-up"}
            )
            current_rows = _integer(row.get("provider_committed_rows"))
            current_elapsed = _number(row.get("elapsed_sec"))
            if current_rows is not None and current_elapsed is not None:
                previous_rows, previous_elapsed, previous_seen = (
                    current_rows,
                    current_elapsed,
                    True,
                )
            continue
        explicit = next(
            (
                _number(value)
                for value in (
                    _nested(row, "eps", "interval_provider_committed_rows_per_sec"),
                    _nested(row, "eps", "interval_rows_per_sec"),
                    row.get("interval_provider_committed_rows_per_sec"),
                    row.get("interval_eps"),
                )
                if _number(value) is not None
            ),
            None,
        )
        current_rows = _integer(row.get("provider_committed_rows"))
        current_elapsed = _number(row.get("elapsed_sec"))
        interval_eps = explicit
        source = "explicit"
        if interval_eps is None and current_rows is not None and current_elapsed is not None:
            base_rows = previous_rows if previous_seen else 0
            base_elapsed = previous_elapsed if previous_seen else 0.0
            delta_elapsed = current_elapsed - base_elapsed
            delta_rows = current_rows - base_rows
            if delta_elapsed > 0 and delta_rows >= 0:
                interval_eps = delta_rows / delta_elapsed
                source = "derived_from_committed_counter_and_elapsed_sec"
            else:
                source = "invalid_counter_delta"
        passed = (
            interval_eps is not None and eps_min <= interval_eps <= eps_max
        )
        evidence.append(
            {
                "index": index,
                "ignored": False,
                "eps": interval_eps,
                "source": source,
                "passed": passed,
            }
        )
        if current_rows is not None and current_elapsed is not None:
            if previous_seen and (
                current_rows < previous_rows or current_elapsed < previous_elapsed
            ):
                reasons.append(f"metrics row {index} counters are not monotonic")
            previous_rows, previous_elapsed, previous_seen = (
                current_rows,
                current_elapsed,
                True,
            )
    return evidence, reasons


def _gate_ingest_rate(
    gates: Gates,
    progress: Mapping[str, Any],
    metrics: Sequence[Mapping[str, Any]],
    errors: Sequence[str],
    *,
    eps_min: float,
    eps_max: float,
    min_fraction: float,
) -> None:
    reasons = list(errors)
    average = _number(
        _nested(progress, "eps", "average_provider_committed_rows_per_sec")
    )
    average_passed = average is not None and eps_min <= average <= eps_max
    if not average_passed:
        reasons.append("average provider-committed EPS is missing or outside bounds")
    intervals, interval_reasons = _metric_interval_evidence(
        metrics, eps_min, eps_max
    )
    reasons.extend(interval_reasons)
    considered = [item for item in intervals if not item.get("ignored")]
    passed_count = sum(item.get("passed") is True for item in considered)
    fraction = passed_count / len(considered) if considered else 0.0
    if not considered:
        reasons.append("no metrics intervals are available")
    if fraction < min_fraction:
        reasons.append("metrics interval EPS pass fraction is below the minimum")
    gates.add(
        "ingest_rate",
        not reasons,
        {
            "bounds": {"minimum": eps_min, "maximum": eps_max},
            "average_provider_committed_rows_per_sec": average,
            "average_passed": average_passed,
            "intervals": intervals,
            "interval_pass_count": passed_count,
            "interval_count": len(considered),
            "interval_pass_fraction": fraction,
            "minimum_pass_fraction": min_fraction,
        },
        reasons,
    )


def _query_record_reasons(
    record: Mapping[str, Any],
    expected_queries: int,
) -> list[str]:
    reasons: list[str] = []
    for field in ALIGNED_FIELDS:
        values = record.get(field)
        if not isinstance(values, list) or len(values) != expected_queries:
            reasons.append(f"{field} is not aligned to {expected_queries} queries")
            continue
        if any(not isinstance(item, list) or len(item) != 1 for item in values):
            reasons.append(f"{field} does not use one-element query entries")
    observations = record.get("query_evidence")
    query_errors = record.get("query_errors")
    if not isinstance(observations, list) or len(observations) != expected_queries:
        reasons.append(
            f"query_evidence does not contain exactly {expected_queries} queries"
        )
        return reasons
    if not isinstance(query_errors, list) or len(query_errors) != expected_queries:
        reasons.append(
            f"query_errors does not contain exactly {expected_queries} queries"
        )
        query_errors = [None] * expected_queries
    for index, observation in enumerate(observations):
        prefix = f"query {index + 1}"
        if not isinstance(observation, Mapping):
            reasons.append(f"{prefix} evidence is not an object")
            continue
        canonical = _number(observation.get("canonical_duration_sec"))
        if canonical is None or canonical < 0:
            reasons.append(f"{prefix} canonical result is missing")
        result_values = record.get("result")
        if (
            isinstance(result_values, list)
            and len(result_values) > index
            and isinstance(result_values[index], list)
            and len(result_values[index]) == 1
            and result_values[index][0] != observation.get("canonical_duration_sec")
        ):
            reasons.append(f"{prefix} aligned canonical result disagrees")
        statement_id = observation.get("statement_id")
        if not isinstance(statement_id, str) or not statement_id:
            reasons.append(f"{prefix} statement ID is missing")
        metrics = _mapping(observation.get("metrics"))
        if metrics.get("result_from_cache") is not False:
            reasons.append(f"{prefix} cache-hit evidence is not exact false")
        if metrics.get("cache_origin_statement_id") not in (None, ""):
            reasons.append(f"{prefix} has a cache origin statement")
        if observation.get("execution_succeeded") is not True:
            reasons.append(f"{prefix} execution did not succeed")
        row_count = _integer(observation.get("result_row_count"))
        if row_count is None or row_count < 0:
            reasons.append(f"{prefix} result row count is missing")
        result_hash = observation.get("result_hash_sha256")
        if (
            not isinstance(result_hash, str)
            or len(result_hash) != 64
            or any(character not in "0123456789abcdefABCDEF" for character in result_hash)
        ):
            reasons.append(f"{prefix} result hash is missing or invalid")
        if observation.get("errors") != []:
            reasons.append(f"{prefix} contains query errors")
        if index < len(query_errors) and query_errors[index] != []:
            reasons.append(f"{prefix} aligned query_errors is non-empty")
        cache_values = record.get("cache_hit")
        if (
            not isinstance(cache_values, list)
            or len(cache_values) <= index
            or cache_values[index] != [False]
        ):
            reasons.append(f"{prefix} aligned cache_hit is not exact false")
    if record.get("producer_progress_error") not in (None, ""):
        reasons.append("producer progress was unavailable at query start")
    return reasons


def _query_sample_report(
    rows: Sequence[Mapping[str, Any]],
    load_errors: Sequence[str],
    *,
    workload: str,
    expected_queries: int,
    expected_interval: float,
    expected_rows: int,
    cadence_tolerance: float,
) -> tuple[bool, dict[str, Any], list[str]]:
    reasons = list(load_errors)
    accepted: list[int] = []
    rejected: list[dict[str, Any]] = []
    active_invalid: list[dict[str, Any]] = []
    timed: list[tuple[int, datetime, datetime, datetime | None]] = []
    for index, record in enumerate(rows):
        sample_reasons: list[str] = []
        started = None
        finished = None
        scheduled = None
        try:
            started = parse_utc_timestamp(record.get("iteration_started_at"))
            finished = parse_utc_timestamp(record.get("iteration_finished_at"))
            if finished < started:
                sample_reasons.append("iteration finishes before it starts")
        except (TypeError, ValueError, OverflowError, OSError):
            sample_reasons.append("iteration start/finish timestamp is invalid")
        try:
            scheduled = parse_utc_timestamp(record.get("scheduled_start_at"))
        except (TypeError, ValueError, OverflowError, OSError):
            sample_reasons.append("scheduled_start_at is invalid")
        if started is not None and finished is not None:
            timed.append((index, started, finished, scheduled))
        if record.get("workload") != workload and record.get("runner") != workload:
            sample_reasons.append(f"record is not labeled {workload}")
        interval = _number(record.get("scheduled_interval_sec"))
        if interval is None or abs(interval - expected_interval) > 1e-9:
            sample_reasons.append(
                f"scheduled interval is not fixed at {expected_interval:g}s"
            )
        raw_rows = _integer(record.get("raw_rows"))
        active = (
            record.get("producer_progress_finished") is False
            and raw_rows is not None
            and raw_rows < expected_rows
        )
        if not active:
            if record.get("producer_progress_finished") is not False:
                sample_reasons.append(
                    "sample did not prove producer_progress_finished exact false"
                )
            if raw_rows is None:
                sample_reasons.append("raw_rows is missing")
            elif raw_rows >= expected_rows:
                sample_reasons.append("sample started at or after completed ingest")
            rejected.append({"index": index, "reasons": sample_reasons})
            continue
        sample_reasons.extend(_query_record_reasons(record, expected_queries))
        if sample_reasons:
            active_invalid.append({"index": index, "reasons": sample_reasons})
            rejected.append({"index": index, "reasons": sample_reasons})
        else:
            accepted.append(index)
    timed.sort(key=lambda item: item[1])
    overlap_failures: list[list[int]] = []
    for previous, current in zip(timed, timed[1:]):
        if current[1] < previous[2]:
            overlap_failures.append([previous[0], current[0]])
    if overlap_failures:
        reasons.append("runner iterations overlap themselves")
    cadence_failures: list[dict[str, Any]] = []
    scheduled_rows = [item for item in timed if item[3] is not None]
    for previous, current in zip(scheduled_rows, scheduled_rows[1:]):
        difference = (current[3] - previous[3]).total_seconds()
        if abs(difference - expected_interval) > cadence_tolerance:
            cadence_failures.append(
                {
                    "previous_index": previous[0],
                    "index": current[0],
                    "scheduled_delta_seconds": difference,
                }
            )
    if cadence_failures:
        reasons.append("fixed-rate scheduled cadence exceeds tolerance")
    if active_invalid:
        reasons.append("one or more active-ingest query samples are invalid")
    if not accepted:
        reasons.append("no valid active-ingest iteration was accepted")
    raw_sequence = [
        (item[1], _integer(rows[item[0]].get("raw_rows")), item[0]) for item in timed
    ]
    raw_monotonic_failures = [
        [previous[2], current[2]]
        for previous, current in zip(raw_sequence, raw_sequence[1:])
        if previous[1] is not None
        and current[1] is not None
        and current[1] < previous[1]
    ]
    if raw_monotonic_failures:
        reasons.append("query raw_rows is not monotonic")
    return (
        not reasons,
        {
            "accepted_indices": accepted,
            "accepted_count": len(accepted),
            "rejected": rejected,
            "rejected_count": len(rejected),
            "active_invalid": active_invalid,
            "self_overlap_failures": overlap_failures,
            "cadence_failures": cadence_failures,
            "raw_rows_monotonic_failures": raw_monotonic_failures,
            "expected_query_count": expected_queries,
            "expected_interval_seconds": expected_interval,
            "cadence_tolerance_seconds": cadence_tolerance,
        },
        reasons,
    )


def _gate_query_workloads(
    gates: Gates,
    dashboard: Sequence[Mapping[str, Any]],
    dashboard_errors: Sequence[str],
    drilldown: Sequence[Mapping[str, Any]],
    drilldown_errors: Sequence[str],
    *,
    expected_rows: int,
    cadence_tolerance: float,
) -> dict[str, Any]:
    reports: dict[str, Any] = {}
    for workload, rows, errors, count, interval in (
        ("dashboard", dashboard, dashboard_errors, 4, 600.0),
        ("drilldown", drilldown, drilldown_errors, 2, 3600.0),
    ):
        passed, evidence, reasons = _query_sample_report(
            rows,
            errors,
            workload=workload,
            expected_queries=count,
            expected_interval=interval,
            expected_rows=expected_rows,
            cadence_tolerance=cadence_tolerance,
        )
        reports[workload] = evidence
        gates.add(f"{workload}_queries", passed, evidence, reasons)
    combined: list[tuple[datetime, int, str, int]] = []
    for workload, rows in (("dashboard", dashboard), ("drilldown", drilldown)):
        for index, row in enumerate(rows):
            try:
                started = parse_utc_timestamp(row.get("iteration_started_at"))
            except (TypeError, ValueError, OverflowError, OSError):
                continue
            raw_rows = _integer(row.get("raw_rows"))
            if raw_rows is not None:
                combined.append((started, raw_rows, workload, index))
    combined.sort()
    failures = [
        {
            "previous": f"{previous[2]}:{previous[3]}",
            "previous_raw_rows": previous[1],
            "current": f"{current[2]}:{current[3]}",
            "current_raw_rows": current[1],
        }
        for previous, current in zip(combined, combined[1:])
        if current[1] < previous[1]
    ]
    gates.add(
        "query_raw_rows_monotonic",
        not failures,
        {
            "sample_count": len(combined),
            "failures": failures,
        },
        ["query raw_rows decreases across workload samples"] if failures else [],
    )
    return reports


def _gate_freshness(
    gates: Gates,
    rows: Sequence[Mapping[str, Any]],
    load_errors: Sequence[str],
    *,
    window_start: datetime | None,
    window_end: datetime | None,
    startup_grace: float,
    max_raw_age: float,
    max_mv_age: float,
) -> dict[str, Any]:
    reasons = list(load_errors)
    accepted: list[int] = []
    rejected: list[dict[str, Any]] = []
    invalid_eligible: list[dict[str, Any]] = []
    if window_start is None or window_end is None:
        reasons.append("measurement window is unavailable")
    grace_end = (
        window_start + timedelta(seconds=startup_grace)
        if window_start is not None
        else None
    )
    for index, row in enumerate(rows):
        sample_reasons: list[str] = []
        try:
            observed = parse_utc_timestamp(row.get("observed_at"))
        except (TypeError, ValueError, OverflowError, OSError):
            observed = None
            sample_reasons.append("observed_at is invalid")
        eligible = (
            observed is not None
            and grace_end is not None
            and window_end is not None
            and grace_end <= observed < window_end
        )
        if not eligible:
            if observed is not None:
                sample_reasons.append(
                    "sample is outside the post-grace measurement window"
                )
            rejected.append({"index": index, "reasons": sample_reasons})
            continue
        raw_age = _number(row.get("raw_commit_age_sec"))
        mv_age = _number(row.get("mv_refresh_age_sec"))
        if raw_age is None or raw_age < 0 or raw_age > max_raw_age:
            sample_reasons.append("raw observational age is missing or outside bounds")
        if mv_age is None or mv_age < 0 or mv_age > max_mv_age:
            sample_reasons.append("MV observational age is missing or outside bounds")
        if row.get("errors") != []:
            sample_reasons.append("freshness sample contains errors")
        semantics = str(row.get("age_semantics") or "").lower()
        if "observational" not in semantics or "not exact" not in semantics:
            sample_reasons.append(
                "age semantics do not identify ages as observational, not exact lag"
            )
        if sample_reasons:
            invalid_eligible.append({"index": index, "reasons": sample_reasons})
            rejected.append({"index": index, "reasons": sample_reasons})
        else:
            accepted.append(index)
    if invalid_eligible:
        reasons.append("one or more post-grace freshness samples are invalid")
    if not accepted:
        reasons.append("no post-grace freshness sample was accepted")
    evidence = {
        "accepted_indices": accepted,
        "accepted_count": len(accepted),
        "rejected": rejected,
        "rejected_count": len(rejected),
        "invalid_eligible": invalid_eligible,
        "startup_grace_seconds": startup_grace,
        "active_measurement_until": (
            iso_utc(window_end) if window_end is not None else None
        ),
        "maximum_raw_observational_age_seconds": max_raw_age,
        "maximum_mv_observational_age_seconds": max_mv_age,
        "age_interpretation": (
            "Raw and MV ages are observational timestamp proxies, not exact "
            "per-record ingestion or materialized-view lag."
        ),
    }
    gates.add("freshness", not reasons, evidence, reasons)
    return evidence


def _gate_reconciliation(
    gates: Gates,
    manifest: Mapping[str, Any],
    progress: Mapping[str, Any],
    evidence: Mapping[str, Any],
    errors: Sequence[str],
    expected_rows: int,
) -> None:
    values = {
        "source_manifest_total_rows": _integer(manifest.get("total_rows")),
        "producer_provider_committed_rows": _integer(
            progress.get("provider_committed_rows")
        ),
        "system_zerobus_provider_committed_records": _integer(
            evidence.get("provider_committed_records")
        ),
    }
    metadata_raw_rows = _integer(_nested(evidence, "table_num_rows", "raw"))
    reasons = list(errors)
    if any(value != expected_rows for value in values.values()):
        reasons.append("source, producer, and system Zerobus rows do not reconcile")
    if metadata_raw_rows is not None and metadata_raw_rows != expected_rows:
        reasons.append("DESCRIBE DETAIL raw numRows is present but does not reconcile")
    provider_errors = _integer(evidence.get("provider_errors"))
    stream_errors = _integer(evidence.get("zerobus_stream_error_count"))
    if provider_errors != 0:
        reasons.append("provider error count is missing or nonzero")
    if stream_errors != 0:
        reasons.append("Zerobus stream error count is missing or nonzero")
    gates.add(
        "row_reconciliation",
        not reasons,
        {
            "expected_rows": expected_rows,
            "counters": values,
            "optional_describe_detail_raw_numRows": metadata_raw_rows,
            "provider_errors": provider_errors,
            "zerobus_stream_error_count": stream_errors,
        },
        reasons,
    )


def _maintenance_class(value: Any) -> str:
    normalized = str(value or "").upper().replace("-", "_").replace(" ", "_")
    if not normalized:
        return "unknown"
    if "NO_OP" in normalized or normalized == "NOOP":
        return "no_op"
    if "COMPLETE_RECOMPUTE" in normalized or "FULL" in normalized:
        return "full"
    return "incremental"


def _gate_evidence(
    gates: Gates,
    evidence: Mapping[str, Any],
    errors: Sequence[str],
) -> None:
    reasons = list(errors)
    required_complete = _nested(
        evidence, "required_dataset_completeness", "complete"
    )
    if evidence.get("complete") is not True:
        reasons.append("evidence_summary.complete is not exact true")
    if required_complete is not True:
        reasons.append("required dataset completeness is not exact true")
    gates.add(
        "evidence_completeness",
        not reasons,
        {
            "complete": evidence.get("complete"),
            "required_dataset_completeness": evidence.get(
                "required_dataset_completeness"
            ),
            "optional_dataset_errors": evidence.get("optional_dataset_errors"),
        },
        reasons,
    )

    maintenance = _mapping(evidence.get("mv_maintenance"))
    types = _list(maintenance.get("maintenance_types"))
    classes = [_maintenance_class(value) for value in types]
    mv_reasons: list[str] = []
    if not types:
        mv_reasons.append("MV planning evidence is missing")
    if _integer(maintenance.get("full_refresh_count")) != 0:
        mv_reasons.append("MV full refresh count is missing or nonzero")
    if any(value not in {"incremental", "no_op"} for value in classes):
        mv_reasons.append("MV maintenance contains an unknown or unsupported type")
    if "full" in classes:
        mv_reasons.append("MV maintenance contains a full refresh")
    gates.add(
        "mv_incrementality",
        not mv_reasons,
        {
            "maintenance_types": types,
            "maintenance_classes": classes,
            "full_refresh_count": maintenance.get("full_refresh_count"),
        },
        mv_reasons,
    )


_VOLATILE_WAREHOUSE_FIELDS = {
    "captured_at",
    "created_time",
    "creator_name",
    "health",
    "jdbc_url",
    "last_modified_time",
    "num_active_sessions",
    "num_clusters",
    "odbc_params",
    "state",
}


def _stable_warehouse_config(value: Any) -> Any:
    """Remove observations that are not warehouse configuration."""
    if isinstance(value, Mapping):
        return {
            str(key): _stable_warehouse_config(item)
            for key, item in value.items()
            if str(key).lower() not in _VOLATILE_WAREHOUSE_FIELDS
        }
    if isinstance(value, list):
        return [_stable_warehouse_config(item) for item in value]
    return value


def _gate_warehouse_configs(
    gates: Gates,
    evidence: Mapping[str, Any],
    dashboard: Sequence[Mapping[str, Any]],
    drilldown: Sequence[Mapping[str, Any]],
    query_reports: Mapping[str, Any],
    window_start: datetime | None,
) -> None:
    snapshots = [
        item
        for item in _list(evidence.get("frozen_warehouse_configs"))
        if isinstance(item, Mapping)
    ]
    targets = _mapping(evidence.get("targets"))
    expected_ids = {
        str(value)
        for value in (
            targets.get("rt_warehouse_id"),
            targets.get("control_warehouse_id"),
        )
        if value not in (None, "")
    }
    actual_ids = {
        str(item.get("warehouse_id"))
        for item in snapshots
        if item.get("warehouse_id") not in (None, "")
    }
    reasons: list[str] = []
    if len(expected_ids) != 2 or actual_ids != expected_ids or len(snapshots) != 2:
        reasons.append("frozen RT/control warehouse snapshots are missing or mismatched")
    changed_during_window: list[str] = []
    for item in snapshots:
        changed = item.get("change_time")
        try:
            changed_at = parse_utc_timestamp(changed)
        except (TypeError, ValueError, OverflowError, OSError):
            changed_at = None
        if changed_at is None or window_start is None or changed_at > window_start:
            changed_during_window.append(str(item.get("warehouse_id")))
        delete_time = item.get("delete_time")
        if delete_time not in (None, ""):
            try:
                if window_start is None or parse_utc_timestamp(delete_time) >= window_start:
                    changed_during_window.append(str(item.get("warehouse_id")))
            except (TypeError, ValueError, OverflowError, OSError):
                changed_during_window.append(str(item.get("warehouse_id")))
    if changed_during_window:
        reasons.append("warehouse change/delete evidence is not frozen before the window")
    runner_configs: list[str] = []
    missing_runner_configs: list[str] = []
    for workload, rows in (("dashboard", dashboard), ("drilldown", drilldown)):
        accepted = _list(_nested(query_reports, workload, "accepted_indices"))
        for index in accepted:
            if not isinstance(index, int) or not 0 <= index < len(rows):
                continue
            config = rows[index].get("rt_warehouse")
            if not isinstance(config, Mapping):
                missing_runner_configs.append(f"{workload}:{index}")
            else:
                runner_configs.append(
                    json.dumps(
                        _stable_warehouse_config(config),
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                )
    if missing_runner_configs:
        reasons.append("accepted query samples omit frozen RT warehouse configuration")
    if runner_configs and len(set(runner_configs)) != 1:
        reasons.append("RT warehouse configuration changed between accepted samples")
    gates.add(
        "warehouse_configuration",
        not reasons,
        {
            "expected_warehouse_ids": sorted(expected_ids),
            "snapshot_warehouse_ids": sorted(actual_ids),
            "changed_or_unproven_warehouse_ids": sorted(set(changed_during_window)),
            "accepted_runner_config_count": len(runner_configs),
            "accepted_runner_config_sha256": sorted(
                {
                    hashlib.sha256(value.encode("utf-8")).hexdigest()
                    for value in runner_configs
                }
            ),
            "missing_runner_configs": missing_runner_configs,
        },
        reasons,
    )


def _all_amounts_zero(total: Any) -> bool:
    if not isinstance(total, Mapping):
        return False
    values = _mapping(total.get("amount_by_currency"))
    amount = total.get("amount")
    candidates = list(values.values())
    if amount is not None:
        candidates.append(amount)
    return bool(candidates or values == {}) and all(
        _decimal(value) == 0 for value in candidates
    )


def _gate_cost(gates: Gates, cost: Mapping[str, Any], errors: Sequence[str]) -> None:
    reasons = list(errors)
    completeness = _mapping(cost.get("completeness"))
    canonical = cost.get("canonical_undiscounted_primary_total")
    canonical_map = _mapping(canonical)
    canonical_amount = _decimal(canonical_map.get("amount"))
    fresh_path = _mapping(cost.get("canonical_undiscounted_fresh_path_total"))
    query_serving = _mapping(
        cost.get("canonical_undiscounted_query_serving_total")
    )
    fresh_path_amount = _decimal(fresh_path.get("amount"))
    query_serving_amount = _decimal(query_serving.get("amount"))
    if cost.get("complete") is not True or completeness.get("complete") is not True:
        reasons.append("cost summary completeness is not exact true")
    if canonical_amount is None:
        reasons.append("canonical undiscounted primary total is missing")
    if fresh_path_amount is None or query_serving_amount is None:
        reasons.append("fresh-path or measured-query cost subledger is missing")
    elif (
        canonical_amount is None
        or fresh_path.get("currency") != canonical_map.get("currency")
        or query_serving.get("currency") != canonical_map.get("currency")
        or fresh_path_amount + query_serving_amount != canonical_amount
    ):
        reasons.append("fresh-path plus measured-query cost does not reconcile")
    if not _all_amounts_zero(cost.get("unknown_total")):
        reasons.append("unknown cost total is missing or nonzero")
    unpriced = _mapping(cost.get("unpriced_total"))
    unpriced_rows = _list(unpriced.get("usage_row_indices"))
    quantities = _mapping(unpriced.get("usage_quantity_by_unit"))
    if unpriced_rows or any(_decimal(value) != 0 for value in quantities.values()):
        reasons.append("unpriced usage is nonzero")
    scenario = _mapping(cost.get("lakehouse_rt_beta_30_percent_off_scenario"))
    if (
        scenario.get("discount_rate") != "0.30"
        or scenario.get("applies_only_to") != "primary_rt_serving_compute"
        or scenario.get("canonical_total_unchanged") is not True
        or scenario.get("canonical_undiscounted_primary_total") != canonical
    ):
        reasons.append("30%-off RT Beta scenario is missing, merged, or changes canonical")
    gates.add(
        "cost_completeness",
        not reasons,
        {
            "complete": cost.get("complete"),
            "completeness": completeness,
            "canonical_undiscounted_primary_total": canonical,
            "canonical_undiscounted_fresh_path_total": fresh_path,
            "canonical_undiscounted_query_serving_total": query_serving,
            "unknown_total": cost.get("unknown_total"),
            "unpriced_total": unpriced,
            "beta_scenario": scenario,
        },
        reasons,
    )

    predictive = _mapping(cost.get("predictive_optimization"))
    predictive_reasons: list[str] = []
    if _integer(predictive.get("failed_operation_count")) != 0:
        predictive_reasons.append(
            "predictive optimization failure count is missing or nonzero"
        )
    if not isinstance(predictive.get("failed_operation_row_indices"), list):
        predictive_reasons.append("predictive optimization failure evidence is missing")
    gates.add(
        "predictive_optimization",
        not predictive_reasons,
        {
            "operation_count": predictive.get("operation_count"),
            "failed_operation_count": predictive.get("failed_operation_count"),
            "failed_operation_row_indices": predictive.get(
                "failed_operation_row_indices"
            ),
        },
        predictive_reasons,
    )


def build_validation_report(
    *,
    source_manifest: Mapping[str, Any],
    ingest_progress: Mapping[str, Any],
    ingest_metrics: Sequence[Mapping[str, Any]],
    dashboard: Sequence[Mapping[str, Any]],
    drilldown: Sequence[Mapping[str, Any]],
    freshness: Sequence[Mapping[str, Any]],
    evidence_summary: Mapping[str, Any],
    cost_summary: Mapping[str, Any],
    input_errors: Mapping[str, Sequence[str]] | None = None,
    expected_rows: int = EXPECTED_ROWS,
    eps_min: float = 900_000.0,
    eps_max: float = 1_100_000.0,
    min_interval_pass_fraction: float = 0.90,
    max_raw_age_seconds: float = 120.0,
    max_mv_age_seconds: float = 300.0,
    freshness_startup_grace_seconds: float = 120.0,
    cadence_tolerance_seconds: float = 5.0,
    input_paths: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    errors = {key: list(value) for key, value in (input_errors or {}).items()}
    gates = Gates()
    all_input_errors = [
        error for values in errors.values() for error in values
    ]
    gates.add(
        "input_availability",
        not all_input_errors,
        {
            "paths": dict(input_paths or {}),
            "errors_by_input": errors,
        },
        all_input_errors,
    )
    window = _mapping(evidence_summary.get("collection_window"))
    try:
        window_start = parse_utc_timestamp(window.get("since"))
        window_end = parse_utc_timestamp(window.get("until"))
        if window_end <= window_start:
            raise ValueError("window is empty")
        normalized_window: dict[str, Any] = {
            "since": iso_utc(window_start),
            "until": iso_utc(window_end),
            "duration_seconds": (window_end - window_start).total_seconds(),
        }
    except (TypeError, ValueError, OverflowError, OSError) as exc:
        window_start = window_end = None
        normalized_window = {
            "since": window.get("since"),
            "until": window.get("until"),
            "error": f"{type(exc).__name__}: {exc}",
        }
    try:
        producer_finished_at = parse_utc_timestamp(
            ingest_progress.get("finished_at")
        )
    except (TypeError, ValueError, OverflowError, OSError):
        producer_finished_at = None
    freshness_window_end = (
        min(window_end, producer_finished_at)
        if window_end is not None and producer_finished_at is not None
        else window_end
    )
    _gate_source_manifest(
        gates,
        source_manifest,
        errors.get("source_manifest", ()),
        expected_rows,
    )
    _gate_producer(
        gates,
        ingest_progress,
        source_manifest,
        errors.get("ingest_progress", ()),
        expected_rows,
    )
    _gate_ingest_rate(
        gates,
        ingest_progress,
        ingest_metrics,
        errors.get("ingest_metrics", ()),
        eps_min=eps_min,
        eps_max=eps_max,
        min_fraction=min_interval_pass_fraction,
    )
    query_reports = _gate_query_workloads(
        gates,
        dashboard,
        errors.get("dashboard", ()),
        drilldown,
        errors.get("drilldown", ()),
        expected_rows=expected_rows,
        cadence_tolerance=cadence_tolerance_seconds,
    )
    freshness_report = _gate_freshness(
        gates,
        freshness,
        errors.get("freshness", ()),
        window_start=window_start,
        window_end=freshness_window_end,
        startup_grace=freshness_startup_grace_seconds,
        max_raw_age=max_raw_age_seconds,
        max_mv_age=max_mv_age_seconds,
    )
    _gate_reconciliation(
        gates,
        source_manifest,
        ingest_progress,
        evidence_summary,
        errors.get("evidence_summary", ()),
        expected_rows,
    )
    _gate_evidence(
        gates,
        evidence_summary,
        errors.get("evidence_summary", ()),
    )
    _gate_warehouse_configs(
        gates,
        evidence_summary,
        dashboard,
        drilldown,
        query_reports,
        window_start,
    )
    _gate_cost(gates, cost_summary, errors.get("cost_summary", ()))
    accepted = all(gate["passed"] for gate in gates.values)
    return {
        "schema_version": 1,
        "accepted": accepted,
        "window": normalized_window,
        "configuration": {
            "expected_rows": expected_rows,
            "eps_bounds": {"minimum": eps_min, "maximum": eps_max},
            "minimum_interval_pass_fraction": min_interval_pass_fraction,
            "maximum_raw_observational_age_seconds": max_raw_age_seconds,
            "maximum_mv_observational_age_seconds": max_mv_age_seconds,
            "freshness_startup_grace_seconds": freshness_startup_grace_seconds,
            "cadence_tolerance_seconds": cadence_tolerance_seconds,
            "freshness_age_semantics": (
                "Observational timestamp proxies only; not exact per-record lag."
            ),
        },
        "gates": gates.values,
        "query_samples": query_reports,
        "freshness_samples": freshness_report,
    }


def validate_files(
    *,
    source_manifest_path: Path,
    ingest_progress_path: Path,
    ingest_metrics_path: Path,
    dashboard_path: Path,
    drilldown_path: Path,
    freshness_path: Path,
    evidence_summary_path: Path,
    cost_summary_path: Path,
    **configuration: Any,
) -> dict[str, Any]:
    source_manifest, source_errors = _read_json(
        source_manifest_path, "source_manifest"
    )
    ingest_progress, progress_errors = _read_json(
        ingest_progress_path, "ingest_progress"
    )
    ingest_metrics, metrics_errors = _read_jsonl(
        ingest_metrics_path, "ingest_metrics"
    )
    dashboard, dashboard_errors = _read_jsonl(dashboard_path, "dashboard")
    drilldown, drilldown_errors = _read_jsonl(drilldown_path, "drilldown")
    freshness, freshness_errors = _read_jsonl(freshness_path, "freshness")
    evidence_summary, evidence_errors = _read_json(
        evidence_summary_path, "evidence_summary"
    )
    cost_summary, cost_errors = _read_json(cost_summary_path, "cost_summary")
    paths = {
        "source_manifest": str(source_manifest_path),
        "ingest_progress": str(ingest_progress_path),
        "ingest_metrics": str(ingest_metrics_path),
        "dashboard": str(dashboard_path),
        "drilldown": str(drilldown_path),
        "freshness": str(freshness_path),
        "evidence_summary": str(evidence_summary_path),
        "cost_summary": str(cost_summary_path),
    }
    return build_validation_report(
        source_manifest=source_manifest,
        ingest_progress=ingest_progress,
        ingest_metrics=ingest_metrics,
        dashboard=dashboard,
        drilldown=drilldown,
        freshness=freshness,
        evidence_summary=evidence_summary,
        cost_summary=cost_summary,
        input_errors={
            "source_manifest": source_errors,
            "ingest_progress": progress_errors,
            "ingest_metrics": metrics_errors,
            "dashboard": dashboard_errors,
            "drilldown": drilldown_errors,
            "freshness": freshness_errors,
            "evidence_summary": evidence_errors,
            "cost_summary": cost_errors,
        },
        input_paths=paths,
        **configuration,
    )


def _positive_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a finite positive number")
    return parsed


def _fraction(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or not 0 <= parsed <= 1:
        raise argparse.ArgumentTypeError("must be between zero and one")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate an offline Databricks benchmark evidence ledger"
    )
    parser.add_argument("--source-manifest", type=Path, required=True)
    parser.add_argument("--ingest-progress", type=Path, required=True)
    parser.add_argument("--ingest-metrics", type=Path, required=True)
    parser.add_argument("--dashboard", type=Path, required=True)
    parser.add_argument("--drilldown", type=Path, required=True)
    parser.add_argument("--freshness", type=Path, required=True)
    parser.add_argument("--evidence-summary", type=Path, required=True)
    parser.add_argument("--cost-summary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-rows", type=int, default=EXPECTED_ROWS)
    parser.add_argument("--eps-min", "--min-eps", type=_positive_float, default=900_000)
    parser.add_argument("--eps-max", "--max-eps", type=_positive_float, default=1_100_000)
    parser.add_argument(
        "--min-interval-pass-fraction", type=_fraction, default=0.90
    )
    parser.add_argument(
        "--max-raw-age-seconds",
        "--max-raw-observational-age-seconds",
        type=_positive_float,
        default=120,
    )
    parser.add_argument(
        "--max-mv-age-seconds",
        "--max-mv-observational-age-seconds",
        type=_positive_float,
        default=300,
    )
    parser.add_argument(
        "--freshness-startup-grace-seconds",
        type=float,
        default=120,
    )
    parser.add_argument(
        "--cadence-tolerance-seconds", type=_positive_float, default=5
    )
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.expected_rows <= 0:
        parser.error("--expected-rows must be positive")
    if args.eps_max < args.eps_min:
        parser.error("--eps-max must be greater than or equal to --eps-min")
    if (
        not math.isfinite(args.freshness_startup_grace_seconds)
        or args.freshness_startup_grace_seconds < 0
    ):
        parser.error("--freshness-startup-grace-seconds must be finite and non-negative")
    report = validate_files(
        source_manifest_path=args.source_manifest.expanduser().resolve(),
        ingest_progress_path=args.ingest_progress.expanduser().resolve(),
        ingest_metrics_path=args.ingest_metrics.expanduser().resolve(),
        dashboard_path=args.dashboard.expanduser().resolve(),
        drilldown_path=args.drilldown.expanduser().resolve(),
        freshness_path=args.freshness.expanduser().resolve(),
        evidence_summary_path=args.evidence_summary.expanduser().resolve(),
        cost_summary_path=args.cost_summary.expanduser().resolve(),
        expected_rows=args.expected_rows,
        eps_min=args.eps_min,
        eps_max=args.eps_max,
        min_interval_pass_fraction=args.min_interval_pass_fraction,
        max_raw_age_seconds=args.max_raw_age_seconds,
        max_mv_age_seconds=args.max_mv_age_seconds,
        freshness_startup_grace_seconds=args.freshness_startup_grace_seconds,
        cadence_tolerance_seconds=args.cadence_tolerance_seconds,
    )
    output = args.output.expanduser().resolve()
    if output.exists() and output.is_dir():
        output = output / "validation_report.json"
    atomic_write_json(output, report)
    print(f"Validation report written to {output}")
    return 0 if report["accepted"] else 1


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return run(argv)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"FATAL: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
