#!/usr/bin/env python3
"""Stream Parquet row groups to Databricks with Zerobus Arrow Flight.

The module intentionally imports neither pyarrow nor Zerobus at import time so
its source-selection, recovery, accounting, and rate-limit logic can be tested
with the Python standard library alone.
"""

from __future__ import annotations

import argparse
import ctypes
import gc
import hashlib
import importlib.metadata
import inspect
import json
import os
import queue
import resource
import signal
import sys
import threading
import time
import traceback
import urllib.parse
import uuid
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from dbx_common import (
    DatabricksRestClient,
    atomic_append_jsonl,
    atomic_write_json,
    iso_utc,
    quote_identifier,
    redact_secrets,
    sql_string,
    statement_rows,
    zerobus_endpoint_errors,
)

EXPECTED_FULL_ROWS = 113_219_565_734
REQUIRED_SDK_DISTRIBUTION = "databricks-zerobus-ingest-sdk"
REQUIRED_SDK_VERSION = "1.8.0"
TARGET_COLUMNS = (
    "sym",
    "bx",
    "bp",
    "bs",
    "ax",
    "ap",
    "as",
    "c",
    "i",
    "t",
    "q",
    "z",
)
SIGNED_LIMITS = {
    8: (-(2**7), 2**7 - 1),
    16: (-(2**15), 2**15 - 1),
    32: (-(2**31), 2**31 - 1),
    64: (-(2**63), 2**63 - 1),
}


@dataclass(frozen=True)
class Task:
    ordinal: int
    file_ordinal: int
    file_path: Path
    relative_path: str
    row_group: int
    rows: int

    def evidence(self) -> dict[str, Any]:
        return {
            "canonical_ordinal": self.ordinal,
            "task_ordinal": self.ordinal,
            "file_ordinal": self.file_ordinal,
            "file": self.relative_path,
            "row_group": self.row_group,
            "row_count": self.rows,
            "task_rows": self.rows,
        }


@dataclass(frozen=True)
class BatchCoordinate:
    task_ordinal: int
    file_ordinal: int
    file: str
    row_group: int
    batch_ordinal: int
    row_offset: int
    row_count: int

    @classmethod
    def from_task(
        cls,
        task: Task,
        *,
        batch_ordinal: int,
        row_offset: int,
        row_count: int,
    ) -> "BatchCoordinate":
        if batch_ordinal < 0 or row_offset < 0 or row_count <= 0:
            raise ValueError("batch ordinal/offset must be non-negative and rows positive")
        if row_offset + row_count > task.rows:
            raise ValueError("batch coordinate extends beyond its row group")
        return cls(
            task_ordinal=task.ordinal,
            file_ordinal=task.file_ordinal,
            file=task.relative_path,
            row_group=task.row_group,
            batch_ordinal=batch_ordinal,
            row_offset=row_offset,
            row_count=row_count,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "task_ordinal": self.task_ordinal,
            "file_ordinal": self.file_ordinal,
            "file": self.file,
            "row_group": self.row_group,
            "batch_ordinal": self.batch_ordinal,
            "row_offset": self.row_offset,
            "row_count": self.row_count,
        }


@dataclass
class PendingBatch:
    coordinate: BatchCoordinate
    rows: int
    uncompressed_arrow_buffer_bytes: int
    logical_offset: Any
    report_offset: int | str
    submitted_at: str
    worker: int
    stream: str

    @property
    def key(self) -> str:
        c = self.coordinate
        return f"{self.stream}:{c.task_ordinal}:{c.batch_ordinal}"

    def evidence(self) -> dict[str, Any]:
        return {
            **self.coordinate.as_dict(),
            "row_count": self.rows,
            "uncompressed_arrow_buffer_bytes": self.uncompressed_arrow_buffer_bytes,
            "logical_offset": self.report_offset,
            "submitted_at": self.submitted_at,
            "worker": self.worker,
            "stream": self.stream,
        }


@dataclass(frozen=True)
class RuntimeModules:
    pa: Any
    pc: Any
    pq: Any
    sdk_class: Any
    options_class: Any
    compression_enum: Any
    sdk_version: str
    pyarrow_version: str


@dataclass
class MemoryTelemetry:
    trim_attempts: int = 0
    trim_successes: int = 0
    trim_unsupported: int = 0
    gc_collected_objects: int = 0
    trim_reclaimed_rss_bytes: int = 0
    last_trim_duration_sec: float | None = None
    last_trim_at: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "trim_attempts": self.trim_attempts,
            "trim_successes": self.trim_successes,
            "trim_unsupported": self.trim_unsupported,
            "gc_collected_objects": self.gc_collected_objects,
            "trim_reclaimed_rss_bytes": self.trim_reclaimed_rss_bytes,
            "last_trim_duration_sec": self.last_trim_duration_sec,
            "last_trim_at": self.last_trim_at,
        }


class RateLimiter:
    """A global row-rate limiter that never repays idle time with a burst."""

    def __init__(self, target_rows_per_sec: float, *, clock: Any = time.monotonic):
        if target_rows_per_sec < 0:
            raise ValueError("target_rows_per_sec must be non-negative")
        self.target_rows_per_sec = float(target_rows_per_sec)
        self._clock = clock
        self._next_send_at = clock()
        self._scheduled_rows = 0
        self._lock = threading.Lock()

    def reserve(self, rows: int) -> float:
        """Reserve a send start and return the required delay in seconds."""
        if rows <= 0:
            raise ValueError("rows must be positive")
        if self.target_rows_per_sec == 0:
            with self._lock:
                self._scheduled_rows += rows
            return 0.0
        with self._lock:
            now = self._clock()
            send_at = max(now, self._next_send_at)
            self._next_send_at = send_at + rows / self.target_rows_per_sec
            self._scheduled_rows += rows
            return max(0.0, send_at - now)

    def wait(self, rows: int, abort_event: threading.Event | None = None) -> bool:
        delay = self.reserve(rows)
        if delay <= 0:
            return abort_event is None or not abort_event.is_set()
        if abort_event is None:
            time.sleep(delay)
            return True
        return not abort_event.wait(delay)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "target_rows_per_sec": self.target_rows_per_sec,
                "scheduled_rows": self._scheduled_rows,
                "next_send_delay_sec": max(0.0, self._next_send_at - self._clock()),
                "no_catch_up": True,
            }


def target_schema(pa_module: Any | None = None) -> Any:
    """Build the nullable Arrow schema that exactly corresponds to create.sql."""
    pa = pa_module or _import_pyarrow()
    return pa.schema(
        [
            pa.field("sym", pa.large_utf8(), nullable=True),
            pa.field("bx", pa.int16(), nullable=True),
            pa.field("bp", pa.float64(), nullable=True),
            pa.field("bs", pa.int64(), nullable=True),
            pa.field("ax", pa.int16(), nullable=True),
            pa.field("ap", pa.float64(), nullable=True),
            pa.field("as", pa.int64(), nullable=True),
            pa.field("c", pa.int16(), nullable=True),
            pa.field("i", pa.list_(pa.int16()), nullable=True),
            pa.field("t", pa.int64(), nullable=True),
            pa.field("q", pa.int64(), nullable=True),
            pa.field("z", pa.int16(), nullable=True),
        ]
    )


def _import_pyarrow() -> Any:
    try:
        import pyarrow as pa
    except ImportError as exc:
        raise RuntimeError(
            "pyarrow is required; install requirements-zerobus.txt"
        ) from exc
    return pa


def _is_list_type(pa: Any, value: Any) -> bool:
    checks = [pa.types.is_list, pa.types.is_large_list]
    if hasattr(pa.types, "is_fixed_size_list"):
        checks.append(pa.types.is_fixed_size_list)
    return any(check(value) for check in checks)


def _prove_unsigned_fits(
    source: Any,
    target_type: Any,
    *,
    column_name: str,
    pa: Any,
    pc: Any,
) -> None:
    """Explicitly prove unsigned scalar/list values fit a signed target."""
    source_type = source.type
    if _is_list_type(pa, source_type) and _is_list_type(pa, target_type):
        flattened = pc.list_flatten(source)
        _prove_unsigned_fits(
            flattened,
            target_type.value_type,
            column_name=f"{column_name}[]",
            pa=pa,
            pc=pc,
        )
        return
    if not (
        pa.types.is_unsigned_integer(source_type)
        and pa.types.is_signed_integer(target_type)
    ):
        return
    bit_width = int(target_type.bit_width)
    if bit_width not in SIGNED_LIMITS:
        raise ValueError(f"unsupported signed target width for {column_name}: {bit_width}")
    maximum = pc.max(source, skip_nulls=True)
    maximum_value = maximum.as_py() if maximum is not None else None
    if maximum_value is not None and maximum_value > SIGNED_LIMITS[bit_width][1]:
        raise OverflowError(
            f"unsigned values in {column_name} do not fit {target_type}: "
            f"maximum {maximum_value}"
        )


def normalize_batch(
    batch: Any,
    *,
    pa_module: Any | None = None,
    pc_module: Any | None = None,
) -> Any:
    """Select the target columns and apply only safe, columnar Arrow casts."""
    pa = pa_module or _import_pyarrow()
    if pc_module is None:
        import pyarrow.compute as pc
    else:
        pc = pc_module
    schema = target_schema(pa)
    missing = [name for name in TARGET_COLUMNS if name not in batch.schema.names]
    if missing:
        raise ValueError(f"Parquet schema is missing required columns: {missing}")
    arrays = []
    for field in schema:
        source = batch.column(batch.schema.get_field_index(field.name))
        _prove_unsigned_fits(
            source,
            field.type,
            column_name=field.name,
            pa=pa,
            pc=pc,
        )
        arrays.append(pc.cast(source, field.type, safe=True))
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def map_ipc_compression(name: str, enum_type: Any) -> Any:
    normalized = name.strip().upper().replace("-", "_")
    if normalized == "LZ4":
        normalized = "LZ4_FRAME"
    if normalized not in {"NONE", "LZ4_FRAME", "ZSTD"}:
        raise ValueError("IPC compression must be one of: none, LZ4_FRAME, ZSTD")
    try:
        return getattr(enum_type, normalized)
    except AttributeError as exc:
        raise RuntimeError(
            f"installed Zerobus SDK omits IPCCompression.{normalized}"
        ) from exc


def discover_source_files(
    directory: Path,
    pattern: str,
    *,
    include_quotes_0: bool = False,
    max_files: int | None = None,
) -> tuple[list[Path], list[str]]:
    if not directory.is_dir():
        raise RuntimeError(f"not a directory: {directory}")
    matched = sorted(
        (path for path in directory.glob(pattern) if path.is_file()),
        key=lambda path: path.relative_to(directory).as_posix(),
    )
    excluded = [
        path.relative_to(directory).as_posix()
        for path in matched
        if path.name == "quotes_0.parquet" and not include_quotes_0
    ]
    selected = [
        path
        for path in matched
        if include_quotes_0 or path.name != "quotes_0.parquet"
    ]
    if max_files is not None:
        selected = selected[:max_files]
    if not selected:
        raise RuntimeError(
            f"no source files matched {directory / pattern} after exclusions"
        )
    return selected, excluded


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def enumerate_tasks_and_manifest(
    directory: Path,
    pattern: str,
    pq: Any,
    *,
    include_quotes_0: bool,
    max_files: int | None,
    max_row_groups: int | None,
    max_rows: int | None = None,
    hash_files: bool = True,
) -> tuple[list[Task], dict[str, Any]]:
    files, excluded = discover_source_files(
        directory,
        pattern,
        include_quotes_0=include_quotes_0,
        max_files=max_files,
    )
    tasks: list[Task] = []
    file_reports: list[dict[str, Any]] = []
    exhausted = False
    selected_total = 0
    for file_ordinal, file_path in enumerate(files):
        stat_before = file_path.stat()
        parquet = pq.ParquetFile(file_path)
        metadata = parquet.metadata
        selected_row_groups: list[int] = []
        for row_group in range(metadata.num_row_groups):
            if max_row_groups is not None and len(tasks) >= max_row_groups:
                exhausted = True
                break
            source_rows = int(metadata.row_group(row_group).num_rows)
            remaining = (
                None
                if max_rows is None
                else max_rows - selected_total
            )
            if remaining is not None and remaining <= 0:
                exhausted = True
                break
            rows = source_rows if remaining is None else min(source_rows, remaining)
            relative = file_path.relative_to(directory).as_posix()
            tasks.append(
                Task(
                    ordinal=len(tasks),
                    file_ordinal=file_ordinal,
                    file_path=file_path.resolve(),
                    relative_path=relative,
                    row_group=row_group,
                    rows=rows,
                )
            )
            selected_row_groups.append(rows)
            selected_total += rows
            if rows < source_rows:
                exhausted = True
                break
        if selected_row_groups:
            print(
                "SOURCE MANIFEST "
                f"{'hashing' if hash_files else 'recording'} "
                f"file={file_path.relative_to(directory).as_posix()} "
                f"selected_rows={sum(selected_row_groups):,}",
                flush=True,
            )
            digest = sha256_file(file_path) if hash_files else None
            stat_after = file_path.stat()
            if (
                stat_before.st_size != stat_after.st_size
                or stat_before.st_mtime_ns != stat_after.st_mtime_ns
            ):
                raise RuntimeError(
                    f"source file changed while manifesting: {file_path}"
                )
            file_reports.append(
                {
                    "file_ordinal": file_ordinal,
                    "path": file_path.relative_to(directory).as_posix(),
                    "source_size_bytes": stat_after.st_size,
                    "source_mtime_ns": stat_after.st_mtime_ns,
                    "sha256": digest,
                    "sha256_status": "complete" if hash_files else "deferred",
                    "parquet_row_groups": int(metadata.num_row_groups),
                    "selected_row_group_rows": selected_row_groups,
                    "selected_rows": sum(selected_row_groups),
                }
            )
        if exhausted:
            break
    if not tasks:
        raise RuntimeError("source selection produced no Parquet row groups")
    manifest_core = {
        "schema_version": 1,
        "root": str(directory),
        "pattern": pattern,
        "quotes_0_parquet_included": include_quotes_0,
        "excluded_files": excluded,
        "max_files": max_files,
        "max_row_groups": max_row_groups,
        "max_rows": max_rows,
        "files": file_reports,
        "tasks": [task.evidence() for task in tasks],
        "selected_file_count": len(file_reports),
        "selected_row_group_count": len(tasks),
        "total_rows": sum(task.rows for task in tasks),
    }
    fingerprint = hashlib.sha256(
        json.dumps(manifest_core, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()
    return tasks, {**manifest_core, "manifest_sha256": fingerprint}


def validate_expected_rows(
    actual_rows: int,
    expected_rows: int = EXPECTED_FULL_ROWS,
    *,
    allow_partial: bool,
) -> None:
    if actual_rows <= 0:
        raise RuntimeError("selected source has no rows")
    if actual_rows != expected_rows and not allow_partial:
        raise RuntimeError(
            f"source has {actual_rows:,} rows, expected {expected_rows:,}; "
            "use --allow-partial only for an intentional qualification run"
        )


def ordinal_ranges(ordinals: Iterable[int]) -> list[list[int]]:
    values = sorted(set(int(value) for value in ordinals))
    if not values:
        return []
    ranges: list[list[int]] = []
    start = previous = values[0]
    for value in values[1:]:
        if value == previous + 1:
            previous = value
            continue
        ranges.append([start, previous])
        start = previous = value
    ranges.append([start, previous])
    return ranges


def expand_ordinal_ranges(value: Any) -> set[int]:
    if not isinstance(value, list):
        raise ValueError("completed task ranges must be a list")
    result: set[int] = set()
    for item in value:
        if (
            not isinstance(item, list)
            or len(item) != 2
            or isinstance(item[0], bool)
            or isinstance(item[1], bool)
        ):
            raise ValueError("completed task range must be [start, end]")
        start, end = int(item[0]), int(item[1])
        if start < 0 or end < start:
            raise ValueError("completed task range is invalid")
        result.update(range(start, end + 1))
    return result


def add_ordinal_range(ranges: list[list[int]], ordinal: int) -> list[list[int]]:
    """Insert one completed ordinal into sorted disjoint inclusive ranges."""
    if ordinal < 0:
        raise ValueError("completed task ordinal must be nonnegative")
    merged: list[list[int]] = []
    start = end = ordinal
    inserted = False
    for current_start, current_end in ranges:
        if current_end + 1 < start:
            merged.append([current_start, current_end])
        elif end + 1 < current_start:
            if not inserted:
                merged.append([start, end])
                inserted = True
            merged.append([current_start, current_end])
        else:
            start = min(start, current_start)
            end = max(end, current_end)
    if not inserted:
        merged.append([start, end])
    return merged


def _report_offset(offset: Any) -> int | str:
    if isinstance(offset, int) and not isinstance(offset, bool):
        return offset
    return str(offset)


def frozen_config(args: argparse.Namespace) -> dict[str, Any]:
    """Configuration that must remain identical across a safe resume."""
    return {
        "workers": args.workers,
        "batch_size": args.batch_size,
        "queue_capacity": args.queue_capacity,
        "target_rows_per_sec": args.target_eps,
        "ipc_compression": args.compression,
        "checkpoint_batches": args.checkpoint_batches,
        "checkpoint_seconds": args.checkpoint_seconds,
        "checkpoint_method": args.checkpoint_method,
        "automatic_recovery": not args.disable_automatic_recovery,
        "recovery_timeout_ms": args.recovery_timeout_ms,
        "recovery_backoff_ms": args.recovery_backoff_ms,
        "recovery_retries": args.recovery_retries,
        "recovery_log_threshold_seconds": args.recovery_log_threshold_seconds,
        "manual_stream_rotation_seconds": args.manual_stream_rotation_seconds,
        "include_quotes_0": args.include_quotes_0,
        "pattern": args.pattern,
        "max_files": args.max_files,
        "max_row_groups": args.max_row_groups,
        "max_rows": args.max_rows,
        "expected_rows": args.expected_rows,
        "allow_partial": args.allow_partial,
        "source_hashes_deferred": args.defer_source_hashes,
        "compact_progress": args.compact_progress,
        "compact_evidence": args.compact_evidence,
        "compact_metrics": args.compact_metrics,
        "memory_trim_interval": args.memory_trim_interval,
        "min_system_available_gib": args.min_system_available_gib,
        "sdk_distribution": REQUIRED_SDK_DISTRIBUTION,
        "sdk_version": REQUIRED_SDK_VERSION,
    }


def target_identity(args: argparse.Namespace) -> dict[str, str]:
    return {
        "catalog": args.catalog,
        "schema": args.schema,
        "table": args.table,
        "full_name": f"{args.catalog}.{args.schema}.{args.table}",
        "host": args.host,
        "zerobus_endpoint": args.endpoint,
    }


def compact_metrics_payload(state: Mapping[str, Any]) -> dict[str, Any]:
    """Return the small, stable ingest-progress record used for publication."""
    statuses = [
        value
        for value in state.get("stream_status", {}).values()
        if isinstance(value, Mapping)
    ]
    rotation_counts: list[int] = []
    for value in statuses:
        candidate = value.get("rotation_count", 0)
        if isinstance(candidate, bool):
            continue
        try:
            rotation_counts.append(int(candidate))
        except (TypeError, ValueError, OverflowError):
            continue
    state_counts = Counter(
        str(value.get("state") or "unknown") for value in statuses
    )
    memory = state.get("memory", {})
    host = state.get("host", {})
    limiter = state.get("rate_limiter", {})
    source = state.get("source", {})
    eps = state.get("eps", {})
    return {
        "schema_version": 1,
        "run_id": state.get("run_id"),
        "observed_at": state.get("updated_at"),
        "elapsed_sec": state.get("elapsed_sec"),
        "source_rows": source.get("total_rows"),
        "provider_committed_rows": state.get("provider_committed_rows"),
        "submitted_rows": state.get("submitted_rows"),
        "pending_rows": state.get("pending_rows"),
        "completed_tasks": state.get("completed_tasks"),
        "target_rows_per_sec": eps.get("target_rows_per_sec"),
        "average_committed_rows_per_sec": eps.get(
            "average_provider_committed_rows_per_sec"
        ),
        "session_committed_rows_per_sec": eps.get(
            "session_provider_committed_rows_per_sec"
        ),
        "terminal_error": state.get("terminal_error"),
        "ambiguous": state.get("ambiguous"),
        "error_count": len(state.get("errors", [])),
        "streams": {
            "count": len(statuses),
            "states": dict(state_counts),
            "rotation_count_total": sum(rotation_counts),
            "rotation_count_min": min(rotation_counts) if rotation_counts else 0,
            "rotation_count_max": max(rotation_counts) if rotation_counts else 0,
        },
        "transport_recovery": state.get("transport_recovery", {}),
        "producer": {
            "process_rss_bytes": memory.get("process_rss_bytes"),
            "process_peak_rss_bytes": memory.get("process_peak_rss_bytes"),
            "system_available_bytes": memory.get("system_available_bytes"),
            "arrow_allocated_bytes": memory.get("arrow_allocated_bytes"),
            "cpu_cores": host.get("producer_process_cpu_cores"),
            "host_cpu_utilization_percent": host.get(
                "host_cpu_utilization_percent"
            ),
            "network_receive_bytes_per_second": host.get(
                "network_receive_bytes_per_second"
            ),
            "network_transmit_bytes_per_second": host.get(
                "network_transmit_bytes_per_second"
            ),
        },
        "rate_limiter": {
            "scheduled_rows": limiter.get("scheduled_rows"),
            "next_send_delay_sec": limiter.get("next_send_delay_sec"),
        },
    }


def validate_resume_journal(
    journal: Mapping[str, Any],
    *,
    manifest: Mapping[str, Any],
    target: Mapping[str, Any],
    config: Mapping[str, Any],
    tasks: Sequence[Task],
) -> set[int]:
    reason = (
        "resume is not provably safe; use a fresh target table and start a new run"
    )
    if journal.get("schema_version") != 1:
        raise RuntimeError(f"{reason}: unsupported progress schema")
    if journal.get("running"):
        raise RuntimeError(f"{reason}: prior journal still says running")
    if not journal.get("clean_checkpoint") or not journal.get("safe_to_resume"):
        raise RuntimeError(f"{reason}: prior run did not end at a clean checkpoint")
    if journal.get("terminal_error") or journal.get("ambiguous"):
        raise RuntimeError(f"{reason}: prior run has terminal or ambiguous evidence")
    if int(journal.get("pending_batches", -1)) != 0:
        raise RuntimeError(f"{reason}: prior journal has pending batches")
    source = journal.get("source", {})
    if source.get("manifest_sha256") != manifest.get("manifest_sha256"):
        raise RuntimeError(f"{reason}: source manifest changed")
    if int(source.get("total_rows", -1)) != int(manifest.get("total_rows", -2)):
        raise RuntimeError(f"{reason}: source row count changed")
    if journal.get("target") != dict(target):
        raise RuntimeError(f"{reason}: target identity changed")
    if journal.get("config") != dict(config):
        raise RuntimeError(f"{reason}: frozen producer configuration changed")
    completed_values = journal.get("completed_task_ordinals")
    try:
        completed = (
            {int(value) for value in completed_values}
            if isinstance(completed_values, list)
            else expand_ordinal_ranges(journal.get("completed_task_ranges", []))
        )
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"{reason}: completed task evidence is invalid") from exc
    valid = {task.ordinal for task in tasks}
    if not completed.issubset(valid):
        raise RuntimeError(f"{reason}: completed task list references unknown tasks")
    committed_from_tasks = sum(task.rows for task in tasks if task.ordinal in completed)
    if committed_from_tasks != int(journal.get("provider_committed_rows", -1)):
        raise RuntimeError(
            f"{reason}: committed rows do not equal completed whole tasks"
        )
    if int(journal.get("logical_raw_rows", -1)) < committed_from_tasks:
        raise RuntimeError(f"{reason}: logical raw-row count is inconsistent")
    return completed


class ProgressJournal:
    """Serialize evidence-ledger and progress-journal state transitions."""

    def __init__(
        self,
        *,
        progress_path: Path,
        ledger_path: Path,
        metrics_path: Path,
        run_id: str,
        manifest: Mapping[str, Any],
        target: Mapping[str, Any],
        config: Mapping[str, Any],
        tasks: Sequence[Task],
        baseline_table_rows: int,
        baseline_unknown: bool,
        secrets: Sequence[str],
        previous: Mapping[str, Any] | None = None,
    ):
        self.progress_path = progress_path
        self.ledger_path = ledger_path
        self.metrics_path = metrics_path
        self.run_id = run_id
        self.manifest = dict(manifest)
        self.target = dict(target)
        self.config = dict(config)
        self.tasks = {task.ordinal: task for task in tasks}
        self._secrets = tuple(value for value in secrets if value)
        self._lock = threading.Lock()
        self._started_monotonic = time.monotonic()
        self._previous_elapsed = float(previous.get("elapsed_sec", 0.0)) if previous else 0.0
        self._session_start_rows = (
            int(previous.get("provider_committed_rows", 0)) if previous else 0
        )
        if previous:
            completed_values = previous.get("completed_task_ordinals")
            completed = (
                {int(value) for value in completed_values}
                if isinstance(completed_values, list)
                else expand_ordinal_ranges(
                    previous.get("completed_task_ranges", [])
                )
            )
        else:
            completed = set()
        committed = (
            int(previous.get("provider_committed_rows", 0)) if previous else 0
        )
        submitted = int(previous.get("submitted_rows", committed)) if previous else 0
        submitted_batches = (
            int(previous.get("submitted_batches", 0)) if previous else 0
        )
        durable_batches = int(previous.get("durable_batches", 0)) if previous else 0
        self._completed_tasks = set(completed)
        self._completed_task_ranges = ordinal_ranges(completed)
        completed_evidence = {
            "completed_task_ranges": [list(item) for item in self._completed_task_ranges],
            "completed_tasks": len(completed),
        }
        if not config.get("compact_progress"):
            completed_evidence["completed_task_ordinals"] = sorted(completed)
        self.state: dict[str, Any] = {
            "schema_version": 1,
            "run_id": run_id,
            "started_at": previous.get("started_at", iso_utc()) if previous else iso_utc(),
            "session_started_at": iso_utc(),
            "updated_at": iso_utc(),
            "running": True,
            "finished": False,
            "clean_checkpoint": False,
            "safe_to_resume": False,
            "ambiguous": False,
            "terminal_error": None,
            "errors": [],
            "resume_count": (
                int(previous.get("resume_count", 0)) + 1 if previous else 0
            ),
            "target": self.target,
            "source": {
                "manifest_sha256": manifest["manifest_sha256"],
                "total_rows": manifest["total_rows"],
                "file_count": manifest["selected_file_count"],
                "task_count": manifest["selected_row_group_count"],
            },
            "config": self.config,
            "provider_committed_rows": committed,
            "logical_raw_rows": baseline_table_rows + committed,
            "baseline_table_rows": baseline_table_rows,
            "baseline_table_rows_unknown": baseline_unknown,
            "submitted_rows": submitted,
            "submitted_batches": submitted_batches,
            "durable_batches": durable_batches,
            "durable_uncompressed_arrow_buffer_bytes": int(
                previous.get("durable_uncompressed_arrow_buffer_bytes", 0)
            )
            if previous
            else 0,
            "pending_rows": 0,
            "pending_batches": 0,
            "pending_batch_evidence": [],
            **completed_evidence,
            "partial_task_rows": {},
            "assigned_tasks": int(previous.get("assigned_tasks", 0)) if previous else 0,
            "stream_status": {},
            "transport_recovery": (
                dict(previous.get("transport_recovery", {}))
                if previous
                else {
                    "event_count": 0,
                    "completed_count": 0,
                    "failed_count": 0,
                    "total_duration_sec": 0.0,
                    "max_duration_sec": 0.0,
                    "last_event": None,
                }
            ),
            "eps": {},
            "memory": {},
            "host": {},
        }
        self._pending: dict[str, PendingBatch] = {}
        self._task_durable_rows: dict[int, int] = {}
        with self._lock:
            self._write_locked()
            self._append_ledger_locked(
                {
                    "event": "run_resumed" if previous else "run_started",
                    "at": iso_utc(),
                    "source_manifest_sha256": manifest["manifest_sha256"],
                    "provider_committed_rows": committed,
                }
            )

    def _elapsed_locked(self) -> float:
        return self._previous_elapsed + max(
            0.0, time.monotonic() - self._started_monotonic
        )

    def _refresh_locked(self) -> None:
        partial = {
            str(ordinal): rows
            for ordinal, rows in sorted(self._task_durable_rows.items())
            if rows > 0
        }
        pending_rows = sum(item.rows for item in self._pending.values())
        elapsed = self._elapsed_locked()
        committed = int(self.state["provider_committed_rows"])
        completed_evidence = {
            "completed_task_ranges": [
                list(item) for item in self._completed_task_ranges
            ],
            "completed_tasks": len(self._completed_tasks),
        }
        if not self.config.get("compact_progress"):
            completed_evidence["completed_task_ordinals"] = sorted(
                self._completed_tasks
            )
        self.state.update(
            {
                "updated_at": iso_utc(),
                "elapsed_sec": elapsed,
                "logical_raw_rows": int(self.state["baseline_table_rows"]) + committed,
                "pending_rows": pending_rows,
                "pending_batches": len(self._pending),
                "pending_batch_evidence": [
                    item.evidence()
                    for item in sorted(
                        self._pending.values(),
                        key=lambda pending: (
                            pending.coordinate.task_ordinal,
                            pending.coordinate.batch_ordinal,
                        ),
                    )
                ],
                **completed_evidence,
                "partial_task_rows": partial,
                "eps": {
                    "target_rows_per_sec": self.config["target_rows_per_sec"],
                    "average_provider_committed_rows_per_sec": (
                        committed / elapsed if elapsed else 0.0
                    ),
                    "session_provider_committed_rows_per_sec": (
                        (committed - self._session_start_rows)
                        / max(0.000001, time.monotonic() - self._started_monotonic)
                    ),
                },
            }
        )

    def _write_locked(self) -> None:
        self._refresh_locked()
        atomic_write_json(
            self.progress_path,
            redact_secrets(self.state, self._secrets),
        )

    def _append_ledger_locked(self, event: Mapping[str, Any]) -> None:
        if self.config.get("compact_evidence") and event.get("event") not in {
            "run_started",
            "run_resumed",
            "transport_wait",
            "terminal_error",
            "run_finished",
        }:
            return
        atomic_append_jsonl(
            self.ledger_path,
            redact_secrets({"run_id": self.run_id, **dict(event)}, self._secrets),
        )

    def record_assignment(self, task: Task, worker: int, stream: str) -> None:
        with self._lock:
            self.state["assigned_tasks"] += 1
            self._append_ledger_locked(
                {
                    "event": "task_assigned",
                    "at": iso_utc(),
                    **task.evidence(),
                    "worker": worker,
                    "stream": stream,
                }
            )
            self._write_locked()

    def record_submitted(self, pending: PendingBatch) -> None:
        with self._lock:
            if pending.key in self._pending:
                raise RuntimeError(f"duplicate pending batch coordinate: {pending.key}")
            self._append_ledger_locked(
                {"event": "batch_submitted", **pending.evidence()}
            )
            self._pending[pending.key] = pending
            self.state["submitted_rows"] += pending.rows
            self.state["submitted_batches"] += 1
            self._write_locked()

    def record_durable(self, pending_batches: Sequence[PendingBatch]) -> None:
        if not pending_batches:
            return
        durable_at = iso_utc()
        with self._lock:
            projected_task_rows = dict(self._task_durable_rows)
            for pending in pending_batches:
                if pending.key not in self._pending:
                    raise RuntimeError(
                        f"durability attempted for unknown pending batch: {pending.key}"
                    )
                ordinal = pending.coordinate.task_ordinal
                if ordinal in self._completed_tasks:
                    raise RuntimeError(
                        f"durability attempted for completed task: {ordinal}"
                    )
                new_rows = projected_task_rows.get(ordinal, 0) + pending.rows
                if new_rows > self.tasks[ordinal].rows:
                    raise RuntimeError(
                        f"durable rows exceed task {ordinal} source row count"
                    )
                projected_task_rows[ordinal] = new_rows
            for pending in pending_batches:
                self._append_ledger_locked(
                    {
                        "event": "batch_durable",
                        **pending.evidence(),
                        "durable_at": durable_at,
                    }
                )
            for pending in pending_batches:
                self._pending.pop(pending.key)
                self.state["provider_committed_rows"] += pending.rows
                self.state["durable_uncompressed_arrow_buffer_bytes"] += (
                    pending.uncompressed_arrow_buffer_bytes
                )
                self.state["durable_batches"] += 1
            for ordinal, rows in list(projected_task_rows.items()):
                if rows == self.tasks[ordinal].rows:
                    self._completed_tasks.add(ordinal)
                    self._completed_task_ranges = add_ordinal_range(
                        self._completed_task_ranges, ordinal
                    )
                    projected_task_rows.pop(ordinal)
            self._task_durable_rows = projected_task_rows
            self._write_locked()

    def stream_update(self, stream_label: str, **values: Any) -> None:
        with self._lock:
            current = dict(self.state["stream_status"].get(stream_label, {}))
            current.update(values)
            self.state["stream_status"][stream_label] = current
            self._write_locked()

    def record_stream_rotation(
        self,
        stream_label: str,
        *,
        generation: int,
        unacked_batches: int,
    ) -> None:
        with self._lock:
            current = dict(self.state["stream_status"].get(stream_label, {}))
            current.update(
                {
                    "generation": generation,
                    "rotation_count": generation - 1,
                    "last_rotation_at": iso_utc(),
                    "last_rotation_unacked_batches": unacked_batches,
                }
            )
            self.state["stream_status"][stream_label] = current
            self._append_ledger_locked(
                {
                    "event": "stream_rotated",
                    "at": current["last_rotation_at"],
                    "stream": stream_label,
                    "generation": generation,
                    "unacked_batches": unacked_batches,
                }
            )
            self._write_locked()

    def record_transport_wait(
        self,
        stream_label: str,
        *,
        generation: int,
        operation: str,
        duration_sec: float,
        status: str,
        pending_batches: int,
        pending_rows: int,
        error: str | None = None,
    ) -> None:
        if status not in {"completed", "failed"}:
            raise ValueError(f"unsupported transport wait status: {status}")
        safe_error = (
            str(redact_secrets(error, self._secrets))
            if error not in (None, "")
            else None
        )
        event = {
            "event": "transport_wait",
            "at": iso_utc(),
            "stream": stream_label,
            "generation": generation,
            "operation": operation,
            "duration_sec": duration_sec,
            "status": status,
            "pending_batches": pending_batches,
            "pending_rows": pending_rows,
            "error": safe_error,
        }
        with self._lock:
            recovery = dict(self.state.get("transport_recovery", {}))
            recovery["event_count"] = int(recovery.get("event_count", 0)) + 1
            counter = f"{status}_count"
            recovery[counter] = int(recovery.get(counter, 0)) + 1
            recovery["total_duration_sec"] = float(
                recovery.get("total_duration_sec", 0.0)
            ) + duration_sec
            recovery["max_duration_sec"] = max(
                float(recovery.get("max_duration_sec", 0.0)),
                duration_sec,
            )
            recovery["last_event"] = {
                key: value for key, value in event.items() if key != "event"
            }
            self.state["transport_recovery"] = recovery
            self._append_ledger_locked(event)
            self._write_locked()

    def terminal_error(self, message: str, *, ambiguous: bool = True) -> None:
        safe = str(redact_secrets(message, self._secrets))
        with self._lock:
            if self.state["terminal_error"] is None:
                self.state["terminal_error"] = safe
            if len(self.state["errors"]) < 50:
                self.state["errors"].append(safe)
            self.state["ambiguous"] = bool(self.state["ambiguous"] or ambiguous)
            self._append_ledger_locked(
                {
                    "event": "terminal_error",
                    "at": iso_utc(),
                    "error": safe,
                    "ambiguous": ambiguous,
                }
            )
            self._write_locked()

    def metrics(
        self,
        *,
        memory: Mapping[str, Any],
        limiter: Mapping[str, Any],
        host: Mapping[str, Any] | None = None,
        record: bool = True,
    ) -> dict[str, Any]:
        with self._lock:
            self.state["memory"] = dict(memory)
            self.state["rate_limiter"] = dict(limiter)
            if host is not None:
                self.state["host"] = dict(host)
            self._write_locked()
            payload = json.loads(json.dumps(self.state))
            if record:
                output = (
                    compact_metrics_payload(payload)
                    if self.config.get("compact_metrics")
                    else payload
                )
                atomic_append_jsonl(self.metrics_path, output)
            return payload

    def finish(self, *, stopped_early: bool) -> dict[str, Any]:
        with self._lock:
            stream_statuses = list(self.state["stream_status"].values())
            streams_clean = (
                len(stream_statuses) == int(self.config["workers"])
                and all(
                    status.get("close_succeeded")
                    and status.get("unacked_inspection_succeeded")
                    and int(status.get("unacked_batches", -1)) == 0
                    for status in stream_statuses
                )
            )
            no_partial_tasks = not self._task_durable_rows
            clean = (
                self.state["terminal_error"] is None
                and not self.state["ambiguous"]
                and not self._pending
                and no_partial_tasks
                and streams_clean
            )
            resume_safe = clean and not self.state["baseline_table_rows_unknown"]
            all_tasks = len(self._completed_tasks) == len(self.tasks)
            expected_rows = int(self.manifest["total_rows"])
            finished = (
                clean
                and all_tasks
                and int(self.state["provider_committed_rows"]) == expected_rows
            )
            self.state.update(
                {
                    "running": False,
                    "finished": finished,
                    "stopped_early": stopped_early or not finished,
                    "clean_checkpoint": clean,
                    "safe_to_resume": resume_safe,
                    "finished_at": iso_utc(),
                }
            )
            self._append_ledger_locked(
                {
                    "event": "run_finished",
                    "at": self.state["finished_at"],
                    "finished": finished,
                    "clean_checkpoint": clean,
                    "safe_to_resume": resume_safe,
                    "provider_committed_rows": self.state[
                        "provider_committed_rows"
                    ],
                }
            )
            self._write_locked()
            return json.loads(json.dumps(self.state))

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            self._refresh_locked()
            return json.loads(json.dumps(self.state))


def load_runtime() -> RuntimeModules:
    if sys.version_info < (3, 10):
        raise RuntimeError("ingest_zerobus.py requires Python 3.10 or newer")
    try:
        sdk_version = importlib.metadata.version(REQUIRED_SDK_DISTRIBUTION)
    except importlib.metadata.PackageNotFoundError as exc:
        raise RuntimeError(
            f"{REQUIRED_SDK_DISTRIBUTION}[arrow]=={REQUIRED_SDK_VERSION} is required"
        ) from exc
    if sdk_version != REQUIRED_SDK_VERSION:
        raise RuntimeError(
            f"{REQUIRED_SDK_DISTRIBUTION} {sdk_version} is installed; "
            f"exactly {REQUIRED_SDK_VERSION} is required"
        )
    try:
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.parquet as pq
        from zerobus.sdk.shared.arrow import (
            ArrowStreamConfigurationOptions,
            IPCCompression,
        )
        from zerobus.sdk.sync import ZerobusSdk
    except ImportError as exc:
        raise RuntimeError(
            "Arrow dependencies are incomplete; install requirements-zerobus.txt"
        ) from exc
    create_signature = inspect.signature(ZerobusSdk.create_arrow_stream)
    expected_parameters = [
        "self",
        "table_name",
        "schema",
        "client_id",
        "client_secret",
        "options",
    ]
    if list(create_signature.parameters) != expected_parameters:
        raise RuntimeError(
            "ZerobusSdk.create_arrow_stream API does not match the pinned v1.8.0 contract"
        )
    for method in (
        "ingest_batch",
        "wait_for_offset",
        "flush",
        "close",
        "get_unacked_batches",
    ):
        stream_type = getattr(
            sys.modules.get("zerobus.sdk.sync"),
            "ZerobusArrowStream",
            None,
        )
        if stream_type is None or not callable(getattr(stream_type, method, None)):
            raise RuntimeError(f"Zerobus Arrow stream API omits {method}()")
    for name in ("NONE", "LZ4_FRAME", "ZSTD"):
        map_ipc_compression(name, IPCCompression)
    try:
        pyarrow_version = importlib.metadata.version("pyarrow")
    except importlib.metadata.PackageNotFoundError:
        pyarrow_version = str(getattr(pa, "__version__", "unknown"))
    return RuntimeModules(
        pa=pa,
        pc=pc,
        pq=pq,
        sdk_class=ZerobusSdk,
        options_class=ArrowStreamConfigurationOptions,
        compression_enum=IPCCompression,
        sdk_version=sdk_version,
        pyarrow_version=pyarrow_version,
    )


def validate_cli(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    positive = {
        "--workers": args.workers,
        "--batch-size": args.batch_size,
        "--queue-capacity": args.queue_capacity,
        "--checkpoint-batches": args.checkpoint_batches,
        "--metrics-interval": args.metrics_interval,
        "--metrics-output-interval": args.metrics_output_interval,
        "--statement-timeout": args.statement_timeout,
    }
    for name, value in positive.items():
        if value <= 0:
            parser.error(f"{name} must be greater than zero")
    if args.target_eps < 0:
        parser.error("--target-eps must be non-negative")
    if args.checkpoint_seconds < 0 or args.memory_trim_interval < 0:
        parser.error(
            "--checkpoint-seconds and --memory-trim-interval must be non-negative"
        )
    if args.manual_stream_rotation_seconds < 0:
        parser.error("--manual-stream-rotation-seconds must be non-negative")
    if args.recovery_timeout_ms <= 0:
        parser.error("--recovery-timeout-ms must be greater than zero")
    if args.recovery_backoff_ms < 0 or args.recovery_retries < 0:
        parser.error(
            "--recovery-backoff-ms and --recovery-retries must be non-negative"
        )
    if args.recovery_log_threshold_seconds < 0:
        parser.error("--recovery-log-threshold-seconds must be non-negative")
    if args.min_system_available_gib < 0:
        parser.error("--min-system-available-gib must be non-negative")
    if args.expected_rows <= 0:
        parser.error("--expected-rows must be greater than zero")
    if args.max_files is not None and args.max_files <= 0:
        parser.error("--max-files must be greater than zero")
    if args.max_row_groups is not None and args.max_row_groups <= 0:
        parser.error("--max-row-groups must be greater than zero")
    if args.max_rows is not None and args.max_rows <= 0:
        parser.error("--max-rows must be greater than zero")
    if not args.client_id or not args.client_secret:
        parser.error(
            "DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET are required"
        )
    if not args.host:
        parser.error("--host or DATABRICKS_HOST is required")
    if not args.endpoint:
        parser.error("--endpoint or ZEROBUS_ENDPOINT is required")
    host = urllib.parse.urlparse(args.host)
    if (
        host.scheme != "https"
        or not host.hostname
        or host.username
        or host.password
        or host.query
        or host.fragment
        or host.path not in ("", "/")
    ):
        parser.error("--host must be a credential-free HTTPS origin")
    endpoint_errors = zerobus_endpoint_errors(args.endpoint)
    if endpoint_errors:
        parser.error("; ".join(endpoint_errors))
    for label, value in (
        ("catalog", args.catalog),
        ("schema", args.schema),
        ("table", args.table),
    ):
        if not value or any(character in value for character in ".\x00"):
            parser.error(f"--{label} must be one non-empty identifier component")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest canonical Parquet row groups with Zerobus Arrow Flight"
    )
    parser.add_argument("--dir", type=Path, required=True)
    parser.add_argument("--pattern", default="*.parquet")
    parser.add_argument("--include-quotes-0", action="store_true")
    parser.add_argument("--max-files", type=int)
    parser.add_argument("--max-row-groups", type=int)
    parser.add_argument(
        "--max-rows",
        type=int,
        help="cap selected source rows exactly, truncating only the final row group",
    )
    parser.add_argument("--expected-rows", type=int, default=EXPECTED_FULL_ROWS)
    parser.add_argument("--allow-partial", action="store_true")
    parser.add_argument(
        "--defer-source-hashes",
        action="store_true",
        help="record source file identity now and calculate hashes after measurement",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("DATABRICKS_HOST")
        or os.environ.get("DATABRICKS_WORKSPACE_URL"),
    )
    parser.add_argument(
        "--endpoint",
        default=os.environ.get("ZEROBUS_ENDPOINT")
        or os.environ.get("ZEROBUS_SERVER_ENDPOINT")
        or os.environ.get("DATABRICKS_ZEROBUS_ENDPOINT"),
    )
    parser.add_argument("--catalog", default=os.environ.get("DATABRICKS_CATALOG", "workspace"))
    parser.add_argument("--schema", default=os.environ.get("DATABRICKS_SCHEMA", "benchmarking"))
    parser.add_argument("--table", default=os.environ.get("DATABRICKS_RAW_TABLE", "quotes"))
    parser.add_argument(
        "--count-warehouse",
        default=os.environ.get("DATABRICKS_CONTROL_WAREHOUSE_ID")
        or os.environ.get("DATABRICKS_RT_WAREHOUSE_ID"),
    )
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--queue-capacity", type=int, default=64)
    parser.add_argument("--target-eps", type=float, default=1_000_000.0)
    parser.add_argument(
        "--compression",
        type=lambda value: value.strip().upper().replace("-", "_"),
        choices=("NONE", "LZ4_FRAME", "ZSTD"),
        default="NONE",
    )
    parser.add_argument("--checkpoint-batches", type=int, default=32)
    parser.add_argument("--checkpoint-seconds", type=float, default=5.0)
    parser.add_argument(
        "--checkpoint-method", choices=("wait", "flush"), default="wait"
    )
    parser.add_argument("--disable-automatic-recovery", action="store_true")
    parser.add_argument("--recovery-timeout-ms", type=int, default=15_000)
    parser.add_argument("--recovery-backoff-ms", type=int, default=2_000)
    parser.add_argument("--recovery-retries", type=int, default=4)
    parser.add_argument(
        "--recovery-log-threshold-seconds",
        type=float,
        default=2.0,
        help=(
            "persist successful acknowledgment waits at or above this duration; "
            "failed waits are always persisted"
        ),
    )
    parser.add_argument(
        "--manual-stream-rotation-seconds",
        type=float,
        default=0.0,
        help="proactively flush, close, inspect, and replace each stream",
    )
    parser.add_argument("--metrics-interval", type=float, default=5.0)
    parser.add_argument(
        "--metrics-output-interval",
        type=float,
        help=(
            "seconds between persisted metrics records; monitoring and safety "
            "checks continue at --metrics-interval"
        ),
    )
    parser.add_argument(
        "--compact-metrics",
        action="store_true",
        help="persist only benchmark ingest-progress fields",
    )
    parser.add_argument(
        "--compact-evidence",
        action="store_true",
        help="persist only run lifecycle and terminal-error ledger events",
    )
    parser.add_argument("--memory-trim-interval", type=float, default=60.0)
    parser.add_argument("--min-system-available-gib", type=float, default=0.0)
    parser.add_argument(
        "--compact-progress",
        action="store_true",
        help="store completed task ranges instead of a growing ordinal list",
    )
    parser.add_argument("--statement-timeout", type=float, default=300.0)
    parser.add_argument("--allow-nonempty-table", action="store_true")
    parser.add_argument("--quiet-worker-logs", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=Path("results/ingest_zerobus"))
    parser.add_argument("--manifest-output", type=Path)
    parser.add_argument("--progress-file", type=Path)
    parser.add_argument("--ledger-file", type=Path)
    parser.add_argument("--metrics-file", type=Path)
    parser.add_argument("--summary-file", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args(argv)
    args.client_id = os.environ.get("DATABRICKS_CLIENT_ID", "").strip()
    args.client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "").strip()
    args.token = os.environ.get("DATABRICKS_TOKEN", "").strip()
    args.host = args.host.rstrip("/") if args.host else args.host
    args.endpoint = args.endpoint.rstrip("/") if args.endpoint else args.endpoint
    if args.metrics_output_interval is None:
        args.metrics_output_interval = args.metrics_interval
    validate_cli(args, parser)
    return args


def _extract_single_value(response: Mapping[str, Any], column: str) -> Any:
    rows = statement_rows(response)
    if len(rows) != 1:
        raise RuntimeError(f"{column} metadata returned an unexpected result shape")
    normalized = {str(key).lower(): value for key, value in rows[0].items()}
    if column.lower() not in normalized:
        raise RuntimeError(f"{column} metadata column is missing")
    return normalized[column.lower()]


def protect_target_table(
    args: argparse.Namespace,
    *,
    resume_journal: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Prove a fresh target from metadata without scanning the data table."""
    report: dict[str, Any] = {
        "attempted": False,
        "available": False,
        "allow_nonempty_table": args.allow_nonempty_table,
        "warehouse_id": args.count_warehouse,
    }
    credentials_available = bool(args.token) or bool(
        args.client_id and args.client_secret
    )
    if credentials_available and args.count_warehouse:
        report["attempted"] = True
        try:
            client = DatabricksRestClient(
                args.host,
                token=args.token or None,
                client_id=args.client_id or None,
                client_secret=args.client_secret or None,
                timeout=min(args.statement_timeout, 60.0),
            )
            target = ".".join(
                quote_identifier(value)
                for value in (args.catalog, args.schema, args.table)
            )
            if resume_journal is not None:
                response = client.execute_statement(
                    "SELECT COALESCE(SUM(committed_records), 0) AS row_count\n"
                    "FROM system.lakeflow.zerobus_ingest\n"
                    f"WHERE table_name = {sql_string('.'.join((args.catalog, args.schema, args.table)))}",
                    args.count_warehouse,
                    timeout=args.statement_timeout,
                )
                report.update(
                    {
                        "available": True,
                        "method": "system.lakeflow.zerobus_ingest",
                        "row_count": int(_extract_single_value(response, "row_count")),
                        "statement_ids": [response.get("statement_id")],
                    }
                )
            else:
                detail_response = client.execute_statement(
                    f"DESCRIBE DETAIL {target}",
                    args.count_warehouse,
                    timeout=args.statement_timeout,
                    catalog=args.catalog,
                    schema=args.schema,
                )
                history_response = client.execute_statement(
                    f"DESCRIBE HISTORY {target} LIMIT 2",
                    args.count_warehouse,
                    timeout=args.statement_timeout,
                    catalog=args.catalog,
                    schema=args.schema,
                )
                detail_rows = statement_rows(detail_response)
                history_rows = statement_rows(history_response)
                if len(detail_rows) != 1:
                    raise RuntimeError(
                        "DESCRIBE DETAIL returned an unexpected result shape"
                    )
                detail = {
                    str(key).lower(): value for key, value in detail_rows[0].items()
                }
                num_files = int(detail["numfiles"])
                size_bytes = int(detail["sizeinbytes"])
                versions = [
                    int(
                        {
                            str(key).lower(): value for key, value in row.items()
                        }["version"]
                    )
                    for row in history_rows
                ]
                fresh = (
                    num_files == 0
                    and size_bytes == 0
                    and versions == [0]
                )
                report.update(
                    {
                        "available": True,
                        "method": "delta_metadata",
                        "fresh": fresh,
                        "num_files": num_files,
                        "size_in_bytes": size_bytes,
                        "history_versions": versions,
                        "row_count": 0 if fresh else None,
                        "statement_ids": [
                            detail_response.get("statement_id"),
                            history_response.get("statement_id"),
                        ],
                    }
                )
        except Exception as exc:
            report["error"] = f"{type(exc).__name__}: {exc}"
    else:
        report["unavailable_reason"] = (
            "workspace OAuth credentials and --count-warehouse are required for "
            "the metadata-only Statement API protection check"
        )
    if resume_journal is not None:
        if report["available"]:
            expected = int(resume_journal["logical_raw_rows"])
            if report["row_count"] != expected:
                raise RuntimeError(
                    f"resume target has {report['row_count']:,} rows, but the clean "
                    f"journal proves {expected:,}; use a fresh target table"
                )
        elif not args.allow_nonempty_table:
            detail = report.get("error") or report.get("unavailable_reason")
            raise RuntimeError(
                "could not verify the resume target from Zerobus metadata; "
                "configure workspace OAuth and a control warehouse, or explicitly "
                "pass --allow-nonempty-table "
                f"({detail})"
            )
        return redact_secrets(report, (args.token, args.client_secret))
    if report["available"]:
        if report.get("fresh") is not True and not args.allow_nonempty_table:
            raise RuntimeError(
                f"{args.catalog}.{args.schema}.{args.table} is not a pristine "
                "version-0 Delta table with zero active files; use a fresh target"
            )
    elif not args.allow_nonempty_table:
        detail = report.get("error") or report.get("unavailable_reason")
        raise RuntimeError(
            "could not prove the target table is pristine from Delta metadata; "
            "configure workspace OAuth and a control warehouse, or explicitly "
            f"pass --allow-nonempty-table ({detail})"
        )
    return redact_secrets(report, (args.token, args.client_secret))


def _proc_value_bytes(path: str, key: str) -> int | None:
    try:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                if line.startswith(f"{key}:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        multiplier = (
                            1024
                            if len(parts) < 3 or parts[2].lower() == "kb"
                            else 1
                        )
                        return int(parts[1]) * multiplier
    except (FileNotFoundError, OSError, ValueError):
        pass
    return None


def memory_snapshot(pa: Any) -> dict[str, int | None]:
    process_rss = _proc_value_bytes("/proc/self/status", "VmRSS")
    process_peak_rss = _proc_value_bytes("/proc/self/status", "VmHWM")
    if process_peak_rss is None:
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        process_peak_rss = int(peak if sys.platform == "darwin" else peak * 1024)
    pool = pa.default_memory_pool()
    return {
        "process_rss_bytes": process_rss,
        "process_peak_rss_bytes": process_peak_rss,
        "system_available_bytes": _proc_value_bytes(
            "/proc/meminfo", "MemAvailable"
        ),
        "system_total_bytes": _proc_value_bytes("/proc/meminfo", "MemTotal"),
        "arrow_allocated_bytes": int(pool.bytes_allocated()),
        "arrow_peak_bytes": int(pool.max_memory()),
    }


def _proc_cpu_ticks(path: str = "/proc/stat") -> tuple[int, int] | None:
    """Return Linux host (total, idle) CPU ticks."""
    try:
        with open(path, encoding="utf-8") as handle:
            parts = handle.readline().split()
        if not parts or parts[0] != "cpu":
            return None
        values = [int(value) for value in parts[1:]]
        if len(values) < 4:
            return None
        idle = values[3] + (values[4] if len(values) > 4 else 0)
        return sum(values), idle
    except (FileNotFoundError, OSError, ValueError):
        return None


def _proc_network_bytes(path: str = "/proc/net/dev") -> tuple[int, int] | None:
    """Return Linux host receive/transmit bytes, excluding loopback."""
    received = transmitted = 0
    interfaces = 0
    try:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                if ":" not in line:
                    continue
                name, values = line.split(":", 1)
                if name.strip() == "lo":
                    continue
                fields = values.split()
                if len(fields) < 9:
                    continue
                received += int(fields[0])
                transmitted += int(fields[8])
                interfaces += 1
    except (FileNotFoundError, OSError, ValueError):
        return None
    return (received, transmitted) if interfaces else None


class HostResourceSampler:
    """Produce interval CPU and network evidence without third-party agents."""

    def __init__(self) -> None:
        self._wall = time.monotonic()
        usage = resource.getrusage(resource.RUSAGE_SELF)
        self._process_cpu = float(usage.ru_utime + usage.ru_stime)
        self._host_cpu = _proc_cpu_ticks()
        self._network = _proc_network_bytes()

    def sample(self) -> dict[str, int | float | None]:
        wall = time.monotonic()
        usage = resource.getrusage(resource.RUSAGE_SELF)
        process_cpu = float(usage.ru_utime + usage.ru_stime)
        host_cpu = _proc_cpu_ticks()
        network = _proc_network_bytes()
        elapsed = max(0.000001, wall - self._wall)
        logical_cpus = os.cpu_count()
        process_cores = max(0.0, process_cpu - self._process_cpu) / elapsed

        host_cpu_percent = None
        if self._host_cpu is not None and host_cpu is not None:
            total_delta = host_cpu[0] - self._host_cpu[0]
            idle_delta = host_cpu[1] - self._host_cpu[1]
            if total_delta > 0:
                host_cpu_percent = 100.0 * max(
                    0.0, min(1.0, 1.0 - idle_delta / total_delta)
                )

        receive_rate = transmit_rate = None
        if self._network is not None and network is not None:
            receive_rate = max(0, network[0] - self._network[0]) / elapsed
            transmit_rate = max(0, network[1] - self._network[1]) / elapsed

        self._wall = wall
        self._process_cpu = process_cpu
        self._host_cpu = host_cpu
        self._network = network
        return {
            "sample_interval_seconds": elapsed,
            "logical_cpu_count": logical_cpus,
            "producer_process_cpu_cores": process_cores,
            "producer_process_cpu_percent_of_host": (
                100.0 * process_cores / logical_cpus if logical_cpus else None
            ),
            "host_cpu_utilization_percent": host_cpu_percent,
            "network_receive_bytes_per_second": receive_rate,
            "network_transmit_bytes_per_second": transmit_rate,
        }


def trim_process_memory(pa: Any, telemetry: MemoryTelemetry) -> dict[str, Any]:
    started = time.monotonic()
    before = memory_snapshot(pa)
    collected = gc.collect()
    supported = False
    success = False
    if sys.platform.startswith("linux"):
        try:
            malloc_trim = ctypes.CDLL("libc.so.6").malloc_trim
            malloc_trim.argtypes = [ctypes.c_size_t]
            malloc_trim.restype = ctypes.c_int
            supported = True
            success = bool(malloc_trim(0))
        except (AttributeError, OSError):
            pass
    after = memory_snapshot(pa)
    before_rss = before["process_rss_bytes"]
    after_rss = after["process_rss_bytes"]
    reclaimed = (
        max(0, before_rss - after_rss)
        if before_rss is not None and after_rss is not None
        else 0
    )
    telemetry.trim_attempts += 1
    telemetry.trim_successes += int(success)
    telemetry.trim_unsupported += int(not supported)
    telemetry.gc_collected_objects += collected
    telemetry.trim_reclaimed_rss_bytes += reclaimed
    telemetry.last_trim_duration_sec = time.monotonic() - started
    telemetry.last_trim_at = iso_utc()
    return {**after, **telemetry.as_dict()}


def persist_unacked_batches(
    batches: Iterable[Any],
    *,
    output_dir: Path,
    stream_label: str,
    pa: Any,
) -> dict[str, Any]:
    artifact_dir = output_dir / "unacked" / stream_label
    artifacts: list[dict[str, Any]] = []
    for index, batch in enumerate(batches):
        artifact_dir.mkdir(parents=True, exist_ok=True)
        path = artifact_dir / f"batch_{index:06d}.arrow"
        sink = pa.OSFile(str(path), "wb")
        try:
            with pa.ipc.new_stream(sink, batch.schema) as writer:
                writer.write_batch(batch)
        finally:
            sink.close()
        with path.open("rb") as handle:
            os.fsync(handle.fileno())
        artifacts.append(
            {
                "index": index,
                "path": str(path),
                "rows": int(batch.num_rows),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    return {
        "inspected_at": iso_utc(),
        "unacked_batches": len(artifacts),
        "unacked_rows": sum(item["rows"] for item in artifacts),
        "artifacts": artifacts,
    }


def checkpoint_pending(
    stream: Any,
    pending: list[PendingBatch],
    journal: ProgressJournal,
    method: str,
    *,
    stream_label: str,
    generation: int,
    log_threshold_seconds: float,
) -> None:
    if not pending:
        return
    started = time.monotonic()
    pending_rows = sum(item.rows for item in pending)
    try:
        if method == "wait":
            stream.wait_for_offset(pending[-1].logical_offset)
        elif method == "flush":
            stream.flush()
        else:
            raise ValueError(f"unknown checkpoint method: {method}")
    except Exception as exc:
        duration_sec = time.monotonic() - started
        journal.record_transport_wait(
            stream_label,
            generation=generation,
            operation=method,
            duration_sec=duration_sec,
            status="failed",
            pending_batches=len(pending),
            pending_rows=pending_rows,
            error=f"{type(exc).__name__}: {exc}",
        )
        print(
            "ZEROBUS TRANSPORT FAILURE "
            f"stream={stream_label} generation={generation} "
            f"operation={method} "
            f"duration_sec={duration_sec:.3f} "
            f"pending_batches={len(pending)} pending_rows={pending_rows:,} "
            f"error={type(exc).__name__}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        raise
    duration_sec = time.monotonic() - started
    if duration_sec >= log_threshold_seconds:
        journal.record_transport_wait(
            stream_label,
            generation=generation,
            operation=method,
            duration_sec=duration_sec,
            status="completed",
            pending_batches=len(pending),
            pending_rows=pending_rows,
        )
        print(
            "ZEROBUS TRANSPORT RECOVERED "
            f"stream={stream_label} generation={generation} "
            f"operation={method} "
            f"duration_sec={duration_sec:.3f} "
            f"pending_batches={len(pending)} pending_rows={pending_rows:,}",
            flush=True,
        )
    durable = list(pending)
    journal.record_durable(durable)
    pending.clear()


def worker(
    worker_id: int,
    task_queue: queue.Queue[Task],
    producer_done: threading.Event,
    stop_requested: threading.Event,
    abort_event: threading.Event,
    runtime: RuntimeModules,
    schema: Any,
    journal: ProgressJournal,
    limiter: RateLimiter,
    args: argparse.Namespace,
) -> None:
    stream_label = f"worker-{worker_id:02d}-arrow"
    stream: Any | None = None
    pending: list[PendingBatch] = []
    close_succeeded = False
    last_checkpoint = time.monotonic()
    generation = 1
    rotation_count = 0
    rotation_deadline: float | None = None
    current_file: Path | None = None
    parquet: Any | None = None
    journal.stream_update(
        stream_label,
        worker=worker_id,
        stream=stream_label,
        state="opening",
        close_succeeded=False,
        unacked_inspection_succeeded=False,
    )
    try:
        sdk = runtime.sdk_class(
            args.endpoint,
            args.host,
            application_name="costbench-databricks/1.0",
        )
        compression = map_ipc_compression(
            args.compression, runtime.compression_enum
        )

        def open_stream() -> Any:
            options = runtime.options_class(
                ipc_compression=compression,
                recovery=not args.disable_automatic_recovery,
                recovery_timeout_ms=args.recovery_timeout_ms,
                recovery_backoff_ms=args.recovery_backoff_ms,
                recovery_retries=args.recovery_retries,
            )
            return sdk.create_arrow_stream(
                f"{args.catalog}.{args.schema}.{args.table}",
                schema,
                args.client_id,
                args.client_secret,
                options=options,
            )

        stream = open_stream()
        if args.manual_stream_rotation_seconds > 0:
            denominator = max(1, args.workers - 1)
            stagger = (
                args.manual_stream_rotation_seconds
                * 0.05
                * (worker_id - 1)
                / denominator
            )
            rotation_deadline = (
                time.monotonic()
                + args.manual_stream_rotation_seconds
                + stagger
            )
        journal.stream_update(
            stream_label,
            state="open",
            opened_at=iso_utc(),
            generation=generation,
            rotation_count=rotation_count,
            automatic_recovery=not args.disable_automatic_recovery,
            manual_stream_rotation_seconds=args.manual_stream_rotation_seconds,
        )
        while not abort_event.is_set():
            if stop_requested.is_set():
                break
            try:
                task = task_queue.get(timeout=0.2)
            except queue.Empty:
                if producer_done.is_set():
                    break
                continue
            journal.record_assignment(task, worker_id, stream_label)
            task_started = time.monotonic()
            task_rows = 0
            task_bytes = 0
            try:
                if task.file_path != current_file:
                    parquet = runtime.pq.ParquetFile(task.file_path)
                    current_file = task.file_path
                if parquet is None:
                    raise AssertionError("Parquet reader was not initialized")
                for batch_ordinal, source_batch in enumerate(
                    parquet.iter_batches(
                        batch_size=args.batch_size,
                        row_groups=[task.row_group],
                        columns=list(TARGET_COLUMNS),
                    )
                ):
                    if abort_event.is_set():
                        raise RuntimeError("aborted after another worker failed")
                    if (
                        rotation_deadline is not None
                        and time.monotonic() >= rotation_deadline
                    ):
                        checkpoint_pending(
                            stream,
                            pending,
                            journal,
                            args.checkpoint_method,
                            stream_label=stream_label,
                            generation=generation,
                            log_threshold_seconds=(
                                args.recovery_log_threshold_seconds
                            ),
                        )
                        stream.flush()
                        stream.close()
                        rotation_label = (
                            f"{stream_label}-generation-{generation:04d}"
                        )
                        rotation_summary = persist_unacked_batches(
                            stream.get_unacked_batches(),
                            output_dir=args.output_dir,
                            stream_label=rotation_label,
                            pa=runtime.pa,
                        )
                        if rotation_summary["unacked_batches"]:
                            raise RuntimeError(
                                f"{stream_label} proactive rotation closed with "
                                f"{rotation_summary['unacked_batches']} "
                                "unacknowledged Arrow batches"
                            )
                        stream = None
                        generation += 1
                        rotation_count += 1
                        journal.record_stream_rotation(
                            stream_label,
                            generation=generation,
                            unacked_batches=0,
                        )
                        stream = open_stream()
                        journal.stream_update(
                            stream_label,
                            state="open",
                            opened_at=iso_utc(),
                            generation=generation,
                            rotation_count=rotation_count,
                        )
                        last_checkpoint = time.monotonic()
                        rotation_deadline = (
                            time.monotonic()
                            + args.manual_stream_rotation_seconds
                        )
                    remaining = task.rows - task_rows
                    if remaining <= 0:
                        break
                    if int(source_batch.num_rows) > remaining:
                        source_batch = source_batch.slice(0, remaining)
                    normalized = normalize_batch(
                        source_batch,
                        pa_module=runtime.pa,
                        pc_module=runtime.pc,
                    )
                    rows = int(normalized.num_rows)
                    coordinate = BatchCoordinate.from_task(
                        task,
                        batch_ordinal=batch_ordinal,
                        row_offset=task_rows,
                        row_count=rows,
                    )
                    if not limiter.wait(rows, abort_event):
                        raise RuntimeError("aborted while waiting for the rate limiter")
                    # This is a cheap in-memory size estimate, not encoded wire
                    # bytes. Serializing here would duplicate the SDK's Arrow
                    # IPC encoding on the hot path. Provider-authoritative
                    # committed bytes are collected from the Zerobus system
                    # table after the run.
                    buffer_bytes = int(normalized.nbytes)
                    ingest_started = time.monotonic()
                    try:
                        offset = stream.ingest_batch(normalized)
                    except Exception as exc:
                        duration_sec = time.monotonic() - ingest_started
                        journal.record_transport_wait(
                            stream_label,
                            generation=generation,
                            operation="ingest_batch",
                            duration_sec=duration_sec,
                            status="failed",
                            pending_batches=len(pending),
                            pending_rows=sum(item.rows for item in pending),
                            error=f"{type(exc).__name__}: {exc}",
                        )
                        print(
                            "ZEROBUS TRANSPORT FAILURE "
                            f"stream={stream_label} generation={generation} "
                            "operation=ingest_batch "
                            f"duration_sec={duration_sec:.3f} "
                            f"error={type(exc).__name__}: {exc}",
                            file=sys.stderr,
                            flush=True,
                        )
                        raise
                    ingest_duration_sec = time.monotonic() - ingest_started
                    if (
                        ingest_duration_sec
                        >= args.recovery_log_threshold_seconds
                    ):
                        journal.record_transport_wait(
                            stream_label,
                            generation=generation,
                            operation="ingest_batch",
                            duration_sec=ingest_duration_sec,
                            status="completed",
                            pending_batches=len(pending),
                            pending_rows=sum(item.rows for item in pending),
                        )
                        print(
                            "ZEROBUS TRANSPORT RECOVERED "
                            f"stream={stream_label} generation={generation} "
                            "operation=ingest_batch "
                            f"duration_sec={ingest_duration_sec:.3f}",
                            flush=True,
                        )
                    submitted_at = iso_utc()
                    evidence = PendingBatch(
                        coordinate=coordinate,
                        rows=rows,
                        uncompressed_arrow_buffer_bytes=buffer_bytes,
                        logical_offset=offset,
                        report_offset=_report_offset(offset),
                        submitted_at=submitted_at,
                        worker=worker_id,
                        stream=stream_label,
                    )
                    pending.append(evidence)
                    journal.record_submitted(evidence)
                    task_rows += rows
                    task_bytes += buffer_bytes
                    checkpoint_due = len(pending) >= args.checkpoint_batches
                    time_due = (
                        args.checkpoint_seconds > 0
                        and time.monotonic() - last_checkpoint
                        >= args.checkpoint_seconds
                    )
                    if checkpoint_due or time_due:
                        checkpoint_pending(
                            stream,
                            pending,
                            journal,
                            args.checkpoint_method,
                            stream_label=stream_label,
                            generation=generation,
                            log_threshold_seconds=(
                                args.recovery_log_threshold_seconds
                            ),
                        )
                        last_checkpoint = time.monotonic()
                if task_rows != task.rows:
                    raise RuntimeError(
                        f"row-group count mismatch for task {task.ordinal}: "
                        f"metadata={task.rows}, iterated={task_rows}"
                    )
                elapsed = max(0.000001, time.monotonic() - task_started)
                if not args.quiet_worker_logs:
                    print(
                        f"[worker {worker_id}] task={task.ordinal} "
                        f"file={task.relative_path} rg={task.row_group} "
                        f"rows={task_rows:,} "
                        f"uncompressed_arrow_buffer_bytes={task_bytes:,} "
                        f"elapsed={elapsed:.3f}s rate={task_rows / elapsed:,.0f} rows/s",
                        flush=True,
                    )
            finally:
                task_queue.task_done()
        if stream is not None and pending:
            stream.flush()
            journal.record_durable(list(pending))
            pending.clear()
        if stream is not None:
            stream.flush()
    except Exception as exc:
        message = f"{stream_label}: {type(exc).__name__}: {exc}"
        journal.terminal_error(message, ambiguous=bool(pending or stream is not None))
        print(
            redact_secrets(
                f"ERROR: {message}\n{traceback.format_exc()}",
                (args.client_secret, args.token),
            ),
            file=sys.stderr,
            flush=True,
        )
        stop_requested.set()
        abort_event.set()
    finally:
        if stream is not None and pending:
            try:
                stream.flush()
                journal.record_durable(list(pending))
                pending.clear()
            except Exception as exc:
                journal.terminal_error(
                    f"{stream_label} final flush failed: {type(exc).__name__}: {exc}",
                    ambiguous=True,
                )
        if stream is not None:
            try:
                stream.close()
                close_succeeded = True
            except Exception as exc:
                journal.terminal_error(
                    f"{stream_label} close failed: {type(exc).__name__}: {exc}",
                    ambiguous=True,
                )
        summary: dict[str, Any] = {
            "stream": stream_label,
            "worker": worker_id,
            "close_succeeded": close_succeeded,
            "unacked_inspection_succeeded": False,
            "unacked_batches": -1,
        }
        if stream is None:
            summary["inspection_error"] = "stream was never created"
        else:
            try:
                unacked = stream.get_unacked_batches()
                summary.update(
                    persist_unacked_batches(
                        unacked,
                        output_dir=args.output_dir,
                        stream_label=stream_label,
                        pa=runtime.pa,
                    )
                )
                summary["unacked_inspection_succeeded"] = True
                if summary["unacked_batches"]:
                    journal.terminal_error(
                        f"{stream_label} closed with "
                        f"{summary['unacked_batches']} unacknowledged Arrow batches; "
                        "artifacts were preserved and will not be replayed",
                        ambiguous=True,
                    )
            except Exception as exc:
                summary["inspection_error"] = (
                    f"{type(exc).__name__}: {exc}"
                )
                journal.terminal_error(
                    f"{stream_label} could not inspect unacknowledged batches "
                    f"after close: {type(exc).__name__}: {exc}",
                    ambiguous=True,
                )
        summary_path = args.output_dir / "streams" / f"{stream_label}.json"
        atomic_write_json(
            summary_path,
            redact_secrets(summary, (args.client_secret, args.token)),
        )
        journal.stream_update(
            stream_label,
            state="closed" if close_succeeded else "failed",
            closed_at=iso_utc(),
            summary_path=str(summary_path),
            **summary,
        )


def metrics_monitor(
    finished: threading.Event,
    stop_requested: threading.Event,
    abort_event: threading.Event,
    runtime: RuntimeModules,
    journal: ProgressJournal,
    limiter: RateLimiter,
    args: argparse.Namespace,
) -> None:
    telemetry = MemoryTelemetry()
    host_sampler = HostResourceSampler()
    last_trim = time.monotonic()
    last_rows = journal.snapshot()["provider_committed_rows"]
    last_time = time.monotonic()
    last_recorded = float("-inf")
    while not finished.wait(args.metrics_interval):
        now = time.monotonic()
        if (
            args.memory_trim_interval > 0
            and now - last_trim >= args.memory_trim_interval
        ):
            memory = trim_process_memory(runtime.pa, telemetry)
            last_trim = time.monotonic()
        else:
            memory = {**memory_snapshot(runtime.pa), **telemetry.as_dict()}
        record_metrics = (
            now - last_recorded >= args.metrics_output_interval
        )
        payload = journal.metrics(
            memory=memory,
            limiter=limiter.snapshot(),
            host=host_sampler.sample(),
            record=record_metrics,
        )
        if record_metrics:
            last_recorded = now
        now = time.monotonic()
        delta_rows = payload["provider_committed_rows"] - last_rows
        interval_eps = delta_rows / max(0.000001, now - last_time)
        total = payload["source"]["total_rows"]
        percentage = (
            100.0 * payload["provider_committed_rows"] / total if total else 100.0
        )
        print(
            "ZEROBUS STATUS "
            f"committed={payload['provider_committed_rows']:,}/{total:,} "
            f"({percentage:.4f}%) submitted={payload['submitted_rows']:,} "
            f"pending={payload['pending_rows']:,} "
            f"tasks={payload['completed_tasks']:,}/{payload['source']['task_count']:,} "
            f"interval_eps={interval_eps:,.0f} "
            f"average_eps={payload['eps']['average_provider_committed_rows_per_sec']:,.0f}",
            flush=True,
        )
        available = memory["system_available_bytes"]
        threshold = int(args.min_system_available_gib * 1024**3)
        if (
            threshold
            and available is not None
            and available < threshold
            and not stop_requested.is_set()
        ):
            journal.terminal_error(
                f"system MemAvailable fell to {available / 1024**3:.2f} GiB, "
                f"below --min-system-available-gib={args.min_system_available_gib:g}",
                ambiguous=False,
            )
            stop_requested.set()
            abort_event.set()
        last_rows = payload["provider_committed_rows"]
        last_time = now
    journal.metrics(
        memory={**memory_snapshot(runtime.pa), **telemetry.as_dict()},
        limiter=limiter.snapshot(),
        host=host_sampler.sample(),
        record=True,
    )


def _load_previous(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise RuntimeError(f"--resume progress journal does not exist: {path}") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"could not read resume journal {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"resume journal is not a JSON object: {path}")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    args.dir = args.dir.expanduser().resolve()
    args.output_dir = args.output_dir.expanduser().resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    progress_path = (
        args.progress_file or args.output_dir / "ingest_progress.json"
    ).expanduser().resolve()
    ledger_path = (
        args.ledger_file or args.output_dir / "ingest_evidence.jsonl"
    ).expanduser().resolve()
    metrics_path = (
        args.metrics_file or args.output_dir / "ingest_metrics.jsonl"
    ).expanduser().resolve()
    manifest_path = (
        args.manifest_output or args.output_dir / "source_manifest.json"
    ).expanduser().resolve()
    summary_path = (
        args.summary_file or args.output_dir / "ingest_summary.json"
    ).expanduser().resolve()
    if not args.resume and any(
        path.exists()
        for path in (progress_path, ledger_path, metrics_path, summary_path)
    ):
        raise RuntimeError(
            "output evidence already exists; choose a fresh --output-dir or use "
            "--resume only with a clean checkpoint"
        )

    runtime = load_runtime()
    schema = target_schema(runtime.pa)
    print(
        "SOURCE MANIFEST enumerating row groups and "
        + (
            "deferring selected-file hashes until after measurement"
            if args.defer_source_hashes
            else "hashing selected files before measurement"
        ),
        flush=True,
    )
    tasks, manifest = enumerate_tasks_and_manifest(
        args.dir,
        args.pattern,
        runtime.pq,
        include_quotes_0=args.include_quotes_0,
        max_files=args.max_files,
        max_row_groups=args.max_row_groups,
        max_rows=args.max_rows,
        hash_files=not args.defer_source_hashes,
    )
    validate_expected_rows(
        int(manifest["total_rows"]),
        args.expected_rows,
        allow_partial=args.allow_partial,
    )
    print(
        "SOURCE MANIFEST complete "
        f"files={manifest['selected_file_count']:,} "
        f"row_groups={manifest['selected_row_group_count']:,} "
        f"rows={manifest['total_rows']:,}",
        flush=True,
    )
    atomic_write_json(manifest_path, manifest)
    config = frozen_config(args)
    config["evidence_paths"] = {
        "output_dir": str(args.output_dir),
        "progress": str(progress_path),
        "ledger": str(ledger_path),
        "metrics": str(metrics_path),
        "manifest": str(manifest_path),
        "summary": str(summary_path),
    }
    target = target_identity(args)
    previous: dict[str, Any] | None = None
    completed: set[int] = set()
    if args.resume:
        previous = _load_previous(progress_path)
        completed = validate_resume_journal(
            previous,
            manifest=manifest,
            target=target,
            config=config,
            tasks=tasks,
        )
        if args.run_id and args.run_id != previous.get("run_id"):
            raise RuntimeError(
                "--run-id does not match the resume journal"
            )
        try:
            with ledger_path.open(encoding="utf-8") as handle:
                first_event = json.loads(handle.readline())
        except (FileNotFoundError, OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                "resume evidence ledger is missing or invalid; use a fresh target table"
            ) from exc
        if first_event.get("run_id") != previous.get("run_id"):
            raise RuntimeError(
                "resume evidence ledger belongs to another run; use a fresh target table"
            )
    table_check = protect_target_table(args, resume_journal=previous)
    if previous is not None:
        baseline_rows = int(previous["baseline_table_rows"])
        baseline_unknown = bool(previous.get("baseline_table_rows_unknown"))
        run_id = str(previous["run_id"])
    else:
        observed = table_check.get("row_count")
        baseline_rows = int(observed) if observed is not None else 0
        baseline_unknown = observed is None
        run_id = args.run_id or (
            f"dbx-zb-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-"
            f"{uuid.uuid4().hex[:8]}"
        )

    journal = ProgressJournal(
        progress_path=progress_path,
        ledger_path=ledger_path,
        metrics_path=metrics_path,
        run_id=run_id,
        manifest=manifest,
        target=target,
        config=config,
        tasks=tasks,
        baseline_table_rows=baseline_rows,
        baseline_unknown=baseline_unknown,
        secrets=(args.client_secret, args.token),
        previous=previous,
    )
    stop_requested = threading.Event()
    abort_event = threading.Event()
    producer_done = threading.Event()
    finished = threading.Event()

    signal_count = 0

    def request_stop(signum: int, _frame: Any) -> None:
        nonlocal signal_count
        signal_count += 1
        stop_requested.set()
        print(
            f"Signal {signum} received; stopping at whole row-group boundaries "
            "and flushing streams...",
            file=sys.stderr,
            flush=True,
        )
        if signal_count > 1:
            abort_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    limiter = RateLimiter(args.target_eps)
    task_queue: queue.Queue[Task] = queue.Queue(maxsize=args.queue_capacity)
    workers = [
        threading.Thread(
            target=worker,
            name=f"zerobus-arrow-{index}",
            args=(
                index,
                task_queue,
                producer_done,
                stop_requested,
                abort_event,
                runtime,
                schema,
                journal,
                limiter,
                args,
            ),
        )
        for index in range(1, args.workers + 1)
    ]
    monitor = threading.Thread(
        target=metrics_monitor,
        name="zerobus-metrics",
        args=(
            finished,
            stop_requested,
            abort_event,
            runtime,
            journal,
            limiter,
            args,
        ),
        daemon=True,
    )
    print(f"Run ID:             {run_id}")
    print(f"Target:             {target['full_name']}")
    print(f"Source files/tasks: {manifest['selected_file_count']:,}/{len(tasks):,}")
    print(f"Source rows:        {manifest['total_rows']:,}")
    print(f"Workers/streams:    {args.workers}")
    print(f"Global target EPS:  {args.target_eps:,.0f}")
    print(f"IPC compression:    {args.compression}")
    print(f"Progress journal:   {progress_path}")
    print(f"Evidence ledger:    {ledger_path}")
    print(
        f"Resume:             {'yes' if previous else 'no'} "
        f"({len(completed):,} tasks already committed)"
    )
    for thread in workers:
        thread.start()
    monitor.start()
    try:
        for task in tasks:
            if task.ordinal in completed:
                continue
            while not stop_requested.is_set() and not abort_event.is_set():
                try:
                    task_queue.put(task, timeout=0.2)
                    break
                except queue.Full:
                    continue
            if stop_requested.is_set() or abort_event.is_set():
                break
    finally:
        producer_done.set()
    for thread in workers:
        thread.join()
    finished.set()
    monitor.join(timeout=max(2.0, args.metrics_interval + 1.0))

    if signal_count > 1:
        journal.terminal_error(
            "second stop signal forced an unclean abort", ambiguous=True
        )
    final = journal.finish(stopped_early=stop_requested.is_set())
    summary = {
        **final,
        "manifest_path": str(manifest_path),
        "progress_path": str(progress_path),
        "evidence_ledger_path": str(ledger_path),
        "metrics_path": str(metrics_path),
        "table_protection": table_check,
        "dependencies": {
            REQUIRED_SDK_DISTRIBUTION: runtime.sdk_version,
            "pyarrow": runtime.pyarrow_version,
        },
        "result": {
            "source_files": manifest["selected_file_count"],
            "source_tasks": manifest["selected_row_group_count"],
            "source_rows": manifest["total_rows"],
            "provider_committed_rows": final["provider_committed_rows"],
            "submitted_rows": final["submitted_rows"],
            "completed_tasks": final["completed_tasks"],
            "finished": final["finished"],
            "clean_checkpoint": final["clean_checkpoint"],
            "safe_to_resume": final["safe_to_resume"],
        },
    }
    atomic_write_json(
        summary_path,
        redact_secrets(summary, (args.client_secret, args.token)),
    )
    print("\n================ ZEROBUS SUMMARY ================")
    print(
        f"Provider committed: {final['provider_committed_rows']:,}/"
        f"{manifest['total_rows']:,}"
    )
    print(f"Submitted:          {final['submitted_rows']:,}")
    print(f"Completed tasks:    {final['completed_tasks']:,}/{len(tasks):,}")
    print(f"Finished:           {final['finished']}")
    print(f"Clean checkpoint:   {final['clean_checkpoint']}")
    print(f"Safe to resume:     {final['safe_to_resume']}")
    print(
        "Average EPS:        "
        f"{final['eps']['average_provider_committed_rows_per_sec']:,.0f}"
    )
    print(f"Summary:            {summary_path}")
    print("==================================================")
    if final["finished"]:
        return 0
    return 130 if final["clean_checkpoint"] and stop_requested.is_set() else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        secrets = (
            os.environ.get("DATABRICKS_CLIENT_SECRET"),
            os.environ.get("DATABRICKS_TOKEN"),
        )
        print(
            f"FATAL: {redact_secrets(f'{type(exc).__name__}: {exc}', secrets)}",
            file=sys.stderr,
        )
        sys.exit(1)
