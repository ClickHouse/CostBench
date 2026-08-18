#!/usr/bin/env python3
"""Stream Parquet row groups to BigQuery through committed Arrow write streams.

This is intentionally a continuous-write benchmark client, not a load-job
wrapper. Each worker owns one long-lived COMMITTED stream and supplies explicit
row offsets. Acknowledged rows are immediately queryable and retrying the same
offset cannot duplicate them within the live process.
"""

from __future__ import annotations

import argparse
import ctypes
import gc
import json
import os
import queue
import resource
import signal
import sys
import threading
import time
import traceback
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery, bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1.writer import AppendRowsStream

from bq_common import (
    append_jsonl,
    atomic_write_json,
    close_google_client,
    iso_utc,
    offset_already_written_matches,
    table_snapshot,
    utc_now,
)
from setup import apply_schema

SCRIPT_DIR = Path(__file__).resolve().parent
TARGET_SCHEMA = pa.schema(
    [
        pa.field("sym", pa.string()),
        pa.field("bx", pa.int64()),
        pa.field("bp", pa.float64()),
        pa.field("bs", pa.int64()),
        pa.field("ax", pa.int64()),
        pa.field("ap", pa.float64()),
        pa.field("as", pa.int64()),
        pa.field("c", pa.int64()),
        pa.field("i", pa.list_(pa.int64())),
        pa.field("t", pa.int64()),
        pa.field("q", pa.int64()),
        pa.field("z", pa.int64()),
    ]
)


@dataclass(frozen=True)
class Task:
    file_path: Path
    row_group: int
    rows: int


@dataclass
class Counters:
    acknowledged_rows: int = 0
    acknowledged_arrow_bytes: int = 0
    append_requests: int = 0
    retries: int = 0
    completed_tasks: int = 0
    active_workers: int = 0
    finished_workers: int = 0
    recovered_already_exists_appends: int = 0
    recovered_already_exists_rows: int = 0
    recovered_server_row_count_appends: int = 0
    recovered_server_row_count_rows: int = 0
    errors: list[str] = field(default_factory=list)
    last_ack_monotonic: float | None = field(default=None, repr=False)
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def update(self, **increments: int) -> None:
        with self.lock:
            for key, value in increments.items():
                setattr(self, key, getattr(self, key) + value)
            if increments.get("acknowledged_rows", 0) > 0:
                self.last_ack_monotonic = time.monotonic()

    def add_error(self, message: str) -> None:
        with self.lock:
            if len(self.errors) < 50:
                self.errors.append(message)

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "acknowledged_rows": self.acknowledged_rows,
                "acknowledged_arrow_bytes": self.acknowledged_arrow_bytes,
                "append_requests": self.append_requests,
                "retries": self.retries,
                "completed_tasks": self.completed_tasks,
                "active_workers": self.active_workers,
                "finished_workers": self.finished_workers,
                "recovered_already_exists_appends": self.recovered_already_exists_appends,
                "recovered_already_exists_rows": self.recovered_already_exists_rows,
                "recovered_server_row_count_appends": self.recovered_server_row_count_appends,
                "recovered_server_row_count_rows": self.recovered_server_row_count_rows,
                "errors": list(self.errors),
            }

    def acknowledged_elapsed(self, started_monotonic: float) -> float | None:
        with self.lock:
            if self.last_ack_monotonic is None:
                return None
            return max(0.0, self.last_ack_monotonic - started_monotonic)


@dataclass
class MemoryTelemetry:
    trim_attempts: int = 0
    trim_successes: int = 0
    trim_unsupported: int = 0
    gc_collected_objects: int = 0
    trim_reclaimed_rss_bytes: int = 0
    last_trim_duration_sec: float | None = None
    last_trim_at: str | None = None
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_trim(
        self,
        *,
        supported: bool,
        success: bool,
        gc_collected: int,
        reclaimed_rss_bytes: int,
        duration_sec: float,
    ) -> None:
        with self.lock:
            self.trim_attempts += 1
            self.trim_successes += int(success)
            self.trim_unsupported += int(not supported)
            self.gc_collected_objects += gc_collected
            self.trim_reclaimed_rss_bytes += reclaimed_rss_bytes
            self.last_trim_duration_sec = duration_sec
            self.last_trim_at = iso_utc(utc_now())

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
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
    """Serialize append starts at a declared global row rate without catch-up bursts."""

    def __init__(self, target_eps: float):
        self.target_eps = target_eps
        self.next_send_at = time.monotonic()
        self.scheduled_rows = 0
        self.lock = threading.Lock()

    def wait(self, rows: int, stop_event: threading.Event) -> bool:
        if self.target_eps <= 0:
            return not stop_event.is_set()
        with self.lock:
            now = time.monotonic()
            send_at = max(now, self.next_send_at)
            self.next_send_at = send_at + rows / self.target_eps
            self.scheduled_rows += rows
        delay = send_at - time.monotonic()
        return not (delay > 0 and stop_event.wait(delay))

    def snapshot(self) -> dict[str, int | float | None]:
        with self.lock:
            return {
                "target_eps": self.target_eps or None,
                "scheduled_rows": self.scheduled_rows,
                "next_send_delay_sec": max(0.0, self.next_send_at - time.monotonic()),
            }


def _proc_value_bytes(path: str, key: str) -> int | None:
    try:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                if line.startswith(f"{key}:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        multiplier = 1024 if len(parts) < 3 or parts[2].lower() == "kb" else 1
                        return int(parts[1]) * multiplier
    except (FileNotFoundError, OSError, ValueError):
        pass
    return None


def memory_snapshot() -> dict[str, int | None]:
    process_rss = _proc_value_bytes("/proc/self/status", "VmRSS")
    process_peak_rss = _proc_value_bytes("/proc/self/status", "VmHWM")
    if process_peak_rss is None:
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        process_peak_rss = int(peak if sys.platform == "darwin" else peak * 1024)
    pool = pa.default_memory_pool()
    return {
        "process_rss_bytes": process_rss,
        "process_peak_rss_bytes": process_peak_rss,
        "system_available_bytes": _proc_value_bytes("/proc/meminfo", "MemAvailable"),
        "system_total_bytes": _proc_value_bytes("/proc/meminfo", "MemTotal"),
        "arrow_allocated_bytes": int(pool.bytes_allocated()),
        "arrow_peak_bytes": int(pool.max_memory()),
    }


def trim_process_memory(telemetry: MemoryTelemetry) -> dict[str, int | None]:
    """Return unused glibc arenas to Linux while retaining observable evidence."""
    started = time.monotonic()
    before = memory_snapshot()
    gc_collected = gc.collect()
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
    after = memory_snapshot()
    before_rss = before["process_rss_bytes"]
    after_rss = after["process_rss_bytes"]
    reclaimed = (
        max(0, before_rss - after_rss)
        if before_rss is not None and after_rss is not None
        else 0
    )
    telemetry.record_trim(
        supported=supported,
        success=success,
        gc_collected=gc_collected,
        reclaimed_rss_bytes=reclaimed,
        duration_sec=time.monotonic() - started,
    )
    return after


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=Path, required=True)
    parser.add_argument("--pattern", default="*.parquet")
    parser.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT"))
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--location", default="US")
    parser.add_argument("--table", default="quotes")
    parser.add_argument("--create-sql", type=Path, default=SCRIPT_DIR / "create.sql")
    parser.add_argument("--parallel", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument(
        "--target-eps",
        type=float,
        default=0.0,
        help="Maximum global append-start rate in rows/s; 0 leaves throughput unconstrained.",
    )
    parser.add_argument(
        "--max-request-bytes",
        type=int,
        default=8_000_000,
        help="Maximum serialized Arrow record-batch bytes before recursive splitting (gRPC AppendRows limit: 20 MB).",
    )
    parser.add_argument("--append-timeout", type=float, default=120.0)
    parser.add_argument("--max-retries", type=int, default=8)
    parser.add_argument("--retry-base-seconds", type=float, default=0.5)
    parser.add_argument("--max-files", type=int)
    parser.add_argument(
        "--max-row-groups",
        type=int,
        help="Read at most this many whole Parquet row groups across the selected files (useful for clean tuning runs).",
    )
    parser.add_argument("--metrics-interval", type=float, default=5.0)
    parser.add_argument(
        "--memory-trim-interval",
        type=float,
        default=60.0,
        help="Run Python GC and glibc malloc_trim at this interval in seconds; 0 disables it.",
    )
    parser.add_argument(
        "--min-system-available-gib",
        type=float,
        default=0.0,
        help="Stop cleanly if Linux MemAvailable drops below this value; 0 disables the guard.",
    )
    parser.add_argument(
        "--quiet-worker-logs",
        action="store_true",
        help="Suppress per-row-group worker lines so the boxed aggregate throughput display remains readable.",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("results/ingest"))
    parser.add_argument("--progress-file", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument(
        "--allow-nonempty-table",
        action="store_true",
        help="Append to an existing table. The default refuses to prevent accidental duplicate benchmark runs.",
    )
    parser.add_argument(
        "--leave-streams-open",
        action="store_true",
        help="Do not finalize committed streams at shutdown (normally undesirable; cleanup_streams.py can finalize them later).",
    )
    args = parser.parse_args()
    if not args.project:
        parser.error("--project or GOOGLE_CLOUD_PROJECT is required")
    if args.parallel < 1 or args.batch_size < 1 or args.max_request_bytes < 1024:
        parser.error("--parallel and --batch-size must be positive; --max-request-bytes must be >= 1024")
    if args.target_eps < 0:
        parser.error("--target-eps must be >= 0")
    if args.max_retries < 0:
        parser.error("--max-retries must be >= 0")
    if args.metrics_interval <= 0 or args.memory_trim_interval < 0:
        parser.error("--metrics-interval must be positive and --memory-trim-interval must be >= 0")
    if args.min_system_available_gib < 0:
        parser.error("--min-system-available-gib must be >= 0")
    if args.max_files is not None and args.max_files < 1:
        parser.error("--max-files must be positive")
    if args.max_row_groups is not None and args.max_row_groups < 1:
        parser.error("--max-row-groups must be positive")
    return args


def enumerate_tasks(
    directory: Path,
    pattern: str,
    max_files: int | None,
    max_row_groups: int | None,
) -> tuple[list[Task], list[Path]]:
    files = sorted(directory.glob(pattern))
    if max_files is not None:
        files = files[:max_files]
    if not files:
        raise RuntimeError(f"no files matched {directory}/{pattern}")
    tasks: list[Task] = []
    selected_files: list[Path] = []
    for file_path in files:
        metadata = pq.ParquetFile(file_path).metadata
        for row_group in range(metadata.num_row_groups):
            tasks.append(Task(file_path, row_group, metadata.row_group(row_group).num_rows))
            if file_path not in selected_files:
                selected_files.append(file_path)
            if max_row_groups is not None and len(tasks) >= max_row_groups:
                return tasks, selected_files
    return tasks, selected_files


def normalize_batch(batch: pa.RecordBatch) -> pa.RecordBatch:
    source_names = set(batch.schema.names)
    missing = [name for name in TARGET_SCHEMA.names if name not in source_names]
    if missing:
        raise ValueError(f"Parquet schema is missing required columns: {missing}")
    arrays = []
    for target_field in TARGET_SCHEMA:
        source = batch.column(batch.schema.get_field_index(target_field.name))
        arrays.append(pc.cast(source, target_field.type, safe=True))
    return pa.RecordBatch.from_arrays(arrays, schema=TARGET_SCHEMA)


def serialized_chunks(batch: pa.RecordBatch, max_bytes: int) -> Iterator[tuple[pa.RecordBatch, bytes]]:
    payload = batch.serialize().to_pybytes()
    if len(payload) <= max_bytes:
        yield batch, payload
        return
    if batch.num_rows <= 1:
        raise ValueError(f"one row serialized to {len(payload):,} bytes, above --max-request-bytes={max_bytes:,}")
    split_at = batch.num_rows // 2
    yield from serialized_chunks(batch.slice(0, split_at), max_bytes)
    yield from serialized_chunks(batch.slice(split_at), max_bytes)


def make_append_stream(write_client: Any, stream_name: str) -> AppendRowsStream:
    template = types.AppendRowsRequest(write_stream=stream_name)
    template.arrow_rows.writer_schema.serialized_schema = TARGET_SCHEMA.serialize().to_pybytes()
    return AppendRowsStream(write_client, template)


def server_row_count(write_client: Any, stream_name: str) -> int:
    stream = write_client.get_write_stream(request={"name": stream_name})
    return int(stream.row_count)


def accept_replayed_append(
    write_client: Any,
    append_stream: AppendRowsStream,
    stream_name: str,
    offset: int,
    rows: int,
    payload_bytes: int,
    counters: Counters,
) -> AppendRowsStream:
    """Account for a retry whose original append was already committed."""
    counters.update(
        acknowledged_rows=rows,
        acknowledged_arrow_bytes=payload_bytes,
        recovered_already_exists_appends=1,
        recovered_already_exists_rows=rows,
    )
    print(
        f"reconciled already-written append stream={stream_name.rsplit('/', 1)[-1]} "
        f"offset={offset} rows={rows}",
        file=sys.stderr,
        flush=True,
    )
    # Continue the next offset on a clean bidirectional connection. A failed
    # AppendRows response can leave subsequent requests on that connection in
    # a failed state even though the replayed batch itself is committed.
    try:
        append_stream.close()
    except Exception:
        pass
    return make_append_stream(write_client, stream_name)


def accept_server_observed_append(
    write_client: Any,
    append_stream: AppendRowsStream,
    stream_name: str,
    offset: int,
    rows: int,
    payload_bytes: int,
    counters: Counters,
) -> AppendRowsStream:
    """Account once when stream metadata proves an ambiguous append committed."""
    counters.update(
        acknowledged_rows=rows,
        acknowledged_arrow_bytes=payload_bytes,
        recovered_server_row_count_appends=1,
        recovered_server_row_count_rows=rows,
    )
    print(
        f"reconciled server-observed append stream={stream_name.rsplit('/', 1)[-1]} "
        f"offset={offset} rows={rows}",
        file=sys.stderr,
        flush=True,
    )
    try:
        append_stream.close()
    except Exception:
        pass
    return make_append_stream(write_client, stream_name)


def send_with_offset(
    write_client: Any,
    append_stream: AppendRowsStream,
    stream_name: str,
    offset: int,
    batch: pa.RecordBatch,
    payload: bytes,
    counters: Counters,
    args: argparse.Namespace,
) -> AppendRowsStream:
    rows = batch.num_rows
    for attempt in range(args.max_retries + 1):
        request = types.AppendRowsRequest(offset=offset)
        request.arrow_rows.rows.serialized_record_batch = payload
        counters.update(append_requests=1)
        try:
            response = append_stream.send(request).result(timeout=args.append_timeout)
            if getattr(response, "error", None) and response.error.code:
                if response.error.code == 6 and offset_already_written_matches(
                    response.error.message, offset, rows
                ):
                    return accept_replayed_append(
                        write_client,
                        append_stream,
                        stream_name,
                        offset,
                        rows,
                        len(payload),
                        counters,
                    )
                raise RuntimeError(f"AppendRows response error: {response.error}")
            counters.update(acknowledged_rows=rows, acknowledged_arrow_bytes=len(payload))
            return append_stream
        except Exception as exc:
            if isinstance(exc, google_exceptions.AlreadyExists):
                if not offset_already_written_matches(str(exc), offset, rows):
                    raise RuntimeError(
                        f"ALREADY_EXISTS did not match replayed batch offset={offset} rows={rows}: {exc}"
                    ) from exc
                return accept_replayed_append(
                    write_client,
                    append_stream,
                    stream_name,
                    offset,
                    rows,
                    len(payload),
                    counters,
                )
            try:
                observed = server_row_count(write_client, stream_name)
            except Exception:
                observed = None
            if observed == offset + rows:
                return accept_server_observed_append(
                    write_client,
                    append_stream,
                    stream_name,
                    offset,
                    rows,
                    len(payload),
                    counters,
                )
            if observed not in (None, offset):
                raise RuntimeError(
                    f"ambiguous stream offset after error: expected {offset} or {offset + rows}, observed {observed}"
                ) from exc
            if attempt >= args.max_retries:
                raise
            counters.update(retries=1)
            try:
                append_stream.close()
            except Exception:
                pass
            delay = min(30.0, args.retry_base_seconds * (2**attempt))
            print(
                f"retrying stream={stream_name.rsplit('/', 1)[-1]} offset={offset} rows={rows} "
                f"attempt={attempt + 2}/{args.max_retries + 1} in {delay:.1f}s: {exc}",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay)
            append_stream = make_append_stream(write_client, stream_name)
    raise AssertionError("unreachable")


def worker(
    worker_id: int,
    stream_name: str,
    tasks: queue.Queue[Task],
    stop_event: threading.Event,
    counters: Counters,
    rate_limiter: RateLimiter,
    args: argparse.Namespace,
) -> None:
    write_client = bigquery_storage_v1.BigQueryWriteClient()
    append_stream = make_append_stream(write_client, stream_name)
    offset = 0
    current_file_path: Path | None = None
    current_parquet: pq.ParquetFile | None = None
    counters.update(active_workers=1)
    try:
        while not stop_event.is_set():
            try:
                task = tasks.get(timeout=0.2)
            except queue.Empty:
                return
            task_started = time.monotonic()
            task_rows = 0
            task_bytes = 0
            try:
                if task.file_path != current_file_path:
                    current_parquet = pq.ParquetFile(task.file_path)
                    current_file_path = task.file_path
                if current_parquet is None:
                    raise AssertionError("Parquet reader was not initialized")
                for source_batch in current_parquet.iter_batches(
                    batch_size=args.batch_size,
                    row_groups=[task.row_group],
                    columns=TARGET_SCHEMA.names,
                ):
                    normalized = normalize_batch(source_batch)
                    for chunk, payload in serialized_chunks(normalized, args.max_request_bytes):
                        if not rate_limiter.wait(chunk.num_rows, stop_event):
                            return
                        append_stream = send_with_offset(
                            write_client,
                            append_stream,
                            stream_name,
                            offset,
                            chunk,
                            payload,
                            counters,
                            args,
                        )
                        offset += chunk.num_rows
                        task_rows += chunk.num_rows
                        task_bytes += len(payload)
                if task_rows != task.rows:
                    raise RuntimeError(f"row-group count mismatch: metadata={task.rows}, sent={task_rows}")
                counters.update(completed_tasks=1)
                elapsed = time.monotonic() - task_started
                if not args.quiet_worker_logs:
                    print(
                        f"[worker {worker_id}] file={task.file_path.name} rg={task.row_group} "
                        f"rows={task_rows:,} arrow_bytes={task_bytes:,} elapsed={elapsed:.3f}s "
                        f"rate={task_rows / elapsed if elapsed else 0:,.0f} rows/s",
                        flush=True,
                    )
            except Exception as exc:
                message = f"worker {worker_id}, {task.file_path.name} rg {task.row_group}: {exc}"
                counters.add_error(message)
                print(f"ERROR: {message}\n{traceback.format_exc()}", file=sys.stderr, flush=True)
                stop_event.set()
            finally:
                tasks.task_done()
    finally:
        try:
            append_stream.close()
        except Exception:
            pass
        try:
            close_google_client(write_client)
        except Exception:
            pass
        counters.update(active_workers=-1, finished_workers=1)


def progress_payload(
    args: argparse.Namespace,
    run_id: str,
    counters: Counters,
    started_at: Any,
    started_monotonic: float,
    starting_table_rows: int,
    expected_rows: int,
    total_tasks: int,
    done: bool,
    memory_telemetry: MemoryTelemetry,
    rate_limiter: RateLimiter,
) -> dict[str, Any]:
    snapshot = counters.snapshot()
    wall_elapsed = max(0.0, time.monotonic() - started_monotonic)
    acknowledged_elapsed = counters.acknowledged_elapsed(started_monotonic)
    elapsed = acknowledged_elapsed if done and acknowledged_elapsed is not None else wall_elapsed
    acknowledged = snapshot["acknowledged_rows"]
    return {
        "schema_version": 1,
        "run_id": run_id,
        "updated_at": iso_utc(utc_now()),
        "started_at": iso_utc(started_at),
        "finished": done,
        "project": args.project,
        "dataset": args.dataset,
        "table": args.table,
        "location": args.location,
        "stream_type": "COMMITTED",
        "delivery_semantics": "exactly_once_within_live_run_via_offsets",
        "starting_table_rows_metadata": starting_table_rows,
        "acknowledged_rows": acknowledged,
        "logical_raw_rows": starting_table_rows + acknowledged,
        "expected_input_rows": expected_rows,
        "total_tasks": total_tasks,
        "elapsed_sec": elapsed,
        "wall_elapsed_sec": wall_elapsed,
        "last_ack_elapsed_sec": acknowledged_elapsed,
        "average_ack_rows_per_sec": acknowledged / elapsed if elapsed else 0.0,
        "rate_limiter": rate_limiter.snapshot(),
        "memory": {
            **memory_snapshot(),
            **memory_telemetry.snapshot(),
        },
        **snapshot,
    }


def metrics_monitor(
    stop_event: threading.Event,
    finished_event: threading.Event,
    metrics_path: Path,
    progress_path: Path,
    args: argparse.Namespace,
    run_id: str,
    counters: Counters,
    started_at: Any,
    started_monotonic: float,
    starting_table_rows: int,
    expected_rows: int,
    total_tasks: int,
    memory_telemetry: MemoryTelemetry,
    rate_limiter: RateLimiter,
) -> None:
    last_rows = 0
    last_time = started_monotonic
    last_trim_time = started_monotonic
    while not finished_event.wait(args.metrics_interval):
        now = time.monotonic()
        if (
            args.memory_trim_interval > 0
            and now - last_trim_time >= args.memory_trim_interval
        ):
            trim_process_memory(memory_telemetry)
            last_trim_time = time.monotonic()
        payload = progress_payload(
            args,
            run_id,
            counters,
            started_at,
            started_monotonic,
            starting_table_rows,
            expected_rows,
            total_tasks,
            done=False,
            memory_telemetry=memory_telemetry,
            rate_limiter=rate_limiter,
        )
        now = time.monotonic()
        delta_rows = payload["acknowledged_rows"] - last_rows
        delta_time = now - last_time
        payload["interval_acknowledged_rows"] = delta_rows
        payload["interval_ack_rows_per_sec"] = delta_rows / delta_time if delta_time else 0.0
        atomic_write_json(progress_path, payload)
        append_jsonl(metrics_path, payload)
        available_bytes = payload["memory"]["system_available_bytes"]
        minimum_available_bytes = int(args.min_system_available_gib * (1024**3))
        if (
            minimum_available_bytes
            and available_bytes is not None
            and available_bytes < minimum_available_bytes
            and not stop_event.is_set()
        ):
            message = (
                f"system MemAvailable fell to {available_bytes / (1024**3):.2f} GiB, "
                f"below --min-system-available-gib={args.min_system_available_gib:g}"
            )
            counters.add_error(message)
            print(f"ERROR: {message}; stopping after in-flight appends", file=sys.stderr, flush=True)
            stop_event.set()
        average_eps = payload["average_ack_rows_per_sec"]
        remaining_rows = max(0, expected_rows - payload["acknowledged_rows"])
        eta_seconds = remaining_rows / average_eps if average_eps else None
        if eta_seconds is None:
            eta = "calculating"
        else:
            eta_total = int(eta_seconds)
            eta_days, eta_remainder = divmod(eta_total, 86_400)
            eta_hours, eta_remainder = divmod(eta_remainder, 3_600)
            eta_minutes, eta_secs = divmod(eta_remainder, 60)
            eta = f"{eta_days}d {eta_hours:02d}h {eta_minutes:02d}m {eta_secs:02d}s"
        progress_pct = (
            100.0 * payload["acknowledged_rows"] / expected_rows
            if expected_rows
            else 100.0
        )
        process_rss = payload["memory"]["process_rss_bytes"]
        system_available = payload["memory"]["system_available_bytes"]
        arrow_allocated = payload["memory"]["arrow_allocated_bytes"]
        rss_gib = process_rss / (1024**3) if process_rss is not None else float("nan")
        available_gib = (
            system_available / (1024**3)
            if system_available is not None
            else float("nan")
        )
        arrow_gib = arrow_allocated / (1024**3) if arrow_allocated is not None else float("nan")
        reclaimed_gib = payload["memory"]["trim_reclaimed_rss_bytes"] / (1024**3)
        rule = "=" * 132
        print(
            f"\n{rule}\n"
            f"INGEST STATUS | elapsed={payload['elapsed_sec']:,.1f}s | progress={progress_pct:.4f}% | "
            f"acknowledged={payload['acknowledged_rows']:,}/{expected_rows:,}\n"
            f"AVERAGE EPS: {average_eps:,.0f}    |    INSTANT EPS: {payload['interval_ack_rows_per_sec']:,.0f}    |    ETA: {eta}\n"
            f"WORKERS: {payload['active_workers']}/{args.parallel} active    |    "
            f"TASKS: {payload['completed_tasks']:,}/{total_tasks:,}    |    "
            f"APPENDS: {payload['append_requests']:,}    |    "
            f"RETRIES (CUMULATIVE): {payload['retries']:,}\n"
            f"RECOVERED ALREADY-WRITTEN APPENDS: {payload['recovered_already_exists_appends']:,} "
            f"({payload['recovered_already_exists_rows']:,} rows)\n"
            f"RECOVERED BY SERVER ROW COUNT: {payload['recovered_server_row_count_appends']:,} "
            f"({payload['recovered_server_row_count_rows']:,} rows)\n"
            f"MEMORY: RSS={rss_gib:.2f} GiB | AVAILABLE={available_gib:.2f} GiB | "
            f"ARROW={arrow_gib:.2f} GiB | TRIMS={payload['memory']['trim_attempts']:,} | "
            f"RECLAIMED (CUMULATIVE)={reclaimed_gib:.2f} GiB\n"
            f"{rule}",
            flush=True,
        )
        last_rows = payload["acknowledged_rows"]
        last_time = now
        if stop_event.is_set():
            return


def main() -> int:
    args = parse_args()
    args.dir = args.dir.expanduser().resolve()
    args.output_dir = args.output_dir.expanduser().resolve()
    progress_path = (args.progress_file or args.output_dir / "ingest_progress.json").expanduser().resolve()
    metrics_path = args.output_dir / "ingest_metrics.jsonl"
    manifest_path = args.output_dir / "write_streams.json"
    summary_path = args.output_dir / "ingest_summary.json"
    run_id = args.run_id or f"bq-{utc_now().strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"

    if not args.dir.is_dir():
        raise RuntimeError(f"not a directory: {args.dir}")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.create_sql:
        apply_schema(args.project, args.dataset, args.location, args.create_sql, run_id)

    tasks_list, files = enumerate_tasks(args.dir, args.pattern, args.max_files, args.max_row_groups)
    expected_rows = sum(task.rows for task in tasks_list)
    task_queue: queue.Queue[Task] = queue.Queue()
    for task in tasks_list:
        task_queue.put(task)

    query_client = bigquery.Client(project=args.project, location=args.location)
    raw_table_id = f"{args.project}.{args.dataset}.{args.table}"
    initial = table_snapshot(query_client, raw_table_id)
    starting_rows = int(initial["num_rows"] or 0)
    if starting_rows and not args.allow_nonempty_table:
        raise RuntimeError(
            f"{raw_table_id} reports {starting_rows:,} existing rows; use a fresh dataset or explicitly pass --allow-nonempty-table"
        )

    parent = f"projects/{args.project}/datasets/{args.dataset}/tables/{args.table}"
    admin_write_client = bigquery_storage_v1.BigQueryWriteClient()
    streams: list[str] = []
    try:
        for _ in range(args.parallel):
            stream = admin_write_client.create_write_stream(
                parent=parent,
                write_stream=types.WriteStream(type_=types.WriteStream.Type.COMMITTED),
            )
            streams.append(stream.name)
    except Exception:
        for stream_name in streams:
            try:
                admin_write_client.finalize_write_stream(name=stream_name)
            except Exception:
                pass
        raise

    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "created_at": iso_utc(utc_now()),
        "project": args.project,
        "dataset": args.dataset,
        "table": args.table,
        "location": args.location,
        "stream_type": "COMMITTED",
        "streams": streams,
        "finalized": False,
        "input_dir": str(args.dir),
        "pattern": args.pattern,
        "files": len(files),
        "row_groups": len(tasks_list),
        "expected_input_rows": expected_rows,
        "parallel": args.parallel,
        "target_eps": args.target_eps or None,
        "batch_size": args.batch_size,
        "max_request_bytes": args.max_request_bytes,
        "max_row_groups": args.max_row_groups,
        "quiet_worker_logs": args.quiet_worker_logs,
        "metrics_interval": args.metrics_interval,
        "memory_trim_interval": args.memory_trim_interval,
        "min_system_available_gib": args.min_system_available_gib,
    }
    atomic_write_json(manifest_path, manifest)

    stop_event = threading.Event()
    finished_event = threading.Event()

    def request_stop(_signum: int, _frame: Any) -> None:
        print("Stop requested; finishing in-flight appends...", file=sys.stderr, flush=True)
        stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    counters = Counters()
    memory_telemetry = MemoryTelemetry()
    rate_limiter = RateLimiter(args.target_eps)
    started_at = utc_now()
    started_monotonic = time.monotonic()
    monitor = threading.Thread(
        target=metrics_monitor,
        args=(
            stop_event,
            finished_event,
            metrics_path,
            progress_path,
            args,
            run_id,
            counters,
            started_at,
            started_monotonic,
            starting_rows,
            expected_rows,
            len(tasks_list),
            memory_telemetry,
            rate_limiter,
        ),
        daemon=True,
    )
    monitor.start()

    print(f"Run ID:        {run_id}")
    print(f"Target:        {raw_table_id} ({args.location})")
    print(f"Input:         {args.dir}/{args.pattern}")
    print(f"Files/tasks:   {len(files)}/{len(tasks_list)}")
    print(f"Expected rows: {expected_rows:,}")
    print(f"Workers:       {args.parallel} committed streams")
    print(f"Target rate:   {args.target_eps:,.0f} rows/s" if args.target_eps else "Target rate:   unconstrained")
    print(f"Batch target:  {args.batch_size:,} rows, split above {args.max_request_bytes:,} Arrow bytes")
    print(
        f"Worker logs:   {'suppressed' if args.quiet_worker_logs else 'enabled'}; "
        f"boxed aggregate metrics every {args.metrics_interval:g}s"
    )
    print(f"Progress:      {progress_path}")
    print(
        f"Memory safety: trim every {args.memory_trim_interval:g}s; "
        f"stop below {args.min_system_available_gib:g} GiB available "
        f"({'disabled' if not args.min_system_available_gib else 'enabled'})"
    )

    workers = [
        threading.Thread(
            target=worker,
            name=f"bq-writer-{index + 1}",
            args=(index + 1, stream_name, task_queue, stop_event, counters, rate_limiter, args),
        )
        for index, stream_name in enumerate(streams)
    ]
    for thread in workers:
        thread.start()
    for thread in workers:
        thread.join()

    finished_event.set()
    monitor.join(timeout=max(2.0, args.metrics_interval + 1.0))

    finalized: list[str] = []
    finalize_errors: list[str] = []
    if not args.leave_streams_open:
        for stream_name in streams:
            try:
                admin_write_client.finalize_write_stream(name=stream_name)
                finalized.append(stream_name)
            except Exception as exc:
                finalize_errors.append(f"{stream_name}: {exc}")
    try:
        close_google_client(admin_write_client)
    except Exception as exc:
        print(f"WARNING: could not close the admin write client cleanly: {exc}", file=sys.stderr, flush=True)

    manifest["finalized"] = len(finalized) == len(streams)
    manifest["finalized_streams"] = finalized
    manifest["finalize_errors"] = finalize_errors
    manifest["finished_at"] = iso_utc(utc_now())
    atomic_write_json(manifest_path, manifest)

    final_progress = progress_payload(
        args,
        run_id,
        counters,
        started_at,
        started_monotonic,
        starting_rows,
        expected_rows,
        len(tasks_list),
        done=True,
        memory_telemetry=memory_telemetry,
        rate_limiter=rate_limiter,
    )
    final_progress["stopped_early"] = stop_event.is_set()
    final_progress["streams_finalized"] = len(finalized)
    final_progress["stream_finalize_errors"] = finalize_errors
    atomic_write_json(progress_path, final_progress)
    append_jsonl(metrics_path, final_progress)

    try:
        final_progress["table_metadata_after_run"] = table_snapshot(query_client, raw_table_id)
    except Exception as exc:
        final_progress["table_metadata_after_run_error"] = str(exc)
    atomic_write_json(summary_path, final_progress)

    acknowledged = final_progress["acknowledged_rows"]
    errors = final_progress["errors"]
    print("\n==================== SUMMARY ====================")
    print(f"Acknowledged:  {acknowledged:,}/{expected_rows:,} rows")
    print(f"Duration:      {final_progress['elapsed_sec']:.1f}s")
    print(f"Average:       {final_progress['average_ack_rows_per_sec']:,.0f} rows/s")
    print(f"Retries:       {final_progress['retries']}")
    print(f"Errors:        {len(errors) + len(finalize_errors)}")
    print(f"Summary:       {summary_path}")
    print("=================================================")
    return 1 if errors or finalize_errors or acknowledged != expected_rows else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        sys.exit(1)
