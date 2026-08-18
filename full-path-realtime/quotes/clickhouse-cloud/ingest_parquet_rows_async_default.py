#!/usr/bin/env python3
"""Acknowledged ClickHouse async-insert benchmark ingester.

This is deliberately separate from ingest_parquet_rows.py.  It keeps
ClickHouse's async-insert flush controls at their effective server/profile
defaults and changes only these two async settings:

    async_insert = 1
    async_insert_deduplicate = 1

The client reads the effective settings back before starting and refuses to
run unless wait_for_async_insert=1.  A successful request therefore means the
server-side async buffer was flushed successfully, rather than merely accepted
into memory.

Tasks are emitted in deterministic (file name, row group) order into one
bounded FIFO.  Consecutive row groups from the same file are combined when
their total row count fits within --batch-size, so logical inserts can be
larger than one Parquet row group.  Workers can complete adjacent tasks out of
order, but no worker independently walks the whole input.  Each logical insert
receives a deterministic insert_deduplication_token; retries reuse both the
exact payload and token.

Requires:
    pip install clickhouse-connect pyarrow
    (pandas additionally required for --format JSONEachRow)
"""

from __future__ import annotations

import argparse
import ctypes
import gc
import hashlib
import json
import os
import queue
import re
import resource
import signal
import sys
import tempfile
import threading
import time
import traceback
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

import clickhouse_connect
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
FORMATS = ("CSV", "JSONEachRow")
CLIENT_ASYNC_SETTINGS = {
    "async_insert": 1,
    "async_insert_deduplicate": 1,
}
REQUIRED_EFFECTIVE_SETTINGS = {
    "async_insert": True,
    "async_insert_deduplicate": True,
    "wait_for_async_insert": True,
}
_CSV_OPTIONS = pacsv.WriteOptions(include_header=False)
_SENTINEL = object()


@dataclass(frozen=True)
class Task:
    index: int
    file_path: Path
    row_groups: tuple[int, ...]
    rows: int


@dataclass
class Counters:
    acknowledged_rows: int = 0
    acknowledged_wire_bytes: int = 0
    acknowledged_inserts: int = 0
    insert_attempts: int = 0
    retries: int = 0
    recovered_retried_inserts: int = 0
    recovered_retried_rows: int = 0
    completed_tasks: int = 0
    active_workers: int = 0
    finished_workers: int = 0
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
                "acknowledged_wire_bytes": self.acknowledged_wire_bytes,
                "acknowledged_inserts": self.acknowledged_inserts,
                "insert_attempts": self.insert_attempts,
                "retries": self.retries,
                "recovered_retried_inserts": self.recovered_retried_inserts,
                "recovered_retried_rows": self.recovered_retried_rows,
                "completed_tasks": self.completed_tasks,
                "active_workers": self.active_workers,
                "finished_workers": self.finished_workers,
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
            self.last_trim_at = iso_utc()

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
    """Serialize logical insert starts at a global row rate, without bursts."""

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


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime | None = None) -> str:
    return (
        (value or utc_now()).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )


def atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
        if temporary_path is None:
            raise AssertionError("temporary progress path was not created")
        os.replace(temporary_path, path)
    except Exception:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise


def append_jsonl(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, sort_keys=True, separators=(",", ":")))
        handle.write("\n")


def _proc_value_bytes(path: str, key: str) -> int | None:
    try:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                if line.startswith(f"{key}:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        multiplier = (
                            1024 if len(parts) < 3 or parts[2].lower() == "kb" else 1
                        )
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
    """Run Python GC and return unused glibc arenas to Linux."""
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
    parser = argparse.ArgumentParser(
        description=(
            "Parquet row ingester using acknowledged default ClickHouse async inserts"
        )
    )
    parser.add_argument("--dir", type=Path, required=True)
    parser.add_argument("--pattern", default="*.parquet")
    parser.add_argument("--database", default="default")
    parser.add_argument("--table", default="quotes")
    parser.add_argument("--create-sql", type=Path, default=SCRIPT_DIR / "create.sql")
    parser.add_argument("--parallel", type=int, required=True)
    parser.add_argument(
        "--batch-size",
        type=int,
        required=True,
        help=(
            "Maximum rows per logical insert. Consecutive row groups from the "
            "same file are combined when they fit; an individual larger row "
            "group is split into inserts of this size."
        ),
    )
    parser.add_argument("--format", choices=FORMATS, default="CSV")
    parser.add_argument(
        "--target-eps",
        type=float,
        default=0.0,
        help="Maximum global logical-insert start rate in rows/s; 0 is unconstrained.",
    )
    parser.add_argument(
        "--queue-depth",
        type=int,
        default=0,
        help="Bounded FIFO depth in logical insert tasks; 0 uses 2x --parallel.",
    )
    parser.add_argument("--insert-timeout", type=float, default=120.0)
    parser.add_argument("--max-retries", type=int, default=8)
    parser.add_argument("--retry-base-seconds", type=float, default=0.5)
    parser.add_argument("--max-files", type=int)
    parser.add_argument(
        "--max-row-groups",
        type=int,
        help="Process at most this many whole row groups across selected files.",
    )
    parser.add_argument("--metrics-interval", type=float, default=10.0)
    parser.add_argument(
        "--memory-trim-interval",
        type=float,
        default=60.0,
        help="Run Python GC and glibc malloc_trim at this interval; 0 disables.",
    )
    parser.add_argument(
        "--min-system-available-gib",
        type=float,
        default=0.0,
        help="Stop if Linux MemAvailable drops below this value; 0 disables.",
    )
    parser.add_argument(
        "--quiet-worker-logs",
        action="store_true",
        help="Suppress per-row-group lines; aggregate boxed metrics remain enabled.",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("results/ingest"))
    parser.add_argument("--progress-file", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument(
        "--allow-nonempty-table",
        action="store_true",
        help="Allow appending to a nonempty table; fresh benchmark tables are safer.",
    )
    args = parser.parse_args()
    if args.parallel < 1 or args.batch_size < 1:
        parser.error("--parallel and --batch-size must be positive")
    if args.target_eps < 0:
        parser.error("--target-eps must be >= 0")
    if args.queue_depth < 0:
        parser.error("--queue-depth must be >= 0")
    if args.insert_timeout <= 0:
        parser.error("--insert-timeout must be positive")
    if args.max_retries < 0 or args.retry_base_seconds < 0:
        parser.error("--max-retries and --retry-base-seconds must be >= 0")
    if args.metrics_interval <= 0 or args.memory_trim_interval < 0:
        parser.error(
            "--metrics-interval must be positive and "
            "--memory-trim-interval must be >= 0"
        )
    if args.min_system_available_gib < 0:
        parser.error("--min-system-available-gib must be >= 0")
    if args.max_files is not None and args.max_files < 1:
        parser.error("--max-files must be positive")
    if args.max_row_groups is not None and args.max_row_groups < 1:
        parser.error("--max-row-groups must be positive")
    if args.format == "JSONEachRow":
        try:
            import pandas  # noqa: F401
        except ImportError:
            parser.error("--format JSONEachRow requires pandas")
    return args


def selected_files(directory: Path, pattern: str, max_files: int | None) -> list[Path]:
    files = sorted(directory.glob(pattern))
    if max_files is not None:
        files = files[:max_files]
    if not files:
        raise RuntimeError(f"no files matched {directory}/{pattern}")
    return files


def iter_tasks(
    files: list[Path],
    max_row_groups: int | None,
    batch_size: int,
) -> Iterator[Task]:
    task_index = 0
    selected_row_groups = 0
    for file_path in files:
        metadata = pq.ParquetFile(file_path).metadata
        pending_row_groups: list[int] = []
        pending_rows = 0
        for row_group in range(metadata.num_row_groups):
            if (
                max_row_groups is not None
                and selected_row_groups >= max_row_groups
            ):
                if pending_row_groups:
                    task_index += 1
                    yield Task(
                        index=task_index,
                        file_path=file_path,
                        row_groups=tuple(pending_row_groups),
                        rows=pending_rows,
                    )
                return

            row_group_rows = metadata.row_group(row_group).num_rows
            if pending_row_groups and pending_rows + row_group_rows > batch_size:
                task_index += 1
                yield Task(
                    index=task_index,
                    file_path=file_path,
                    row_groups=tuple(pending_row_groups),
                    rows=pending_rows,
                )
                pending_row_groups = []
                pending_rows = 0

            pending_row_groups.append(row_group)
            pending_rows += row_group_rows
            selected_row_groups += 1

            # A single row group can itself exceed --batch-size.  It remains
            # one FIFO task and is split into multiple logical inserts by the
            # worker, preserving the historical small-batch behavior.
            if pending_rows >= batch_size:
                task_index += 1
                yield Task(
                    index=task_index,
                    file_path=file_path,
                    row_groups=tuple(pending_row_groups),
                    rows=pending_rows,
                )
                pending_row_groups = []
                pending_rows = 0

        # Never combine row groups across file boundaries.  This keeps each
        # task's source identity and deduplication token unambiguous.
        if pending_row_groups:
            task_index += 1
            yield Task(
                index=task_index,
                file_path=file_path,
                row_groups=tuple(pending_row_groups),
                rows=pending_rows,
            )


def scan_inputs(
    files: list[Path],
    max_row_groups: int | None,
    batch_size: int,
) -> tuple[int, int, int]:
    total_tasks = 0
    total_row_groups = 0
    expected_rows = 0
    for task in iter_tasks(files, max_row_groups, batch_size):
        total_tasks += 1
        total_row_groups += len(task.row_groups)
        expected_rows += task.rows
    return total_tasks, total_row_groups, expected_rows


def _list_to_ch_array_literal(column: pa.Array) -> pa.Array:
    as_strings = pc.cast(column, pa.list_(pa.string()))
    joined = pc.binary_join(as_strings, ",")
    return pc.binary_join_element_wise("[", joined, "]", "")


def make_list_transform(schema: pa.Schema):
    list_columns = [
        index
        for index, field_value in enumerate(schema)
        if pa.types.is_list(field_value.type)
        or pa.types.is_large_list(field_value.type)
    ]
    if not list_columns:
        return None

    def transform(batch: pa.RecordBatch) -> pa.RecordBatch:
        arrays = list(batch.columns)
        for index in list_columns:
            arrays[index] = _list_to_ch_array_literal(arrays[index])
        return pa.RecordBatch.from_arrays(arrays, names=batch.schema.names)

    return transform


def serialize_csv(batch: pa.RecordBatch) -> bytes:
    buffer = BytesIO()
    pacsv.write_csv(batch, buffer, _CSV_OPTIONS)
    return buffer.getvalue()


def serialize_jsoneachrow(batch: pa.RecordBatch) -> bytes:
    frame = batch.to_pandas()
    return frame.to_json(orient="records", lines=True, date_format="iso").encode(
        "utf-8"
    )


SERIALIZERS = {
    "CSV": serialize_csv,
    "JSONEachRow": serialize_jsoneachrow,
}
FORMAT_INSERT_SETTINGS = {
    "CSV": {},
    "JSONEachRow": {"date_time_input_format": "best_effort"},
}


def normalize_host(value: str) -> str:
    return re.sub(r"^https?://", "", value.strip()).rstrip("/").split(":", 1)[0]


def make_client(args: argparse.Namespace, database: str):
    return clickhouse_connect.get_client(
        host=args.host,
        port=args.port,
        username=args.user,
        password=args.password,
        database=database,
        secure=True,
        send_receive_timeout=args.insert_timeout,
        settings=dict(CLIENT_ASYNC_SETTINGS),
    )


def close_client(client: Any) -> None:
    close = getattr(client, "close", None)
    if callable(close):
        close()


def effective_async_settings(client: Any) -> dict[str, str]:
    result = client.query(
        "SELECT name, toString(value) "
        "FROM system.settings "
        "WHERE name = 'wait_for_async_insert' OR name LIKE 'async_insert%' "
        "ORDER BY name"
    )
    return {str(name): str(value) for name, value in result.result_rows}


def setting_enabled(value: str | None) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def validate_effective_async_settings(settings: dict[str, str]) -> None:
    failures = [
        f"{name}={settings.get(name)!r}"
        for name, expected in REQUIRED_EFFECTIVE_SETTINGS.items()
        if setting_enabled(settings.get(name)) is not expected
    ]
    if failures:
        raise RuntimeError(
            "required acknowledged async-insert settings are not effective: "
            + ", ".join(failures)
            + ". This client only sets async_insert=1 and "
            "async_insert_deduplicate=1; configure wait_for_async_insert=1 "
            "in the user/profile/server defaults."
        )


def apply_schema(client: Any, create_sql: Path) -> None:
    raw_sql = create_sql.read_text(encoding="utf-8")
    cleaned = re.sub(r"--[^\n]*", "", raw_sql)
    statements = [
        statement.strip() for statement in cleaned.split(";") if statement.strip()
    ]
    for index, statement in enumerate(statements, start=1):
        head = " ".join(statement.split())[:100]
        print(
            f"DDL {index}/{len(statements)}: {head}"
            f"{'...' if len(' '.join(statement.split())) > 100 else ''}",
            flush=True,
        )
        client.command(statement)


def deterministic_deduplication_token(
    run_id: str,
    input_root: Path,
    task: Task,
    batch_index: int,
    rows: int,
) -> str:
    try:
        source = str(task.file_path.relative_to(input_root))
    except ValueError:
        source = str(task.file_path)
    row_groups = ",".join(str(value) for value in task.row_groups)
    identity = f"{run_id}\x1f{source}\x1f{row_groups}\x1f{batch_index}\x1f{rows}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def format_row_groups(row_groups: tuple[int, ...]) -> str:
    if len(row_groups) == 1:
        return str(row_groups[0])
    if row_groups == tuple(range(row_groups[0], row_groups[-1] + 1)):
        return f"{row_groups[0]}-{row_groups[-1]}"
    return ",".join(str(value) for value in row_groups)


def send_with_retry(
    client: Any,
    payload: bytes,
    rows: int,
    token: str,
    columns: list[str],
    counters: Counters,
    args: argparse.Namespace,
) -> Any:
    insert_settings = {
        **FORMAT_INSERT_SETTINGS[args.format],
        "insert_deduplication_token": token,
    }
    for attempt in range(args.max_retries + 1):
        counters.update(insert_attempts=1)
        try:
            client.raw_insert(
                args.table,
                insert_block=payload,
                column_names=columns,
                fmt=args.format,
                settings=insert_settings,
            )
            increments = {
                "acknowledged_rows": rows,
                "acknowledged_wire_bytes": len(payload),
                "acknowledged_inserts": 1,
            }
            if attempt:
                increments.update(
                    recovered_retried_inserts=1,
                    recovered_retried_rows=rows,
                )
            counters.update(**increments)
            return client
        except Exception as exc:
            if attempt >= args.max_retries:
                raise
            counters.update(retries=1)
            delay = min(30.0, args.retry_base_seconds * (2**attempt))
            print(
                f"retrying token={token[:12]} rows={rows:,} "
                f"attempt={attempt + 2}/{args.max_retries + 1} in {delay:.1f}s: {exc}",
                file=sys.stderr,
                flush=True,
            )
            try:
                close_client(client)
            except Exception:
                pass
            if delay > 0:
                time.sleep(delay)
            client = make_client(args, args.database)
    raise AssertionError("unreachable")


def produce_tasks(
    task_queue: queue.Queue[Any],
    files: list[Path],
    args: argparse.Namespace,
    stop_event: threading.Event,
    counters: Counters,
) -> None:
    try:
        for task in iter_tasks(files, args.max_row_groups, args.batch_size):
            while not stop_event.is_set():
                try:
                    task_queue.put(task, timeout=0.2)
                    break
                except queue.Full:
                    continue
            if stop_event.is_set():
                return
        for _ in range(args.parallel):
            while not stop_event.is_set():
                try:
                    task_queue.put(_SENTINEL, timeout=0.2)
                    break
                except queue.Full:
                    continue
    except Exception as exc:
        counters.add_error(f"task producer: {exc}")
        print(f"ERROR: task producer: {exc}", file=sys.stderr, flush=True)
        stop_event.set()


def worker(
    worker_id: int,
    task_queue: queue.Queue[Any],
    stop_event: threading.Event,
    counters: Counters,
    rate_limiter: RateLimiter,
    columns: list[str],
    args: argparse.Namespace,
) -> None:
    client = None
    current_file_path: Path | None = None
    current_parquet: pq.ParquetFile | None = None
    current_transform = None
    counters.update(active_workers=1)
    try:
        client = make_client(args, args.database)
        while True:
            if stop_event.is_set():
                return
            try:
                item = task_queue.get(timeout=0.2)
            except queue.Empty:
                continue
            if item is _SENTINEL:
                task_queue.task_done()
                return
            task: Task = item
            task_started = time.monotonic()
            task_rows = 0
            task_bytes = 0
            task_inserts = 0
            try:
                if task.file_path != current_file_path:
                    current_parquet = pq.ParquetFile(task.file_path)
                    current_file_path = task.file_path
                    current_transform = (
                        make_list_transform(current_parquet.schema_arrow)
                        if args.format == "CSV"
                        else None
                    )
                if current_parquet is None:
                    raise AssertionError("Parquet reader was not initialized")
                task_table = None
                combined_table = None
                if len(task.row_groups) == 1:
                    source_batches = current_parquet.iter_batches(
                        batch_size=args.batch_size,
                        row_groups=list(task.row_groups),
                        columns=columns,
                    )
                else:
                    task_table = current_parquet.read_row_groups(
                        list(task.row_groups),
                        columns=columns,
                    )
                    combined_table = task_table.combine_chunks()
                    source_batches = combined_table.to_batches(
                        max_chunksize=args.batch_size
                    )
                for batch_index, source_batch in enumerate(source_batches):
                    if stop_event.is_set():
                        return
                    batch = (
                        current_transform(source_batch)
                        if current_transform is not None
                        else source_batch
                    )
                    payload = SERIALIZERS[args.format](batch)
                    rows = batch.num_rows
                    token = deterministic_deduplication_token(
                        args.run_id,
                        args.dir,
                        task,
                        batch_index,
                        rows,
                    )
                    if not rate_limiter.wait(rows, stop_event):
                        return
                    client = send_with_retry(
                        client,
                        payload,
                        rows,
                        token,
                        columns,
                        counters,
                        args,
                    )
                    task_rows += rows
                    task_bytes += len(payload)
                    task_inserts += 1
                    del payload, batch, source_batch
                del source_batches, combined_table, task_table
                if task_rows != task.rows:
                    raise RuntimeError(
                        f"task row-count mismatch: metadata={task.rows}, "
                        f"acknowledged={task_rows}"
                    )
                counters.update(completed_tasks=1)
                elapsed = time.monotonic() - task_started
                if not args.quiet_worker_logs:
                    print(
                        f"[worker {worker_id}] task={task.index} "
                        f"file={task.file_path.name} "
                        f"rgs={format_row_groups(task.row_groups)} "
                        f"rows={task_rows:,} inserts={task_inserts:,} "
                        f"wire_bytes={task_bytes:,} elapsed={elapsed:.3f}s "
                        f"rate={task_rows / elapsed if elapsed else 0:,.0f} rows/s",
                        flush=True,
                    )
            except Exception as exc:
                message = (
                    f"worker {worker_id}, {task.file_path.name} "
                    f"rgs {format_row_groups(task.row_groups)}: {exc}"
                )
                counters.add_error(message)
                print(
                    f"ERROR: {message}\n{traceback.format_exc()}",
                    file=sys.stderr,
                    flush=True,
                )
                stop_event.set()
            finally:
                task_queue.task_done()
    except Exception as exc:
        message = f"worker {worker_id} initialization: {exc}"
        counters.add_error(message)
        print(
            f"ERROR: {message}\n{traceback.format_exc()}",
            file=sys.stderr,
            flush=True,
        )
        stop_event.set()
    finally:
        current_parquet = None
        current_transform = None
        if client is not None:
            try:
                close_client(client)
            except Exception:
                pass
        counters.update(active_workers=-1, finished_workers=1)


def progress_payload(
    args: argparse.Namespace,
    counters: Counters,
    started_at: datetime,
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
    elapsed = (
        acknowledged_elapsed
        if done and acknowledged_elapsed is not None
        else wall_elapsed
    )
    acknowledged = snapshot["acknowledged_rows"]
    return {
        "schema_version": 1,
        "run_id": args.run_id,
        "updated_at": iso_utc(),
        "started_at": iso_utc(started_at),
        "finished": done,
        "database": args.database,
        "table": args.table,
        "insert_mode": "async_insert_acknowledged_default_flush_settings",
        "delivery_semantics": "idempotent_retry_via_insert_deduplication_token",
        "starting_table_rows": starting_table_rows,
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


def format_eta(seconds: float | None) -> str:
    if seconds is None:
        return "calculating"
    total = max(0, int(seconds))
    days, remainder = divmod(total, 86_400)
    hours, remainder = divmod(remainder, 3_600)
    minutes, secs = divmod(remainder, 60)
    return f"{days}d {hours:02d}h {minutes:02d}m {secs:02d}s"


def metrics_monitor(
    stop_event: threading.Event,
    finished_event: threading.Event,
    metrics_path: Path,
    progress_path: Path,
    args: argparse.Namespace,
    counters: Counters,
    started_at: datetime,
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
        payload["interval_ack_rows_per_sec"] = (
            delta_rows / delta_time if delta_time else 0.0
        )
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
                f"system MemAvailable fell to "
                f"{available_bytes / (1024**3):.2f} GiB, below "
                f"--min-system-available-gib={args.min_system_available_gib:g}"
            )
            counters.add_error(message)
            print(
                f"ERROR: {message}; stopping after in-flight inserts",
                file=sys.stderr,
                flush=True,
            )
            stop_event.set()

        average_eps = payload["average_ack_rows_per_sec"]
        remaining_rows = max(0, expected_rows - payload["acknowledged_rows"])
        eta = format_eta(remaining_rows / average_eps if average_eps else None)
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
        arrow_gib = (
            arrow_allocated / (1024**3) if arrow_allocated is not None else float("nan")
        )
        reclaimed_gib = payload["memory"]["trim_reclaimed_rss_bytes"] / (1024**3)
        rule = "=" * 132
        print(
            f"\n{rule}\n"
            f"INGEST STATUS | elapsed={payload['elapsed_sec']:,.1f}s | "
            f"progress={progress_pct:.4f}% | "
            f"acknowledged={payload['acknowledged_rows']:,}/{expected_rows:,}\n"
            f"AVERAGE ACK EPS: {average_eps:,.0f}    |    "
            f"INSTANT ACK EPS: {payload['interval_ack_rows_per_sec']:,.0f}    |    "
            f"ETA: {eta}\n"
            f"WORKERS: {payload['active_workers']}/{args.parallel} active    |    "
            f"TASKS: {payload['completed_tasks']:,}/{total_tasks:,}    |    "
            f"ACKNOWLEDGED INSERTS: {payload['acknowledged_inserts']:,}    |    "
            f"ATTEMPTS: {payload['insert_attempts']:,}\n"
            f"RETRIES (CUMULATIVE): {payload['retries']:,}    |    "
            f"RECOVERED RETRIED INSERTS: "
            f"{payload['recovered_retried_inserts']:,} "
            f"({payload['recovered_retried_rows']:,} rows)\n"
            f"MEMORY: RSS={rss_gib:.2f} GiB | AVAILABLE={available_gib:.2f} GiB | "
            f"ARROW={arrow_gib:.2f} GiB | "
            f"TRIMS={payload['memory']['trim_attempts']:,} | "
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
    fqdn = os.environ.get("FQDN")
    password = os.environ.get("PASSWORD")
    if not fqdn or password is None:
        raise RuntimeError("FQDN and PASSWORD environment variables are required")
    args.host = normalize_host(fqdn)
    args.port = 8443
    args.user = os.environ.get("CH_USER", "default")
    args.password = password
    args.dir = args.dir.expanduser().resolve()
    args.create_sql = args.create_sql.expanduser().resolve()
    args.output_dir = args.output_dir.expanduser().resolve()
    args.progress_file = (
        (args.progress_file or args.output_dir / "ingest_progress.json")
        .expanduser()
        .resolve()
    )
    args.run_id = args.run_id or (
        f"ch-async-default-{utc_now().strftime('%Y%m%dT%H%M%SZ')}-"
        f"{uuid.uuid4().hex[:8]}"
    )
    metrics_path = args.output_dir / "ingest_metrics.jsonl"
    manifest_path = args.output_dir / "ingest_manifest.json"
    summary_path = args.output_dir / "ingest_summary.json"

    if not args.dir.is_dir():
        raise RuntimeError(f"not a directory: {args.dir}")
    if not args.create_sql.is_file():
        raise RuntimeError(f"create SQL not found: {args.create_sql}")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    files = selected_files(args.dir, args.pattern, args.max_files)
    total_tasks, total_row_groups, expected_rows = scan_inputs(
        files,
        args.max_row_groups,
        args.batch_size,
    )
    if total_tasks == 0:
        raise RuntimeError("selected input contains no row groups")
    columns = pq.ParquetFile(files[0]).schema_arrow.names

    admin = make_client(args, "default")
    database_client = None
    stop_event = threading.Event()
    finished_event = threading.Event()
    counters = Counters()
    memory_telemetry = MemoryTelemetry()

    def request_stop(_signum: int, _frame: Any) -> None:
        print(
            "Stop requested; finishing in-flight acknowledged inserts...",
            file=sys.stderr,
            flush=True,
        )
        stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    try:
        admin.command(f"CREATE DATABASE IF NOT EXISTS `{args.database}`")
        database_client = make_client(args, args.database)
        apply_schema(database_client, args.create_sql)
        settings = effective_async_settings(database_client)
        validate_effective_async_settings(settings)
        server_version = str(database_client.command("SELECT version()"))
        starting_rows = int(
            database_client.command(
                f"SELECT count() FROM `{args.database}`.`{args.table}`"
            )
        )
        if starting_rows and not args.allow_nonempty_table:
            raise RuntimeError(
                f"{args.database}.{args.table} already contains "
                f"{starting_rows:,} rows; use a fresh database or explicitly pass "
                "--allow-nonempty-table"
            )

        manifest = {
            "schema_version": 1,
            "run_id": args.run_id,
            "created_at": iso_utc(),
            "server_version": server_version,
            "host": args.host,
            "database": args.database,
            "table": args.table,
            "input_dir": str(args.dir),
            "pattern": args.pattern,
            "files": [str(path) for path in files],
            "row_groups": total_row_groups,
            "logical_tasks": total_tasks,
            "expected_input_rows": expected_rows,
            "parallel": args.parallel,
            "queue_depth": args.queue_depth or 2 * args.parallel,
            "target_eps": args.target_eps or None,
            "batch_size": args.batch_size,
            "format": args.format,
            "requested_client_async_settings": CLIENT_ASYNC_SETTINGS,
            "effective_async_settings": settings,
            "wait_for_async_insert_overridden_by_client": False,
            "memory_trim_interval": args.memory_trim_interval,
            "min_system_available_gib": args.min_system_available_gib,
        }
        atomic_write_json(manifest_path, manifest)

        # Benchmark time starts after schema creation, settings validation, and
        # the initial row-count guard.  Only the actual continuous ingest is
        # included in acknowledged throughput.
        rate_limiter = RateLimiter(args.target_eps)
        started_at = utc_now()
        started_monotonic = time.monotonic()
        queue_depth = args.queue_depth or 2 * args.parallel
        task_queue: queue.Queue[Any] = queue.Queue(maxsize=queue_depth)
        producer = threading.Thread(
            target=produce_tasks,
            name="ch-task-producer",
            args=(task_queue, files, args, stop_event, counters),
            daemon=True,
        )
        monitor = threading.Thread(
            target=metrics_monitor,
            name="ch-metrics-monitor",
            args=(
                stop_event,
                finished_event,
                metrics_path,
                args.progress_file,
                args,
                counters,
                started_at,
                started_monotonic,
                starting_rows,
                expected_rows,
                total_tasks,
                memory_telemetry,
                rate_limiter,
            ),
            daemon=True,
        )
        workers = [
            threading.Thread(
                target=worker,
                name=f"ch-async-writer-{index + 1}",
                args=(
                    index + 1,
                    task_queue,
                    stop_event,
                    counters,
                    rate_limiter,
                    columns,
                    args,
                ),
            )
            for index in range(args.parallel)
        ]

        print(f"Run ID:          {args.run_id}")
        print(f"Target:          {args.database}.{args.table}")
        print(f"ClickHouse:      {server_version} at {args.host}:{args.port}")
        print(f"Input:           {args.dir}/{args.pattern}")
        print(f"Files:           {len(files)}")
        print(f"Row groups:      {total_row_groups:,}")
        print(f"Logical tasks:   {total_tasks:,}")
        print(f"Expected rows:   {expected_rows:,}")
        print(f"Starting rows:   {starting_rows:,}")
        print(f"Workers:         {args.parallel} threads, one client per worker")
        print(f"FIFO depth:      {queue_depth} logical insert tasks")
        print(
            f"Target rate:     {args.target_eps:,.0f} acknowledged rows/s"
            if args.target_eps
            else "Target rate:     unconstrained"
        )
        print(f"Batch target:    {args.batch_size:,} rows per logical insert")
        print(f"Wire format:     {args.format}")
        print(
            "Client settings: async_insert=1, async_insert_deduplicate=1 "
            "(only async settings overridden)"
        )
        print(
            f"Effective ACK:   wait_for_async_insert="
            f"{settings['wait_for_async_insert']} (server/profile default)"
        )
        worker_log_status = "suppressed" if args.quiet_worker_logs else "enabled"
        print(
            f"Worker logs:     {worker_log_status}; "
            f"boxed metrics every {args.metrics_interval:g}s"
        )
        print(f"Progress:        {args.progress_file}")
        print(
            f"Memory safety:   trim every {args.memory_trim_interval:g}s; "
            f"stop below {args.min_system_available_gib:g} GiB available "
            f"({'disabled' if not args.min_system_available_gib else 'enabled'})"
        )

        monitor.start()
        for thread in workers:
            thread.start()
        producer.start()
        for thread in workers:
            thread.join()
        finished_event.set()
        monitor.join(timeout=max(2.0, args.metrics_interval + 1.0))

        final_rows = int(
            database_client.command(
                f"SELECT count() FROM `{args.database}`.`{args.table}`"
            )
        )
        observed_delta = final_rows - starting_rows
        current = counters.snapshot()
        if (
            not stop_event.is_set()
            and not current["errors"]
            and current["acknowledged_rows"] == expected_rows
            and observed_delta != current["acknowledged_rows"]
        ):
            counters.add_error(
                "server row-count reconciliation failed: "
                f"acknowledged={current['acknowledged_rows']}, "
                f"observed_delta={observed_delta}"
            )

        final_progress = progress_payload(
            args,
            counters,
            started_at,
            started_monotonic,
            starting_rows,
            expected_rows,
            total_tasks,
            done=True,
            memory_telemetry=memory_telemetry,
            rate_limiter=rate_limiter,
        )
        final_progress.update(
            {
                "stopped_early": stop_event.is_set(),
                "server_table_rows_after_run": final_rows,
                "server_observed_rows_added": observed_delta,
                "effective_async_settings": settings,
                "server_version": server_version,
            }
        )
        atomic_write_json(args.progress_file, final_progress)
        append_jsonl(metrics_path, final_progress)
        atomic_write_json(summary_path, final_progress)

        acknowledged = final_progress["acknowledged_rows"]
        errors = final_progress["errors"]
        print("\n========================= SUMMARY =========================")
        print(f"Acknowledged:    {acknowledged:,}/{expected_rows:,} rows")
        print(f"Server delta:    {observed_delta:,} rows")
        print(f"Duration:        {final_progress['elapsed_sec']:.1f}s")
        print(
            f"Average ACK EPS: {final_progress['average_ack_rows_per_sec']:,.0f} rows/s"
        )
        print(f"Retries:         {final_progress['retries']} cumulative")
        print(
            f"Recovered:       {final_progress['recovered_retried_inserts']:,} "
            f"retried inserts ({final_progress['recovered_retried_rows']:,} rows)"
        )
        print(f"Errors:          {len(errors)}")
        print(f"Summary:         {summary_path}")
        print("===========================================================")
        return 1 if errors or acknowledged != expected_rows else 0
    finally:
        finished_event.set()
        if database_client is not None:
            try:
                close_client(database_client)
            except Exception:
                pass
        try:
            close_client(admin)
        except Exception:
            pass


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"FATAL: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
