#!/usr/bin/env python3
"""
Parallel one-shot ingester for a directory of Parquet files into ClickHouse.

Each (file, row_group) is dispatched exactly once. Files are enqueued in
sorted-name order; row groups within a file are enqueued in index order.
Workers pull tasks from a shared queue, so parallelism is not capped by
file count.

Requires:
    pip install clickhouse-connect pyarrow thriftpy2

Also requires parquet.thrift (Apache official schema) sibling to this script.

True zero-decode ingestion: for each row group, copy the column-chunk bytes
verbatim from the source parquet file and synthesize a fresh single-row-group
parquet file around them. No Arrow decode, no decompression, no recompression.
The footer is parsed and re-serialized with thriftpy2's spec-strict
TCompactProtocol, so the output is readable by Apache thrift parsers
(pyarrow, ClickHouse).

Usage:
    export FQDN="your-service.clickhouse.cloud"
    export PASSWORD="your_password"
    export CH_USER="your_user"

    python3 ingest_parquet_dir.py \
        --dir ./parquet_data \
        --pattern '*.parquet' \
        --database hits_100b \
        --table hits \
        --parallel 16 \
        --create-sql create.sql \
        --async-insert-busy-timeout-max-ms 5000 \
        --async-insert-max-data-size 10485760
"""

import argparse
import multiprocessing as mp
import os
import re
import struct
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import clickhouse_connect
import pyarrow.parquet as pq
import thriftpy2
from thriftpy2.protocol import TCompactProtocol
from thriftpy2.transport import TMemoryBuffer

# Load Apache parquet.thrift at module import. parquet.thrift must sit next
# to this script. thriftpy2 generates Python classes from it at runtime.
_THRIFT_PATH = Path(__file__).resolve().parent / "parquet.thrift"
if not _THRIFT_PATH.exists():
    sys.exit(f"ERROR: parquet.thrift not found next to script: {_THRIFT_PATH}")
parquet_thrift = thriftpy2.load(str(_THRIFT_PATH), module_name="parquet_thrift")

FQDN     = os.environ["FQDN"]
PASSWORD = os.environ["PASSWORD"]
USER     = os.environ.get("CH_USER", "default")


def make_client(database: str, use_async_insert: bool, async_insert_busy_timeout_max_ms: int, async_insert_max_data_size: int):
    settings = {}
    if use_async_insert:
        settings = {
            "async_insert":                          1,
            "wait_for_async_insert":                 0,
            "async_insert_deduplicate":              0,
            "async_insert_use_adaptive_busy_timeout": 0,
            "async_insert_busy_timeout_max_ms":      async_insert_busy_timeout_max_ms,
            "async_insert_max_data_size":            async_insert_max_data_size,
        }
    else:
        # Synchronous inserts: each request returns only after the server has
        # written the part. Bounds server memory by what one decode allocates.
        settings = {
            "async_insert": 0,
        }
    return clickhouse_connect.get_client(
        host=FQDN,
        port=8443,
        username=USER,
        password=PASSWORD,
        database=database,
        secure=True,
        send_receive_timeout=120,
        settings=settings,
    )


def enumerate_tasks(directory: Path, pattern: str, max_files: int | None,
                    row_groups_per_insert: int) -> tuple[list[tuple[Path, list[int], int]], list[Path]]:
    """
    Returns (tasks, files). Each task is (file_path, [rg_indices], total_rows).
    Row groups are grouped in batches of row_groups_per_insert *within the same
    file* — we never combine row groups across files. The last batch in each
    file may be smaller than N.
    """
    files = sorted(directory.glob(pattern))
    if not files:
        sys.exit(f"ERROR: no files matched {directory}/{pattern}")
    if max_files is not None:
        files = files[:max_files]
    if row_groups_per_insert < 1:
        sys.exit("ERROR: --row-groups-per-insert must be >= 1")

    tasks: list[tuple[Path, list[int], int]] = []
    for f in files:
        pf = pq.ParquetFile(f)
        meta = pf.metadata
        n_rgs = meta.num_row_groups
        for start in range(0, n_rgs, row_groups_per_insert):
            rg_indices = list(range(start, min(start + row_groups_per_insert, n_rgs)))
            total_rows = sum(meta.row_group(i).num_rows for i in rg_indices)
            tasks.append((f, rg_indices, total_rows))
    return tasks, files


def load_source_fmd(path: Path):
    """Read and parse FileMetaData from a parquet file using thriftpy2."""
    with open(path, "rb") as f:
        f.seek(-8, 2)
        footer_len = struct.unpack("<I", f.read(4))[0]
        magic = f.read(4)
        if magic != b"PAR1":
            raise ValueError(f"Not a parquet file (bad trailer): {path}")
        f.seek(-(8 + footer_len), 2)
        footer_bytes = f.read(footer_len)

    buf   = TMemoryBuffer(footer_bytes)
    proto = TCompactProtocol(buf)
    fmd   = parquet_thrift.FileMetaData()
    fmd.read(proto)
    return fmd


def extract_row_groups_as_parquet_bytes(path: Path, rg_indices: list[int], fmd_cache: dict) -> tuple[bytes, int]:
    """
    Build a complete parquet file containing the given row groups (from the
    same source file), copying column-chunk bytes verbatim and writing a
    fresh footer via thriftpy2.

    Each row group lands at a different offset in the new file, so each one
    needs its column offsets rewritten with a different shift. The new
    FileMetaData lists all N row groups with their adjusted offsets.

    Mutates the cached FileMetaData in place (workers are single-threaded so
    this is safe per process), then restores the originals in a finally.
    """
    if path not in fmd_cache:
        fmd_cache[path] = load_source_fmd(path)
    fmd = fmd_cache[path]

    # For each requested row group: locate byte range and read raw bytes.
    rgs: list = []   # list of (src_rg, raw_bytes, src_rg_start_offset)
    for rg_idx in rg_indices:
        src_rg = fmd.row_groups[rg_idx]
        starts, ends = [], []
        for cc in src_rg.columns:
            cm = cc.meta_data
            start = cm.dictionary_page_offset if cm.dictionary_page_offset is not None else cm.data_page_offset
            starts.append(start)
            ends.append(start + cm.total_compressed_size)
        rg_start = min(starts)
        rg_end   = max(ends)
        with open(path, "rb") as f:
            f.seek(rg_start)
            raw = f.read(rg_end - rg_start)
        rgs.append((src_rg, raw, rg_start))

    # Save state we are about to mutate (for restoration).
    saved_row_groups = fmd.row_groups
    saved_num_rows   = fmd.num_rows
    saved_cols = [
        [(cc.meta_data.data_page_offset,
          cc.meta_data.dictionary_page_offset,
          cc.meta_data.bloom_filter_offset,
          cc.file_offset,
          cc.offset_index_offset,
          cc.offset_index_length,
          cc.column_index_offset,
          cc.column_index_length)
         for cc in src_rg.columns]
        for src_rg, _, _ in rgs
    ]

    try:
        # Walk the row groups, placing each at the next free offset in the
        # output file (which starts at offset 4, right after PAR1). Compute
        # per-row-group shift = (new_offset - original_offset).
        cumulative = 4  # bytes already laid down in the output file
        total_rows = 0
        for src_rg, raw, src_rg_start in rgs:
            shift = cumulative - src_rg_start
            for cc in src_rg.columns:
                cm = cc.meta_data
                cm.data_page_offset += shift
                if cm.dictionary_page_offset is not None:
                    cm.dictionary_page_offset += shift
                cc.file_offset = (cm.dictionary_page_offset
                                  if cm.dictionary_page_offset is not None
                                  else cm.data_page_offset)
                # Null out references to structures we don't copy.
                cm.bloom_filter_offset = None
                cc.offset_index_offset = None
                cc.offset_index_length = None
                cc.column_index_offset = None
                cc.column_index_length = None
            cumulative += len(raw)
            total_rows += src_rg.num_rows

        fmd.row_groups = [src_rg for src_rg, _, _ in rgs]
        fmd.num_rows   = total_rows

        # Serialize new footer via thriftpy2
        footer_buf   = TMemoryBuffer()
        footer_proto = TCompactProtocol(footer_buf)
        fmd.write(footer_proto)
        footer_bytes = footer_buf.getvalue()

        # Assemble: PAR1 + raw_rg0 + raw_rg1 + ... + footer + footer_len + PAR1
        out = BytesIO()
        out.write(b"PAR1")
        for _, raw, _ in rgs:
            out.write(raw)
        out.write(footer_bytes)
        out.write(struct.pack("<I", len(footer_bytes)))
        out.write(b"PAR1")

        return out.getvalue(), total_rows
    finally:
        for (src_rg, _, _), saved in zip(rgs, saved_cols):
            for cc, (dpo, dico, bfo, foff, oio, oil, cio, cil) in zip(src_rg.columns, saved):
                cc.meta_data.data_page_offset       = dpo
                cc.meta_data.dictionary_page_offset = dico
                cc.meta_data.bloom_filter_offset    = bfo
                cc.file_offset                      = foff
                cc.offset_index_offset              = oio
                cc.offset_index_length              = oil
                cc.column_index_offset              = cio
                cc.column_index_length              = cil
        fmd.row_groups = saved_row_groups
        fmd.num_rows   = saved_num_rows


# Backward-compat alias for the single-row-group case (used by debug_synth.py)
def extract_row_group_as_parquet_bytes(path: Path, rg_idx: int, fmd_cache: dict) -> tuple[bytes, int]:
    return extract_row_groups_as_parquet_bytes(path, [rg_idx], fmd_cache)


def worker(
    worker_id: int,
    task_queue: mp.Queue,
    database: str,
    table: str,
    use_async_insert: bool,
    async_insert_busy_timeout_max_ms: int,
    async_insert_max_data_size: int,
    target_rps: float,
    shared_total_rows,            # mp.Value('q')
    global_start_time: float,
    error_queue: mp.Queue,
):
    client    = make_client(database, use_async_insert, async_insert_busy_timeout_max_ms, async_insert_max_data_size)
    inserts   = 0
    rows_sent = 0
    w_start   = time.time()
    fmd_cache: dict = {}

    while True:
        item = task_queue.get()
        if item is None:
            break
        task_idx, total_tasks, file_path, rg_indices, expected_rows = item

        try:
            t0 = time.time()
            parquet_bytes, row_count = extract_row_groups_as_parquet_bytes(file_path, rg_indices, fmd_cache)
            t_extract = time.time() - t0

            t1 = time.time()
            client.raw_insert(table, insert_block=parquet_bytes, fmt="Parquet")
            t_insert = time.time() - t1

            inserts   += 1
            rows_sent += row_count

            # GLOBAL rate limit: shared counter across all workers. Atomically
            # bump the global row count, then compute throttle based on how
            # far ahead of target we are *in aggregate*. This lets fast
            # workers absorb the slack from slow ones so total throughput
            # converges to target_rps regardless of per-worker latency variance.
            throttle_sec = 0.0
            if target_rps > 0:
                with shared_total_rows.get_lock():
                    shared_total_rows.value += row_count
                    global_total = shared_total_rows.value
                elapsed_global  = time.time() - global_start_time
                expected_global = elapsed_global * target_rps
                if global_total > expected_global:
                    excess_rows  = global_total - expected_global
                    throttle_sec = excess_rows / target_rps

            t_total = t_extract + t_insert
            throttle_str = f" THROTTLE={throttle_sec*1000:.0f}ms" if throttle_sec > 0 else ""
            # Render rg range compactly: "rg=0-3" or "rg=42" for singletons
            rg_str = (f"{rg_indices[0]}-{rg_indices[-1]}"
                      if len(rg_indices) > 1 else f"{rg_indices[0]}")
            print(
                f"[worker {worker_id}] task {task_idx}/{total_tasks} "
                f"file={file_path.name} rg={rg_str} rows={row_count:,} "
                f"extract={t_extract*1000:.0f}ms insert={t_insert*1000:.0f}ms "
                f"total={t_total*1000:.0f}ms{throttle_str}",
                flush=True,
            )

            if throttle_sec > 0:
                time.sleep(throttle_sec)
        except Exception as exc:
            tb  = traceback.format_exc()
            rg_str = f"{rg_indices[0]}-{rg_indices[-1]}" if len(rg_indices) > 1 else f"{rg_indices[0]}"
            msg = f"{file_path.name} rg {rg_str}: {exc}"
            error_queue.put(msg)
            print(f"[worker {worker_id}] ERROR: {msg}\n{tb}", file=sys.stderr, flush=True)

    duration = time.time() - w_start
    rate     = rows_sent / duration if duration > 0 else 0
    print(
        f"[worker {worker_id}] done — {inserts} inserts, {rows_sent:,} rows in {duration:.1f}s "
        f"({rate:,.0f} rows/s)",
        flush=True,
    )


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def live_eps_monitor(stop_event: threading.Event, database: str, table: str,
                     start_rows: int, start_time: float, interval: float):
    """
    Background thread: polls count() every `interval` seconds and prints
    rows delta and rolling EPS since the previous sample.
    """
    client = make_client("default", False, 0, 0)
    last_rows = start_rows
    last_t    = start_time
    while not stop_event.is_set():
        # Sleep in small slices so we can stop promptly when workers finish
        slept = 0.0
        while slept < interval and not stop_event.is_set():
            time.sleep(min(0.5, interval - slept))
            slept += 0.5
        if stop_event.is_set():
            break
        try:
            now      = time.time()
            cur_rows = client.command(f"SELECT count() FROM `{database}`.`{table}`")
            delta    = cur_rows - last_rows
            dt       = now - last_t
            rate     = delta / dt if dt > 0 else 0
            total    = cur_rows - start_rows
            elapsed  = now - start_time
            avg_rate = total / elapsed if elapsed > 0 else 0
            bar      = "═" * 88
            print(
                f"\n{bar}\n"
                f"  t={elapsed:7.1f}s    "
                f"rows={cur_rows:>14,}    "
                f"+{delta:>12,}    "
                f"inst={rate:>10,.0f} rows/s    "
                f"avg={avg_rate:>10,.0f} rows/s\n"
                f"{bar}",
                flush=True,
            )
            last_rows = cur_rows
            last_t    = now
        except Exception as exc:
            print(f"[live-eps] query failed: {exc}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir",                              required=True)
    parser.add_argument("--pattern",                          default="*.parquet")
    parser.add_argument("--database",                         default="default")
    parser.add_argument("--table",                            default="hits")
    parser.add_argument("--parallel",                         type=int, required=True)
    parser.add_argument("--max-files",                        type=int, default=None,
                        help="Only process the first N files (alphanumeric order). Useful when more files are still being copied in.")
    parser.add_argument("--create-sql",                       default="create.sql")
    parser.add_argument("--async-insert-busy-timeout-max-ms", type=int, default=60000,
                        help="Ignored when --no-async-insert is set.")
    parser.add_argument("--async-insert-max-data-size",       type=int, default=16777216,
                        help="Ignored when --no-async-insert is set.")
    parser.add_argument("--no-async-insert",                  action="store_true",
                        help="Use synchronous inserts (async_insert=0). Server allocates bounded memory per request.")
    parser.add_argument("--target-rps",                       type=int, default=0,
                        help="Target rows per second across ALL workers. Workers share a global row counter; "
                             "any worker can fire as long as the aggregate is at or below target, so fast workers "
                             "absorb slack from slow ones. Implies --no-async-insert. "
                             "This is a CEILING ONLY — it can only throttle workers down, never speed them up.")
    parser.add_argument("--live-eps-interval",                type=float, default=5.0,
                        help="Seconds between live EPS samples printed from the main process. 0 disables.")
    parser.add_argument("--row-groups-per-insert",            type=int, default=1,
                        help="Combine N consecutive row groups (same file) into one INSERT. Reduces part count "
                             "and amortizes per-insert overhead. Last batch per file may be smaller than N.")
    args = parser.parse_args()

    # --target-rps implies sync inserts
    use_async_insert = not (args.no_async_insert or args.target_rps > 0)
    target_rps       = float(args.target_rps)  # global target, 0 = unlimited

    directory  = Path(args.dir)
    create_sql = Path(args.create_sql)

    if not directory.is_dir():
        sys.exit(f"ERROR: not a directory: {directory}")
    if not create_sql.exists():
        sys.exit(f"ERROR: create SQL not found: {create_sql}")

    tasks, files = enumerate_tasks(directory, args.pattern, args.max_files, args.row_groups_per_insert)
    total_tasks  = len(tasks)
    total_rows   = sum(rc for _, _, rc in tasks)

    admin = make_client("default", use_async_insert, args.async_insert_busy_timeout_max_ms, args.async_insert_max_data_size)
    admin.command(f"CREATE DATABASE IF NOT EXISTS `{args.database}`")

    # Always run every statement in create.sql — they should be idempotent
    # (use `CREATE TABLE IF NOT EXISTS` / `CREATE MATERIALIZED VIEW IF NOT
    # EXISTS`) so re-runs are safe. clickhouse-connect's command() expects
    # one statement at a time, so split on `;` after stripping `--` line
    # comments (otherwise a `;` inside a comment splits the wrong way and
    # the leftover comment fragments break SQL parsing on the server).
    db_client = make_client(args.database, use_async_insert, args.async_insert_busy_timeout_max_ms, args.async_insert_max_data_size)
    raw_sql = create_sql.read_text()
    cleaned = re.sub(r"--[^\n]*", "", raw_sql)
    statements = [s.strip() for s in cleaned.split(";") if s.strip()]
    for stmt in statements:
        head = stmt.splitlines()[0][:80]
        print(f"Running DDL: {head}{'...' if len(stmt) > 80 else ''}")
        db_client.command(stmt)

    start_rows = admin.command(f"SELECT count() FROM `{args.database}`.`{args.table}`")
    start_ts   = now_utc()
    start_time = time.time()

    print(f"Start time:    {start_ts}")
    print(f"Directory:     {directory}  (pattern: {args.pattern})")
    print(f"Files:         {len(files)}")
    print(f"Tasks:         {total_tasks} ({args.row_groups_per_insert} row group(s) per insert)")
    print(f"Rows total:    {total_rows:,}")
    print(f"Target:        {args.database}.{args.table}")
    print(f"Workers:       {args.parallel}")
    if use_async_insert:
        print(f"Insert mode:   async (busy_timeout_max_ms={args.async_insert_busy_timeout_max_ms}, "
              f"max_data_size={args.async_insert_max_data_size})")
    else:
        print(f"Insert mode:   sync (async_insert=0)")
    if args.target_rps > 0:
        print(f"Target rate:   {args.target_rps:,} rows/s total (global limiter, shared across all workers)")
    else:
        print(f"Target rate:   unlimited")
    print(f"Starting rows: {start_rows:,}")
    print()
    print("File order:")
    for f in files:
        print(f"  {f.name}")
    print()

    task_queue:  mp.Queue = mp.Queue()
    error_queue: mp.Queue = mp.Queue()

    # Global shared row counter for the rate limiter. Workers atomically bump
    # it after each insert; throttle decisions are based on this aggregate.
    shared_total_rows = mp.Value('q', 0)

    # Enqueue tasks in strict file/row-group order. Workers will pick them up
    # roughly in this order — async_insert means physical ingest order is not
    # preserved anyway, which matches the agreed "dispatch order" semantics.
    for i, (path, rg_indices, rows) in enumerate(tasks, start=1):
        task_queue.put((i, total_tasks, path, rg_indices, rows))
    for _ in range(args.parallel):
        task_queue.put(None)  # sentinel per worker

    # Live EPS monitor thread (queries count() periodically)
    stop_monitor = threading.Event()
    monitor_thread = None
    if args.live_eps_interval > 0:
        monitor_thread = threading.Thread(
            target=live_eps_monitor,
            args=(stop_monitor, args.database, args.table, start_rows, start_time, args.live_eps_interval),
            daemon=True,
        )
        monitor_thread.start()

    processes = []
    for w in range(args.parallel):
        p = mp.Process(
            target=worker,
            args=(
                w + 1, task_queue,
                args.database, args.table,
                use_async_insert,
                args.async_insert_busy_timeout_max_ms,
                args.async_insert_max_data_size,
                target_rps,
                shared_total_rows,
                start_time,
                error_queue,
            ),
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    # Stop live EPS monitor
    stop_monitor.set()
    if monitor_thread is not None:
        monitor_thread.join(timeout=10)

    end_ts        = now_utc()
    elapsed       = time.time() - start_time
    final_rows    = admin.command(f"SELECT count() FROM `{args.database}`.`{args.table}`")
    rows_ingested = final_rows - start_rows

    errors = []
    while not error_queue.empty():
        errors.append(error_queue.get())

    print()
    print("==================== SUMMARY ====================")
    print(f"Start time:    {start_ts}")
    print(f"End time:      {end_ts}")
    print(f"Duration:      {elapsed:.1f}s (~{elapsed/60:.1f} min)")
    print(f"Files:         {len(files)}")
    print(f"Row groups:    {total_tasks}")
    print(f"Rows expected: {total_rows:,}")
    print(f"Starting rows: {start_rows:,}")
    print(f"Final rows*:   {final_rows:,}")
    print(f"Rows ingested: {rows_ingested:,}")
    print(f"Errors:        {len(errors)}")
    if errors:
        for e in errors:
            print(f"  {e}", file=sys.stderr)
    print("* wait_for_async_insert=0 — rows may still be buffering")
    print("=================================================")

    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
