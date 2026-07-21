#!/usr/bin/env python3
"""
Parallel row-streaming ingester for a directory of Parquet files into
ClickHouse, using async inserts.

Counterpart to ingest_parquet_dir.py, but instead of copying parquet
row-group bytes verbatim, each row group is DECODED with pyarrow and its
rows are streamed to ClickHouse as row-oriented inserts (CSV or
JSONEachRow) of --batch-size rows each. This simulates the "raw rows
arriving from endpoints" ingestion pattern (benchmark counterpart to
Snowpipe Streaming).

Task model matches ingest_parquet_dir.py: each (file, row_group) is
dispatched exactly once via a shared queue, so parallelism is not capped
by file count. Batches are cut WITHIN a row group; the last batch of a
row group may be smaller than --batch-size.

All inserts use async inserts (async_insert=1, wait_for_async_insert=0).

Requires:
    pip install clickhouse-connect pyarrow
    (pandas additionally required for --format JSONEachRow)

Usage:
    export FQDN="your-service.clickhouse.cloud"
    export PASSWORD="your_password"
    export CH_USER="your_user"

    python3 ingest_parquet_rows.py \
        --dir ./parquet_data \
        --pattern '*.parquet' \
        --database hits_100b \
        --table hits \
        --parallel 16 \
        --batch-size 50000 \
        --format CSV \
        --create-sql create.sql \
        --async-insert-busy-timeout-max-ms 5000 \
        --async-insert-max-data-size 10485760
"""

import argparse
import multiprocessing as mp
import os
import re
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import clickhouse_connect
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

FQDN     = os.environ["FQDN"]
PASSWORD = os.environ["PASSWORD"]
USER     = os.environ.get("CH_USER", "default")

FORMATS = ("CSV", "JSONEachRow")


def make_client(database: str, async_insert_busy_timeout_max_ms: int, async_insert_max_data_size: int):
    return clickhouse_connect.get_client(
        host=FQDN,
        port=8443,
        username=USER,
        password=PASSWORD,
        database=database,
        secure=True,
        send_receive_timeout=120,
        settings={
            "async_insert":                          1,
            "wait_for_async_insert":                 0,
            "async_insert_deduplicate":              0,
            "async_insert_use_adaptive_busy_timeout": 0,
            "async_insert_busy_timeout_max_ms":      async_insert_busy_timeout_max_ms,
            "async_insert_max_data_size":            async_insert_max_data_size,
        },
    )


def enumerate_tasks(directory: Path, pattern: str, max_files: int | None) -> tuple[list[tuple[Path, int, int]], list[Path]]:
    """Returns (tasks, files). Each task is (file_path, rg_index, num_rows)."""
    files = sorted(directory.glob(pattern))
    if not files:
        sys.exit(f"ERROR: no files matched {directory}/{pattern}")
    if max_files is not None:
        files = files[:max_files]

    tasks: list[tuple[Path, int, int]] = []
    for f in files:
        meta = pq.ParquetFile(f).metadata
        for i in range(meta.num_row_groups):
            tasks.append((f, i, meta.row_group(i).num_rows))
    return tasks, files


# ---------------------------------------------------------------------------
# Batch serialization: pyarrow RecordBatch -> row-oriented bytes
# ---------------------------------------------------------------------------

_CSV_OPTS = pacsv.WriteOptions(include_header=False)


def _list_to_ch_array_literal(col: pa.Array) -> pa.Array:
    """
    Vectorized list<numeric> -> string column of ClickHouse array literals,
    e.g. [1,2,3] (empty list -> []). All in Arrow C++, no per-row Python.
    The CSV writer quotes the string (it contains commas); ClickHouse parses
    quoted array literals in CSV natively.
    """
    as_str = pc.cast(col, pa.list_(pa.string()))
    joined = pc.binary_join(as_str, ",")
    return pc.binary_join_element_wise("[", joined, "]", "")


def make_list_transform(schema: pa.Schema):
    """
    Returns a per-batch transform converting all list columns to CH array
    literal strings, or None if the schema has no list columns.
    """
    list_cols = [i for i, f in enumerate(schema)
                 if pa.types.is_list(f.type) or pa.types.is_large_list(f.type)]
    if not list_cols:
        return None

    def transform(batch: pa.RecordBatch) -> pa.RecordBatch:
        arrays = list(batch.columns)
        for i in list_cols:
            arrays[i] = _list_to_ch_array_literal(arrays[i])
        return pa.RecordBatch.from_arrays(arrays, names=batch.schema.names)

    return transform


def serialize_csv(batch) -> bytes:
    buf = BytesIO()
    pacsv.write_csv(batch, buf, _CSV_OPTS)
    return buf.getvalue()


def serialize_jsoneachrow(batch) -> bytes:
    # pandas' C JSON serializer; ISO dates so ClickHouse parses them with
    # date_time_input_format=best_effort (set per insert below).
    df = batch.to_pandas()
    return df.to_json(orient="records", lines=True, date_format="iso").encode("utf-8")


SERIALIZERS = {
    "CSV":         serialize_csv,
    "JSONEachRow": serialize_jsoneachrow,
}

# Per-insert settings needed for a given format.
INSERT_SETTINGS = {
    "CSV":         {},
    "JSONEachRow": {"date_time_input_format": "best_effort"},
}


def worker(
    worker_id: int,
    task_queue: mp.Queue,
    database: str,
    table: str,
    columns: list,
    batch_size: int,
    fmt: str,
    async_insert_busy_timeout_max_ms: int,
    async_insert_max_data_size: int,
    shared_total_rows,            # mp.Value('q')
    error_queue: mp.Queue,
):
    client    = make_client(database, async_insert_busy_timeout_max_ms, async_insert_max_data_size)
    serialize = SERIALIZERS[fmt]
    insert_settings = INSERT_SETTINGS[fmt]
    inserts   = 0
    rows_sent = 0
    w_start   = time.time()
    pf_cache: dict[Path, pq.ParquetFile] = {}
    transform = None       # list-column -> CH array literal (CSV only)
    transform_ready = False

    while True:
        item = task_queue.get()
        if item is None:
            break
        task_idx, total_tasks, file_path, rg_idx, expected_rows = item

        try:
            pf = pf_cache.get(file_path)
            if pf is None:
                pf = pf_cache[file_path] = pq.ParquetFile(file_path)
            if not transform_ready:
                if fmt == "CSV":
                    transform = make_list_transform(pf.schema_arrow)
                transform_ready = True

            t0 = time.time()
            t_serialize = 0.0
            t_insert    = 0.0
            task_batches = 0
            task_rows    = 0
            task_bytes   = 0

            for batch in pf.iter_batches(batch_size=batch_size, row_groups=[rg_idx]):
                ts = time.time()
                if transform is not None:
                    batch = transform(batch)
                payload = serialize(batch)
                t_serialize += time.time() - ts

                ti = time.time()
                client.raw_insert(
                    table,
                    insert_block=payload,
                    column_names=columns,
                    fmt=fmt,
                    settings=insert_settings,
                )
                t_insert += time.time() - ti

                task_batches += 1
                task_rows    += batch.num_rows
                task_bytes   += len(payload)

            inserts   += task_batches
            rows_sent += task_rows
            with shared_total_rows.get_lock():
                shared_total_rows.value += task_rows

            t_total  = time.time() - t0
            t_decode = t_total - t_serialize - t_insert
            task_rate = task_rows / t_total if t_total > 0 else 0
            print(
                f"[worker {worker_id}] task {task_idx}/{total_tasks} "
                f"file={file_path.name} rg={rg_idx} rows={task_rows:,} "
                f"batches={task_batches} bytes={task_bytes:,} "
                f"decode={t_decode*1000:.0f}ms serialize={t_serialize*1000:.0f}ms "
                f"insert={t_insert*1000:.0f}ms total={t_total*1000:.0f}ms "
                f"rate={task_rate:,.0f} rows/s",
                flush=True,
            )
        except Exception as exc:
            tb  = traceback.format_exc()
            msg = f"{file_path.name} rg {rg_idx}: {exc}"
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


# Bold white on green, full-width — cuts through worker log scroll.
_HL   = "\033[1;97;42m"
_RST  = "\033[0m"


def live_eps_monitor(stop_event: threading.Event, start_time: float, interval: float,
                     shared_total_rows, stats_path: Path):
    """
    Background thread: every `interval` seconds, from the workers' shared
    sent-rows counter (pure client-side — with wait_for_async_insert=0 the
    server-side count only reflects buffer flush timing, not throughput):
      - prints one highlighted banner line (inst + avg rows/s)
      - appends the sample to stats_path (TSV) so the full throughput
        history survives the scrollback and can be charted later
    """
    stats = open(stats_path, "w", buffering=1)
    stats.write("elapsed_s\tsent_rows\tdelta_rows\tinst_rows_s\tavg_rows_s\n")
    last_sent = 0
    last_t    = start_time
    while not stop_event.is_set():
        slept = 0.0
        while slept < interval and not stop_event.is_set():
            time.sleep(min(0.5, interval - slept))
            slept += 0.5
        if stop_event.is_set():
            break
        now      = time.time()
        sent     = shared_total_rows.value
        dt       = now - last_t
        elapsed  = now - start_time
        delta    = sent - last_sent
        inst     = delta / dt if dt > 0 else 0
        avg      = sent / elapsed if elapsed > 0 else 0
        line = (f"  ▶ t={elapsed:8.1f}s   sent={sent:>15,}   +{delta:>12,}   "
                f"inst={inst:>11,.0f} rows/s   avg={avg:>11,.0f} rows/s")
        print(f"\n{_HL}{line:<118}{_RST}\n", flush=True)
        stats.write(f"{elapsed:.1f}\t{sent}\t{delta}\t{inst:.0f}\t{avg:.0f}\n")
        last_sent = sent
        last_t    = now
    stats.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir",                              required=True)
    parser.add_argument("--pattern",                          default="*.parquet")
    parser.add_argument("--database",                         default="default")
    parser.add_argument("--table",                            default="hits")
    parser.add_argument("--parallel",                         type=int, required=True)
    parser.add_argument("--batch-size",                       type=int, required=True,
                        help="Rows collected client-side per INSERT. Batches are cut within a row group; "
                             "the last batch of each row group may be smaller.")
    parser.add_argument("--format",                           choices=FORMATS, default="CSV",
                        help="Row-oriented wire format. CSV: fastest client-side serialization (pyarrow). "
                             "JSONEachRow: closest to 'raw events from endpoints' (requires pandas).")
    parser.add_argument("--max-files",                        type=int, default=None,
                        help="Only process the first N files (alphanumeric order).")
    parser.add_argument("--create-sql",                       default="create.sql")
    parser.add_argument("--async-insert-busy-timeout-max-ms", type=int, default=60000)
    parser.add_argument("--async-insert-max-data-size",       type=int, default=16777216)
    parser.add_argument("--live-eps-interval",                type=float, default=5.0,
                        help="Seconds between live EPS samples printed from the main process. 0 disables.")
    args = parser.parse_args()

    if args.batch_size < 1:
        sys.exit("ERROR: --batch-size must be >= 1")
    if args.format == "JSONEachRow":
        try:
            import pandas  # noqa: F401
        except ImportError:
            sys.exit("ERROR: --format JSONEachRow requires pandas (pip install pandas)")

    directory  = Path(args.dir)
    create_sql = Path(args.create_sql)

    if not directory.is_dir():
        sys.exit(f"ERROR: not a directory: {directory}")
    if not create_sql.exists():
        sys.exit(f"ERROR: create SQL not found: {create_sql}")

    tasks, files = enumerate_tasks(directory, args.pattern, args.max_files)
    total_tasks  = len(tasks)
    total_rows   = sum(rc for _, _, rc in tasks)

    # Column names / order come from the parquet schema of the first file.
    columns = pq.ParquetFile(files[0]).schema_arrow.names

    admin = make_client("default", args.async_insert_busy_timeout_max_ms, args.async_insert_max_data_size)
    admin.command(f"CREATE DATABASE IF NOT EXISTS `{args.database}`")

    # Run every statement in create.sql — they should be idempotent
    # (CREATE TABLE IF NOT EXISTS etc.). Strip -- comments, split on ';'.
    db_client = make_client(args.database, args.async_insert_busy_timeout_max_ms, args.async_insert_max_data_size)
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
    print(f"Tasks:         {total_tasks} (1 row group per task)")
    print(f"Rows total:    {total_rows:,}")
    print(f"Target:        {args.database}.{args.table}")
    print(f"Columns:       {len(columns)} (from parquet schema)")
    print(f"Workers:       {args.parallel}")
    print(f"Batch size:    {args.batch_size:,} rows per insert")
    print(f"Wire format:   {args.format}")
    print(f"Insert mode:   async (busy_timeout_max_ms={args.async_insert_busy_timeout_max_ms}, "
          f"max_data_size={args.async_insert_max_data_size})")
    print(f"Starting rows: {start_rows:,}")
    print()
    print("File order:")
    for f in files:
        print(f"  {f.name}")
    print()

    task_queue:  mp.Queue = mp.Queue()
    error_queue: mp.Queue = mp.Queue()
    shared_total_rows = mp.Value('q', 0)

    for i, (path, rg_idx, rows) in enumerate(tasks, start=1):
        task_queue.put((i, total_tasks, path, rg_idx, rows))
    for _ in range(args.parallel):
        task_queue.put(None)  # sentinel per worker

    stop_monitor = threading.Event()
    monitor_thread = None
    if args.live_eps_interval > 0:
        stats_path = Path(f"throughput_{datetime.now().strftime('%Y%m%d_%H%M%S')}.tsv")
        print(f"Throughput samples also logged to: {stats_path}\n")
        monitor_thread = threading.Thread(
            target=live_eps_monitor,
            args=(stop_monitor, start_time, args.live_eps_interval, shared_total_rows, stats_path),
            daemon=True,
        )
        monitor_thread.start()

    processes = []
    for w in range(args.parallel):
        p = mp.Process(
            target=worker,
            args=(
                w + 1, task_queue,
                args.database, args.table, columns,
                args.batch_size, args.format,
                args.async_insert_busy_timeout_max_ms,
                args.async_insert_max_data_size,
                shared_total_rows,
                error_queue,
            ),
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    stop_monitor.set()
    if monitor_thread is not None:
        monitor_thread.join(timeout=10)

    end_ts        = now_utc()
    elapsed       = time.time() - start_time
    final_rows    = admin.command(f"SELECT count() FROM `{args.database}`.`{args.table}`")
    rows_ingested = final_rows - start_rows
    rows_sent     = shared_total_rows.value

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
    print(f"Rows sent:     {rows_sent:,} ({rows_sent/elapsed:,.0f} rows/s client-side)")
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
