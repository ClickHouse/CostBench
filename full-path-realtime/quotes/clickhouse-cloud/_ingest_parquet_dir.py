#!/usr/bin/env python3
"""
Parallel one-shot ingester for a directory of Parquet files into ClickHouse.

Each (file, row_group) is dispatched exactly once. Files are enqueued in
sorted-name order; row groups within a file are enqueued in index order.
Workers pull tasks from a shared queue, so parallelism is not capped by
file count.

Requires:
    pip install clickhouse-connect pyarrow

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
import sys
import time
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import clickhouse_connect
import pyarrow.parquet as pq

FQDN     = os.environ["FQDN"]
PASSWORD = os.environ["PASSWORD"]
USER     = os.environ.get("CH_USER", "default")


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
    """
    Returns (tasks, files). Tasks are (file_path, row_group_index, row_count).
    Row counts are read from parquet metadata (cheap, footer-only).
    If max_files is set, only the first N files (alphanumeric sort) are used.
    """
    files = sorted(directory.glob(pattern))
    if not files:
        sys.exit(f"ERROR: no files matched {directory}/{pattern}")
    if max_files is not None:
        files = files[:max_files]

    tasks: list[tuple[Path, int, int]] = []
    for f in files:
        pf = pq.ParquetFile(f)
        meta = pf.metadata
        for rg in range(meta.num_row_groups):
            tasks.append((f, rg, meta.row_group(rg).num_rows))
    return tasks, files


def worker(
    worker_id: int,
    task_queue: mp.Queue,
    database: str,
    table: str,
    async_insert_busy_timeout_max_ms: int,
    async_insert_max_data_size: int,
    error_queue: mp.Queue,
):
    client    = make_client(database, async_insert_busy_timeout_max_ms, async_insert_max_data_size)
    inserts   = 0
    rows_sent = 0
    w_start   = time.time()

    cached_path = None
    cached_pf   = None

    while True:
        item = task_queue.get()
        if item is None:
            break
        task_idx, total_tasks, file_path, rg_idx, expected_rows = item

        try:
            if cached_path != file_path:
                cached_pf   = pq.ParquetFile(file_path)
                cached_path = file_path

            arrow_table = cached_pf.read_row_group(rg_idx)
            row_count   = arrow_table.num_rows

            print(
                f"[worker {worker_id}] task {task_idx}/{total_tasks} "
                f"file={file_path.name} rg={rg_idx} rows={row_count:,}",
                flush=True,
            )

            buf = BytesIO()
            pq.write_table(arrow_table, buf, compression="snappy")
            client.raw_insert(table, insert_block=buf.getvalue(), fmt="Parquet")

            inserts   += 1
            rows_sent += row_count
        except Exception as exc:
            msg = f"{file_path.name} rg {rg_idx}: {exc}"
            error_queue.put(msg)
            print(f"[worker {worker_id}] ERROR: {msg}", file=sys.stderr, flush=True)

    duration = time.time() - w_start
    print(
        f"[worker {worker_id}] done — {inserts} inserts, {rows_sent:,} rows in {duration:.1f}s",
        flush=True,
    )


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


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
    parser.add_argument("--async-insert-busy-timeout-max-ms", type=int, required=True)
    parser.add_argument("--async-insert-max-data-size",       type=int, required=True)
    args = parser.parse_args()

    directory  = Path(args.dir)
    create_sql = Path(args.create_sql)

    if not directory.is_dir():
        sys.exit(f"ERROR: not a directory: {directory}")
    if not create_sql.exists():
        sys.exit(f"ERROR: create SQL not found: {create_sql}")

    tasks, files = enumerate_tasks(directory, args.pattern, args.max_files)
    total_tasks  = len(tasks)
    total_rows   = sum(rc for _, _, rc in tasks)

    admin = make_client("default", args.async_insert_busy_timeout_max_ms, args.async_insert_max_data_size)
    admin.command(f"CREATE DATABASE IF NOT EXISTS `{args.database}`")

    if not admin.command(f"EXISTS TABLE `{args.database}`.`{args.table}`"):
        db_client = make_client(args.database, args.async_insert_busy_timeout_max_ms, args.async_insert_max_data_size)
        db_client.command(create_sql.read_text())

    start_rows = admin.command(f"SELECT count() FROM `{args.database}`.`{args.table}`")
    start_ts   = now_utc()
    start_time = time.time()

    print(f"Start time:    {start_ts}")
    print(f"Directory:     {directory}  (pattern: {args.pattern})")
    print(f"Files:         {len(files)}")
    print(f"Row groups:    {total_tasks}")
    print(f"Rows total:    {total_rows:,}")
    print(f"Target:        {args.database}.{args.table}")
    print(f"Workers:       {args.parallel}")
    print(f"async_insert_busy_timeout_max_ms: {args.async_insert_busy_timeout_max_ms}")
    print(f"async_insert_max_data_size:       {args.async_insert_max_data_size}")
    print(f"Starting rows: {start_rows:,}")
    print()
    print("File order:")
    for f in files:
        print(f"  {f.name}")
    print()

    task_queue:  mp.Queue = mp.Queue()
    error_queue: mp.Queue = mp.Queue()

    # Enqueue tasks in strict file/row-group order. Workers will pick them up
    # roughly in this order — async_insert means physical ingest order is not
    # preserved anyway, which matches the agreed "dispatch order" semantics.
    for i, (path, rg, rows) in enumerate(tasks, start=1):
        task_queue.put((i, total_tasks, path, rg, rows))
    for _ in range(args.parallel):
        task_queue.put(None)  # sentinel per worker

    processes = []
    for w in range(args.parallel):
        p = mp.Process(
            target=worker,
            args=(
                w + 1, task_queue,
                args.database, args.table,
                args.async_insert_busy_timeout_max_ms,
                args.async_insert_max_data_size,
                error_queue,
            ),
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

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
