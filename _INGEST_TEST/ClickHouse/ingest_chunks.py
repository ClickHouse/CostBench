#!/usr/bin/env python3
"""
Parallel chunked TSV ingester for ClickHouse using async inserts.

Requires:
    pip install clickhouse-connect

Usage:
    export FQDN="your-service.clickhouse.cloud"
    export PASSWORD="your_password"
    export CH_USER="your_user"

    python3 ingest_chunks.py \
        --file hits.tsv \
        --database hits_100b \
        --table hits \
        --parallel 8 \
        --chunk-size 100000 \
        --total-rows 1000000000000 \
        --create-sql create.sql \
        --async-insert-busy-timeout-max-ms 5000 \
        --async-insert-max-data-size 10485760
"""

import argparse
import multiprocessing as mp
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import clickhouse_connect

FQDN     = os.environ["FQDN"]
PASSWORD = os.environ["PASSWORD"]
USER     = os.environ.get("CH_USER", "default")


def parse_columns(create_sql_path: Path) -> list[str]:
    text = create_sql_path.read_text()
    inner = re.search(r"CREATE TABLE \w+\s*\((.+)\)", text, re.DOTALL | re.IGNORECASE)
    if not inner:
        sys.exit(f"ERROR: could not parse columns from {create_sql_path}")
    columns = []
    for line in inner.group(1).splitlines():
        line = line.strip().rstrip(",")
        if not line or re.match(r"(PRIMARY|UNIQUE|INDEX|KEY|CONSTRAINT)\b", line, re.IGNORECASE):
            continue
        col = re.split(r"\s+", line)[0].strip("`\"'")
        if col:
            columns.append(col)
    return columns


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


def compute_worker_segments(path: Path, num_workers: int) -> list[tuple[int, int]]:
    file_size    = path.stat().st_size
    segment_size = file_size // num_workers
    segments     = []

    with path.open("rb") as fh:
        for i in range(num_workers):
            start = i * segment_size if i > 0 else 0
            if i > 0:
                fh.seek(start)
                fh.readline()
                start = fh.tell()

            if i == num_workers - 1:
                end = -1
            else:
                end_approx = (i + 1) * segment_size
                fh.seek(end_approx)
                fh.readline()
                end = fh.tell()

            segments.append((start, end))

    return segments


def worker(
    worker_id: int,
    path: Path,
    start_byte: int,
    end_byte: int,
    database: str,
    table: str,
    columns: list,
    chunk_size: int,
    worker_rows: int,
    async_insert_busy_timeout_max_ms: int,
    async_insert_max_data_size: int,
    error_queue: mp.Queue,
):
    client      = make_client(database, async_insert_busy_timeout_max_ms, async_insert_max_data_size)
    count       = 0
    chunk_num   = 0
    rows_sent   = 0
    w_start     = time.time()

    with path.open("rb") as fh:
        fh.seek(start_byte)

        while rows_sent < worker_rows:
            # Rewind segment if we hit the end
            if end_byte != -1 and fh.tell() >= end_byte:
                fh.seek(start_byte)
            elif end_byte == -1:
                pos = fh.tell()
                fh.seek(0, 2)  # seek to EOF to get size
                eof = fh.tell()
                fh.seek(pos)
                if pos >= eof:
                    fh.seek(start_byte)

            take  = min(chunk_size, worker_rows - rows_sent)
            lines = []
            for _ in range(take):
                if end_byte != -1 and fh.tell() >= end_byte:
                    break
                line = fh.readline()
                if not line:
                    break
                lines.append(line)

            if not lines:
                fh.seek(start_byte)
                continue

            chunk_num += 1
            rows_sent += len(lines)
            tsv_data   = b"".join(lines)

            try:
                print(f"[worker {worker_id}] chunk {chunk_num} — {len(lines):,} rows ({rows_sent:,}/{worker_rows:,})", flush=True)
                client.raw_insert(table, insert_block=tsv_data, column_names=columns, fmt="TabSeparated")
                count += 1
            except Exception as exc:
                error_queue.put(str(exc))
                print(f"[worker {worker_id}] ERROR: {exc}", file=sys.stderr, flush=True)

    duration = time.time() - w_start
    print(f"[worker {worker_id}] done — {count} chunks in {duration:.1f}s", flush=True)


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file",                              required=True)
    parser.add_argument("--database",                          default="default")
    parser.add_argument("--table",                             default="hits")
    parser.add_argument("--parallel",                          type=int, required=True)
    parser.add_argument("--chunk-size",                        type=int, required=True)
    parser.add_argument("--total-rows",                        type=int, required=True)
    parser.add_argument("--create-sql",                        default="create.sql")
    parser.add_argument("--async-insert-busy-timeout-max-ms",  type=int, required=True)
    parser.add_argument("--async-insert-max-data-size",        type=int, required=True)
    args = parser.parse_args()

    path       = Path(args.file)
    create_sql = Path(args.create_sql)

    if not path.exists():
        sys.exit(f"ERROR: file not found: {path}")
    if not create_sql.exists():
        sys.exit(f"ERROR: create SQL not found: {create_sql}")

    columns = parse_columns(create_sql)

    admin = make_client("default", args.async_insert_busy_timeout_max_ms, args.async_insert_max_data_size)
    admin.command(f"CREATE DATABASE IF NOT EXISTS `{args.database}`")

    if not admin.command(f"EXISTS TABLE `{args.database}`.`{args.table}`"):
        db_client = make_client(args.database, args.async_insert_busy_timeout_max_ms, args.async_insert_max_data_size)
        db_client.command(create_sql.read_text())

    start_rows = admin.command(f"SELECT count() FROM `{args.database}`.`{args.table}`")
    start_ts   = now_utc()
    start_time = time.time()

    segments = compute_worker_segments(path, args.parallel)

    # Distribute total_rows evenly across workers
    base_rows  = args.total_rows // args.parallel
    remainder  = args.total_rows % args.parallel

    print(f"Start time:    {start_ts}")
    print(f"File:          {path}")
    print(f"Target:        {args.database}.{args.table}")
    print(f"Columns:       {len(columns)} (from {create_sql})")
    print(f"Chunk size:    {args.chunk_size:,} rows")
    print(f"Total rows:    {args.total_rows:,}")
    print(f"Workers:       {args.parallel}")
    print(f"Rows/worker:   {base_rows:,} (first {remainder} workers get +1)")
    print(f"async_insert_busy_timeout_max_ms: {args.async_insert_busy_timeout_max_ms}")
    print(f"async_insert_max_data_size:       {args.async_insert_max_data_size}")
    print(f"Starting rows: {start_rows:,}")
    print()

    error_queue = mp.Queue()

    processes = []
    for w in range(args.parallel):
        start_byte, end_byte = segments[w]
        worker_rows = base_rows + (1 if w < remainder else 0)
        p = mp.Process(
            target=worker,
            args=(
                w + 1, path, start_byte, end_byte,
                args.database, args.table, columns,
                args.chunk_size, worker_rows,
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