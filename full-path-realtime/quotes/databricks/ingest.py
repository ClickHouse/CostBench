#!/usr/bin/env python3
"""
Parallel row-group ingester for a directory of Parquet files into Databricks.

Approach
--------
Each (file, row_group) task is dispatched exactly once to a pool of workers.
Each worker does the following per row group:

  1. Read   — pyarrow reads one row group from the source Parquet file into an
              Arrow table (~614k rows, ~47 MB per row group in this dataset).

  2. Encode — writes the Arrow table back out as a fresh in-memory Parquet
              buffer, coercing timestamps from nanoseconds → microseconds
              (Databricks' maximum timestamp precision).

  3. Upload — PUTs the buffer to a Unity Catalog volume staging path via the
              Databricks Files API.

  4. Insert — runs:
                INSERT INTO <table>
                SELECT col1, col2, ...
                FROM parquet.`/Volumes/.../staging/<file>_rg<N>.parquet`
              Databricks reads the Parquet natively — no SQL string building,
              no value escaping, no type conversion overhead on the client.

  5. Cleanup — DELETEs the staging file from the volume.

  6. Rate limit — checks a shared mp.Value counter across all workers; if the
                  aggregate rows/s exceeds --target-rps, the worker sleeps to
                  throttle back down.

Requires:
    pip install databricks-sdk pyarrow

Usage:
    export DATABRICKS_HOST="https://dbc-....cloud.databricks.com"
    export DATABRICKS_TOKEN="dapi..."

    python3 ingest.py \
        --dir ~/data/stockhouse \
        --warehouse 47486f539b196632 \
        --parallel 32
"""

import argparse
import multiprocessing as mp
import os
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# Derive warehouse ID from DATABRICKS_HTTP_PATH if set (/sql/1.0/warehouses/<id>)
_http_path    = os.environ.get("DATABRICKS_HTTP_PATH", "")
_warehouse_id = _http_path.rstrip("/").split("/")[-1] if _http_path else ""

# Support DATABRICKS_SERVER_HOSTNAME as an alias for DATABRICKS_HOST
_hostname = os.environ.get("DATABRICKS_SERVER_HOSTNAME", "")
if _hostname and not os.environ.get("DATABRICKS_HOST"):
    host = f"https://{_hostname}" if not _hostname.startswith("http") else _hostname
    os.environ["DATABRICKS_HOST"] = host


def make_client() -> WorkspaceClient:
    return WorkspaceClient()


def run_sql(client: WorkspaceClient, warehouse_id: str, statement: str):
    resp     = client.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=warehouse_id,
        wait_timeout="0s",
    )
    stmt_id   = resp.statement_id
    t_submit  = time.time()
    t_running = None

    state = resp.status.state
    while state in (StatementState.PENDING, StatementState.RUNNING):
        if state == StatementState.RUNNING and t_running is None:
            t_running = time.time()
        time.sleep(0.2)
        resp  = client.statement_execution.get_statement(statement_id=stmt_id)
        state = resp.status.state

    t_done = time.time()

    if state == StatementState.FAILED:
        raise RuntimeError(resp.status.error.message)

    if t_running is not None:
        queue_ms = int((t_running - t_submit) * 1000)
        exec_ms  = int((t_done   - t_running) * 1000)
    else:
        queue_ms = 0
        exec_ms  = int((t_done - t_submit) * 1000)

    return resp, queue_ms, exec_ms


def count_rows(client: WorkspaceClient, warehouse_id: str, catalog: str, schema: str, table: str) -> int:
    try:
        resp, _, _ = run_sql(client, warehouse_id, f"SELECT count(*) FROM {catalog}.{schema}.{table}")
        data = resp.result.data_array if resp.result else None
        val  = data[0][0] if data else None
        return int(val) if val is not None else 0
    except Exception:
        return 0


def enumerate_tasks(directory: Path, pattern: str, max_files, row_groups_per_insert):
    files = sorted(directory.glob(pattern))
    if not files:
        sys.exit(f"ERROR: no files matched {directory}/{pattern}")
    if max_files is not None:
        files = files[:max_files]
    tasks = []
    for f in files:
        meta  = pq.ParquetFile(f).metadata
        n_rgs = meta.num_row_groups
        for start in range(0, n_rgs, row_groups_per_insert):
            rg_indices = list(range(start, min(start + row_groups_per_insert, n_rgs)))
            total_rows = sum(meta.row_group(i).num_rows for i in rg_indices)
            tasks.append((f, rg_indices, total_rows))
    return tasks, files


def worker(
    worker_id,
    task_queue,
    warehouse_id,
    catalog, schema, table, volume,
    target_rps,
    shared_total_rows,
    global_start_time,
    error_queue,
):
    client    = make_client()
    inserts   = 0
    rows_sent = 0
    w_start   = time.time()

    while True:
        item = task_queue.get()
        if item is None:
            break
        task_idx, total_tasks, file_path, rg_indices, _ = item

        rg_str      = (f"{rg_indices[0]}-{rg_indices[-1]}"
                       if len(rg_indices) > 1 else f"{rg_indices[0]}")
        volume_path = (
            f"/Volumes/{catalog}/{schema}/{volume}/staging/"
            f"{file_path.stem}_rg{rg_indices[0]:04d}.parquet"
        )

        try:
            # Read row groups → in-memory Parquet bytes
            t0          = time.time()
            pf          = pq.ParquetFile(file_path)
            arrow_table = pa.concat_tables([pf.read_row_group(i) for i in rg_indices])
            buf         = BytesIO()
            pq.write_table(arrow_table, buf, compression="snappy",
                           coerce_timestamps="us", allow_truncated_timestamps=True)
            parquet_bytes = buf.getvalue()
            row_count     = arrow_table.num_rows
            t_read        = time.time() - t0

            # Upload to staging volume
            t1 = time.time()
            client.files.upload(file_path=volume_path, contents=BytesIO(parquet_bytes), overwrite=True)
            t_upload = time.time() - t1

            # INSERT INTO SELECT — Databricks reads Parquet natively
            t2 = time.time()
            col_list = ", ".join([
                "sym", "bx", "bp", "bs", "ax", "ap", "`as`",
                "c", "i", "t", "q", "z",
            ])
            _, queue_ms, exec_ms = run_sql(
                client,
                warehouse_id,
                f"INSERT INTO {catalog}.{schema}.{table} "
                f"SELECT {col_list} FROM parquet.`{volume_path}`",
            )
            t_insert = time.time() - t2

            try:
                client.files.delete(file_path=volume_path)
            except Exception:
                pass

            inserts   += 1
            rows_sent += row_count

            # Global rate limiter
            throttle_sec = 0.0
            if target_rps > 0:
                with shared_total_rows.get_lock():
                    shared_total_rows.value += row_count
                    global_total = shared_total_rows.value
                elapsed_global  = time.time() - global_start_time
                expected_global = elapsed_global * target_rps
                if global_total > expected_global:
                    throttle_sec = (global_total - expected_global) / target_rps

            throttle_str = f" THROTTLE={throttle_sec*1000:.0f}ms" if throttle_sec > 0 else ""
            mb = len(parquet_bytes) / 1e6
            print(
                f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] "
                f"[worker {worker_id}] task {task_idx}/{total_tasks} "
                f"file={file_path.name} rg={rg_str} rows={row_count:,} size={mb:.1f}MB "
                f"read={t_read*1000:.0f}ms upload={t_upload*1000:.0f}ms "
                f"insert={t_insert*1000:.0f}ms queue={queue_ms}ms exec={exec_ms}ms{throttle_str}",
                flush=True,
            )

            if throttle_sec > 0:
                time.sleep(throttle_sec)

        except Exception as exc:
            try:
                client.files.delete(file_path=volume_path)
            except Exception:
                pass
            tb  = traceback.format_exc()
            msg = f"{file_path.name} rg {rg_str}: {exc}"
            error_queue.put(msg)
            print(f"[worker {worker_id}] ERROR: {msg}\n{tb}", file=sys.stderr, flush=True)

    duration = time.time() - w_start
    rate     = rows_sent / duration if duration > 0 else 0
    print(
        f"[worker {worker_id}] done — {inserts} inserts, {rows_sent:,} rows "
        f"in {duration:.1f}s ({rate:,.0f} rows/s)",
        flush=True,
    )


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def live_monitor(stop_event, warehouse_id, catalog, schema, table,
                 start_rows, start_time, interval):
    client    = make_client()
    last_rows = start_rows
    last_t    = start_time
    while not stop_event.is_set():
        slept = 0.0
        while slept < interval and not stop_event.is_set():
            time.sleep(min(0.5, interval - slept))
            slept += 0.5
        if stop_event.is_set():
            break
        try:
            now      = time.time()
            cur_rows = count_rows(client, warehouse_id, catalog, schema, table)
            delta    = cur_rows - last_rows
            dt       = now - last_t
            rate     = delta / dt if dt > 0 else 0
            total    = cur_rows - start_rows
            elapsed  = now - start_time
            avg_rate = total / elapsed if elapsed > 0 else 0
            bar      = "═" * 88
            print(
                f"\n{bar}\n"
                f"  [{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}]    "
                f"t={elapsed:7.1f}s    "
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
            print(f"[live-monitor] query failed: {exc}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Parallel row-group Parquet → Databricks ingester")
    parser.add_argument("--dir",                     required=True)
    parser.add_argument("--pattern",                 default="*.parquet")
    parser.add_argument("--catalog",                 default="workspace")
    parser.add_argument("--schema",                  default="benchmarking")
    parser.add_argument("--table",                   default="quotes")
    parser.add_argument("--volume",                  default="parquet_staging")
    parser.add_argument("--warehouse",               default=_warehouse_id,
                        help="SQL warehouse ID (default: last segment of DATABRICKS_HTTP_PATH)")
    parser.add_argument("--parallel",                type=int, required=True)
    parser.add_argument("--max-files",               type=int, default=None)
    parser.add_argument("--row-groups-per-insert",   type=int, default=1,
                        help="Combine N consecutive row groups per INSERT. Reduces Delta transaction log pressure.")
    parser.add_argument("--target-rps",              type=int, default=0,
                        help="Target rows/s across all workers. 0 = unlimited.")
    parser.add_argument("--live-eps-interval",       type=float, default=30.0)
    args = parser.parse_args()

    directory  = Path(args.dir)
    target_rps = float(args.target_rps)

    if not args.warehouse:
        sys.exit("ERROR: --warehouse or DATABRICKS_HTTP_PATH env var is required")
    if not directory.is_dir():
        sys.exit(f"ERROR: not a directory: {directory}")

    client = make_client()

    print("Enumerating row groups...")
    tasks, files = enumerate_tasks(directory, args.pattern, args.max_files, args.row_groups_per_insert)
    total_tasks  = len(tasks)

    print("Creating staging volume if needed...")
    run_sql(client, args.warehouse, f"CREATE VOLUME IF NOT EXISTS {args.catalog}.{args.schema}.{args.volume}")

    start_rows = count_rows(client, args.warehouse, args.catalog, args.schema, args.table)
    start_ts   = now_utc()
    start_time = time.time()

    print(f"Start time:    {start_ts}")
    print(f"Directory:     {directory}  (pattern: {args.pattern})")
    print(f"Files:         {len(files)}")
    print(f"Tasks:         {total_tasks} ({args.row_groups_per_insert} row group(s) per insert)")
    print(f"Target:        {args.catalog}.{args.schema}.{args.table}")
    print(f"Volume:        {args.catalog}.{args.schema}.{args.volume}/staging/")
    print(f"Workers:       {args.parallel}")
    if args.target_rps > 0:
        print(f"Target rate:   {args.target_rps:,} rows/s (global, shared across all workers)")
    else:
        print(f"Target rate:   unlimited")
    print(f"Starting rows: {start_rows:,}")
    print()
    print("File order:")
    for f in files:
        print(f"  {f.name}")
    print()

    task_queue        = mp.Queue()
    error_queue       = mp.Queue()
    shared_total_rows = mp.Value('q', 0)

    for i, (path, rg_indices, total_rows) in enumerate(tasks, start=1):
        task_queue.put((i, total_tasks, path, rg_indices, total_rows))
    for _ in range(args.parallel):
        task_queue.put(None)

    stop_monitor   = threading.Event()
    monitor_thread = None
    if args.live_eps_interval > 0:
        monitor_thread = threading.Thread(
            target=live_monitor,
            args=(stop_monitor, args.warehouse, args.catalog, args.schema, args.table,
                  start_rows, start_time, args.live_eps_interval),
            daemon=True,
        )
        monitor_thread.start()

    processes = []
    for w in range(args.parallel):
        p = mp.Process(
            target=worker,
            args=(w + 1, task_queue, args.warehouse,
                  args.catalog, args.schema, args.table, args.volume,
                  target_rps, shared_total_rows, start_time, error_queue),
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    stop_monitor.set()
    if monitor_thread:
        monitor_thread.join(timeout=10)

    end_ts        = now_utc()
    elapsed       = time.time() - start_time
    final_rows    = count_rows(client, args.warehouse, args.catalog, args.schema, args.table)
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
    print(f"Starting rows: {start_rows:,}")
    print(f"Final rows:    {final_rows:,}")
    print(f"Rows ingested: {rows_ingested:,}")
    print(f"Errors:        {len(errors)}")
    if errors:
        for e in errors:
            print(f"  {e}", file=sys.stderr)
    print("=================================================")

    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
