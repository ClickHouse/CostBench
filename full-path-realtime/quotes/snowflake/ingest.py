#!/usr/bin/env python3
"""
Parallel row-group ingester for a directory of Parquet files into Snowflake.

Snowflake analogue of the Databricks `ingest.py` and the ClickHouse
`ingest_parquet_dir.py` — same skeleton (one task per (file,row_group), shared
worker pool, global rows/s limiter, count(*)-polling live monitor) so the
bench2cost numbers are apples-to-apples. Measures END-TO-END CLIENT EPS: the
full read → upload → load cycle, as the client experiences it.

Per row group, each worker:
  1. Read    — pyarrow reads the row group(s) into an Arrow table.
  2. Encode  — writes a fresh in-memory Parquet buffer (snappy).
  3. Upload  — PUT the buffer to an internal stage via the connector's
               in-memory file_stream (no temp files), AUTO_COMPRESS=FALSE.
  4. Load    — COPY INTO <table> FROM @stage/<file> MATCH_BY_COLUMN_NAME,
               FORCE=TRUE (so replays of the same file still load — sustained rate).
  5. Cleanup — REMOVE the staged file.
  6. Rate-limit — shared mp.Value across workers; sleep if aggregate > --target-rps.

Run this from a client CO-LOCATED with the Snowflake account region (the PUT is
measured client work; a cross-region client measures the network, not Snowflake).

Requires: pip install snowflake-connector-python pyarrow cryptography
Auth: key-pair (rsa_key.p8 on the box).

Usage:
    python3 ingest.py --dir /data/quotes --parallel 32 \
        --database BENCH2COST --schema STOCKHOUSE --table QUOTES \
        --stage QUOTES_INT_STAGE --warehouse BENCH2COST_GEN2_LARGE
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
import snowflake.connector as sc
from cryptography.hazmat.primitives import serialization

ACCOUNT  = os.environ["SF_ACCOUNT"]
USER     = os.environ["SF_USER"]
KEY_PATH = os.environ.get("SF_KEY", "/home/ubuntu/bench/keys/rsa_key.p8")


def _pkb():
    pk = serialization.load_pem_private_key(open(KEY_PATH, "rb").read(), password=None)
    return pk.private_bytes(serialization.Encoding.DER,
                            serialization.PrivateFormat.PKCS8,
                            serialization.NoEncryption())


def make_client(database, schema, warehouse, role):
    con = sc.connect(account=ACCOUNT, user=USER, private_key=_pkb(),
                     database=database, schema=schema, warehouse=warehouse,
                     role=role, login_timeout=30)
    return con


def enumerate_tasks(directory, pattern, max_files, row_groups_per_insert):
    files = sorted(directory.glob(pattern))
    if not files:
        sys.exit(f"ERROR: no files matched {directory}/{pattern}")
    if max_files is not None:
        files = files[:max_files]
    tasks = []
    usable = []
    for f in files:
        try:
            meta = pq.ParquetFile(f).metadata   # skip partial/in-flight downloads
        except Exception:
            continue
        usable.append(f)
        n = meta.num_row_groups
        for start in range(0, n, row_groups_per_insert):
            rg = list(range(start, min(start + row_groups_per_insert, n)))
            tasks.append((f, rg, sum(meta.row_group(i).num_rows for i in rg)))
    return tasks, usable


def count_rows(cur, db, schema, table):
    try:
        cur.execute(f"SELECT count(*) FROM {db}.{schema}.{table}")
        return cur.fetchone()[0]
    except Exception:
        return 0


def worker(worker_id, task_queue, args, shared_total_rows, global_start, error_queue):
    con = make_client(args.database, args.schema, args.warehouse, args.role)
    cur = con.cursor()
    inserts = rows_sent = 0
    w_start = time.time()

    while True:
        item = task_queue.get()
        if item is None:
            break
        task_idx, total_tasks, file_path, rg_indices, _ = item
        rg_str = f"{rg_indices[0]}-{rg_indices[-1]}" if len(rg_indices) > 1 else f"{rg_indices[0]}"
        staged = f"{file_path.stem}_rg{rg_indices[0]:04d}.parquet"
        stage_path = f"@{args.stage}/staging"
        tmp = f"/dev/shm/{staged}"   # tmpfs (RAM) -> effectively in-memory, no real disk I/O

        try:
            # Read row group(s) -> re-encode to a temp parquet on tmpfs
            t0 = time.time()
            pf = pq.ParquetFile(file_path)
            tbl = pa.concat_tables([pf.read_row_group(i) for i in rg_indices])
            pq.write_table(tbl, tmp, compression="snappy")
            row_count = tbl.num_rows
            t_read = time.time() - t0

            # PUT the temp file to the internal stage (AUTO_COMPRESS off: parquet already compressed)
            t1 = time.time()
            cur.execute(f"PUT file://{tmp} {stage_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
            t_upload = time.time() - t1

            # COPY the staged file into the table
            t2 = time.time()
            cur.execute(
                f"COPY INTO {args.database}.{args.schema}.{args.table} "
                f"FROM {stage_path}/{staged} FILE_FORMAT=(TYPE=PARQUET) "
                f"MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE FORCE=TRUE"
            )
            t_insert = time.time() - t2

            try:
                cur.execute(f"REMOVE {stage_path}/{staged}")
            except Exception:
                pass
            try:
                os.remove(tmp)
            except Exception:
                pass

            inserts += 1
            rows_sent += row_count

            throttle = 0.0
            if args.target_rps > 0:
                with shared_total_rows.get_lock():
                    shared_total_rows.value += row_count
                    total = shared_total_rows.value
                expected = (time.time() - global_start) * args.target_rps
                if total > expected:
                    throttle = (total - expected) / args.target_rps

            print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] [w{worker_id}] "
                  f"task {task_idx}/{total_tasks} {file_path.name} rg={rg_str} "
                  f"rows={row_count:,} read={t_read*1000:.0f}ms put={t_upload*1000:.0f}ms "
                  f"copy={t_insert*1000:.0f}ms" + (f" THROTTLE={throttle*1000:.0f}ms" if throttle else ""),
                  flush=True)
            if throttle > 0:
                time.sleep(throttle)

        except Exception as exc:
            for cleanup in (lambda: cur.execute(f"REMOVE {stage_path}/{staged}"), lambda: os.remove(tmp)):
                try:
                    cleanup()
                except Exception:
                    pass
            error_queue.put(f"{file_path.name} rg {rg_str}: {exc}")
            print(f"[w{worker_id}] ERROR {exc}\n{traceback.format_exc()}", file=sys.stderr, flush=True)

    con.close()
    dur = time.time() - w_start
    print(f"[w{worker_id}] done {inserts} inserts, {rows_sent:,} rows in {dur:.1f}s "
          f"({rows_sent/dur:,.0f}/s)", flush=True)


def live_monitor(stop, args, start_rows, start_time, interval):
    con = make_client(args.database, args.schema, args.warehouse, args.role)
    cur = con.cursor()
    last_rows, last_t = start_rows, start_time
    while not stop.is_set():
        slept = 0.0
        while slept < interval and not stop.is_set():
            time.sleep(min(0.5, interval - slept)); slept += 0.5
        if stop.is_set():
            break
        now = time.time()
        cur_rows = count_rows(cur, args.database, args.schema, args.table)
        dt = now - last_t
        inst = (cur_rows - last_rows) / dt if dt > 0 else 0
        avg = (cur_rows - start_rows) / (now - start_time) if now > start_time else 0
        bar = "=" * 88
        print(f"\n{bar}\n  t={now-start_time:7.1f}s  rows={cur_rows:>14,}  "
              f"+{cur_rows-last_rows:>12,}  inst={inst:>10,.0f}/s  avg={avg:>10,.0f}/s\n{bar}", flush=True)
        last_rows, last_t = cur_rows, now
    con.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--pattern", default="*.parquet")
    ap.add_argument("--database", default="BENCH2COST")
    ap.add_argument("--schema", default="STOCKHOUSE")
    ap.add_argument("--table", default="QUOTES")
    ap.add_argument("--stage", default="QUOTES_INT_STAGE")
    ap.add_argument("--warehouse", default="BENCH2COST_GEN2_LARGE")
    ap.add_argument("--role", default="ACCOUNTADMIN")
    ap.add_argument("--parallel", type=int, required=True)
    ap.add_argument("--max-files", type=int, default=None)
    ap.add_argument("--row-groups-per-insert", type=int, default=1)
    ap.add_argument("--target-rps", type=int, default=0)
    ap.add_argument("--live-eps-interval", type=float, default=30.0)
    args = ap.parse_args()

    directory = Path(args.dir)
    if not directory.is_dir():
        sys.exit(f"ERROR: not a directory: {directory}")

    print("Enumerating row groups...")
    tasks, files = enumerate_tasks(directory, args.pattern, args.max_files, args.row_groups_per_insert)

    con = make_client(args.database, args.schema, args.warehouse, args.role)
    cur = con.cursor()
    start_rows = count_rows(cur, args.database, args.schema, args.table)
    start_time = time.time()
    con.close()

    print(f"Files: {len(files)}  Tasks: {len(tasks)}  Workers: {args.parallel}")
    print(f"Target: {args.database}.{args.schema}.{args.table} via @{args.stage}  WH={args.warehouse}")
    print(f"Rate:   {f'{args.target_rps:,}/s' if args.target_rps else 'unlimited'}   Start rows: {start_rows:,}\n")

    task_queue, error_queue = mp.Queue(), mp.Queue()
    shared = mp.Value('q', 0)
    for i, (p, rg, n) in enumerate(tasks, 1):
        task_queue.put((i, len(tasks), p, rg, n))
    for _ in range(args.parallel):
        task_queue.put(None)

    stop = threading.Event()
    mon = None
    if args.live_eps_interval > 0:
        mon = threading.Thread(target=live_monitor, args=(stop, args, start_rows, start_time, args.live_eps_interval), daemon=True)
        mon.start()

    procs = [mp.Process(target=worker, args=(w+1, task_queue, args, shared, start_time, error_queue))
             for w in range(args.parallel)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

    stop.set()
    if mon:
        mon.join(timeout=10)

    con = make_client(args.database, args.schema, args.warehouse, args.role)
    final_rows = count_rows(con.cursor(), args.database, args.schema, args.table)
    con.close()
    elapsed = time.time() - start_time
    errs = []
    while not error_queue.empty():
        errs.append(error_queue.get())

    print(f"\n{'='*50}\nSUMMARY")
    print(f"Duration:      {elapsed:.1f}s (~{elapsed/60:.1f} min)")
    print(f"Rows ingested: {final_rows-start_rows:,}")
    print(f"Avg EPS:       {(final_rows-start_rows)/elapsed:,.0f}/s")
    print(f"Errors:        {len(errs)}")
    for e in errs[:10]:
        print("  " + e)
    sys.exit(1 if errs else 0)


if __name__ == "__main__":
    main()
