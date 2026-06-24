#!/usr/bin/env python3
"""
Parallel chunked TSV ingester for Databricks Delta tables.

Adapted from the ClickHouse ingest_chunks.py pattern: N worker processes
each read a byte segment of a local TSV file and INSERT chunks into the
target Databricks table via parameterized binding. Each INSERT is one
Delta commit on the target — streaming-like granularity, no PySpark
needed.

Setup:
    pip install databricks-sql-connector
    export DATABRICKS_SERVER_HOSTNAME=...
    export DATABRICKS_HTTP_PATH=...
    export DATABRICKS_TOKEN=...

Example (mirrors the ClickHouse invocation pattern):
    python3 databricks_ingest_chunks.py \
        --file hits.tsv \
        --target 'workspace.clickbench.`100b_clustered`' \
        --parallel 16 \
        --chunk-size 20000 \
        --total-rows 100000000000

Concurrency note:
    Unlike ClickHouse async_insert which absorbs hundreds of concurrent
    requests server-side, a Databricks SQL warehouse is bounded by its
    cluster count. Medium ≈ 10 concurrent statements before queuing;
    Large ≈ 20; with auto-scaling, multiply by max_clusters. Pushing
    --parallel beyond that just queues workers.

Loop behavior:
    Workers replay their byte segment when exhausted, same as the
    ClickHouse script — so --total-rows can exceed the source row
    count (e.g. 100M-row TSV replayed to produce 100B rows).

Throughput upgrade path:
    If parameterized INSERT throughput isn't enough, stage Parquet
    chunks to S3 and switch the worker INSERT to COPY INTO. Each
    COPY INTO is still one Delta commit, but skips all Python
    parsing/serialization. See notes at the bottom of this file.
"""

import argparse
import multiprocessing as mp
import os
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

from databricks import sql


# ---- type parsers (module-level, so they're picklable for mp.Process) -------

def _parse_int(v):  return int(v)
def _parse_date(v): return date.fromisoformat(v)
def _parse_ts(v):   return datetime.fromisoformat(v.replace(' ', 'T'))
def _parse_str(v):  return v

PARSE_BY_TYPE = {
    "int":           _parse_int,
    "integer":       _parse_int,
    "bigint":        _parse_int,
    "smallint":      _parse_int,
    "tinyint":       _parse_int,
    "date":          _parse_date,
    "timestamp":     _parse_ts,
    "timestamp_ntz": _parse_ts,
}


# ---- helpers ----------------------------------------------------------------

def require_env(name):
    v = os.getenv(name)
    if not v:
        sys.exit(f"ERROR: set {name}")
    return v


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def discover_schema(host, http_path, token, target):
    """Run DESCRIBE TABLE to discover column names and types."""
    with sql.connect(server_hostname=host, http_path=http_path,
                     access_token=token) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DESCRIBE TABLE {target}")
            rows = cur.fetchall()
    columns, types = [], []
    for row in rows:
        col_name, data_type = row[0], row[1]
        # DESCRIBE TABLE emits a section break (empty col, then "#") for
        # partition info. Stop reading at the break.
        if not col_name or col_name.startswith('#'):
            break
        columns.append(col_name)
        types.append(data_type)
    return columns, types


def make_parsers(types):
    return [PARSE_BY_TYPE.get(t.lower(), _parse_str) for t in types]


def compute_worker_segments(path, num_workers):
    """Line-aligned byte segments, one per worker (same as ClickHouse version)."""
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


# ---- worker -----------------------------------------------------------------

def worker(
    worker_id, file_path, start_byte, end_byte,
    target, columns, types,
    chunk_size, worker_rows,
    host, http_path, token,
    per_worker_rps,
    error_queue,
):
    parsers = make_parsers(types)
    placeholders = "(" + ",".join(["?"] * len(columns)) + ")"
    insert_sql = (
        f"INSERT INTO {target} ({','.join(columns)}) "
        f"VALUES {placeholders}"
    )

    try:
        conn = sql.connect(server_hostname=host, http_path=http_path,
                           access_token=token)
        cur = conn.cursor()
    except Exception as exc:
        error_queue.put(f"[w{worker_id}] connect failed: {exc}")
        return

    rows_sent = 0
    chunk_num = 0
    w_start = time.monotonic()
    fh = file_path.open("rb")
    fh.seek(start_byte)

    try:
        while rows_sent < worker_rows:
            # Rewind to segment start if we hit segment end (or EOF for the
            # last worker). Same loop-back semantics as the ClickHouse script.
            if end_byte != -1 and fh.tell() >= end_byte:
                fh.seek(start_byte)
            elif end_byte == -1:
                pos = fh.tell()
                fh.seek(0, 2)
                eof = fh.tell()
                fh.seek(pos)
                if pos >= eof:
                    fh.seek(start_byte)

            take = min(chunk_size, worker_rows - rows_sent)
            tuples = []
            for _ in range(take):
                if end_byte != -1 and fh.tell() >= end_byte:
                    break
                raw = fh.readline()
                if not raw:
                    break
                try:
                    line_str = raw.decode('utf-8', errors='replace')
                    parts = line_str.rstrip('\n').rstrip('\r').split('\t')
                    if len(parts) != len(parsers):
                        # Pad or truncate to expected column count
                        if len(parts) < len(parsers):
                            parts = parts + [''] * (len(parsers) - len(parts))
                        else:
                            parts = parts[:len(parsers)]
                    tuples.append(tuple(p(parts[i]) for i, p in enumerate(parsers)))
                except Exception as exc:
                    error_queue.put(f"[w{worker_id}] parse: {exc}")
                    continue

            if not tuples:
                fh.seek(start_byte)
                continue

            chunk_num += 1
            try:
                t0 = time.monotonic()
                cur.executemany(insert_sql, tuples)
                insert_s = time.monotonic() - t0
            except Exception as exc:
                error_queue.put(f"[w{worker_id}] INSERT: {exc}")
                continue

            rows_sent += len(tuples)
            elapsed = time.monotonic() - w_start
            rps = rows_sent / elapsed if elapsed > 0 else 0

            print(
                f"[w{worker_id:>3d} c{chunk_num:>5d}] "
                f"{len(tuples):>6,} rows in {insert_s:>5.1f}s | "
                f"sent={rows_sent:>11,}/{worker_rows:,} | "
                f"rps={rps:>8,.0f}",
                flush=True,
            )

            # Per-worker throttle
            if per_worker_rps > 0:
                expected = rows_sent / per_worker_rps
                if elapsed < expected:
                    time.sleep(expected - elapsed)

    finally:
        fh.close()
        try: cur.close()
        except Exception: pass
        try: conn.close()
        except Exception: pass

    print(f"[w{worker_id:>3d}] done: {rows_sent:,} rows in "
          f"{time.monotonic() - w_start:.1f}s", flush=True)


# ---- main -------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--file",                required=True,
                   help="Local TSV file (e.g. hits.tsv).")
    p.add_argument("--target",              required=True,
                   help="Fully-qualified target table. "
                        "Quote shell-side if name contains backticks.")
    p.add_argument("--parallel",            type=int, required=True,
                   help="Number of worker processes.")
    p.add_argument("--chunk-size",          type=int, required=True,
                   help="Rows per INSERT (= per Delta commit).")
    p.add_argument("--total-rows",          type=int, required=True,
                   help="Total rows to ingest across all workers. "
                        "If > source row count, workers replay.")
    p.add_argument("--target-rows-per-sec", type=int, default=0,
                   help="Cluster-wide rate throttle. 0 = no throttle. "
                        "Divided evenly across workers.")
    args = p.parse_args()

    path = Path(args.file)
    if not path.exists():
        sys.exit(f"ERROR: file not found: {path}")

    host      = require_env("DATABRICKS_SERVER_HOSTNAME")
    http_path = require_env("DATABRICKS_HTTP_PATH")
    token     = require_env("DATABRICKS_TOKEN")

    print(f"Start time     : {now_utc()}")
    print(f"File           : {path} ({path.stat().st_size / 1e9:.2f} GB)")
    print(f"Target         : {args.target}")
    print(f"Workers        : {args.parallel}")
    print(f"Chunk size     : {args.chunk_size:,} rows / INSERT")
    print(f"Total rows     : {args.total_rows:,}")
    if args.target_rows_per_sec:
        per_w = args.target_rows_per_sec // args.parallel
        print(f"Throttle       : {args.target_rows_per_sec:,} rows/sec total "
              f"(~{per_w:,}/worker)")
    else:
        print(f"Throttle       : off")

    print("→ Discovering target schema via DESCRIBE TABLE")
    columns, types = discover_schema(host, http_path, token, args.target)
    print(f"  {len(columns)} columns")

    print("→ Computing per-worker byte segments")
    segments = compute_worker_segments(path, args.parallel)

    base_rows  = args.total_rows // args.parallel
    remainder  = args.total_rows % args.parallel
    per_worker = (args.target_rows_per_sec // args.parallel
                  if args.target_rows_per_sec else 0)

    print()
    error_queue = mp.Queue()
    procs = []
    t0 = time.monotonic()

    for w in range(args.parallel):
        start_byte, end_byte = segments[w]
        worker_rows = base_rows + (1 if w < remainder else 0)
        proc = mp.Process(
            target=worker,
            args=(
                w + 1, path, start_byte, end_byte,
                args.target, columns, types,
                args.chunk_size, worker_rows,
                host, http_path, token,
                per_worker,
                error_queue,
            ),
        )
        proc.start()
        procs.append(proc)

    for proc in procs:
        proc.join()

    elapsed = time.monotonic() - t0

    errors = []
    while not error_queue.empty():
        errors.append(error_queue.get())

    print()
    print("=" * 70)
    print(f"End time       : {now_utc()}")
    print(f"Duration       : {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f"Target total   : {args.total_rows:,}")
    print(f"Avg rps target : {args.total_rows / elapsed:,.0f}")
    print(f"Errors         : {len(errors)}")
    for e in errors[:20]:
        print(f"  {e}", file=sys.stderr)
    if len(errors) > 20:
        print(f"  ... {len(errors) - 20} more", file=sys.stderr)
    print("=" * 70)
    sys.exit(1 if errors else 0)


# ---------------------------------------------------------------------------
# Throughput upgrade path
# -----------------------
# Parameterized INSERT via executemany is the simplest approach but isn't
# the fastest path Databricks offers. If you need more throughput:
#
#   1. Stage TSV (or pre-converted Parquet) chunks to S3 in a bucket the
#      workspace can read.
#   2. In worker(), replace the executemany() call with:
#          cur.execute(
#              f"COPY INTO {target} FROM '{chunk_s3_path}' "
#              "FILEFORMAT = CSV "
#              "FORMAT_OPTIONS('sep' = '\\t', 'header' = 'false') "
#              "COPY_OPTIONS ('mergeSchema' = 'false')"
#          )
#   3. Skip the per-row parsing entirely. Each COPY INTO is still one
#      Delta commit but reads the file natively. Expect 5-10x higher
#      per-worker throughput.
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    main()
