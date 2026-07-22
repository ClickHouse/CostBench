#!/usr/bin/env python3
"""
Snowpipe Streaming ingester for the quotes dataset -> interactive table QUOTES_IT.

Replaces the COPY-based ingest.py: instead of PUT+COPY on a warehouse, this streams rows
directly into the interactive table via the Snowpipe Streaming channel API (serverless, no
ingest warehouse). Mirrors the SDK usage from Snowflake's interactive-tables + Snowpipe
Streaming guide:
    from snowflake.ingest.streaming import StreamingIngestClient
    with StreamingIngestClient(client_name, db_name, schema_name, pipe_name, profile_json) as c:
        with c.open_channel(channel_name)[0] as ch:
            ch.append_row(row_dict, str(offset_token))

Per-(file,row_group) work is split across N worker processes; each worker owns one channel.
A shared counter + --target-rps throttles the aggregate rate; a monitor prints live EPS.
Cost is tracked separately via METERING_HISTORY (SERVICE_TYPE='SNOWPIPE_STREAMING'); this
client uses no virtual warehouse.

  python3 stream_quotes.py --dir /data/quotes --schema STOCKHOUSE_T2 --pipe QUOTES_IT_PIPE \
      --profile profile.json --parallel 8 --target-rps 1000000

Requires:  pip install snowpipe-streaming pyarrow   (imports as snowflake.ingest.streaming)
"""
import argparse
import json
import multiprocessing as mp
import os
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq


def write_profile(path, account, user, key_path):
    """Write the SDK profile.json (key-pair auth) if it doesn't already exist."""
    if os.path.exists(path):
        return
    prof = {
        "account": account,
        "user": user,
        "host": f"{account}.snowflakecomputing.com",
        "port": "443",          # SDK config parser requires port as a STRING, not int
        "scheme": "https",
        "private_key": Path(key_path).read_text(),   # PEM contents
    }
    Path(path).write_text(json.dumps(prof))
    print(f"wrote {path}", file=sys.stderr)


def enumerate_tasks(directory, pattern, max_files, rg_per_task):
    """(file, [row_group_indices]) tasks; skip unreadable/partial files so the streamer can
    coexist with an in-progress download."""
    files = sorted(Path(directory).glob(pattern))
    if max_files:
        files = files[:max_files]
    tasks = []
    for f in files:
        try:
            n = pq.ParquetFile(f).num_row_groups
        except Exception:
            continue
        for s in range(0, n, rg_per_task):
            tasks.append((str(f), list(range(s, min(s + rg_per_task, n)))))
    return tasks


def worker(wid, tasks, args, shared_rows, global_start):
    from snowflake.ingest.streaming import StreamingIngestClient
    client = StreamingIngestClient(
        client_name=f"QUOTES_STREAM_{wid}",
        db_name=args.database,
        schema_name=args.schema,
        pipe_name=args.pipe,
        profile_json=args.profile,
    )
    offset = 0
    sent = 0
    try:
        ch = client.open_channel(f"ch_{wid}")[0]
        with ch:
            for path, rgs in tasks:
                try:
                    pf = pq.ParquetFile(path)
                    for rg in rgs:
                        for row in pf.read_row_group(rg).to_pylist():
                            ch.append_row(row, str(offset))
                            offset += 1
                            sent += 1
                    # publish progress + throttle to the global target rate
                    with shared_rows.get_lock():
                        shared_rows.value += sent
                        total = shared_rows.value
                    sent = 0
                    if args.target_rps > 0:
                        expected = (time.time() - global_start) * args.target_rps
                        if total > expected:
                            time.sleep((total - expected) / args.target_rps)
                except Exception as exc:
                    print(f"[w{wid}] task {path} rg{rgs[0]} error: {exc}", file=sys.stderr, flush=True)
    finally:
        try:
            client.close()
        except Exception:
            pass


def monitor(shared_rows, interval, global_start, stop):
    last = 0
    while not stop.value:
        time.sleep(interval)
        with shared_rows.get_lock():
            total = shared_rows.value
        el = time.time() - global_start
        inst = (total - last) / interval
        print(f"  t={el:8.1f}s  rows={total:,}  +{total-last:,}  inst={inst:,.0f}/s  "
              f"avg={total/el if el else 0:,.0f}/s", flush=True)
        last = total


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--pattern", default="quotes_*.parquet")
    ap.add_argument("--database", default="BENCH2COST")
    ap.add_argument("--schema", default="STOCKHOUSE_T2")
    ap.add_argument("--pipe", default="QUOTES_IT_PIPE")
    ap.add_argument("--profile", default="profile.json")
    ap.add_argument("--parallel", type=int, default=8)
    ap.add_argument("--max-files", type=int, default=None)
    ap.add_argument("--row-groups-per-task", type=int, default=8)
    ap.add_argument("--target-rps", type=int, default=0, help="aggregate rows/s ceiling (0=unlimited)")
    ap.add_argument("--live-eps-interval", type=float, default=15.0)
    args = ap.parse_args()

    # build profile.json from env if missing (key-pair auth, same key as the runners)
    write_profile(args.profile,
                  os.environ.get("SF_ACCOUNT", ""),
                  os.environ.get("SF_USER", ""),
                  os.environ.get("SF_KEY", "/home/ubuntu/bench/keys/rsa_key.p8"))

    tasks = enumerate_tasks(args.dir, args.pattern, args.max_files, args.row_groups_per_task)
    if not tasks:
        sys.exit(f"no tasks from {args.dir}/{args.pattern}")
    print(f"{len(tasks)} tasks across {args.parallel} channels -> "
          f"{args.database}.{args.schema} via pipe {args.pipe}", flush=True)

    shared_rows = mp.Value("q", 0)
    stop = mp.Value("b", 0)
    global_start = time.time()
    mon = mp.Process(target=monitor, args=(shared_rows, args.live_eps_interval, global_start, stop))
    mon.start()

    # round-robin tasks across workers
    buckets = [tasks[i::args.parallel] for i in range(args.parallel)]
    procs = [mp.Process(target=worker, args=(w, buckets[w], args, shared_rows, global_start))
             for w in range(args.parallel)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    stop.value = 1
    mon.join(timeout=2)

    el = time.time() - global_start
    with shared_rows.get_lock():
        total = shared_rows.value
    print(f"DONE: streamed {total:,} rows in {el:.0f}s (avg {total/el if el else 0:,.0f}/s)", flush=True)


if __name__ == "__main__":
    main()
