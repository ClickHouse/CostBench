#!/usr/bin/env python3
"""
Kafka producer for the quotes dataset — the Redshift streaming-ingestion source.

Runs on the broker EC2 alongside the local single-broker Kafka. Reads the quotes parquet and
publishes one JSON record per row to the topic (plaintext, in-box). Multi-process: per-(file,
row_group) work split across N workers, each its own librdkafka Producer (batched + lz4). A shared
counter + --target-rps throttles the aggregate rate; a monitor prints EPS. Same shape as
../../snowflake/t2/stream_quotes.py; the sink is the local Kafka broker instead of Snowpipe.

  python3.11 produce_quotes.py --bootstrap localhost:9092 --topic quotes \
      --dir /data/quotes --parallel 8 --target-rps 1000000

Requires: pip install -r requirements.txt   (confluent-kafka, pyarrow)
"""
import argparse, json, multiprocessing as mp, sys, time
from pathlib import Path
import pyarrow.parquet as pq
from confluent_kafka import Producer


def make_producer(bootstrap):
    return Producer({
        "bootstrap.servers": bootstrap,
        "security.protocol": "PLAINTEXT",
        "compression.type": "lz4",
        "linger.ms": 100,
        "batch.size": 1 << 20,
        "queue.buffering.max.messages": 2_000_000,
        "queue.buffering.max.kbytes": 1 << 20,
        "acks": "1",
    })


def enumerate_tasks(directory, pattern, max_files, rg_per_task):
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
    p = make_producer(args.bootstrap)
    sent = 0
    for path, rgs in tasks:
        try:
            pf = pq.ParquetFile(path)
            for rg in rgs:
                for row in pf.read_row_group(rg).to_pylist():
                    p.produce(args.topic, key=str(row.get("sym", "")),
                              value=json.dumps(row, separators=(",", ":"), default=str))
                    sent += 1
                    if sent % 10000 == 0:
                        p.poll(0)
                with shared_rows.get_lock():
                    shared_rows.value += sent
                    total = shared_rows.value
                sent = 0
                if args.target_rps > 0:
                    expected = (time.time() - global_start) * args.target_rps
                    if total > expected:
                        time.sleep((total - expected) / args.target_rps)
        except Exception as exc:
            print(f"[w{wid}] {path} rg{rgs[0]} error: {exc}", file=sys.stderr, flush=True)
    p.flush(30)


def monitor(shared_rows, interval, global_start, stop):
    last = 0
    while not stop.value:
        time.sleep(interval)
        with shared_rows.get_lock():
            total = shared_rows.value
        el = time.time() - global_start
        print(f"  t={el:8.1f}s  rows={total:,}  +{total-last:,}  "
              f"inst={(total-last)/interval:,.0f}/s  avg={total/el if el else 0:,.0f}/s", flush=True)
        last = total


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="quotes")
    ap.add_argument("--dir", required=True, help="local dir of quotes_*.parquet")
    ap.add_argument("--pattern", default="quotes_*.parquet")
    ap.add_argument("--parallel", type=int, default=8)
    ap.add_argument("--max-files", type=int, default=None)
    ap.add_argument("--row-groups-per-task", type=int, default=8)
    ap.add_argument("--target-rps", type=int, default=0, help="aggregate rows/s ceiling (0=unlimited)")
    ap.add_argument("--live-eps-interval", type=float, default=15.0)
    args = ap.parse_args()

    tasks = enumerate_tasks(args.dir, args.pattern, args.max_files, args.row_groups_per_task)
    if not tasks:
        sys.exit(f"no tasks from {args.dir}/{args.pattern}")
    print(f"{len(tasks)} tasks across {args.parallel} producers -> topic {args.topic} @ {args.bootstrap}", flush=True)

    shared_rows = mp.Value("q", 0)
    stop = mp.Value("b", 0)
    global_start = time.time()
    mon = mp.Process(target=monitor, args=(shared_rows, args.live_eps_interval, global_start, stop))
    mon.start()
    buckets = [tasks[i::args.parallel] for i in range(args.parallel)]
    procs = [mp.Process(target=worker, args=(w, buckets[w], args, shared_rows, global_start))
             for w in range(args.parallel)]
    for p in procs: p.start()
    for p in procs: p.join()
    stop.value = 1
    mon.join(timeout=2)
    el = time.time() - global_start
    with shared_rows.get_lock():
        total = shared_rows.value
    print(f"DONE: produced {total:,} rows in {el:.0f}s (avg {total/el if el else 0:,.0f}/s)", flush=True)


if __name__ == "__main__":
    main()
