#!/usr/bin/env python3
"""
Clean-slate reset before a timed T2 run: fresh MSK topic -> fresh MVs -> fresh datashare -> verify.

Run this BEFORE every timed run. Three reasons it must be a single scripted step:
  * the three MVs must all exist BEFORE the stream starts, or the initial build lands inside the
    steady-state refresh measurements;
  * recreating the MVs invalidates the datashare, so the reader mount must be rebuilt after them;
  * a stale topic replays old offsets and corrupts the latency-vs-volume curve.

PARTITION COUNT IS A REAL TUNING KNOB, NOT A DETAIL
Redshift streaming ingestion runs ONE CONSUMER PER TOPIC PARTITION, so the partition count caps how
much ingest work can proceed in parallel. Measured 2026-08-12: with `sym`/`t` promoted to typed
columns (3 JSON parses per record) a 6-partition topic capped ingest at ~630K rows/s and adding RPUs
did not help (128 -> 256 RPU changed nothing) — the cap was consumer parallelism, not compute.
For reference, Snowflake's Snowpipe Streaming run used 8 channels, so 6 partitions was actually LESS
ingest parallelism than the vendor we're comparing against.

retention.bytes is PER PARTITION, so it must be scaled DOWN as partitions go up or the brokers'
disks are over-committed:  worst case per broker ~= partitions * retention.bytes  (RF3 over 3 brokers).
The default here keeps that near ~190 GB against 500 GB disks. retention.ms is the real guard (the
consumer reads live; we don't need history) — an unbounded topic once filled the disks and wedged the
cluster.

  python reset_run.py                      # 24 partitions, 8 GiB/partition
  python reset_run.py --partitions 6 --retention-bytes-gib 32
Env: RS_USER / RS_PASSWORD (+ optional RS_DB). No AWS credentials needed — topic admin is the Kafka
protocol and everything else is SQL.
"""
import argparse
import os
import sys
import time

import redshift_connector
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, ResourceType

BOOT = ("b-1.cbquotesrtmsk.3tt8fa.c2.kafka.eu-west-2.amazonaws.com:9092,"
        "b-2.cbquotesrtmsk.3tt8fa.c2.kafka.eu-west-2.amazonaws.com:9092,"
        "b-3.cbquotesrtmsk.3tt8fa.c2.kafka.eu-west-2.amazonaws.com:9092")
WRITER = "cb-quotes-rt-wg.244449518788.eu-west-2.redshift-serverless.amazonaws.com"
READER = "cb-quotes-rt-reader-wg.244449518788.eu-west-2.redshift-serverless.amazonaws.com"
WRITER_NS = "0bd64832-8c31-474d-8024-a54514061c52"
READER_NS = "ad3f5975-bb2c-49de-a8db-212d2451cdbe"
OBJECTS = ("quotes_streamed", "quotes_typed", "quotes_daily")


def conn(host):
    c = redshift_connector.connect(host=host, port=5439, database=os.environ.get("RS_DB", "quotes"),
                                   user=os.environ["RS_USER"], password=os.environ["RS_PASSWORD"])
    c.autocommit = True
    return c


def do(cur, sql, label=None, fatal=False):
    """Run one statement. Non-fatal failures are expected on the DROPs (objects may not exist);
    Redshift has no IF EXISTS for DATASHARE/DATABASE."""
    tag = label or " ".join(sql.split())[:64]
    t = time.time()
    try:
        cur.execute(sql)
        try:
            rows = cur.fetchall()
        except Exception:
            rows = None
        print(f"  OK   ({time.time()-t:5.1f}s) {tag}" + (f" -> {rows[:4]}" if rows else ""), flush=True)
        return rows
    except Exception as exc:
        kind = "FAIL" if fatal else "skip"
        print(f"  {kind} ({time.time()-t:5.1f}s) {tag}\n         {str(exc)[:170]}", flush=True)
        if fatal:
            sys.exit(1)
        return None


def statements(raw):
    """Split a .sql file into statements, stripping line comments FIRST (comments contain ';')."""
    nc = "\n".join((l[:l.index("--")] if "--" in l else l) for l in raw.splitlines())
    return [s.strip() for s in nc.split(";") if s.strip()]


def reset_topic(topic, partitions, retention_ms, retention_bytes):
    cfg = {"retention.ms": str(retention_ms), "retention.bytes": str(retention_bytes),
           "cleanup.policy": "delete"}
    a = AdminClient({"bootstrap.servers": BOOT, "security.protocol": "PLAINTEXT"})
    if topic in a.list_topics(timeout=20).topics:
        a.delete_topics([topic], operation_timeout=30)[topic].result(timeout=60)
        for _ in range(40):
            time.sleep(3)
            if topic not in a.list_topics(timeout=20).topics:
                break
        print("  deleted old topic", flush=True)
    a.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=3, config=cfg)],
                    operation_timeout=30)[topic].result(timeout=60)
    time.sleep(4)
    md = a.list_topics(timeout=20).topics[topic]
    cr = ConfigResource(ResourceType.TOPIC, topic)
    got = a.describe_configs([cr])[cr].result(timeout=20)
    rep = len(next(iter(md.partitions.values())).replicas)
    print(f"  created: {len(md.partitions)} partitions, RF={rep}, "
          f"retention.ms={got['retention.ms'].value}, retention.bytes={got['retention.bytes'].value}",
          flush=True)
    per_broker_gib = partitions * retention_bytes / 2**30
    print(f"  worst-case log per broker ~{per_broker_gib:,.0f} GiB (disks are 500 GiB)", flush=True)
    if per_broker_gib > 400:
        print("  WARNING: retention.bytes x partitions over-commits the broker disks — lower it.",
              flush=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--topic", default="quotes")
    ap.add_argument("--partitions", type=int, default=24,
                    help="one Redshift stream consumer per partition — this caps ingest parallelism")
    ap.add_argument("--retention-ms", type=int, default=1800000, help="30 min; consumer reads live")
    ap.add_argument("--retention-bytes-gib", type=float, default=8.0, help="PER PARTITION")
    ap.add_argument("--setup-sql", default="sql/setup_streaming.sql")
    args = ap.parse_args()

    print(f"=== 1. recreate topic `{args.topic}` ({args.partitions} partitions, RF3, bounded retention) ===")
    reset_topic(args.topic, args.partitions, args.retention_ms,
                int(args.retention_bytes_gib * 2**30))

    print("\n=== 2. recreate the three MVs (before any data flows) ===")
    w = conn(WRITER); wc = w.cursor()
    do(wc, "DROP DATASHARE quotes_share", "drop datashare (holds refs to the MVs)")
    for o in ("quotes_daily", "quotes_typed"):
        do(wc, f"DROP MATERIALIZED VIEW {o}", f"drop {o}")
    do(wc, "DROP MATERIALIZED VIEW quotes_streamed CASCADE", "drop quotes_streamed")
    do(wc, "DROP SCHEMA kafka", "drop external schema kafka (so the URI is re-read)")
    for s in statements(open(args.setup_sql).read()):
        do(wc, s, " ".join(s.split())[:64], fatal=True)
    print("  verify all three exist and are EMPTY:")
    for o in OBJECTS:
        do(wc, f"SELECT COUNT(*) FROM {o}", f"{o} rows")
    do(wc, "SELECT TRIM(name), autorefresh FROM SVV_MV_INFO WHERE schema_name='public' ORDER BY 1",
       "autorefresh flags (only quotes_streamed should be t)")

    print("\n=== 3. re-establish the datashare ===")
    do(wc, "CREATE DATASHARE quotes_share", fatal=True)
    do(wc, "ALTER DATASHARE quotes_share ADD SCHEMA public", fatal=True)
    for o in OBJECTS:
        do(wc, f"ALTER DATASHARE quotes_share ADD TABLE public.{o}", f"share {o}", fatal=True)
    do(wc, f"GRANT USAGE ON DATASHARE quotes_share TO NAMESPACE '{READER_NS}'",
       "grant to reader", fatal=True)
    wc.close(); w.close()

    r = conn(READER); rc = r.cursor()
    do(rc, "DROP DATABASE quotes_shared", "drop old mount")
    do(rc, f"CREATE DATABASE quotes_shared FROM DATASHARE quotes_share OF NAMESPACE '{WRITER_NS}'",
       "mount quotes_shared", fatal=True)
    do(rc, "DROP SCHEMA shared_public", "drop old external schema")
    do(rc, "CREATE EXTERNAL SCHEMA shared_public FROM REDSHIFT DATABASE 'quotes_shared' SCHEMA 'public'",
       "external schema shared_public", fatal=True)
    print("  reader sees (should be 0 rows each):")
    for o in OBJECTS:
        do(rc, f"SELECT COUNT(*) FROM shared_public.{o}", f"reader {o}", fatal=True)
    print("  search_path resolution check (the query files use unqualified names):")
    do(rc, "SET search_path TO shared_public", "set search_path")
    for o in OBJECTS:
        do(rc, f"SELECT COUNT(*) FROM {o}", f"unqualified {o}", fatal=True)
    rc.close(); r.close()
    print("\nRESET COMPLETE — clean topic, empty MVs, datashare live, reader resolves unqualified names.")


if __name__ == "__main__":
    main()
