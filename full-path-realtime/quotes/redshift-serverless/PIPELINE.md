# Redshift Serverless real-time pipeline — data flow (verified)

The one-liner, verified and made precise:

> producer (EC2) → Amazon MSK → Redshift **streaming ingestion** into a **streaming materialized
> view** (Redshift *pulls* from the stream; `AUTO REFRESH`), which **forks** into a **typed row MV**
> and a **daily rollup MV** (both refreshed on their own cadence — neither can auto-refresh), read
> from a **separate reader workgroup** over a live datashare.

Two corrections to the earlier description of this pipeline:
1. It is a **fork, not a cascade**. `quotes_typed` and `quotes_daily` are both defined **directly on
   `quotes_streamed`**, and each is refreshed with `RESTRICT`. Refreshing a child does **not** cascade
   into the streaming MV. (The old text said the rollup refresh "cascades through the raw layer" —
   that behaviour is deliberately avoided now: it would conflate ingest with child maintenance and
   stack the two refresh lags.)
2. Reads run on a **separate workgroup**, not the ingest one.

```text
EC2 producer  --1M EPS-->  MSK topic `quotes`
                                  |
                                  v
                          quotes_streamed          streaming MV, SUPER payload, AUTO REFRESH YES
                            /            \          (the only auto-refreshing object)
                           v              v
                    quotes_typed      quotes_daily
                    typed columns     (sym, day) rollup
                    SORTKEY(sym,t)    SORTKEY(sym,day)
                    manual refresh    manual refresh        <- independent cadences, RESTRICT
                           \              /
                            \            /
                          live datashare `quotes_share`
                                  |
                                  v
                      READER workgroup (own RPUs)
              drilldown_super / drilldown_typed / dashboard
```

## Step by step (with the real objects)

1. **Producer (EC2)** — `produce_quotes.py` (confluent-kafka, 16 procs) reading `/data/quotes`,
   writing to MSK over **plaintext `:9092`** at ~1M EPS (measured 1.00M/s sustained).

2. **Amazon MSK** — provisioned cluster `cb-quotes-rt-msk` (**3× `kafka.m7g.xlarge`**, 3 AZs). Topic
   `quotes`, 6 partitions, **RF=3**, **bounded retention** (`retention.ms=1800000` +
   `retention.bytes=32 GB/partition`) — Redshift consumes live, and an unbounded topic once filled
   the broker disks. Measured ingress: **~28.5 MB/s compressed** at ~999K EPS (lz4 ≈ 4.8:1).

3. **Redshift streaming ingestion (pull)** — DB `quotes`, **writer** workgroup `cb-quotes-rt-wg`
   (Serverless, base 128 / max 256 RPU, enhanced VPC routing):
   - `CREATE EXTERNAL SCHEMA kafka FROM KAFKA URI '<MSK TLS :9094>' AUTHENTICATION none` — Redshift
     connects to MSK over **TLS** (Amazon-trusted cert).
   - **`quotes_streamed`** — streaming MV, `AUTO REFRESH YES`, lands each record as a `SUPER`
     payload via `JSON_PARSE(FROM_VARBYTE(kafka_value,'utf-8'))`. Deliberately minimal: AWS warns
     that shredding N typed columns at ingest re-parses each record N times and raises latency.
     Redshift auto-refreshes this roughly every ~8 s. Measured keep-up: **1M EPS, offset lag 0,
     ~6–9 s freshness at the 128 RPU floor**.

4. **`quotes_typed`** — typed row projection **on `quotes_streamed`**: the 12 quote fields cast to
   physical columns (`i` stays SUPER — it's an array), Kafka metadata retained as a watermark.
   `DISTSTYLE EVEN` + `SORTKEY (sym, t)` — EVEN deliberately, since `DISTKEY(sym)` would put all of
   one symbol on a single slice and make the single-symbol drilldown effectively single-slice.

5. **`quotes_daily`** — `(sym, day)` rollup, **also on `quotes_streamed`** (not on `quotes_typed`),
   re-extracting the few fields it needs. That small duplication keeps dashboard freshness
   independent of the typed branch. Uses only COUNT/SUM/MIN/MAX and `DATE_TRUNC` so it stays inside
   Redshift's incremental-refresh subset.

6. **Refresh controller** — `monitor_lag.py` runs one process against the writer with independent,
   single-flight loops: `quotes_typed` continuously (next refresh a couple of seconds after the
   previous finishes) and `quotes_daily` fixed-rate (60 s). Both use `RESTRICT`, never `CASCADE`.
   Every attempt is journalled to `refresh_*.jsonl` with the server's own `refresh_type`
   (Incremental vs Full) from `SYS_MV_REFRESH_HISTORY`.

7. **Reads** — on the **reader** workgroup `cb-quotes-rt-reader-wg` (own namespace, 32 RPU) via the
   live datashare `quotes_share`. Three roles, all timed by `runner_redshift.py`
   (server-side timings from `SYS_QUERY_HISTORY`, result cache off):
   - `--role drilldown_super` → `quotes_streamed` (live SUPER navigation)
   - `--role drilldown_typed` → `quotes_typed` (typed columns) — same logic, for a direct comparison
   - `--role dashboard` → `quotes_daily`

   The reader workgroup **must not be publicly accessible** (a publicly-accessible consumer cannot
   read datashare objects), so the runners execute from the in-VPC producer box.

## Freshness & cost pointers
- **Freshness**: `SYS_STREAM_SCAN_STATES` is point-in-time — `monitor_lag.py` samples it live into
  `lag_*.jsonl`. Rollup freshness = streaming auto-refresh lag + the child's own cadence.
- **Cost**: `get_metrics.py`, run **once per workgroup**:
  `writer RPU-seconds + reader RPU-seconds + Redshift Managed Storage + MSK broker-hours + MSK
  storage + client cross-AZ`. Per-MV refresh counts/durations/incremental-vs-full are reported, but
  not per-MV dollars — Serverless bills per workgroup per minute while ingest and refreshes overlap.

DDL: `sql/setup_streaming.sql`. Datashare: `sql/setup_datashare.sql`. Rationale + reviewer
questions: `ARCHITECTURE.md`.
