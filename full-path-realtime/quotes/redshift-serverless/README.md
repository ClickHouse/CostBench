# Real-time quotes benchmark — Amazon Redshift Serverless (design notes)

**Status (2026-08-14): T2 completed.** The run ingested **113,219,565,734 rows** at an observed
average **999,933 rows/s** through the production MSK path. The committed results, hourly reader
allocation, reproducible cost summaries, pairwise charts, and global charts are under `results/t2/`,
`costs/out/t2/`, and `visualizations/`.

**Implemented source: Amazon MSK** (provisioned, `TLS_PLAINTEXT`, unauthenticated) — producer → MSK
**plaintext :9092**, Redshift → MSK **TLS :9094** (`AUTHENTICATION none`). This is the AWS-recommended
real-time path (below), and the recommendation at ~1M EPS all along.

> **Why not the self-managed single-broker Kafka we started with?** It was the lean/cheap way to prove
> the pipeline, and it did prove the producer→Kafka side (~1.45M EPS). But **Redshift streaming
> ingestion requires TLS with a CA-trusted server certificate**: it rejected the broker's plaintext
> listener (`Broker transport failure`) and then a self-signed TLS listener (`SSL handshake failed` —
> Redshift verifies the cert, and offers no skip-verify/custom-truststore). MSK's Amazon-issued broker
> certs are trusted out of the box, so it's the viable source. (mTLS via ACM Private CA also works but
> is ~$400/mo + fiddly — not worth it for a benchmark.)

## Why this document

The other vendors use their native real-time path (Snowflake = Snowpipe Streaming push SDK;
ClickHouse = synchronous incremental MV). To keep the comparison fair, Redshift must use **its**
recommended real-time path. Per AWS, that is **streaming ingestion into a materialized view** on
Amazon Kinesis Data Streams (KDS) or Amazon MSK — a first-class, GA feature, not a workaround.

## Recommended architecture

```
producer  ──►  Amazon MSK (or Kinesis Data Streams)
                     │
                     ▼
        quotes_streamed — streaming MATERIALIZED VIEW (AUTO REFRESH YES)
        - defined directly on the stream via an EXTERNAL SCHEMA
        - stores the payload minimally as SUPER; NO heavy transforms here
                     │
          ┌──────────┴───────────┐        a FORK, not a cascade:
          ▼                      ▼        both children are defined DIRECTLY on
   quotes_typed            quotes_daily   quotes_streamed and refreshed with RESTRICT
   typed row projection    (sym, day) aggregate
   SORTKEY (sym, t)        SORTKEY (sym, day)
   manual incremental      manual incremental
          │                      │
          └──────────┬───────────┘
                     ▼  live datashare
        READER workgroup (separate namespace + RPUs)
        drilldown on SUPER  |  drilldown on typed  |  dashboard on rollup
```

Data flows **directly from the stream into the MV — no S3 staging** — at seconds-level latency and
hundreds of MB/s of ingest. ([Streaming ingestion to a materialized view][si])

**Why a fork rather than `streamed → typed → daily`:** chaining would stack the typed refresh lag on
top of the daily refresh lag and make dashboard freshness depend on the typed branch. Keeping both
children directly on the streaming MV lets them be scheduled, measured and costed independently.

**Why both a SUPER and a typed read path:** querying the SUPER payload directly is not comparable to
Snowflake's typed `QUOTES_IT` or ClickHouse's typed table, so the benchmark runs the *same* drilldown
logic against both and reports the difference. A small 571.9M-row characterization favored typed
columns, but the complete active-ingestion run reversed that result: typed was **2.11× slower in
accumulated runtime overall**. Q1 was nearly equal (1.01× cumulative); typed Q2 was 4.42× slower.

## Best practices (AWS-supported)

1. **Land raw in the streaming MV; transform downstream.** Keep the streaming MV minimal — store the
   record as `SUPER`/`VARBYTE` and do JSON parsing + the `(sym, day)` aggregation in *separate
   downstream MVs*. Heavy transforms in the streaming MV reduce ingest throughput and can prevent
   auto-refresh. ([Streaming ingestion][si])
2. **Refresh the children explicitly with `RESTRICT`, not `CASCADE`.** Cascading refresh of nested MVs
   exists (GA July 2025) and would refresh a whole chain in one operation
   ([announcement][cr]), but this benchmark deliberately does **not** use it: our two children are a
   fork on the same streaming MV, and `CASCADE` would also refresh the streaming MV underneath —
   conflating ingest with child maintenance. `RESTRICT` (the default, passed explicitly) refreshes
   only the named MV, so each child consumes whatever state of `quotes_streamed` is committed when
   its own refresh transaction begins. **Verify both children report *incremental* refresh** in
   `SYS_MV_REFRESH_HISTORY` — a silent switch to full recompute is fatal at 113B rows.
   (Note: `AUTO REFRESH` is rejected on an MV defined on another MV, so a controller drives them.)
3. **`AUTO REFRESH YES`** on the streaming MV; fall back to scheduled `REFRESH` only if an SLA needs a
   fixed cadence. ([Streaming ingestion][si])
4. **One streaming MV per stream/topic.** Each MV is a separate stream consumer and shares stream
   bandwidth; multiple MVs on one topic slow ingestion. ([MSK best practices][msk])
5. **Serverless sizing:** start around **8 RPU per 4 MSK partitions** and let Serverless auto-scale;
   monitor with the `SYS_*` views. ([MSK best practices][msk])
6. **Downstream physical design:** sort/dist keys on the raw + rollup layers; rely on
   auto-table-optimization; monitor unsorted region (the Redshift analog of Snowflake clustering depth).

## At ~1M events/sec: prefer MSK over Kinesis Data Streams

Kinesis Data Streams is quota'd at **1 MB/s and 1,000 records/s per shard**, so ~1M small-record EPS
needs ~1,000 shards or KPL record aggregation. **Amazon MSK (Kafka) scales more naturally by
partition at this throughput**, and AWS's near-real-time best-practices guidance is written around
MSK — so MSK is the recommended source for the benchmark's ~1M EPS rate. ([MSK best practices][msk])

## Mapping to the benchmark components

| Benchmark piece                  | Redshift Serverless equivalent                                          |
|----------------------------------|-------------------------------------------------------------------------|
| Snowpipe Streaming pipe          | MSK → Redshift streaming ingestion                                      |
| Ingest (~1M EPS)                 | producer → **MSK** → `quotes_streamed` (`AUTO REFRESH YES`)             |
| Raw landing                      | `quotes_streamed` — streaming MV, `SUPER` payload (also a read target)  |
| Typed raw table (`QUOTES_IT`)    | `quotes_typed` — typed projection MV, manual incremental refresh        |
| `CLUSTER BY (sym, t)`            | `SORTKEY (sym, t)` + `DISTSTYLE EVEN` (not `DISTKEY(sym)` — see below)  |
| Rollup (`QUOTES_DAILY`)          | `quotes_daily` — `(sym, day)` MV, manual incremental refresh            |
| Interactive read warehouse       | separate **reader** workgroup + namespace, via live datashare           |
| Dashboard / drilldown            | same query set; drilldown run twice (SUPER vs typed) for comparison     |
| Freshness lag (`behind_by`)      | stream scan lag (`SYS_STREAM_SCAN_STATES`) + each child's refresh cadence |
| Storage                          | Redshift Managed Storage (`SVV_TABLE_INFO`)                             |
| Snowpipe + IMV credits           | **writer** RPU-seconds (ingest + both refreshes)                        |
| Read compute                     | **reader** RPU-seconds                                                  |
| Cost                             | one shared writer-capacity + MSK path; hourly allocated reader query cost |

`DISTSTYLE EVEN` is deliberate: `DISTKEY(sym)` would place every row of one symbol on a single slice,
making the single-symbol drilldown effectively single-slice. The sort key supplies the pruning while
EVEN preserves scan parallelism.

The reader workgroup **must not be publicly accessible** — a publicly-accessible consumer cannot read
datashare objects — so the read-runners execute from the in-VPC producer box.

## How it differs from the other vendors (surface in the comparison, don't hide)

- **Ingest model:** push-SDK (Snowflake) vs **queue + pull-refresh MV** (Redshift) vs synchronous
  incremental MV (ClickHouse).
- **No interactive-warehouse 5s statement cap** → the Snowflake "timeout → fallback" story has no
  Redshift analog; the Redshift comparison is Serverless latency + concurrency scaling under load.
- **Freshness is refresh-cadence-driven**, not a continuous background service.
- **Extra cost line:** MSK provisioned broker-hours + storage that the push-SDK vendors do not carry.

## Answers to the original open questions (measured, not assumed)

| Question | Answer (as of 2026-08-12) |
|---|---|
| MSK sizing for ~1M EPS | **3× `kafka.m7g.xlarge`, 3 AZ, RF=3, 24 partitions.** Measured ingress **~28.5 MB/s compressed** at ~999K EPS (lz4 ≈ 4.8:1, ~138 B/row raw). |
| Serverless RPU floor | Writer keeps up with **1M EPS at 128 RPU, base=max**: per-partition offset lag **0**, active streaming freshness median 5 s. Reader is 32 RPU, `max = base`. |
| Does the rollup refresh incrementally? | **Yes** — both children report *"updated MV incrementally"*. Keep to COUNT/SUM/MIN/MAX and bucket the day with `DATE_TRUNC` (a `::date` cast risks the mutable-date-time rule that forces full recompute). Typed refresh: 74 s initial build → **17.6 s incremental** under load, 0.4 s quiesced. |
| Freshness metric | `SYS_STREAM_SCAN_STATES` (`lag_from_latest`, `max_latency_s`) — **point-in-time, must be sampled live**; `monitor_lag.py` writes it to `lag_*.jsonl`. Rollup freshness = that lag + the child's cadence. |
| Cost attribution | Writer: 128 RPU × full 113,227-second producer uptime. MSK: broker-hours + prorated storage. This **one shared fresh path** is reused by both read variants. Reader queries: committed hourly `compute_seconds` allocated by statement elapsed share. Client cross-AZ and RMS are excluded from the main comparison. |

## Implementation (built)

- `sql/setup_streaming.sql` — external schema + the three-object fork
- `sql/setup_datashare.sql` — writer→reader live datashare (all three read targets)
- `sql/queries_dashboard.sql`, `queries_drilldown_super.sql`, `queries_drilldown_typed.sql`
- `produce_quotes.py` — MSK producer (~1M EPS, 16 procs, lz4)
- `monitor_lag.py` — freshness sampler **+** independent single-flight refresh controller
- `runner_redshift.py` — read-runner, `--role dashboard|drilldown_super|drilldown_typed`
- `costs/` — reproducible London pricing, hourly-allocation query summaries, shared fresh-path cost, and pairwise full-path summaries
- `get_metrics.py` — optional post-run usage/storage/refresh audit; not required to regenerate published query costs
- `visualizations/` — Redshift pairwise, representation, freshness, fresh-path, and full-path renderers
- `infra/` — terraform: MSK, writer workgroup, reader namespace/workgroup

See `PIPELINE.md` for the verified data flow, `ARCHITECTURE.md` for rationale + reviewer questions,
`RUNBOOK.md` for how to run it.
The global manifest includes one Redshift dashboard series, separate SUPER and typed drill-down
series, one shared fresh-path bar, and two read-path score alternatives.

## Sources

- [Streaming ingestion to a materialized view — Amazon Redshift Developer Guide][si]
- [Best practices to implement near-real-time analytics using Redshift Streaming Ingestion with Amazon MSK — AWS Big Data Blog][msk]
- [Amazon Redshift announces cascading refresh of nested materialized views (Jul 2025)][cr]
- [Amazon Redshift real-time streaming ingestion GA for KDS and MSK][ga]

[si]: https://docs.aws.amazon.com/redshift/latest/dg/materialized-view-streaming-ingestion.html
[msk]: https://aws.amazon.com/blogs/big-data/best-practices-to-implement-near-real-time-analytics-using-amazon-redshift-streaming-ingestion-with-amazon-msk/
[cr]: https://aws.amazon.com/about-aws/whats-new/2025/07/amazon-redshift-cascading-refresh-nested-materialized-views/
[ga]: https://aws.amazon.com/about-aws/whats-new/2022/11/amazon-redshift-real-time-streaming-ingestion-kds-msk/
