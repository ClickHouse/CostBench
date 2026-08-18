# Redshift Serverless real-time architecture — for review

**Purpose:** benchmark a real-time analytics pipeline (ingest market-quote data at **~1M events/sec**
with a rollup attached, then serve dashboard + drilldown queries) on **Amazon Redshift Serverless**,
to compare **cost and latency** against Snowflake and ClickHouse doing the *same* workload. This doc
is for an AWS-savvy reviewer to sanity-check the architecture and, critically, whether the **MSK cost
we attribute to Redshift is fair and right-sized** — it's a cost line the other two vendors don't have.

## Architecture

```
 producer EC2                Amazon MSK (provisioned)        Redshift Serverless — WRITER workgroup
 ┌──────────────────┐  :9092 ┌──────────────────────┐ :9094 ┌──────────────────────────────────────┐
 │ produce_quotes.py│ ─────▶ │ topic `quotes`       │ ────▶ │ EXTERNAL SCHEMA kafka (TLS,AUTH none)│
 │ 16 procs, ~1M EPS│  plain │ 24 partitions, RF=3  │  TLS  │        ▼                             │
 └──────────────────┘        │ 3× kafka.m7g.xlarge  │       │ quotes_streamed  streaming MV, SUPER │
                             │ retention capped     │       │                  AUTO REFRESH YES    │
                             └──────────────────────┘       │       ╱          ╲                   │
                                                            │      ▼            ▼                  │
                                                            │ quotes_typed   quotes_daily          │
                                                            │ typed cols     (sym,day) rollup      │
                                                            │ SORTKEY(sym,t) SORTKEY(sym,day)      │
                                                            │ manual refresh manual refresh        │
                                                            └───────────────┬──────────────────────┘
                                                                            │ live datashare
                                                                            ▼
                                                            ┌──────────────────────────────────────┐
                                                            │ READER workgroup (own namespace/RPUs)│
                                                            │ drilldown_super | drilldown_typed |  │
                                                            │ dashboard                            │
                                                            └──────────────────────────────────────┘
```

- **Ingest:** a producer streams the dataset into an MSK topic at ~1M EPS (plaintext `:9092`, fast).
- **Redshift streaming ingestion (GA):** an `EXTERNAL SCHEMA … FROM KAFKA` + a **streaming
  materialized view** (`quotes_streamed`, `AUTO REFRESH YES`) pulls from the topic over MSK's **TLS
  `:9094`**, landing each record as a `SUPER` payload. Kept minimal on purpose — AWS's guidance is
  that shredding N typed columns at ingest re-parses each record N times and raises ingest latency.
- **Fork, not cascade:** `quotes_typed` (typed columns) and `quotes_daily` (`(sym,day)` rollup) are
  **both defined directly on `quotes_streamed`** and refreshed independently with `RESTRICT`.
  Chaining them would stack the two refresh lags and make dashboard freshness depend on the typed
  branch. Neither child can `AUTO REFRESH` (Redshift disallows it on an MV defined on another MV),
  so a controller drives them: typed continuously, daily fixed-rate.
- **Read (compute-isolated):** all three query suites run on a **separate reader workgroup** over a
  live datashare, so read latency and read cost are attributable and don't perturb ingest.
  The reader **must not be publicly accessible** — a publicly-accessible consumer cannot read
  datashare objects — so the runners execute from the in-VPC producer box.
- **The SUPER-vs-typed comparison is a deliberate result:** the same drilldown logic runs against
  the live `SUPER` payload *and* the typed projection, so the cost of semi-structured access is
  measured rather than assumed.
- **Sizing:** writer base **128 RPU / max 128 RPU** for the measured run (keeps up with 1M EPS at 128
  floor, offset lag 0, ~6–9 s freshness); reader **32 RPU, max = base** during characterization;
  MSK 3× `kafka.m7g.xlarge` (Graviton; 3 AZ, RF=3), EBS 500 GB/broker.

## Why MSK (and not a push API, and not a local broker)

1. **Redshift has no low-latency *push* ingest.** Its GA real-time path is **streaming ingestion**:
   Redshift *pulls* from a stream (**Kinesis Data Streams or MSK**) into a materialized view. `COPY` is
   batch-from-S3; `INSERT` is row-at-a-time and far too slow. So a **stream service is mandatory** —
   there is no Redshift equivalent of a client SDK that writes rows straight into a table.
2. **MSK over Kinesis Data Streams at ~1M EPS.** KDS is quota'd at **1 MB/s and 1,000 records/s per
   shard**, so ~1M small-record EPS needs ~1,000 shards (or KPL record aggregation). **MSK scales by
   partition**, and AWS's near-real-time best-practice guidance for Redshift streaming ingestion is
   written around MSK. (KDS On-Demand is a possible alternative — see "For the reviewer".)
3. **MSK over a self-managed single-broker Kafka on the box.** We tried the lean/cheap option first
   and it **does not work with Redshift**: Redshift streaming ingestion requires **TLS with a
   CA-trusted server certificate**. It rejected our broker's plaintext listener (`Broker transport
   failure`) and then a self-signed TLS listener (`SSL handshake failed` — Redshift verifies the cert
   and offers no skip-verify). **MSK presents Amazon-issued, publicly-trusted broker certs**, so
   `AUTHENTICATION none` (one-way TLS) works out of the box. mTLS via ACM Private CA also works but
   costs ~$400/mo and is fiddly.

## Why this differs from Snowflake and ClickHouse (and why that matters for cost)

The other two vendors ingest via a **push** model — the client writes rows *directly* into the
warehouse's own ingest endpoint. **No broker sits in the data path, so there is no separate queue cost.**

| | Real-time ingest model | Broker / queue in the path? | Extra cost line |
|---|---|---|---|
| **Snowflake** | Snowpipe Streaming **push SDK** — client pushes rows to a serverless ingest endpoint | No | none (Snowpipe Streaming credits only) |
| **ClickHouse** | Direct client **INSERT** (native/HTTP, async) into a synchronous incremental MV | No | none (just CH compute) |
| **Redshift** | **Pull** — streaming ingestion from a stream into a materialized view | **Yes — MSK (or KDS) is required** | **MSK broker-hours + storage** |

So Redshift structurally needs an extra component (the stream) that Snowflake and ClickHouse do not.
This is **not us handicapping Redshift** — it's Redshift's own recommended real-time architecture. To
keep the comparison apples-to-apples on the *outcome* (rows queryable at ~1M EPS with seconds-level
freshness), each vendor uses **its** native real-time path; Redshift's happens to include a broker,
and that broker is a genuine cost of running Redshift in real time.

## Cost accounting (what we publish)

The published fresh-data-path model is **128 writer RPU × producer uptime + MSK broker-hours +
prorated MSK storage**. It is one shared write path for both read representations and is never split
or doubled. Query cost is separate: the committed hourly reader `compute_seconds` allocation is
distributed by each statement's elapsed share of its start-hour. This is normalized query cost, not
a literal invoice reconstruction. Client cross-AZ is excluded by benchmark-owner policy, and the
final RMS snapshot is retained as evidence but excluded because it is not time-integrated.

## Operational notes that shaped the design (both hit during bring-up)

- **TLS is mandatory** (see above) → drove the move to MSK.
- **Topic retention must be bounded.** With MSK default (7-day, unbounded) retention, at 1M EPS ×
  ~138 B/row × RF3 the 100 GB brokers filled at ~720M rows and wedged (cluster still reported
  `ACTIVE`). Fix: `retention.ms`/`retention.bytes` cap (Redshift consumes live, so retention isn't
  needed) + larger EBS. This is an MSK-operational cost/config point, not a Redshift limitation.

## Sort-key maintenance did not keep up (measured, 2026-08-14)

Drilldown latency grew ~linearly with volume, with periodic collapses. The scan-level evidence
(`results/t2/scan_evidence_reader.csv`, `table_state_writer.csv`, `auto_optimization_writer.csv`,
`vacuum_history_writer.csv`) shows why:

| signal | value |
|---|---|
| `SVV_TABLE_INFO.unsorted`, both large MVs | **99.97%** at end of run |
| block pruning achieved | ~88–92% of blocks skipped — **partial, not absent** |
| drilldown read amplification | **97–146× rows scanned per row returned** (9–19B scanned to keep ~55–89M) |
| VacuumSort attempts / internal-error terminations | **58 / 44** |
| net rows sorted by the 3 completed vacuums on each large MV | **0** |
| `charged_extra_compute_for_automatic_optimization_seconds` | **0** |
| `extraComputeForAutomaticOptimization` on the workgroup | **null (disabled — the serverless default)** |

Three distinct causes, worth separating:

1. **Structural, and inherent to this workload.** Every streaming batch spans the whole symbol range,
   so each batch overlaps the entire leading-key range and lands in the *global* unsorted region —
   the expensive-merge pattern AWS's vacuum guidance describes. A timestamp-leading sort key would
   merge cheaply but would not serve symbol-only, all-history drilldowns. `SORTKEY (sym, t)` remains
   the right choice for the queries; it is simply expensive to maintain under this ingest pattern.
2. **Resourcing, and fixable.** Extra compute for autonomics is **disabled by default** on serverless
   workgroups, and AWS documents that autonomics are then *"temporarily suspended during periods of
   high system load"*. Our writer was saturated (streaming auto-refresh + near-continuous
   `quotes_typed` refresh + 60s `quotes_daily` refresh + 1M rows/s), i.e. exactly that condition.
   Enable before any rerun (billable, counts toward serverless usage; not exposed by the terraform
   AWS provider as of 5.100, so set it post-apply):
   ```
   aws redshift-serverless update-workgroup --workgroup-name cb-quotes-rt-wg \
       --extra-compute-for-automatic-optimization --region eu-west-2
   ```
3. **Service-side, and unexplained.** 44 tasks terminating with Redshift-reported *internal errors* is
   not postponement. Report these as service-reported failures with **root cause unknown absent an
   AWS Support investigation** — not as misconfiguration.

Note on reading the evidence: `is_rrscan = 't'` was true for nearly every query, but that flag only
means Redshift **attempted** a range-restricted scan. It does not prove blocks were skipped — compare
scanned rows against rows returned instead.

Also measured: the typed projection is **larger on disk than the SUPER original** (5.33 TB vs
3.70 TB for identical rows, +44%), because twelve materialised typed columns cost more than the
compressed `SUPER` payload plus `sym`/`t`.

## For the reviewer — please sanity-check

1. **Is streaming-ingestion-from-a-stream the canonical / only GA real-time path for Redshift?** Are we
   missing a lower-cost or lower-friction option (Firehose→Redshift, auto-copy from S3, Zero-ETL, any
   push mechanism) that would avoid or shrink the MSK cost?
2. **Source choice for ~1M EPS: MSK provisioned vs MSK Serverless vs Kinesis Data Streams.** We chose
   **MSK provisioned on Graviton `m7g.xlarge`** (better price/perf than m5; MSK Serverless is both very
   expensive and IAM-auth only; KDS has 1 MB/s · 1,000-rec/s-per-shard quotas / On-Demand pricing). The
   AWS MSK Sizing sheet suggests 9× m7g.xlarge at 100 MB/s *uncompressed*; our payload is compact JSON
   with **lz4**, and we **measured** (CloudWatch `BytesInPerSec`) **~28.5 MB/s compressed at ~999K EPS**
   (~138 B/row uncompressed → lz4 ≈ 4.8:1). Feeding 28.5 MB/s back into the sheet → ~3 brokers, so
   **3× m7g.xlarge (RF=3, 3 AZ) is confirmed right-sized** (~40% of the 72 MB/s aggregate ingest
   entitlement). Fair, or should we size differently?
3. **Auth realism:** we use MSK **unauthenticated TLS** (`AUTHENTICATION none`). Representative, or
   should a published benchmark use IAM auth? (Cost-neutral; realism only.)
4. **Redshift Serverless sizing:** base=max 128 RPU for the measured writer and base=max 32 RPU for
   the reader. At 128 RPU the streaming MV kept up with 1M EPS with offset lag 0 across 24 partitions.
   Could a lower writer floor sustain the same complete pipeline and freshness?
5. **Cost-model scope:** client cross-AZ and RMS are explicitly excluded from the main comparison.
   Is there any required standing component beyond writer uptime + MSK uptime that should be surfaced?
6. **Sort-key maintenance (see the section above).** Should the rerun enable extra autonomics compute,
   and is there anything else that would let automatic sort keep pace with 1M-row/s ingest — or is a
   periodic explicit `VACUUM SORT` window the honest answer for a table growing this fast? Also worth
   a second opinion: are the 44 internal-error vacuum terminations worth raising with AWS Support
   before publication?
