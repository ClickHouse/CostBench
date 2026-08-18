# BigQuery Storage Write API: benchmark architecture, semantics, limits, and pricing

Last verified against Google Cloud documentation on 2026-08-11. Quotas and
prices can change; recheck the linked primary sources before publication.

## Executive summary

This benchmark uses the **BigQuery Storage Write API over gRPC**, not load jobs
and not the legacy JSON streaming API. Specifically, it uses:

- application-created `COMMITTED` streams;
- one long-lived stream and gRPC connection per worker;
- Apache Arrow record batches;
- an explicit monotonically increasing row offset for every append; and
- acknowledgements from `AppendRows` as the client throughput boundary.

For a committed stream, Google documents that rows are available to queries as
soon as the backend acknowledges the append. There is no user-visible flush or
batch-commit step in this mode. Explicit offsets make a replay idempotent
within that stream and provide exactly-once delivery semantics when the client
manages offsets correctly.

The current benchmark configuration is:

| Setting | Benchmark value |
|---|---:|
| Destination | BigQuery `US` multi-region |
| Stream type | `COMMITTED` |
| Parallel streams/connections | 40 |
| Wire format | Apache Arrow |
| Target source rate | 1,000,000 rows/s |
| Target rows per source batch | 131,000 |
| Client payload split threshold | 16,000,000 Arrow bytes |
| Delivery claim | Exactly once within each live stream through offsets |

## How BigQuery ingestion evolved

BigQuery's three relevant ingestion paths come from different product eras and
were designed for different workload shapes:

1. **Load jobs** are the original batch-ingestion path. They remain the right
   choice for bulk, one-time, nightly, or otherwise latency-insensitive loads.
   A load job reads one or more files, builds managed BigQuery storage, and
   makes the job's result visible atomically when the job completes.
2. **Streaming inserts (`tabledata.insertAll`)** were added in September 2013
   to make acknowledged rows immediately queryable. This supplied the original
   low-latency BigQuery ingestion path, but with HTTP/JSON transport,
   best-effort rather than guaranteed de-duplication, and streaming-buffer
   limitations.
3. **The Storage Write API over gRPC** became generally available in 2021.
   Google describes it as the preferred ingestion path and as a unified API for
   streaming and batch use cases. It introduced a new stream-oriented backend,
   binary Arrow/Protocol Buffer transport, explicit stream offsets,
   exactly-once semantics within a stream, and higher default throughput.

There is now a naming trap in Google's documentation. Current documentation
calls `tabledata.insertAll` the **Storage Write API (REST)**, while the newer
service is called the **Storage Write API (gRPC)**. In this benchmark:

- **legacy JSON streaming API** always means REST `tabledata.insertAll`;
- **Storage Write API** means the newer gRPC `AppendRows` API unless explicitly
  qualified otherwise.

Google still supports the REST path, but recommends the gRPC API for new
projects because it has lower pricing and stronger features.

Primary history sources:

- [BigQuery release notes: streaming inserts added in September 2013](https://docs.cloud.google.com/bigquery/docs/release-notes-archive#september_18_2013)
- [Google's 2022 Write API overview: gRPC API GA in 2021](https://cloud.google.com/blog/topics/developers-practitioners/bigquery-write-api-explained-overview-write-api)
- [Current REST streaming documentation and migration recommendation](https://docs.cloud.google.com/bigquery/docs/write-api-rest)

## Real-time ingestion paths at a glance

| Dimension | Load jobs (`bq load`, load-type `jobs.insert`) | Legacy JSON streaming (`tabledata.insertAll`) | Storage Write API gRPC used here |
|---|---|---|---|
| Intended workload | Bulk and recurring batch | Low-latency row streaming | High-throughput streaming and transactional batch |
| Transport | File load job, usually Cloud Storage; local upload also supported | HTTP request containing JSON rows | Long-lived bidirectional gRPC stream containing Arrow or Protocol Buffer rows |
| Native Parquet input | **Yes** | No | No; this client reads Parquet and serializes Arrow |
| Visibility | Entire load becomes visible when the job completes | SQL-queryable after successful acknowledgement | Committed rows SQL-queryable after backend acknowledgement |
| Server-side holding area | The load job remains a batch boundary until atomic completion | Explicit write-optimized streaming buffer; queryable, but some operations cannot see recent rows | No user-controlled buffer/flush boundary for committed streams; internal mechanics are not exposed |
| Retry/delivery model | Atomic job; retry with a stable job identity or durable staged files | Ambiguous failures can duplicate; `insertId` de-duplication is best effort | Explicit offsets provide idempotent retries and exactly-once semantics within each stream |
| Partial row failure | Job is atomic | A successful HTTP response can still contain per-row `insertErrors` | Append response identifies row errors; ordered offsets make recovery auditable |
| Default `US`/`EU` throughput quota | Job/capacity model rather than streaming-byte quota | 1 GB/s per project | 3 GB/s per destination project |
| Key cadence quota | 1,500 load jobs per destination table per day | No load-job count; streaming quotas apply | No load-job count; byte, connection, and stream-creation quotas apply |
| Ingestion price | Free in shared `default-pipeline`; dedicated `PIPELINE` capacity is separately purchased | $0.01 per 200 MiB, with a 1 KB minimum for every successful row | $0.025/GiB; first 2 TiB/month/billing account free |
| Provider evidence | `INFORMATION_SCHEMA.JOBS_BY_PROJECT`, `job_type = 'LOAD'` | `INFORMATION_SCHEMA.STREAMING_TIMELINE_BY_*` | `INFORMATION_SCHEMA.WRITE_API_TIMELINE_BY_*` |
| Fit for this benchmark | **No**: hard cadence ceiling and batch visibility | Technically real-time, but weaker delivery, higher price, and older supported path | **Yes**: recommended real-time path with immediate visibility and retry-safe offsets |

## Why `bq load` is not feasible for this real-time workload

### It can ingest Parquet, but it is still a job

BigQuery load jobs support native Parquet and can load from Cloud Storage or a
local file. In that narrow sense, they resemble a server-side file import:

```text
Parquet file bytes -> BigQuery load job -> server parses Parquet -> atomic table update
```

This is useful for initial population or large periodic batches. It does not
make load jobs a streaming API. Every submission creates a BigQuery job, waits
for pipeline capacity, processes the file, and exposes the batch only when the
job completes. Loading a local file avoids a separate Cloud Storage staging
step, but it does not remove the job boundary or its quotas.

### The hard per-table job limit rules out one-second cadence

The benchmark source emits approximately one million rows every second. Even
if all arrivals from each second were combined into one load job, preserving
that cadence would require:

```text
1 job/second x 86,400 seconds/day = 86,400 load jobs/table/day
```

BigQuery's published limit is:

```text
1,500 load jobs per destination table per day
```

Failed load jobs also count. At one job per second, the benchmark would consume
the complete table allowance in:

```text
1,500 seconds = 25 minutes
```

That limit also participates in the fixed 1,500 table-modifications-per-day
limit and cannot be increased. The project-level allowance is 100,000 load
jobs/day, but the much smaller per-table ceiling fails first for this workload.

To stay under 1,500 loads/day with no retry allowance, the producer would have
to wait an average of at least:

```text
86,400 / 1,500 = 57.6 seconds between load jobs
```

Actual visibility lag would be longer because the batch must first accumulate
and then the job must start and finish. Google's own batch-loading guidance
uses a five-minute incremental schedule as the example that preserves retry
headroom. This is fundamentally different from acknowledging and querying
fresh events continuously.

See the current [load-job quotas](https://docs.cloud.google.com/bigquery/quotas#load_jobs)
and [batch-loading guidance](https://docs.cloud.google.com/bigquery/docs/batch-loading-data).

### More batching would change the workload, not solve it

Combining 58 or more one-second source batches into each load job could satisfy
the daily job count, but it would deliberately withhold new data for roughly a
minute or more. Larger batches improve bulk efficiency by sacrificing the
freshness contract this benchmark is intended to test.

Sharding every interval into different destination tables would also change
the schema and query path: queries would need wildcard-table scans or a union
across shards, materialized-view maintenance would no longer target the same
base table, and the comparison would no longer represent the original
workload. Partitioning does not remove the documented per-destination-table
load-job limit.

### Load completion capacity is not guaranteed by the free path

Free load jobs use the shared `default-pipeline` slot pool. Google does not
guarantee the available capacity or throughput of that pool. Dedicated
`PIPELINE` reservations can provide predictable capacity, but they add a
capacity charge and still do not remove the per-table job-cadence ceiling.

The free load-job price also does not make the surrounding transport free.
Cloud Storage staging can incur object-storage and applicable transfer costs;
loading directly from the EC2 client's local file avoids Cloud Storage staging
but not AWS network-egress charges. Normal BigQuery table storage begins after
either form of load completes.

Therefore `bq load` is a valid bulk-load baseline, but it is structurally
infeasible as the continuous ingestion path for this benchmark. Its free
ingestion price does not compensate for the missing real-time cadence.

## Why the legacy JSON streaming API was not selected

### It is genuinely real-time

The legacy `tabledata.insertAll` API should not be dismissed as a batch path.
Google documents that acknowledged rows are immediately available to SQL
queries. At the benchmark's measured average of roughly 91.6 provider input
bytes per row, one million rows/s is also below the current 1 GB/s `US`
multi-region project quota in pure byte-throughput terms.

It is therefore technically capable of real-time ingestion. It was not chosen
because the newer gRPC path is Google's recommended successor and better
matches the benchmark's high-throughput and delivery requirements.

### Its retry guarantee is weaker

An HTTP/network failure can be ambiguous: BigQuery might have accepted rows
even though the client never received the response. Retrying can consequently
create duplicates. The optional `insertId` asks BigQuery to de-duplicate
replays, but Google documents this as best effort for up to one minute and says
it must not be relied upon to guarantee the absence of duplicates.

There is another accounting burden: HTTP success does not mean that every row
succeeded. The client must inspect `insertErrors` and selectively retry failed
rows. This is weaker and more operationally complex than using a committed
gRPC stream whose exact offset establishes whether a batch was already
accepted.

### The wire protocol is less efficient for this workload

`tabledata.insertAll` sends JSON over HTTP. Current limits include:

- 10 MB maximum HTTP request;
- 10 MB maximum row;
- 50,000 rows maximum per request; and
- 500 rows per request as Google's performance recommendation.

At one million rows/s, 500-row requests would imply approximately 2,000 HTTP
requests per second. Larger requests can reduce request count, but Google warns
that oversized row batches can reduce throughput. The gRPC API instead reuses
long-lived connections and transports binary Arrow/Protocol Buffer batches.

### It has an explicit streaming buffer with operational limitations

Legacy streamed rows first reside in write-optimized storage. They are
queryable, but some non-query operations do not immediately see them:

- recent rows might be unavailable to table-copy jobs for minutes and, in rare
  cases, up to 90 minutes;
- `tabledata.list` does not include rows still in the write-optimized buffer;
- ingestion-time partition values can remain temporarily unassigned; and
- table/schema metadata changes are eventually consistent with the streaming
  system.

These restrictions do not prevent real-time SQL, but they make the path less
uniform operationally than the current gRPC service.

### It costs more, particularly for narrow rows

Current list pricing is:

```text
Legacy tabledata.insertAll:
  $0.01 per 200 MiB of successfully inserted rows
  = approximately $0.0512/GiB
  individual rows billed with a 1 KB minimum

Storage Write API gRPC:
  $0.025/GiB
  first 2 TiB per month per billing account free
```

The normalized legacy rate is approximately twice the gRPC rate before the
gRPC free allowance. More importantly for this dataset, `insertAll` applies
its minimum independently to every row. With 113.2 billion relatively narrow
events, the 1 KB minimum alone corresponds to roughly 113.2 TB of metered row
volume before considering any row whose encoded size exceeds the minimum. The
gRPC price is based on Storage Write API input bytes and does not inherit that
legacy per-row minimum.

See the [current ingestion-pricing table](https://cloud.google.com/bigquery/pricing#data_ingestion_pricing)
and [legacy REST streaming behavior](https://docs.cloud.google.com/bigquery/docs/write-api-rest).

## Real-time selection for this benchmark

The choice is based on workload semantics rather than novelty:

```text
Load jobs
  -> native Parquet and cheap bulk ingestion
  -> rejected because the job cadence and visibility boundary are batch-only

Legacy tabledata.insertAll
  -> immediate query visibility and technically feasible streaming
  -> rejected because delivery is best effort, JSON/HTTP is less efficient,
     and the path costs more and is no longer Google's recommendation

Storage Write API gRPC, committed streams
  -> immediate query visibility, binary long-lived transport, high quota,
     explicit offsets, and exactly-once semantics within each stream
  -> selected
```

## The end-to-end write path used here

```text
Parquet row group on EC2
  -> PyArrow batch and schema normalization
  -> serialized Arrow record batch
  -> global 1,000,000-row/s append-start limiter
  -> AppendRows over a long-lived bidirectional gRPC connection
  -> BigQuery validates the stream offset and accepts the append
  -> acknowledgement received by the client
  -> rows are immediately queryable from the committed stream
```

`ingest_parquet_rows.py` creates 40 application-created committed streams at
startup. Each worker exclusively owns one stream and its connection. Offsets
start at zero independently for every stream and advance by the acknowledged
row count.

The Storage Write API is asynchronous, and Google recommends multiple in-flight
requests for maximum throughput. This Python benchmark waits for each append's
response before sending the next append on the same stream. It obtains
concurrency from 40 streams rather than pipelining multiple outstanding appends
on one stream. That simpler model was sufficient to sustain the benchmark's
one-million-row/s target.

This path does not create BigQuery load jobs and is therefore not constrained
by the load-job cadence quotas that make once-per-second `bq load` unsuitable
for this workload.

## Clustering, sort-on-ingest, and background reclustering

### Short answer

The direct answer to **“does BigQuery sort every received Storage Write API
batch before it is durably written and acknowledged?”** is **no documented
guarantee**.

Google's detailed clustering explanation says that, as new data is inserted,
BigQuery **may perform a local sort for the new data or may defer that sorting
until enough data has accumulated for a storage write**. The behavior is
opportunistic. BigQuery can apply clustering-aware organization while writing
new data, but it is not required to complete a local sort for every
`AppendRows` batch before acknowledging it.

This matters because an `AppendRows` batch is a client transport unit, not a
documented BigQuery physical storage unit. BigQuery does not expose a
one-batch-to-one-file or one-batch-to-one-block mapping. A successful
committed-stream acknowledgement means the rows are committed and queryable;
it does **not** mean that the batch has completed an ingest-time sort or that
the table has reached its final optimized clustering layout.

The Storage Write API consequently has no counterpart to Snowflake's explicit
`CLUSTER_AT_INGEST_TIME = TRUE` guarantee. BigQuery has no
`CLUSTER_AT_INGEST_TIME` request or table option, and acknowledgement does not
wait on a documented pre-clustering phase.

The defensible description is therefore:

```text
BigQuery clustered table
  = local sorting of new data when BigQuery chooses to do it
  + deferred block formation or sorting when BigQuery chooses to wait
  + automatic background reclustering when block layout needs optimization
  != a guaranteed globally sorted row sequence at append acknowledgement
  != CLUSTER_AT_INGEST_TIME = TRUE
```

Primary sources for this distinction:

- [Google Cloud: BigQuery may locally sort new data or defer sorting](https://cloud.google.com/blog/products/data-analytics/skip-the-maintenance-speed-up-queries-with-bigquerys-clustering)
- [BigQuery clustered tables and automatic reclustering](https://docs.cloud.google.com/bigquery/docs/clustered-tables)
- [Storage Write API committed-stream query visibility](https://docs.cloud.google.com/bigquery/docs/write-api-streaming)

### What is configured in this benchmark

The raw table is deliberately unpartitioned and clustered from the start:

```sql
CREATE TABLE quotes (...)
CLUSTER BY sym, t;
```

The materialized view is also clustered:

```sql
CREATE MATERIALIZED VIEW quotes_daily
CLUSTER BY sym, day
AS ...;
```

No clustering setting is passed to `AppendRows`. Clustering belongs to the
destination table's metadata, not to an individual Storage Write API stream or
append request. If the target table had no `CLUSTER BY` definition, the API
would not infer one from row contents or source ordering.

The raw-table order mirrors the ClickHouse `MergeTree ORDER BY (sym, t)` intent:

- `sym` is first because both drill-down queries filter `sym = 'AAPL'`;
- `t` is second to colocate rows by time within a symbol and preserve the same
  layout intent as ClickHouse; and
- the table remains unpartitioned because the canonical drill-down queries do
  not contain a time-range predicate, so partitioning would change the
  workload rather than merely map its layout.

For BigQuery block pruning, the first clustering column is the most important.
The `sym = 'AAPL'` predicate can eliminate storage blocks whose clustering
metadata cannot contain `AAPL`. The second `t` column would provide its
strongest additional pruning when a query also constrains time after
constraining `sym`; merely grouping or ordering by time is not the same as a
selective time predicate.

### Physical storage terminology

The following terms describe different layers of the write path. They should
not be collapsed into a single “sorted on ingest” step.

| Term | Meaning for this benchmark |
|---|---|
| `AppendRows` batch | The Arrow payload and row group sent by one client request. It provides transport batching and stream-offset semantics, but Google does not define it as a physical storage block or file. |
| Write-optimized storage | BigQuery's term for storage that can serve recently streamed rows before all ordinary managed-storage operations and physical-layout work are available. Immediate SQL visibility does not prove final storage organization. |
| Capacitor | BigQuery's proprietary columnar storage format. Managed table data is encoded into columnar blocks with statistics and metadata used by the query engine. |
| Colossus | Google's distributed file system that provides the persistent storage layer underneath BigQuery, including replication, encryption, and distribution. |
| Storage block | BigQuery's adaptively sized physical organization and pruning unit. Clustering colocates related key ranges at block level, not by promising a globally sorted row sequence. |
| Weak sort order | The clustering objective in which blocks tend toward non-overlapping clustering-key ranges. New blocks can overlap existing ranges, so the layout can remain correct and queryable without being fully optimized. |
| Local sort | Sorting that BigQuery may perform for newly inserted data before creating or updating blocks. Google explicitly says this can occur, but does not guarantee it for each incoming batch. |
| Deferred sorting | BigQuery may postpone sorting new data until enough data has accumulated for another storage write. This is why acknowledgement cannot be treated as evidence that pre-clustering completed. |
| Automatic reclustering | Free managed background work that restores or improves clustering when newly added data is not optimally grouped with existing ranges. Google publishes no completion SLA or per-append watermark for it. |

The durable managed-storage picture is therefore approximately:

```text
Storage Write API Arrow batches
  -> acknowledged rows become queryable
  -> BigQuery may locally sort now, or defer sorting
  -> managed columnar storage uses Capacitor blocks
  -> Capacitor data persists on Colossus
  -> automatic reclustering improves overlapping/suboptimal block ranges
```

This is an architectural model assembled from Google's public descriptions,
not a promise that every acknowledged gRPC row has already passed through each
physical stage. Google does not publish the precise buffering, Capacitor-file
creation, or local-sort timing for an individual committed-stream append.

Storage references:

- [Google Cloud: BigQuery storage overview, Capacitor, Colossus, and clustering](https://cloud.google.com/blog/topics/developers-practitioners/bigquery-explained-storage-overview)
- [Google Cloud: separation of storage and compute and the Capacitor format](https://cloud.google.com/blog/products/bigquery/separation-of-storage-and-compute-in-bigquery)
- [BigQuery clustered-table storage blocks and block pruning](https://docs.cloud.google.com/bigquery/docs/clustered-tables)
- [BigQuery streaming visibility and write-optimized storage](https://docs.cloud.google.com/bigquery/docs/streaming-data-into-bigquery)

### What BigQuery means by “sorted”

BigQuery calls clustering a user-defined column sort order, but the physical
unit is an adaptively sized **storage block**, not an individually addressable
sorted row tree. BigQuery sorts and groups data by the clustering columns and
stores similar values in the same or nearby blocks. Each block carries
clustering metadata that lets the query engine prune irrelevant blocks.

This has several consequences:

- clustering provides colocation and block pruning, not uniqueness;
- it does not promise one total row order across the entire table;
- SQL output is still unordered unless the query contains `ORDER BY`;
- clustering-column order matters from left to right;
- BigQuery supports at most four clustering columns; and
- clustered-table query cost is finalized only after execution because the
  engine determines which blocks were pruned at runtime.

Google's deeper description qualifies the high-level statement that BigQuery
sorts data written to clustered tables: for newly inserted data it may perform
a local sort, or it may defer sorting until there is enough data for another
storage write. “Clustered table” therefore describes the maintained block-level
layout objective, not an append-level sorting barrier.

Google also states that clustering does not guarantee a reduction in the slots
required by a query. It can reduce bytes scanned and query work, but final
latency and slot consumption remain measured outcomes.

### What happens while one million rows per second are arriving

Google documents two permitted paths for new data: BigQuery may locally sort
it during the write path, or it may defer sorting until enough data has
accumulated. When new blocks are written, their clustering-key ranges can
overlap currently active blocks, and matching values can remain distributed
across multiple blocks. That is why block optimization remains necessary and
automatic reclustering runs in the background.

For this benchmark, the relevant sequence is:

```text
AppendRows acknowledgement
  -> rows are immediately queryable
  -> the append might have been locally sorted, or sorting might be deferred
  -> queries use whatever clustering/block metadata is currently available
  -> BigQuery can improve suboptimal or overlapping block layout later
     through automatic reclustering
```

There is no published completion interval or clustering-freshness SLA that
lets the client wait for “fully reclustered.” Consequently, active-ingestion
query runtimes and billed bytes correctly include the state of the physical
layout while the platform is simultaneously accepting and organizing data.
That is part of the end-to-end real-time system behavior, not benchmark noise
to remove.

Post-ingestion queries might improve as background layout work and ordinary
data caching progress. Do not attribute such an improvement solely to
reclustering without additional evidence.

### Comparison with ClickHouse and Snowflake

| System | What happens before newly written data becomes queryable | What happens afterward |
|---|---|---|
| ClickHouse `MergeTree ORDER BY (sym, t)` | Each newly written immutable data part is physically sorted by the `ORDER BY` key. With async inserts, the server first combines buffered inserts and then sorts the flushed part. | Background merges combine already sorted parts into larger sorted parts, reduce part count, and perform engine-specific maintenance. |
| Snowpipe Streaming pre-clustering | In the current high-performance architecture, a target with clustering keys can have incoming data sorted during ingestion before commit. Default pipes pre-cluster when clustering keys exist; custom pipes expose `CLUSTER_AT_INGEST_TIME = TRUE`. | Snowflake recommends leaving automatic clustering enabled because ingest-time pre-clustering does not eliminate later maintenance needs. |
| BigQuery `CLUSTER BY sym, t` | BigQuery may locally sort newly inserted data or defer sorting. The Storage Write API exposes no ingest-time clustering switch, and acknowledgement is not documented as waiting for a local sort or globally optimized block layout. | BigQuery writes and optimizes clustered storage blocks and automatically reclusters in the background when new data is not optimally grouped with existing matching key ranges. |

The closest semantic comparison is therefore:

```text
ClickHouse: sorted immutable part before part commit
Snowflake:  optional/pipe-driven pre-clustering before streaming commit
BigQuery:   opportunistic local sorting plus managed block optimization
```

All three aim to preserve selective-query performance as data arrives, but the
physical unit and the visibility contract differ. BigQuery `CLUSTER BY` is the
appropriate layout counterpart for the benchmark; it should not be presented
as proof that every acknowledged batch has undergone a ClickHouse-style
per-part sort or Snowflake-style pre-cluster phase.

### Is BigQuery clustering free?

For BigQuery, **yes, automatic reclustering is a free operation**. Google also
states that automatic reclustering has no effect on query capacity. There is
no separately billed reclustering job, slot ledger, or Snowflake-style
automatic-clustering credit charge to add to this benchmark.

That statement has a precise boundary:

| Item | BigQuery treatment |
|---|---|
| Declaring `CLUSTER BY sym, t` | No separate clustering surcharge |
| Automatic background reclustering | Free operation; Google says it does not affect query capacity |
| Storage Write API ingestion | Still billed under the Storage Write API ingestion SKU |
| Raw-table and MV storage | Still billed as normal BigQuery storage over time |
| Dashboard and drill-down queries | Still billed by processed bytes under on-demand pricing, or consume purchased capacity under reservation pricing |
| Manual `UPDATE`, CTAS, or another user query used to rewrite layout | Normal billed query/DML work; not free automatic reclustering |

Clustering can reduce query cost indirectly. Under on-demand pricing, pruned
blocks are not scanned and do not contribute their columns' bytes to the
completed job's processed/billed byte total. Under capacity pricing, pruning
can reduce work, but job slot-seconds are telemetry rather than a separate bill
on top of the reservation.

The benchmark therefore records automatic reclustering as **zero separate
maintenance cost** while retaining these independent ledgers:

- Storage Write API ingestion;
- raw-table and MV storage;
- MV refresh maintenance;
- dashboard queries; and
- raw-table drill-down queries.

### What can be verified

The configured cluster keys are visible in table metadata:

```bash
bq show \
  --format=prettyjson \
  "$GOOGLE_CLOUD_PROJECT:$BQ_DATASET.quotes" \
  | jq '.clustering.fields'
```

Expected result:

```json
[
  "sym",
  "t"
]
```

BigQuery does not expose a per-append “sorted” flag or a user-facing automatic
reclustering completion watermark. The observable benchmark effect is the
completed query's runtime, `total_bytes_processed`, `total_bytes_billed`, and
`total_slot_ms` while ingestion is active and after it finishes.

Primary references:

- [Introduction to BigQuery clustered tables](https://docs.cloud.google.com/bigquery/docs/clustered-tables)
- [Creating clustered tables](https://docs.cloud.google.com/bigquery/docs/creating-clustered-tables)
- [Querying clustered tables and block-pruning rules](https://docs.cloud.google.com/bigquery/docs/querying-clustered-tables)
- [Google Cloud: local sorting can occur or be deferred](https://cloud.google.com/blog/products/data-analytics/skip-the-maintenance-speed-up-queries-with-bigquerys-clustering)
- [Google Cloud: Capacitor, Colossus, and BigQuery storage](https://cloud.google.com/blog/topics/developers-practitioners/bigquery-explained-storage-overview)
- [Snowpipe Streaming pre-clustering](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-pipe-object#pre-clustering-data-during-ingestion)
- [ClickHouse inserts create sorted immutable parts](https://clickhouse.com/blog/updates-in-clickhouse-1-purpose-built-engines)

## Is this server-side buffering?

The answer depends on what “buffering” means.

### Query visibility: no user-visible buffer in this mode

`COMMITTED` streams do not have an explicit flush or later commit boundary.
Google's contract is:

```text
successful backend acknowledgement -> rows available for query
```

Therefore the benchmark does **not** wait for a server batch to fill, a load
job to start, or a periodic flush before querying acknowledged rows.

### Internal storage mechanics: not an exposed contract

BigQuery necessarily performs internal ingestion and storage work, but Google
does not document an internal buffer boundary or a physical-layout completion
point for committed gRPC streams. It does not expose internal buffer sizes,
flush thresholds, or physical compaction timing as controls for committed
streams. Do not infer that an acknowledged row is already reclustered or
present in its final columnar block. The supported claim is only that it is
queryable after acknowledgement.

Automatic clustering/reclustering and physical storage organization remain
asynchronous. They are separate from the committed-stream visibility contract.

### Backpressure is not durable producer buffering

If a connection reaches its processing capacity, BigQuery may reject requests
or queue them until in-flight work falls. That is transport backpressure, not a
durable source queue that the producer can rely on during an outage.

Google recommends that production pipelines persist unacknowledged source data
outside BigQuery—for example in Pub/Sub—and resume after failures. In this
benchmark, the Parquet files remain the durable source. The ingester retries
transient failures, but it is not a production message broker or a general
crash-resumable streaming service.

### Other stream types do provide explicit server-side buffering

| Stream type | Visibility and delivery behavior | Used here? |
|---|---|---:|
| Default | Immediately queryable; at-least-once; no stream creation or client offsets required | No |
| `COMMITTED` | Immediately queryable after acknowledgement; exactly-once within a stream when explicit offsets are used | **Yes** |
| `PENDING` | Rows remain invisible in a pending buffer until streams are finalized and atomically batch-committed | No |
| `BUFFERED` | Rows remain buffered until `FlushRows`; advanced mode generally intended for connectors such as Apache Beam | No |

`PENDING` would model an atomic batch load, not continuously queryable
real-time ingestion. `BUFFERED` would add a user-controlled visibility delay.
Neither matches this benchmark's required ingestion path.

## Delivery semantics and retry behavior

### Exactly once is scoped to a stream

For an application-created stream, BigQuery accepts an append at an explicit
offset only when that offset equals the current end of the stream. This makes a
retry of the same offset idempotent:

- `ALREADY_EXISTS` means that offset was already written;
- `OUT_OF_RANGE` means the requested offset is beyond the current stream end.

The ingester uses this behavior to reconcile ambiguous network outcomes. If
the original append committed but its success response was lost, replaying the
same offset returns `ALREADY_EXISTS`. The ingester verifies that the reported
expected and received offsets match the exact batch, counts the rows once, and
opens a clean append connection. It can also call `GetWriteStream` and accept
the batch once when the server row count proves that the ambiguous append
committed.

### What the exactly-once statement does not mean

- It is not a table-wide de-duplication key.
- It does not de-duplicate the same logical event written to two streams.
- It does not protect a fresh rerun that creates new streams and writes into an
  already populated table.
- It does not make the complete 40-stream run one atomic transaction.
- It does not make the current client automatically resume after a process or
  host failure.

The benchmark therefore describes its guarantee as
`exactly_once_within_live_run_via_offsets`. A fresh dataset and the ingester's
non-empty-table guard prevent accidental cross-run duplication.

### Ordering

Requests and responses are ordered within each bidirectional connection, and
offsets define row position within each application-created stream. There is no
global ordering guarantee across 40 concurrent streams. SQL results remain
unordered unless the query contains `ORDER BY`.

## Connections and throughput

An application-created stream permits only one active connection. The
benchmark follows this rule by assigning one worker and one long-lived
connection to each stream.

Google states that one connection generally supports at least 1 MB/s and can
exceed 10 MB/s depending on network bandwidth, schema, and server load. It
recommends long-lived connections, large requests (greater than 1 MB), gradual
stream creation, and asynchronous appends.

The benchmark creates its streams once at startup and reuses them for the
entire endurance run. It closes and reopens the append connection only for
error recovery. At normal completion, it finalizes the streams. Finalization
is optional for committed streams because their data was already committed,
but it releases the stream and prevents further appends.

An idle committed stream has a documented three-day TTL. This is not a problem
for the active endurance run because all streams receive traffic continuously.

## Current quotas and hard limits

The following are the published defaults as of 2026-08-11:

| Limit | `US`/`EU` multi-region | Other regions | Benchmark relevance |
|---|---:|---:|---|
| Aggregate write throughput per destination project | 3 GB/s | 300 MB/s | Applies across all Storage Write API connections targeting the project/location |
| Concurrent write connections | 20,000 | 5,000 | Based on the client/credential project; benchmark uses 40 |
| `CreateWriteStream` rate | 10,000/hour/project/region | Same | Benchmark creates 40 once |
| Maximum gRPC `AppendRows` request | 20 MB | 20 MB | Benchmark splits Arrow payloads above 16,000,000 bytes |

Important details:

- Throughput quota is charged to the project containing the destination
  dataset, not necessarily the client credential project.
- Connection quota is based on the project initiating the request.
- Quotas are shared with other workloads in the same relevant project and
  location.
- Quota increases can be requested, but Google recommends requesting large
  increases well before a benchmark.
- Sustained traffic and unexpected spikes should be monitored separately;
  short bursts can trigger `RESOURCE_EXHAUSTED`/quota errors even when a
  long-window average looks safe.

### Headroom for this benchmark

The smoke run's provider timeline reported:

```text
457,832,696 input bytes / 5,000,000 rows = 91.5665 bytes/row
```

At 1,000,000 rows/s this implies approximately 91.6 MB/s of provider-observed
row input, only about 3.1% of the `US` multi-region's 3 GB/s quota. This is
roughly 32.8× throughput headroom, before accounting for any other project
writers or transient bursts.

This estimate must be reconciled with the final run's
`WRITE_API_TIMELINE_BY_PROJECT` evidence. Arrow payload bytes, network bytes,
stored table bytes, and provider input bytes are different measurements.

## Features relevant to the benchmark

- Real-time query visibility after acknowledged committed writes.
- Exactly-once delivery within each stream through explicit offsets.
- Efficient binary gRPC transport rather than per-request HTTP JSON.
- Apache Arrow and protocol buffer input formats.
- Multiple concurrent streams targeting the same table.
- Asynchronous ordered responses on each bidirectional connection.
- Schema-change notification in append responses.
- Retry-safe stream offsets for ambiguous network/server outcomes.
- Per-minute provider telemetry through
  `INFORMATION_SCHEMA.WRITE_API_TIMELINE_BY_PROJECT`.
- Cloud Monitoring metrics for throughput, connections, and errors.
- DML support for recently written gRPC Storage Write API rows, with the
  documented multi-statement-transaction limitation.

## Limitations and operational cautions

### Application-created streams add complexity

The default stream scales more simply and avoids stream-creation quotas, but it
provides at-least-once rather than offset-based exactly-once semantics. The
committed-stream choice is defensible here because the benchmark requires an
auditable count with retry-safe appends.

### No cross-stream transaction in committed mode

Every successful append becomes visible immediately. The 40 streams cannot be
made visible as one all-or-nothing transaction in committed mode. Pending
streams support atomic batch commit, but would withhold data from queries and
change the benchmark semantics.

### The client must handle ambiguous outcomes

A timeout can occur after the backend committed the rows but before the client
received the response. Blindly treating that as failure or retrying without an
offset can create incorrect accounting or duplicates. Exact offsets and server
row-count reconciliation are therefore essential.

### Schema evolution is not instantaneous

The first append on a connection supplies the Arrow schema. BigQuery can notify
the writer about a table schema change, but documentation says detection can
take minutes. To use the new schema, the client must reconnect with it. This
benchmark fixes the schema for the run rather than testing online schema
evolution.

### Request size is a serialized-wire limit

The 20 MB `AppendRows` limit applies to the request, not to a Parquet file or
row-group size. The ingester serializes Arrow first and recursively splits a
batch when its payload exceeds the configured 16,000,000-byte safety
threshold. Row counts alone are not a safe sizing mechanism because variable
width columns change serialized size.

### Physical layout is asynchronous

Immediate queryability does not mean that clustering and physical layout work
has completed. Query latency and scanned bytes can evolve while BigQuery
reorganizes newly written data. That is part of the end-to-end system behavior
being measured.

### The benchmark client is not a production durable queue

The Parquet source protects the benchmark input, but the process does not
checkpoint enough state to transparently resume the same logical run after a
host loss. Restarting against the same non-empty table with new streams would
need explicit source checkpoints and persisted stream offsets to avoid
duplication. A production pipeline normally puts a durable queue such as
Pub/Sub ahead of the writer and uses a dead-letter path for poison records.

### Long-lived Arrow clients need memory observation

Python and Arrow can free objects without promptly returning allocator arenas
to Linux. This is a client-host behavior, not BigQuery server buffering. The
benchmark records RSS, Arrow allocations, and Linux `MemAvailable`, performs
periodic `malloc_trim`, and stops if available memory crosses the configured
safety floor.

## Pricing model

### Storage Write API ingestion charge

As of 2026-08-11, Google lists the BigQuery Storage Write API at:

```text
$0.025 per GiB
first 2 TiB per month per billing account free
```

The free allowance is shared at billing-account/month scope. It is not granted
separately to each project or benchmark run. Report both gross metered usage
and the final net billed amount.

The pricing page's 1 KB minimum per individual row belongs to the legacy
`tabledata.insertAll` streaming-insert line immediately above the Storage Write
API price. Do **not** apply that legacy per-row minimum to this gRPC Storage
Write API benchmark.

### Which byte counter to use

There is no query-job-style `total_bytes_billed` field for Storage Write API
ingestion. Use:

```text
region-REGION.INFORMATION_SCHEMA.WRITE_API_TIMELINE_BY_PROJECT
  -> total_input_bytes
```

`total_input_bytes` is documented as the total bytes from rows in each
one-minute bucket. It is the best provider-side run-usage measure, but it is
not an invoice field. Sum successful `OK` buckets for the benchmark, retain
non-`OK` buckets separately, and use the Cloud Billing export or invoice as
the authority for the charged SKU quantity, free-tier application, discounts,
credits, currency, and taxes.

Do not substitute any of these for provider input bytes:

- Parquet file size;
- serialized Arrow bytes recorded by the client;
- network bytes transferred;
- logical or physical table storage bytes;
- query bytes processed; or
- retry request counts.

### Illustrative full-run estimate

Using the smoke-run rate of 91.5665 provider input bytes per row:

```text
113,219,565,734 rows
  x 91.5665 bytes/row
  = approximately 9.4288 TiB

Gross list price before free allowance:
  9,655.14 GiB x $0.025/GiB = approximately $241.38

If the complete shared 2 TiB monthly allowance were still available:
  (9,655.14 - 2,048) GiB x $0.025/GiB = approximately $190.18
```

This is a planning estimate, not the final run charge. Use the bounded final
timeline and Cloud Billing export after ingestion completes.

### Costs not included in the ingestion rate

The $0.025/GiB Storage Write API rate does not replace these separate cost
components:

| Component | Separate treatment |
|---|---|
| Raw table storage | Normal BigQuery logical- or physical-storage billing over time |
| MV storage | Storage for persisted materialized-view data |
| MV maintenance | On-demand bytes billed or reservation capacity consumed by refresh jobs |
| Dashboard/drill-down queries | On-demand billed bytes or capacity pricing, depending on the project model |
| Source EC2 host | AWS compute cost |
| Source network egress | Any applicable AWS data-transfer charge; Google Cloud data ingress is not the Storage Write API SKU |
| Evidence queries | BigQuery query cost/slots for `INFORMATION_SCHEMA` collection |

The Storage Write API itself is a separate ingestion SKU, not a query job. Do
not multiply its work by query slot-seconds or add a query-processing charge to
the same ingestion bytes.

## Evidence and monitoring

### Client evidence

The ingester writes:

- `ingest_progress.json`: latest cumulative acknowledgement state;
- `ingest_metrics.jsonl`: time series of acknowledged rows, Arrow bytes,
  retries, workers, rate-limiter state, and memory;
- `write_streams.json`: committed stream manifest and finalization status; and
- `ingest_summary.json`: final client totals.

Client throughput is calculated from acknowledged rows divided by elapsed
time. It is not inferred from scheduled batches and does not issue repeated
`SELECT COUNT(*)` queries.

### Provider evidence

`collect_evidence.py` exports the per-minute provider timeline. The core source
query is equivalent to:

```sql
SELECT
  start_timestamp,
  dataset_id,
  table_id,
  stream_type,
  error_code,
  total_requests,
  total_rows,
  total_input_bytes
FROM `PROJECT`.`region-us`.INFORMATION_SCHEMA.WRITE_API_TIMELINE_BY_PROJECT
WHERE start_timestamp >= TIMESTAMP(@since)
  AND dataset_id = @dataset
  AND table_id = 'quotes'
ORDER BY start_timestamp, error_code;
```

For the final run:

1. Sum `OK` rows and input bytes.
2. Reconcile provider `OK` rows with client acknowledged rows.
3. Reconcile any `ALREADY_EXISTS` bucket with the client's exact-offset
   recovery counter.
4. Treat every other non-`OK` bucket as an error requiring explanation.
5. Bound the collection with explicit run start and end timestamps.
6. Reconcile usage with the Cloud Billing export.

The `AppendRows` latency chart in the console reflects bidirectional connection
duration rather than individual request latency. Use request-level Cloud
Monitoring metrics and the benchmark client's per-append observations when
request latency matters.

## Why this is the correct counterpart for this benchmark

The relevant comparison is continuous, immediately queryable ingestion:

| System path | Benchmark intent |
|---|---|
| Snowpipe Streaming | Continuously append rows without load-job cadence |
| ClickHouse async inserts | Continuously accept small/frequent client batches and combine work server-side |
| BigQuery committed Storage Write API | Continuously append acknowledged, immediately queryable rows with offset-based retry safety |

The products do not implement identical internal buffering. The comparable
outcome is that a continuous event source can sustain the target rate while
acknowledged raw rows become queryable and downstream aggregate queries remain
operational.

## Official references

- [BigQuery ingestion-method overview](https://docs.cloud.google.com/bigquery/docs/loading-data)
- [Batch loading data](https://docs.cloud.google.com/bigquery/docs/batch-loading-data)
- [Load-job and streaming quotas](https://docs.cloud.google.com/bigquery/quotas)
- [Legacy JSON streaming / Storage Write API REST](https://docs.cloud.google.com/bigquery/docs/write-api-rest)
- [`tabledata.insertAll` REST reference](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll)
- [Release-note history: streaming inserts](https://docs.cloud.google.com/bigquery/docs/release-notes-archive#september_18_2013)
- [Google's Storage Write API history and alternatives overview](https://cloud.google.com/blog/topics/developers-practitioners/bigquery-write-api-explained-overview-write-api)
- [Introduction to the Storage Write API](https://docs.cloud.google.com/bigquery/docs/write-api-intro)
- [Storage Write API gRPC overview and stream types](https://docs.cloud.google.com/bigquery/docs/write-api)
- [Committed streams and exactly-once offsets](https://docs.cloud.google.com/bigquery/docs/write-api-streaming)
- [Storage Write API best practices](https://docs.cloud.google.com/bigquery/docs/write-api-best-practices)
- [Storage Write API quotas and limits](https://docs.cloud.google.com/bigquery/quotas#write-api-limits)
- [Write API timeline schema](https://docs.cloud.google.com/bigquery/docs/information-schema-write-api)
- [Supported protocol buffer and Arrow types](https://docs.cloud.google.com/bigquery/docs/supported-data-types)
- [Clustered-table storage, automatic reclustering, and pricing](https://docs.cloud.google.com/bigquery/docs/clustered-tables)
- [Create clustered tables](https://docs.cloud.google.com/bigquery/docs/creating-clustered-tables)
- [Query clustered tables](https://docs.cloud.google.com/bigquery/docs/querying-clustered-tables)
- [BigQuery pricing: data ingestion](https://cloud.google.com/bigquery/pricing#data_ingestion_pricing)
- [Cloud Billing export](https://docs.cloud.google.com/billing/docs/how-to/export-data-bigquery-tables/standard-usage)
