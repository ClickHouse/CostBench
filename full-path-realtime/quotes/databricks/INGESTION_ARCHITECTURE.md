# Databricks ingestion architecture decision

This decision was reviewed on September 15, 2026 for the CostBench
Lakehouse//RT experiment. The benchmark continuously replays the canonical
113,219,565,734-row quotes capture, queries the raw table for drill-downs, and
queries a daily pre-aggregation for dashboard requests while ingestion is
active.

## Executive decision

Use this measured path:

```text
same-region replay producer
  -> Zerobus gRPC with Arrow Flight
  -> Unity Catalog managed Delta quotes
       -> raw drill-down queries on Lakehouse//RT
       -> strict-incremental quotes_daily materialized view
            -> dashboard queries on Lakehouse//RT
```

This is the shortest Databricks-supported path from an external high-volume
producer into a Lakehouse//RT-readable table. It is also the closest match to
the native direct-write paths selected for ClickHouse, Snowflake, and BigQuery.

The decision is not that Zerobus is always preferable. It is preferable for
this workload because the destination is Delta, the producer already has
ready-to-ingest records, no streaming transformation is required before raw
landing, and seconds-level table materialization meets the analytical
freshness model.

## Keep the layers separate

Several Databricks features use “streaming” or “real-time” in their names but
solve different parts of a system:

- **Zerobus Ingest is an ingestion transport.** It accepts pushed records,
  durably buffers them, and materializes them into managed Delta.
- **A streaming table is a specialized managed Delta data object.** It adds
  Lakeflow flow ownership, checkpointing, and process-once streaming
  semantics. It is not itself a network transport.
- **Structured Streaming is a processing engine.** It reads a source such as
  Kafka or files, executes transformations, and writes a sink.
- **Structured Streaming real-time mode is an execution trigger.** It lowers
  processing latency for immediate operational decisions.
- **Liquid clustering and Predictive Optimization are table-layout and
  maintenance choices.** They are orthogonal to the ingestion transport.
- **Lakehouse//RT is read-serving compute.** It executes selective SQL reads;
  it does not ingest, transform, refresh, or maintain tables.

## Option 1: direct Zerobus to a managed Delta table — selected

```text
producer -> Zerobus -> managed Delta quotes
```

Why it fits:

- Databricks describes Zerobus as the most direct path when the destination is
  the lakehouse: no broker, partition fleet, or ingestion pipeline is needed.
- The official guidance recommends Arrow Flight for columnar or batched
  workloads. The replay input is Parquet and the producer naturally emits
  Arrow record batches.
- Default documented limits are 100,000 records/second and 100 MB/second per
  gRPC stream, 10 GB/second per target table, and unlimited concurrent streams
  per workspace. Capacity still has to be qualified against this record shape.
- Databricks documents approximately 150 ms to durability and approximately
  five seconds until data is materialized in the table. The benchmark measures
  actual provider metadata rather than claiming these published figures.
- Lakehouse//RT directly supports Unity Catalog managed Delta tables and
  materialized views.
- Zerobus is serverless and billed under the Jobs Serverless SKU. All ingestion
  and background maintenance cost remains inside the Databricks ledger.

Trade-offs that must remain visible:

- Delivery is at least once. Durable offsets, recovery attempts, and final
  reconciliation must be retained.
- A durability acknowledgement does not prove that a row is queryable yet.
- The Arrow Flight interface is Beta.
- Zerobus does not support recreating a target table. Every qualification case
  must use a fresh target object.

### Zerobus pricing

Zerobus has volume pricing rather than a provisioned hourly charge. The
official AWS list-price page displayed the following rates on September 15,
2026:

- Premium: USD 0.050 per GB sent to the ingestion API;
- Enterprise: USD 0.064 per GB sent to the ingestion API.

Underlying Zerobus compute is included. Databricks translates usage at 0.143
DBU per GB, so charges can appear on the bill as Jobs Serverless or Automated
Serverless DBUs. There is no separate Zerobus stream, cluster, or idle-hour
charge.

These costs remain separate and must also be reported:

- the replay producer;
- cloud network transfer, if any;
- managed Delta storage;
- serverless materialized-view refresh;
- Predictive Optimization serverless compute; and
- Lakehouse//RT read compute.

The run ledger uses `system.billing.usage` and the price version from
`system.billing.list_prices` that overlaps the measured window. It does not
infer billable volume from compressed Parquet or Delta storage size.

### Zerobus interface choice

Zerobus itself provides several client interfaces:

- **Arrow Flight — selected:** Databricks recommends it for columnar or batched
  workloads. It maps directly from the replay's Parquet/Arrow batches and
  avoids converting every row to JSON. It is Beta.
- **Protocol Buffers:** recommended for production row-oriented streams. It is
  a reasonable alternative for live applications, but converting this
  columnar capture into individual protobuf messages adds unnecessary producer
  work.
- **JSON over gRPC:** easiest to develop, but adds serialization and parsing
  overhead that is not representative of this prepared columnar source.
- **REST:** intended for large fleets of low-frequency devices. Per-request
  handshakes and the 10,000 requests/second default quota make it the wrong
  interface for one producer targeting roughly one million rows/second.
- **Kafka-compatible API:** useful for existing Kafka producers, but currently
  Beta with a 50,000 messages/second default workspace quota. It offers no
  benefit to this replay producer.

## Option 2: Zerobus to a streaming table — not a distinct transport

```text
producer -> Zerobus -> streaming table
```

Databricks states that Zerobus writes to standard managed Delta tables and
streaming tables with the same Zerobus limits and quotas. This changes the
target object, not the wire path.

A streaming table is useful when the table needs Lakeflow-managed flows,
checkpoints, process-once semantics, or low-latency streaming transformations.
It also has different lifecycle semantics: a backing pipeline owns it, query
definition changes normally affect only newly processed rows, and some table
operations are restricted.

It does not add a needed capability here:

- raw quotes require no transformation before landing;
- the raw table must remain a general queryable record of every event;
- exact batch-equivalent aggregation belongs in the materialized view; and
- Zerobus ingestion into streaming tables is itself Beta.

Lakehouse//RT can read a streaming table, so this route is technically viable.
It should be a separate characterization only if Databricks claims a concrete
advantage for this workload. Current documentation claims the same Zerobus
limits and quotas, not an ingestion advantage.

## Option 3: Lakeflow or Structured Streaming micro-batch into Delta

Typical variants are:

```text
producer -> Kafka/MSK -> Structured Streaming or Lakeflow -> managed Delta
producer -> cloud files -> Auto Loader or Lakeflow -> managed Delta
```

This is the recommended family when data already resides in Kafka or cloud
files, or when parsing, validation, deduplication, joins, CDC, or medallion
transformations must happen before the destination table.

For this benchmark it adds a broker or file-staging hop plus Spark/Lakeflow
compute and checkpoint management. Those components add latency, cost, sizing,
and failure modes without performing a required transformation. Using Auto
Loader would also turn the producer benchmark into a cloud-file-arrival
benchmark.

Default Structured Streaming micro-batch can write Delta and is appropriate
for analytical ETL measured in seconds or minutes. It is a valid alternative
architecture, but not the shortest native ingest path for an external producer
whose destination is already known.

## Option 4: Structured Streaming real-time mode — different workload

The September 2026 Redis article uses:

```text
Kafka -> Structured Streaming real-time mode
      -> stateful per-user scoring
      -> custom ForeachWriter
      -> Redis
      -> application request
```

That architecture optimizes immediate operational decisions. Redis, not
Lakehouse//RT, is the serving layer. The article's application does not land
the computed result in Delta and does not issue analytical SQL against a raw
table and materialized view.

Current Databricks real-time-mode documentation lists Delta as unsupported as
both source and sink. Supported paths center on Kafka-compatible sources and
Kafka or custom sinks. Therefore, using real-time mode for this benchmark
would require another hop after processing to populate the required Delta
table. That would test an operational stream processor plus a custom sink, not
Lakehouse//RT's native ingest-to-query path.

Real-time mode cannot consume Zerobus as a source. Zerobus is a single-sink
ingestion service, including when a Kafka producer uses its Kafka-compatible
API; it is not a Kafka broker with a consumer interface.

A valid composition can put real-time mode upstream and implement a custom
`ForeachWriter` that calls the Zerobus SDK:

```text
Kafka or Kinesis
  -> real-time-mode flow
  -> custom ForeachWriter using Zerobus
  -> managed Delta table or streaming table
```

Real-time mode is enabled on the upstream Structured Streaming or Lakeflow
flow, not on the target streaming table. Zerobus is the custom output bridge
that works around real-time mode's lack of a native Delta sink.

That composition is useful when each source event requires low-latency
processing before landing in Delta. For this pass-through replay, it adds
Kafka or Kinesis, processing compute, another serialization boundary, custom
sink code, and two recovery/delivery protocols without performing a required
transformation. Its millisecond processing latency still terminates at
Zerobus's seconds-level Delta materialization and then at the
materialized-view refresh cycle. It provides no benefit for this analytical
benchmark unless an independent millisecond operational transformation is
also being tested.

Use real-time mode when an event must trigger an action within milliseconds,
such as fraud blocking, personalization, or online feature publication. Use
the default micro-batch mode for analytical ETL whose latency objective is
seconds or minutes.

Real-time mode and Lakehouse//RT are unrelated despite their similar names:

- real-time mode continuously **processes** events;
- Lakehouse//RT quickly **queries** data already materialized in Unity Catalog.

## Lakehouse//RT and the materialized view

Lakehouse//RT explicitly supports materialized views. It can issue the
benchmark's `SELECT` statements against
`costbench.rt_qualification.quotes_daily` while its serverless Lakeflow
pipeline refreshes the object.

Lakehouse//RT reads the last completed materialized-view snapshot. It does not
refresh the view, and it does not reconcile rows that are present in `quotes`
but absent from a lagging `quotes_daily` snapshot. DDL, `REFRESH`, and system
table queries must use the unmeasured control SQL warehouse. MV freshness is
therefore measured separately from query runtime.

## Option 5: SQL inserts, COPY, or staged files — legacy baseline

The previous Databricks experiment uploaded Parquet to a Unity Catalog volume
and drove ingestion through a SQL warehouse. This remains possible, but it
adds staging and SQL-warehouse work and is no longer the most direct
Databricks ingestion path.

It is useful only as a historical baseline. Selecting it for the new run would
test the old loading architecture rather than the current Lakehouse//RT and
Zerobus architecture.

## Cross-vendor comparability

The selected path applies the same principle used elsewhere in CostBench:
choose the provider's native production ingestion route into its queryable
analytical storage.

- ClickHouse uses native asynchronous inserts into the raw MergeTree while its
  incremental materialized view is maintained.
- Snowflake uses the Snowpipe Streaming push SDK into its queryable raw object.
- BigQuery uses long-lived Storage Write API streams.
- Redshift uses MSK because Redshift's supported native streaming ingestion
  requires Kinesis or MSK; the broker is part of that provider's recommended
  path.
- Databricks uses Zerobus because it provides a direct push API into managed
  Delta and explicitly removes the broker or ingestion-pipeline hop.

Fairness does not require forcing every provider through Kafka. It requires
including every mandatory component and its cost for the provider's selected
native path.

## Meeting answer

If asked “Why not streaming tables, Structured Streaming, or real-time mode?”:

> Streaming tables are a Delta target type, Structured Streaming is a
> transformation engine, and real-time mode is a millisecond processing
> trigger. None is a substitute for the producer-to-Delta transport. Our
> workload needs direct high-rate landing into a raw Delta table and exact
> incremental pre-aggregation for analytical reads. Zerobus is Databricks'
> shortest supported producer-to-Delta path, while Lakehouse//RT serves the raw
> table and materialized view. We would choose Lakeflow or real-time mode if we
> needed broker-based transformations or millisecond actions before storage,
> but we do not.

## Official references

- [Zerobus Ingest overview](https://docs.databricks.com/aws/en/ingestion/zerobus-overview)
- [Use Zerobus Ingest](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest)
- [Zerobus quotas, latency, guarantees, and table behavior](https://docs.databricks.com/aws/en/ingestion/zerobus-quotas)
- [Streaming tables](https://docs.databricks.com/aws/en/ldp/concepts/streaming-tables)
- [Structured Streaming real-time mode concepts](https://docs.databricks.com/aws/en/structured-streaming/real-time/concepts)
- [Real-time mode source and sink support](https://docs.databricks.com/aws/en/structured-streaming/real-time/reference)
- [Real-time mode in Lakeflow pipelines](https://docs.databricks.com/aws/en/ldp/real-time)
- [Incremental refresh for materialized views](https://docs.databricks.com/aws/en/ldp/incremental-refresh)
- [Lakehouse Real-Time](https://docs.databricks.com/aws/en/compute/sql-warehouse/real-time)
- [Redis and Databricks real-time personalization architecture](https://redis.io/blog/delivering-real-time-personalization-with-databricks-and-redis/)
