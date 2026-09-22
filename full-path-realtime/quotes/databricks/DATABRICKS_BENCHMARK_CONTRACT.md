# Databricks benchmark contract

This file is the provider-specific contract for the Databricks implementation
of the quotes full-path real-time analytics benchmark. The repository-level
`../README.md` remains the accepted cross-provider contract and operating
guide. The September Serverless SQL baseline is included in the accepted global
result set; a future run is publishable after it passes the applicable gates. The completed
September Serverless SQL baseline and its CostBench normalized pricing contract
are specified in [SEPTEMBER_INTEGRATION.md](SEPTEMBER_INTEGRATION.md).
PR #42 allocations are fully integrated, including MV refresh. The accepted
real-time cost window ends at producer completion; post-ingestion activity is
outside this comparison. The scoped full-path total is $698.25310220.
Lakehouse//RT-specific requirements below govern a future Lakehouse//RT run.

## Workload identity

- The source is the complete **113,219,565,734-row** StockHouse capture used by
  the accepted ClickHouse Cloud T2 run. The producer enumerates the canonical
  file list in its recorded order, each file's row groups in ascending index
  order, and rows in their physical row-group order. It does not add
  `quotes_0.parquet`, omit empty files, loop the capture, or stop at the
  100-billion-row presentation cap.
- Every submitted batch records its canonical file, row-group, and row-range
  coordinates. Parallel streams may become durable in a different physical
  order, but they must not change the canonical source sequence or membership.
- The source-rate target is **1,000,000 provider-committed events per second**.
  Reported EPS is the number of rows for which Zerobus has confirmed durability
  divided by the corresponding active ingest interval. Rows read, queued,
  submitted, or merely accepted by the client are not committed throughput.
- Ingestion uses direct Zerobus Apache Arrow Flight `DoPut` with Arrow
  `RecordBatch` payloads. Staging files, `COPY INTO`, SQL `INSERT`, Auto Loader,
  Kafka, and JSON or Protobuf re-encoding are different variants and cannot be
  substituted into the primary result.
- The producer decodes Parquet into Arrow and performs only vectorized
  schema-safe casts before submission. It must not perform an extra Arrow IPC
  serialization for instrumentation. Client `RecordBatch.nbytes` is labeled as
  an uncompressed buffer estimate; provider `committed_bytes` from
  `system.lakeflow.zerobus_ingest` is authoritative.
- The producer globally paces submissions to the 1M-EPS target while preserving
  headroom. Stream count and Arrow batch sizing are tuning parameters, not
  throughput evidence. Qualification must preserve interval host CPU and
  network telemetry and reject a producer whose p95 utilization exceeds its
  frozen thresholds.
- Structured Streaming real-time mode is not part of this architecture. As of
  September 2026, its [supported sources and sinks](https://docs.databricks.com/aws/en/structured-streaming/real-time/reference)
  exclude Delta, so it cannot directly read the benchmark raw table or use a
  native Delta sink. A Kafka or Kinesis source plus a custom `ForeachWriter`
  that calls Zerobus could land its output in Delta, but would add a broker,
  processing compute, custom sink, and another recovery protocol, changing the
  measured workload and cost boundary.

## Provider mapping

| Benchmark concept | Databricks implementation |
|---|---|
| Continuous ingest | Zerobus Ingest over direct Apache Arrow Flight |
| Delivery | Provider-documented at-least-once streams with durability offsets |
| Raw physical intent | Unpartitioned Unity Catalog managed Delta table, liquid-clustered by `(sym, t)` |
| Incremental summary | Strictly incremental Delta materialized view grouped by `(sym, day)` |
| MV cadence | `TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE` |
| Dashboard path | Four queries directly against `quotes_daily` |
| Drill-down path | Two queries directly against `quotes` |
| Query serving | Lakehouse//RT through the Statement Execution API |
| Canonical query time | Query-history `total_duration_ms`, excluding result fetch |
| Query telemetry | Statement ID, compilation, execution, queue/other phases, result-fetch, and cache fields |

## Environment and managed-storage prerequisites

`create.sql` intentionally creates neither a catalog nor a schema and specifies
no `LOCATION`. Before running it, an administrator must:

1. create the Unity Catalog catalog and schema represented by `__CATALOG__` and
   `__SCHEMA__`;
2. configure an explicit, non-default managed storage location on the catalog;
   schemas may inherit that catalog location;
3. grant the benchmark principals the required catalog, schema, table,
   materialized-view, pipeline, and query permissions; and
4. enable the required Zerobus, serverless pipeline, and Lakehouse//RT features
   in one supported region.

The namespace model is one dedicated `costbench` catalog, one reusable
`rt_qualification` schema, and one fresh `rt_full_<run_id>` schema per full
attempt. Qualification cases must run sequentially and must recreate the raw
table and MV before each case. Qualification must never target a full-run
schema.

The raw target must remain a Unity Catalog **managed Delta table**. An external
table, a table that resolves to Unity Catalog metastore default storage, a
path-backed table, or a DDL with an explicit table `LOCATION` is outside this
contract. A schema inherited from the explicit catalog managed location is
valid. This managed-storage requirement is shared by the Zerobus target and
Lakehouse//RT.

The raw table has exactly the 12 canonical columns. `UINT8` logical fields use
`SMALLINT`; captured `UINT64` fields use `BIGINT` only after the source preflight
proves that every value fits the signed 64-bit range. The table is unpartitioned
and uses `CLUSTER BY (sym, t)`. Deletion vectors, row tracking, and change data
feed must be enabled. Row tracking and CDF support incremental maintenance;
they are not synthetic benchmark columns.

The materialized view is Delta, unpartitioned, and uses
`CLUSTER BY (sym, day)`. Its defining query contains only the canonical
`COUNT`, `MIN`, `MAX`, and `SUM` aggregates. `day` is the UTC calendar date
derived arithmetically from Unix epoch milliseconds, independent of a session
time zone. The view must be created with `REFRESH POLICY INCREMENTAL STRICT`;
a refresh that cannot remain incremental must fail rather than silently
perform a full recomputation.

## At-least-once delivery, recovery, and reconciliation

Zerobus provides at-least-once, not exactly-once, delivery. Arrow Flight can
split one logical batch into multiple transport messages, so a failed logical
batch can be partially durable. The benchmark does not claim transactionality
for a complete Arrow batch.

- Track every pending Arrow batch in memory until its durability acknowledgment.
  Persist only the compact durable task ranges and aggregate row/batch counters
  needed for recovery and exact final reconciliation; do not publish a
  per-batch event ledger.
- Advance the durable source checkpoint only after `wait_for_offset()` or
  `flush()` confirms the corresponding rows are durable. `close()` must flush
  all outstanding work on graceful shutdown.
- Enable SDK recovery for retryable transport failures and retain proactive
  ten-minute stream rotation. Persist every slow/failed acknowledgment wait,
  including its duration and recovery outcome. After a terminal stream failure,
  attempt `close()` and always persist `get_unacked_batches()`, even when
  `close()` repeats the terminal error.
- Replaying an unacknowledged batch can duplicate rows. Because the canonical
  12-column raw schema has no added idempotency key, an ambiguous terminal
  replay makes a measured run non-publishable. Start again with the fresh-table
  DDL unless a separately documented, correctness-proven reconciliation can
  establish exact source membership without changing the measured table.
- Inspect the Zerobus durable fallback location. Loading fallback Parquet after
  a possibly published copy can also duplicate rows; fallback recovery is
  performed outside measured windows and requires the same exact
  reconciliation.

At completion, reconcile the source manifest total, client durability
acknowledgments, and Zerobus provider usage/monitoring records. The required
value is exactly **113,219,565,734** in all applicable provider counters, with
no unexplained unacknowledged or fallback rows. Do not issue `SELECT COUNT(*)`
against the raw table or materialized view. Counter agreement is necessary but
not sufficient: canonical result captures, compact durable source ranges, and
stable source hashes must also exclude omissions offset by duplicates. The full
row-group task manifest is transient recovery state and is excluded from the
published result package after it has driven validation.

Operational recovery is allowed to finish a characterization run, but it does
not relax publication correctness. Any provider-committed surplus over the
client's source/durability total is reported as replay/duplicate rows and makes
the result non-publishable.

## Materialized-view and current-result semantics

`quotes_daily` is a persisted snapshot. A successful refresh computes the
correct result of its defining query as of the source snapshot processed by
that refresh. Between refresh completions, a direct query returns that persisted
snapshot; Databricks does not merge the unmaterialized raw-table delta into the
dashboard answer at query time.

Therefore:

- dashboard results are current **as of the last completed MV refresh**, not
  necessarily as of query start;
- strict incremental policy controls refresh method, not zero-lag freshness;
- trigger-on-update with a one-minute minimum interval is a cadence control,
  not a one-minute completion SLA; and
- dashboard comparisons must use the last completed MV refresh interval, while
  raw drill-down comparisons use the recorded producer and Zerobus progress.

Every refresh records its provider update ID, start and finish times, status,
refresh method, and serverless pipeline usage. The harness never queries the
raw table or materialized-view contents for monitoring. Databricks does not
expose the exact Delta source snapshot consumed by a materialized-view refresh
in the documented event-log schema, so the post-run analysis brackets
raw-to-MV freshness using Zerobus commit times and the successful refresh start
and finish times. Refresh output-row metrics are retained only as refresh-work
telemetry; they are not treated as the total MV row count or source coverage.
Raw visibility is reported separately from Zerobus durability acknowledgment.

## Query workload and scheduling

`queries_mv.sql` contains the four accepted dashboard queries in this order:
single-symbol all-time summary, watchlist all-time summary, top historical
movers, and daily market activity. `queries_raw.sql` contains the two accepted
ClickHouse T2 drill-down translations in this order: hourly OHLCV and the
risk/liquidity profile. Symbol predicates, lack of a time predicate, grouping
grain, ordering, limits, population statistics, and output order are part of
the contract.

- Start all four dashboard statements every fixed-rate **600 seconds**.
- Start both drill-down statements every fixed-rate **3,600 seconds**.
- Schedules are anchored to their runner start time, not to the prior
  iteration's finish. A runner never overlaps itself; when an iteration
  overruns, its next iteration starts immediately and records the overrun.
- Each statement runs once per iteration and failures retain their array
  position as `null`. Dashboard and drill-down runners may overlap one another
  as they do in the accepted workload.
- Execute measured reads on Lakehouse//RT using only the Statement Execution
  API. A Thrift/JDBC/ODBC path that does not explicitly select Statement
  Execution, a conventional SQL warehouse, or notebook wall-clock timing is a
  different variant.

Lakehouse//RT always applies ANSI semantics. The drill-down SQL uses explicit
numeric casts and guarded division. Its skew is `mu3 / mu2^(3/2)` and its
kurtosis is `mu4 / mu2^2`: raw population kurtosis, not sample or excess
kurtosis. `percentile_approx` is not ClickHouse `quantilesTDigest`; correctness
uses a declared tolerance and the algorithmic difference must be disclosed.

## Timing, cache, and compute sizing

The canonical elapsed time for each measured statement is
`system.query.history.total_duration_ms / 1000`. Databricks defines this field
as total provider-side statement duration excluding result fetch. Do not use
client wall-clock, time-to-first-row, or
`total_duration_ms + result_fetch_duration_ms`.

For every statement, retain its Statement Execution API ID and aligned
per-query arrays for at least:

- canonical total duration;
- `compilation_duration_ms`;
- `execution_duration_ms`;
- `result_fetch_duration_ms`;
- queue and other available phase durations;
- bytes/files/partitions read and rows produced where available; and
- `from_result_cache`, `cache_origin_statement_id`, and
  `read_io_cache_percent`.

Compilation and execution are supporting telemetry; they are not added
together to replace canonical total duration. Query-history collection may lag,
so the collector retries by statement ID and fails closed if canonical
telemetry never appears.

Disable query-result reuse with `use_cached_result = false` in the measured
Statement Execution session. Verify every measured history record has
`from_result_cache = false` (and no foreign cache-origin statement). A missing
cache field or cache hit invalidates that observation. Provider-managed data/IO
caching is not represented as a result-cache hit and may not be user
disableable; retain `read_io_cache_percent` and disclose that physical cache
state instead of describing the run as wholly cache-free.

Target approximately **16 CPU per query when the provider exposes a comparable
control**. Lakehouse//RT currently exposes named query sizes rather than a
published CPU count, so choose the nearest available query size using
provider/account-team evidence, freeze it before the measured run, and record
the name, effective resource evidence, autoscaling maximum, auto-stop setting,
and all changes. If no defensible 16-CPU mapping exists, say so and report the
chosen named size; do not invent a CPU equivalence. Autoscaling for concurrency
does not change the per-query-size contract.

## Cost boundaries

Use one declared UTC measurement window and preserve provider billing exports.
Report these components separately before applying a documented pricing model:

- source producer compute, storage reads, and network egress;
- Zerobus ingestion usage;
- managed raw Delta storage and automatic table maintenance;
- serverless materialized-view refresh compute and persisted MV storage;
- Lakehouse//RT warehouse usage, including billed minimum/idle and autoscaled
  usage during the window; and
- cloud storage, requests, and any separately billed platform services.

Attribute measured query cost from the Lakehouse//RT billing SKU without
double-counting shared warehouse uptime between dashboard and drill-down
runners. MV refresh is fresh-data-path cost, not measured-query cost. Setup,
DDL, preflight, correctness queries, system-table collection, failed tuning
runs, and post-run analysis are excluded from the benchmark score but itemized
as excluded costs. Credits, commitments, negotiated pricing, taxes, and the
Beta introductory discount are recorded separately from resource consumption.

## Validity gates before a publishable run

1. Use a fresh raw table and MV created from the checked-in rendered DDL; save
   the online preflight and rendered SQL.
2. Prove the source manifest has 113,219,565,734 rows and that dispatch follows
   canonical file, row-group, and row order.
3. Verify the raw object is a non-default-storage Unity Catalog managed Delta
   table with exactly 12 columns, no partition columns, liquid clustering on
   `(sym, t)`, and deletion vectors, row tracking, and CDF enabled.
4. Verify the MV is Delta, unpartitioned, liquid-clustered on `(sym, day)`,
   trigger-on-update at most once per minute, and `INCREMENTAL STRICT`.
   `EXPLAIN CREATE MATERIALIZED VIEW` must report incremental eligibility.
   Record the system-managed-job preview state and the MV schedule's
   Performance optimized setting separately; the full run must preserve the
   performance mode accepted during qualification.
5. Before ingestion, require the Unity Catalog API effective Predictive
   Optimization flag to be `ENABLE` for both the raw table and MV. Direct and
   inherited enablement are accepted; missing or `DISABLE` evidence fails.
6. Require every refresh in the measured window to succeed incrementally.
   Any full refresh, strict-policy failure, or unexplained refresh gap
   invalidates the affected window.
7. Reconcile exact source, acknowledged, provider, fallback, and queryable
   counts; require no unexplained duplicates, omissions, or terminal recovery.
8. Demonstrate approximately 1M provider-committed EPS over the complete run
   using a predeclared tolerance and show the time series, not only its mean.
9. Record raw and MV provider metadata continuously and attach every dashboard
   result to the last completed refresh interval. Publish metadata-only
   raw-to-MV lower and upper bounds, never refresh age as exact source lag.
10. Require exactly four dashboard and two drill-down statements in canonical
   order, fixed-rate cadence evidence, and no self-overlap.
11. Require Lakehouse//RT Statement Execution IDs, canonical total duration,
    compilation/execution telemetry, `from_result_cache = false`, and the
    frozen query-size evidence for every accepted observation.
12. Validate query results at matched source/MV checkpoints outside timed
    iterations, including tolerance checks for floating-point moments and
    approximate percentiles.
13. Separate active-ingestion from post-ingestion observations and preserve
    raw JSONL, refresh, ingest, query-history, billing, schema, and table-detail
    evidence without hand edits.

## Beta disclosure

The run must record the feature release stages and documentation date observed
at execution time. As of September 2026, Lakehouse//RT, the Apache Arrow
Zerobus record format, writing with Zerobus into liquid-clustered tables, and
the materialized-view `REFRESH POLICY` clause are Beta. The system-managed-job
preview used to configure MV refresh performance is also Beta. Results must be
labeled as a Databricks Beta-path evaluation;
performance, behavior, pricing, and supported SQL can change before general
availability.
