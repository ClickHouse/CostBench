# BigQuery benchmark contract

This file is the provider-specific contract for adding BigQuery to the quotes
full-path real-time analytics benchmark. It makes the equivalence boundaries
explicit; `README.md` is the operating guide.

## Workload identity

- Input is the same ordered set of StockHouse Parquet files and row groups used
  for the accepted ClickHouse Cloud T2 run.
- The configured target is approximately 1 million events/second. Reported
  throughput is always Storage Write API **acknowledged rows per second**, never
  the target or rows merely read from Parquet.
- The canonical client uses 40 streams for headroom and globally paces append
  starts with `--target-eps 1000000`. The cap does not replace acknowledgement
  measurement or provider reconciliation.
- Raw dashboard/drill-down SQL keeps the same symbol predicates, absence of time
  predicates, grouping grain, ordering, and limits as ClickHouse T2.
- Dashboard queries run every 600 seconds and drill-down queries every 3,600
  seconds from scheduled starts. A runner never overlaps itself. An overrun
  starts the next iteration immediately and is logged.
- Every measured query is an INTERACTIVE query job with result cache disabled.

## Provider mapping

| Benchmark concept | BigQuery implementation |
|---|---|
| Continuous ingest | Storage Write API over long-lived gRPC connections |
| Delivery | Application-created `COMMITTED` streams with explicit offsets |
| Wire representation | Serialized Apache Arrow schema and record batches |
| Raw physical intent | Unpartitioned table clustered by `(sym, t)` |
| Incremental summary | Native incremental materialized view grouped by `(sym, day)` |
| MV cadence | Automatic best-effort refresh, one-minute frequency cap |
| Dashboard path | Direct query of `quotes_daily` |
| Drill-down path | Direct query of clustered `quotes` |
| Cache policy | `QueryJobConfig(use_query_cache=False)` |
| Query compute evidence | `finalExecutionDurationMs`, `totalSlotMs`, bytes processed/billed, job ID |
| Ingest evidence | Client acknowledgments plus `WRITE_API_TIMELINE_BY_PROJECT` |
| MV evidence | refresh watermark samples plus automatic refresh jobs |

## Raw storage and clustering

The canonical raw table is intentionally not partitioned. Both raw queries use
`WHERE sym = 'AAPL'` without a time filter, so time partitioning would either be
unused or would require changing the workload. `CLUSTER BY sym, t` is the
closest BigQuery counterpart to ClickHouse `ORDER BY (sym, t)`, but it is not a
promise of identical byte layout: BigQuery manages clustering and reclustering
asynchronously. Any provider-specific partitioned variant must be reported as a
separate tuned variant, not substituted into the primary comparison.

There is no separate charge for BigQuery's automatic reclustering, and Google
states that it has no effect on query capacity. The raw table's stored bytes,
Storage Write API ingestion, and queries are still charged normally. A manual
query/DML rewrite used to reorganize existing data would be a chargeable job;
the benchmark does not perform one.

## Ingestion semantics

Each client worker owns one committed stream. Every append carries the exact
next row offset. If an acknowledgment is lost, the client checks the server's
stream row count before retrying the same batch. If an exact replay returns
`ALREADY_EXISTS`, the client accepts it only when the server-reported received
offset is the requested offset and the expected offset is exactly
`requested_offset + batch_rows`; it counts that batch once and reopens the
append connection. Acknowledged committed rows are queryable immediately.
When the stream row count itself proves that an ambiguous append committed, the
client applies the same count-once and reconnect rule and records that recovery
separately.

The benchmark client records process RSS, peak RSS, Linux system-available
memory, and Arrow allocation. On the declared Linux source host it performs
Python garbage collection plus glibc `malloc_trim(0)` every 60 seconds and
stops cleanly below the explicit 16 GiB `MemAvailable` floor. These client-side
settings are part of the run configuration and their activity/duration remains
in the ingest metrics.

The client guarantees exactly-once retries **within a live run**. It does not
claim crash-resumable source checkpointing: after a process/host crash, finalize
the recorded streams with `cleanup_streams.py` and start a new run in a fresh
dataset. Reusing a partially populated dataset would make source position
ambiguous. The ingester therefore refuses a non-empty table unless the operator
explicitly overrides that guard.

`ingest_progress.json` is written atomically and is the runners' primary row
progress signal. It is the starting table row metadata plus acknowledged rows.
The runner also records `tables.get` row metadata so delayed metadata can be
seen rather than mistaken for lost ingestion.

## Materialized-view semantics and freshness

`quotes_daily` uses only incremental-MV-compatible aggregate operations:
`COUNT`, `MIN`, `MAX`, and `SUM`, grouped by symbol and UTC date. Automatic
refresh is asynchronous and best effort; `refresh_interval_minutes = 1` is a
frequency cap, not a one-minute SLA.

A direct BigQuery query of the materialized view returns current base-table
results by combining cached MV data with unmaterialized base-table changes when
possible. Therefore:

- refresh-watermark lag measures physical MV maintenance lag;
- it does **not** imply that a direct MV query returns stale results;
- a lagging watermark can increase the dashboard query's bytes, slots, and
  latency because more base-table delta must be reconciled.

No `max_staleness` option is set. The one-minute monitor records
`last_refresh_time`, `refresh_watermark`, status, and its own metadata-query job.

The benchmark intentionally leaves `max_staleness` unset. It ingests
approximately one million events per second and queries newly ingested data
immediately, so any configuration that permits stale answers is out of scope.

## Query equivalence notes

Dashboard queries are direct translations of the four ClickHouse T2 MV
queries. Drill-down Q1 uses `MIN_BY`/`MAX_BY` as BigQuery's counterpart to
`argMin`/`argMax`.

BigQuery has no native population skew or kurtosis aggregate. Drill-down Q2
derives population central moments in one aggregate pass:

- skew is `mu3 / mu2^(3/2)`;
- kurtosis is `mu4 / mu2^2`, raw population kurtosis rather than excess
  kurtosis, matching ClickHouse `kurtPop`.

BigQuery `APPROX_QUANTILES(..., 100)` is used for p95/p99. It is not the same
algorithm as ClickHouse `quantilesTDigest`; the percentile values need a
tolerance-based correctness check and the paper must disclose the algorithmic
difference. BigQuery returns the values as a named `{p95, p99}` struct so an
undefined percentile can remain `NULL`; BigQuery cannot serialize an output
array containing `NULL` elements. The existing Snowflake skew/kurtosis SQL uses
sample estimators, so those expressions must be reconciled before making a
strict three-system semantic-equivalence claim.

## Accounting contract

Every dashboard/drill-down JSONL line contains aligned arrays:

```json
{
  "result": [[0.42], [1.07]],
  "billed_slot_sec": [[12.3], [41.8]],
  "billed_bytes": [[10485760], [987654321]]
}
```

The outer index is the query number and each inner list is the trial list. One
scheduled iteration has one trial. Errors retain the shape with `null` runtime;
slot/byte values are retained when BigQuery reports consumed resources for a
failed job. `processed_bytes` and `query_jobs` provide supporting detail.

Despite the inherited field name, `billed_slot_sec` is `totalSlotMs / 1000`:
slot-seconds consumed by the job, including retries. On on-demand pricing,
bytes billed are the direct query billing basis and slot-seconds are resource
telemetry. Under capacity pricing, bytes billed are informational and capacity
cost must be allocated separately. Never add query slot-seconds, MV refresh
slot-seconds, Write API charges, and storage as though they were one billing
unit. Report them as separate components before applying a documented pricing
model.

Automatic MV refresh jobs are not the dashboard query jobs. They are exported
separately, as are the evidence-collector jobs, to prevent double counting.
Automatic refresh is not free: under on-demand pricing it is charged by bytes
processed during refresh, and under capacity pricing it consumes slots. The
persisted MV also incurs storage charges. See `COST_ACCOUNTING.md` for the full
ledger and post-run aggregation command.

Project system-table collection is an initialization requirement, not an
optional post-run repair. The execution principal needs `bigquery.jobs.create`
and `bigquery.jobs.listAll` for `JOBS_BY_PROJECT`, plus
`bigquery.tables.list` at project scope for
`WRITE_API_TIMELINE_BY_PROJECT`. The standard least-privilege project roles are
`roles/bigquery.user`, `roles/bigquery.resourceViewer`, and
`roles/bigquery.metadataViewer`; dataset write access remains separate.

Collect with an explicit UTC `--since` at least one minute before ingestion
started. The margin includes the first provider timeline bucket, whose
`start_timestamp` is truncated to the beginning of the minute. Never treat an
empty timeline file as evidence of zero rows or bytes.

## Validity gates before a publishable run

1. Use a fresh dataset and save the online `preflight.py` report.
2. Record project, location, pricing model/reservation, source host/region, and
   client version/environment.
3. Use the selected stream count, explicit source-rate cap, and Arrow batch size
   from `TUNING.md`, then
   validate them under the complete full-path workload. Use observed
   acknowledgment rate and provider `WRITE_API_TIMELINE`, not configuration or
   short tuning results, as final-run throughput evidence.
4. Require a non-empty Write API timeline and reconcile expected rows,
   client-acknowledged rows, and provider successful rows exactly. Require no
   unexpected error buckets; allow `ALREADY_EXISTS` only when it reconciles to
   client-recorded recovery of an exact-offset replay.
5. Require bounded RSS across completed trim cycles and no activation of the
   system-memory safety guard.
6. Confirm every measured query has `cache_hit = false` and non-null job IDs.
7. Validate query results at matched row-progress checkpoints, including
   canonical row captures/hashes and tolerance checks for approximate
   percentiles and floating-point moments. Run this outside timed iterations.
8. Separate active-ingestion query windows from post-ingestion windows.
9. Export automatic MV refresh jobs and table/storage snapshots.
10. Preserve raw JSONL/JSON evidence; derived matched datasets must cite source
   file, iteration, job ID, and matching rule.
