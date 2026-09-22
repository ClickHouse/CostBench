# Canonical Serverless SQL baseline

This run ingests the complete 113,219,565,734-row source while serving the
canonical dashboard and drill-down workloads on Serverless SQL `X-Small`.
Lakehouse//RT is unavailable and is not part of this result.

The later Lakehouse//RT run must preserve the source, SQL, cadence, cache
policy, timing source, stream settings, table definitions, and three-hour
post-ingest query window. Only the query warehouse ID, mode, and qualified size
change.

## Frozen runtime

- Raw target:
  `costbench.rt_full_serverless_baseline_r3_20260918.quotes`
- MV target:
  `costbench.rt_full_serverless_baseline_r3_20260918.quotes_daily`
- Source rows: exactly 113,219,565,734
- Target ingest rate: 1,000,000 provider-acknowledged rows/second
- Expected ingest duration at target: about 31h27m
- Zerobus: 16 Arrow Flight streams, 50,000-row batches
- Automatic recovery: enabled; 15-second attempt timeout, 2-second backoff,
  four attempts
- Proactive stream rotation: every ten minutes, zero-unacknowledged proof
- Recovery evidence: acknowledgment waits of at least two seconds and every
  failed wait are persisted with duration and outcome
- MV monitor: every 60 seconds
- Dashboard: four queries every 600 seconds
- Drill-down: two queries every 3,600 seconds
- Post-ingest query window: exactly three hours
- Query warehouse: `<query-warehouse-name>`,
  Serverless `X-Small`, ID `<query-warehouse-id>`, scaling 1–1
- Control warehouse: `<control-warehouse-name>`, Serverless `2X-Small`,
  ID `<control-warehouse-id>`
- Canonical query latency:
  `system.query.history.total_duration_ms / 1000`
- Result cache: disabled; any hit rejects the observation

## Cost exposure

The latest provider evidence averages approximately 73 bytes sent to Zerobus
per row. The complete source is therefore expected to send about 8.27 TB.
Public Zerobus list pricing implies approximately USD 414 at USD 0.050/GB or
USD 529 at USD 0.064/GB. Canonical cost uses settled account billing rather
than this estimate.

The X-Small Serverless SQL warehouse consumes 6 DBU/hour while running. At the
current Ireland account list price of USD 0.91/DBU, approximately 34.5 hours of
ingestion plus post-ingest queries exposes about USD 188 of query-warehouse
cost. MV refresh, Predictive Optimization, monitor warehouse, EC2, S3, EBS,
and network costs are additional. Obtain explicit spend approval before
launch.

## Create the fresh namespace

Run
[`create_full_serverless_baseline_r3_20260918.sql`](create_full_serverless_baseline_r3_20260918.sql)
once on `<control-warehouse-name>` as the benchmark administrator. It contains:

- fresh schema creation with no `DROP` or `IF NOT EXISTS`;
- explicit raw-table DDL rather than copying mutable properties from another
  target;
- service-principal grants;
- `EXPLAIN CREATE MATERIALIZED VIEW` before actual MV creation;
- MV ownership transfer to `<benchmark-service-principal>`, while retaining the benchmark administrator's
  explicit `MANAGE` and `SELECT`; and
- all metadata-only verification statements.

The active canonical target is
`costbench.rt_full_serverless_baseline_r3_20260918`.

The first target was consumed by an aborted September 17 attempt in which the
query wrapper inherited hard-coded qualification namespace defaults. R2 then
held 1M EPS for 17h17m before two Databricks-side HTTP/2 stream resets stopped
it at 62,222,954,896 durable rows. Settled provider totals exactly matched the
client durability watermark and excluded the two pending batches, but the
query/ingest outage invalidated that run. R3 enables SDK recovery, preserves
unacknowledged batches after terminal closure, and records recovery duration.
Any final replay surplus remains non-publishable.

## Pre-launch gates

Use the SQL verification gate in
[SIX_HOUR_INGEST_MV.md](SIX_HOUR_INGEST_MV.md), substituting the full schema.
Require:

- raw Delta version 0, zero files and zero bytes;
- `CLUSTER BY (sym, t)`;
- deletion vectors, row tracking, and CDF enabled;
- effective Predictive Optimization `ENABLE`;
- MV ownership by `<benchmark-service-principal>`;
- `CLUSTER BY (sym, day)`;
- `INCREMENTAL STRICT` and one-minute trigger;
- incremental eligibility with no issues;
- both warehouses running with their frozen settings;
- `<benchmark-service-principal>` has `CAN MONITOR` on both warehouses;
- no prior benchmark process is running; and
- no background maintenance from prior qualification targets overlaps launch.

Do not run a warm-up data query. Starting state and physical cache telemetry
are recorded; provider-managed IO cache is not user-disableable.

## Launch

On the dedicated EC2 producer:

```sh
tmux new -As serverless-full
/home/ubuntu/costbench/full-path-realtime/quotes/databricks/run_full_serverless_baseline.sh
```

Type the exact target and enter the benchmark service-principal secret. One
orchestrator starts:

1. complete-source Zerobus ingestion;
2. metadata-only freshness monitoring;
3. the four-query dashboard runner;
4. the two-query drill-down runner;
5. three additional hours of post-ingest queries;
6. final MV catch-up;
7. post-window hashing of all selected source files (193 in the completed September package);
8. provider evidence collection; and
9. an initial cost summary.

The reviewable run layout matches the structured BigQuery result package:

```text
serverless_baseline_full_<timestamp>/
├── run_context.json
├── ingest/
├── freshness/
├── mv/
├── raw/
├── evidence/
├── costs/
├── validation/
└── charts/
```

`mv/` contains dashboard JSONL and `raw/` contains drill-down JSONL.
`ingest/ingest_metrics.jsonl` is a compact one-minute progress series rather
than a five-second dump of the complete producer state. `freshness/` contains a
one-minute metadata-only MV series. `evidence/` contains only normalized
provider reconciliation, MV refresh, and clustering status.

Runtime-only recovery and diagnostic files are written to
`/data/databricks-state/<run-id>/`, outside the result package:

- the row-group task manifest;
- a lifecycle/error-only ingest ledger;
- raw provider exports; and
- process logs.

The producer still samples resources every five seconds for safety checks, but
persists only one compact sample per minute. The runtime state is retained on
the producer for troubleshooting and settled validation; it is not downloaded
or published with benchmark results. The 4h08m characterization compacted from
355 MB of runtime artifacts to a 1.35 MB review package, establishing the
packaging behavior before the canonical run.

The orchestrator watches child processes and fails immediately if a query or
freshness process exits during ingestion. It fails closed on stream
interruption, unacknowledged data, cache hits, query errors, full MV refresh,
row mismatch, missing source hashes, or incomplete final MV catch-up.

## Stop and recovery

Do not interrupt a healthy run. One `SIGTERM` or `Ctrl-C` requests clean
stream flushing; a second signal makes the result ambiguous and
non-publishable. A manually stopped canonical run is not complete and cannot
be promoted as the baseline.

Zerobus cannot safely recreate or resume an ambiguous target. Use a new schema
for any retry.

## Settled cost

Initial billing can be incomplete for 24 hours. At least 24 hours after the
complete measurement window, run:

```sh
/home/ubuntu/costbench/full-path-realtime/quotes/databricks/collect_full_serverless_settled.sh \
  /data/databricks-runs/serverless_baseline_full_<RUN_TIMESTAMP>
```

This recollects the immutable full window with the query warehouse ID, MV
pipeline ID, target Predictive Optimization operations, Zerobus usage,
control/query warehouse usage, account price history, and storage metadata. Raw
exports remain in runtime state; the command writes only settled cost, compact
provider/clustering summaries, final validation, and an updated artifact
manifest into the result package.
Stop `<query-warehouse-name>` manually when the orchestrator completes to
avoid its 80-minute post-run idle timeout.
