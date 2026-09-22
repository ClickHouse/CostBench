# Databricks result collection reference

## Scope and filename notation

This reference explains how each artifact in the compact result package is
collected. It is intentionally generic: run identifiers, literal timestamps,
and observed result values belong in the artifacts, not in this document.

- `<timestamp>` is the UTC token assigned by the launcher or settled collector.
- `<nn>` is the zero-padded worker number.
- JSONL files contain one JSON object per line.
- Runtime credentials are inputs only. Writers redact secrets before persisting
  provider responses.

## Shared collection rules

### Direct and derived data

- **Direct workload SQL** is executed by `databricks-sql-connector` on the query
  warehouse. The connector statement ID is the join key to provider history.
- **Direct control and evidence SQL** is submitted through
  `POST /api/2.0/sql/statements`, polled through
  `GET /api/2.0/sql/statements/<statement-id>`, and, when needed, read through
  same-workspace result-chunk links.
- **Direct REST metadata** comes from the endpoint named under the relevant
  filename.
- **Zerobus SDK observations** come from stream calls such as `ingest_batch`,
  `wait_for_offset`, `flush`, `close`, and `get_unacked_batches`.
- **Local observations** come from the producer journal, source files,
  filesystem metadata, monotonic clocks, process statistics, `/proc`, and the
  PyArrow memory pool.
- **Offline derivations** read previously collected files only. They do not
  query a benchmark table or call a Databricks endpoint.

Field names that include `provider_committed_rows` in producer files are
historical producer-journal names. In those files the value is a client
durability counter advanced after successful SDK acknowledgement. The
provider-authoritative committed count comes from
`system.lakeflow.zerobus_ingest`.

### Collection windows

- `run_context.json` fixes the run's UTC `since` and `until` boundaries.
- Settled evidence uses the half-open interval
  `since <= event_time < until`.
- Query-history evidence additionally filters by query warehouse and run tag.
- Predictive Optimization operations use interval overlap:
  `start_time < until` and `coalesce(end_time, until) > since`.
- Freshness monitoring is a live cumulative view. Each sample uses
  `since <= event_time <= observed_at`; `observed_at` is captured before its
  control SQL is executed.
- `DESCRIBE` and Unity Catalog responses are collection-time metadata
  snapshots, not time-travel observations.

### Runtime audit material

The result directory is a compact publication package, not a full replay
bundle. The collection workflow retains the following outside the package:

- the source manifest with file, row-group, task, size, and mtime identity;
- the producer transition ledger;
- full query JSONL with result hashes, result row counts, normalized Query
  History records, and per-query errors;
- raw provider exports and collector statement/error logs;
- process stdout and stderr logs.

`compact_query_results.py` preserves the full query bytes in the retained audit
location before rewriting the packaged query JSONL. `validate_run.py` uses the
full audit form when available. Reproducing every offline derivation therefore
requires both the compact package and its retained runtime audit inputs.

Before publication, deployment-specific workspace/endpoint coordinates,
warehouse identifiers and names, service-principal/user identity, S3/storage
locations, and producer-local absolute paths are replaced with stable
placeholders. This sanitization changes no benchmark metric. The final manifest
records the public sanitization profile and hashes the sanitized bytes.

## `run_context.json`

**How data was collected**

- Producer: `run_full_serverless_baseline.sh`, with final updates by the
  settled collection launcher.
- Direct source: launcher arguments and environment configuration, followed by
  selected fields read from producer, freshness, reconciliation, and
  validation outputs.
- Collection class: orchestration metadata and offline copying; this file does
  not issue SQL or REST requests itself.

The launcher writes the file before measurement and updates it atomically as
the run reaches later states. It records target names, warehouse identifiers,
configured cadences, source-row expectation, ingest controls, collection
window, producer finish, package status, and client/provider reconciliation.
It is the authority for the window reused by settled evidence collection.

## `mv/dashboard_<timestamp>.jsonl`

**How data was collected**

- Producers: `run_dashboard.py` and shared execution code in
  `runner_common.py`.
- Direct SQL source: the statements in `queries_mv.sql`, executed against the
  materialized view through `databricks-sql-connector`.
- Direct REST source:
  `GET /api/2.0/sql/history/queries` with
  `filter_by.statement_ids=<statement-id>`, `include_metrics=true`, and one
  requested result.
- Local source: monotonic client duration plus snapshots of producer progress,
  freshness progress, and frozen query-warehouse metadata.
- Packaged form: `compact_query_results.py` rewrites the full audit JSONL after
  validation.

Each line represents one dashboard iteration. The workload covers the
single-symbol summary, watchlist summary, historical movers, and daily market
activity defined in `queries_mv.sql`. Statements within an iteration execute
sequentially and carry run, workload, query-number, and iteration tags.

The canonical query duration is:

```text
duration_ms =
  first present Query History field of
  duration, total_duration_ms, total_time_ms

result_seconds = duration_ms / 1000
```

The canonical result remains usable only when connector execution succeeded,
Query History is final with status `FINISHED`, result-cache evidence is exact
`false`, no cache-origin statement exists, and the observation has no other
error. `client_wall_time` is retained separately and is not substituted for
provider duration.

The compact file keeps published latency arrays, statement IDs, cache flags,
I/O-cache percentage, scheduling metadata, watermarks, and a sanitized
warehouse snapshot. Full result hashes, row counts, and Query History payloads
remain in the runtime audit copy.

## `raw/drilldown_<timestamp>.jsonl`

**How data was collected**

- Producers: `run_drilldown.py` and `runner_common.py`.
- Direct SQL source: the statements in `queries_raw.sql`, executed against the
  raw Delta table through `databricks-sql-connector`.
- Direct REST source: the same statement-ID lookup on
  `GET /api/2.0/sql/history/queries` used by dashboard collection.
- Local source: monotonic client duration and producer/freshness progress
  snapshots.
- Packaged form: `compact_query_results.py` performs the same audit-preserving
  compaction as for the dashboard file.

Each line represents one drill-down iteration. The SQL file defines the
hourly market summary and the risk/liquidity profile. Query tags, canonical
duration selection, final-status checks, and cache rejection follow the
dashboard rules above.

The runner drains the ordered result, counts rows, and hashes type-aware JSON
framing in the full audit record. Those validation fields are intentionally
removed from the compact package.

## `freshness/mv_freshness_<timestamp>.jsonl`

**How data was collected**

- Producer: `monitor_freshness.py`.
- Direct SQL sources, submitted on the control warehouse:
  - `system.lakeflow.zerobus_ingest`, aggregated for the target table;
  - `DESCRIBE HISTORY <raw-table> LIMIT 1`;
  - `event_log(TABLE(<materialized-view>))`, bounded to
    `update_progress` and `planning_information` events.
- Local source: the current producer progress journal.
- Derived source: per-sample watermarks, maintenance classification, row gap,
  and observational ages.

The monitor appends one compact sample per configured interval. It correlates
the latest completed `update_progress` event with its
`planning_information` event by update ID. `mv_source_rows` is the planning
event's single-source `num_rows` value.

The row-gap formula is:

```text
mv_rows_behind =
  max(0, provider_committed_rows - mv_source_rows)
```

The formula is evaluated only when both operands exist. Provider rows come
from `system.lakeflow.zerobus_ingest`, not from the producer's client counter.

Raw-commit, producer-progress, and MV-refresh ages are
`max(0, observed_at - source_timestamp)`. They are timestamp-age proxies, not
per-record lag measurements. `mv_rows` remains null because refresh output
rows and aggregate MV rows do not prove raw-to-MV coverage; the monitor does
not query MV contents.

## `freshness/freshness_progress.json`

**How data was collected**

- Producer: `monitor_freshness.py`.
- Direct and derived sources: exactly the same SQL, producer snapshot, and
  formulas as `freshness/mv_freshness_<timestamp>.jsonl`.

After each observation, the monitor atomically replaces this file with the
latest compact sample. It is a current-state snapshot, not an aggregate over
the JSONL history.

## `freshness/mv_refresh_allocation.csv`

**How data was collected**

- Exact SQL: query 1 in `export_allocation_details.sql`.
- Direct sources: `system.billing.usage` and
  `system.billing.list_prices`.
- Attribution: UC catalog/schema/table metadata is used to discover every
  `dlt_pipeline_id` for the target MV; all rows for those pipeline IDs are then
  included, including rows whose UC table fields are null.
- Allocation: billing intervals are clipped to
  `[run_start, producer_finished)` and `usage_quantity` is prorated by exact
  microsecond overlap using `DECIMAL(38, 18)`.
- Cost: each detailed allocated DBU line is joined to the one effective list
  price covering its complete source billing interval.

The CSV contains one billing allocation line per provider usage record. It is
not redundant with freshness JSONL: freshness records MV data lag and refresh
watermarks, while this file records refresh billing DBUs and list cost over
time. Public packaging pseudonymizes billing record and pipeline IDs.

## `ingest/ingest_metrics.jsonl`

**How data was collected**

- Producer: `ingest_zerobus.py`.
- Zerobus SDK source: submitted rows and rows made durable after
  `wait_for_offset` or `flush`.
- Local host source: `time.monotonic`, `resource.getrusage`, `/proc/stat`,
  `/proc/net/dev`, `/proc/self/status`, `/proc/meminfo`, `os.cpu_count`, and
  the PyArrow memory pool.
- Derived source: elapsed-time rates, process CPU cores, host CPU utilization,
  network rates, stream-state counts, and rate-limiter state.

The monitor samples frequently for safety but appends compact records at the
configured publication interval and at shutdown. Cumulative average ingest
rate is durable rows divided by elapsed time. Offline validation can derive an
interval rate from adjacent samples:

```text
interval_rows_per_second =
  (durable_rows[i] - durable_rows[i-1])
  / (elapsed_seconds[i] - elapsed_seconds[i-1])
```

Host metrics describe the machine running the producer, not Databricks
server-side compute. Arrow buffer bytes are an in-memory estimate, not encoded
wire bytes.

## `ingest/ingest_progress.json`

**How data was collected**

- Producer: `ingest_zerobus.py`; the measured launcher copies the final atomic
  progress journal into the package.
- Zerobus SDK source: stream offsets, acknowledgement checkpoints, flushes,
  recovery state, closure state, and unacknowledged-batch inspection.
- Local source: source-task completion, pending work, rate-limiter state,
  memory, CPU, network, and error state.

The journal is updated throughout ingestion and contains compact completed-task
ranges, submitted and client-durable row counters, pending rows/batches,
stream states, transport recovery, completion flags, and clean-checkpoint
status. Whole-task accounting is checked against the durable row counter.

## `ingest/ingest_summary.json`

**How data was collected**

- Producer: `ingest_zerobus.py` after all workers and the metrics monitor stop.
- Direct source: the final producer journal and worker stream states.
- Metadata SQL source: the target-protection result collected before streams
  open, using `DESCRIBE DETAIL` and `DESCRIBE HISTORY` for a fresh target.
- Local source: installed SDK/PyArrow versions and evidence paths.

The summary is a final superset of progress state. It adds dependency versions,
target-protection metadata, source/task totals, and a concise final result
block. Its SDK durability counter must be reconciled later with provider
telemetry; it is not itself the provider system-table count.

## `ingest/source_hashes_post_run.json`

**How data was collected**

- Producer: `hash_source_manifest.py`.
- Direct source: local source files and the retained source manifest.
- Collection class: post-measurement filesystem observation; no Databricks
  call is made.

For every selected file, the script verifies size and nanosecond mtime against
the source manifest, streams SHA-256, and verifies size and mtime again. It
also hashes the serialized source-manifest bytes and records per-file path,
ordinal, size, mtime, and digest plus aggregate file/byte counts.

## `ingest/streams/worker-<nn>-arrow.json`

**How data was collected**

- Producer: the corresponding `ingest_zerobus.py` worker in its `finally`
  path.
- Direct Zerobus SDK source: `close()` and `get_unacked_batches()`.
- Local source: any diagnostic Arrow files persisted from SDK-returned
  unacknowledged batches.

One file is written per worker. It records worker/stream identity, whether
close and inspection succeeded, unacknowledged batch/row counts, persisted
diagnostic artifacts, and inspection errors. The file exists even on a worker
failure so that ambiguous durability is visible.

## `ingest/zerobus_ingest_allocation.csv`

**How data was collected**

- Exact SQL: query 3 in `export_allocation_details.sql`.
- Direct source: `system.billing.usage`.
- Attribution:
  `product_features.lakeflow_connect.zerobus_request_type = 'GRPC'` plus the
  raw table's Unity Catalog table ID.
- Allocation: DBU intervals are clipped to
  `[run_start, producer_finished)` and prorated by exact microsecond overlap
  using `DECIMAL(38, 18)`.

The CSV contains one signed billing line per interval, with source and
benchmark-allocated DBUs. It is not redundant with ingest metrics or provider
reconciliation: those contain row/byte throughput and durability, not billed
DBUs. Public packaging pseudonymizes billing record and table IDs.

## `ingest/predictive_optimization_allocation.csv`

**How data was collected**

- Exact SQL: query 2 in `export_allocation_details.sql`.
- Direct source:
  `system.storage.predictive_optimization_operations_history`.
- Filter: target catalog/schema/raw/MV names and operation intervals
  overlapping `[run_start, producer_finished)`.
- Values: operation type/status, start/end, provider operation metrics, and
  `usage_quantity` reported in `ESTIMATED_DBU`.

This CSV is the sole packaged operation-level Predictive Optimization detail.
The compact clustering JSON retains table configuration and operation
summaries/counts but no longer duplicates the operation array. Public
packaging pseudonymizes metastore and operation IDs.

## `evidence/provider_reconciliation.json`

**How data was collected**

- Packaged producer: `compact_evidence.py`.
- Raw collector: `collect_evidence.py`, with raw exports retained outside the
  package.
- Direct SQL source:
  - aggregate records, bytes, errors, versions, and times from
    `system.lakeflow.zerobus_ingest`;
  - optional stream/error evidence from
    `system.lakeflow.zerobus_stream` after live schema inspection;
  - `DESCRIBE DETAIL <raw-table>` and
    `DESCRIBE TABLE EXTENDED <materialized-view> AS JSON`.
- Direct REST source:
  `GET /api/2.1/unity-catalog/tables/<encoded-full-name>` contributes target
  metadata collection completeness.
- Derived source: `evidence_summary.json`, built offline by
  `collect_evidence.py`, then projected by `compact_evidence.py`.

The compact file carries provider-authoritative committed records/bytes/errors,
commit bounds, optional stream errors, metadata row-count fields when exposed,
and required/optional dataset completeness. No raw-table `COUNT(*)` is used as
a substitute.

## `evidence/mv_refresh_summary.json`

**How data was collected**

- Packaged producer: `compact_evidence.py`.
- Direct SQL source collected by `collect_evidence.py`:
  `event_log(TABLE(<materialized-view>))`, restricted to
  `planning_information` and `update_progress` in the settled window.
- Derived source: offline decoding of planning details and aggregation in
  `evidence_summary.json`.

For each planning event, the collector extracts the chosen
`maintenance_type`. `NO_OP` is classified as neutral, complete/full
recomputation as full, and other known techniques as incremental. The compact
file reports type counts, class counts, and the derived full-refresh count.

## `evidence/clustering_status.json`

**How data was collected**

- Packaged producer: `compact_evidence.py`.
- Direct SQL sources collected by `collect_evidence.py`:
  - `DESCRIBE DETAIL <raw-table>`;
  - `DESCRIBE TABLE EXTENDED <materialized-view> AS JSON`;
  - `system.storage.predictive_optimization_operations_history`, filtered to
    the target tables and overlapping collection window.
- Direct REST source inherited from `validation/preflight.json`:
  `GET /api/2.1/unity-catalog/tables/<encoded-full-name>`.
- Derived source: offline normalization of nested table properties and
  operation metrics.

The file keeps table format/location metadata, clustering columns, relevant
Delta features and properties, compression settings, configured/effective
Predictive Optimization state, aggregate operation summaries, and the count
of clustering/optimize operations. Detailed operation rows live only in
`ingest/predictive_optimization_allocation.csv`. It does not inspect table
contents.

## `validation/preflight.json`

**How data was collected**

- Producer: `preflight.py`.
- Local sources: configuration checks, credential-presence booleans, query/DDL
  files, and package discovery/version probes run in the configured Python
  interpreters.
- Direct SQL source through the Statement Execution REST API:
  - a query-warehouse connectivity/current-namespace probe;
  - `EXPLAIN` of the materialized-view creation statement;
  - `EXPLAIN FORMATTED` for every workload statement;
  - a permission probe against `system.query.history`;
  - `DESCRIBE DETAIL`, `DESCRIBE HISTORY`, and
    `DESCRIBE TABLE EXTENDED ... AS JSON` target checks.
- Direct REST sources:
  - `GET /api/2.0/sql/warehouses/<warehouse-id>`;
  - `GET /api/2.0/sql/config/warehouses`;
  - `GET /api/2.0/sql/history/queries` for statement visibility;
  - `GET /api/2.1/unity-catalog/tables/<encoded-full-name>`;
  - `GET /api/2.1/unity-catalog/metastore_summary`;
  - `GET /api/2.1/unity-catalog/catalogs/<encoded-catalog>`;
  - `POST /oidc/v1/token` for the scoped Zerobus authorization probe.
- Network source: a TLS connection to the configured Zerobus endpoint.

The SQL probes compile workload and MV definitions or inspect metadata; they
do not execute benchmark scans. The report records sanitized observations,
warnings, and errors. Tokens, client secrets, and authorization headers are
not written.

## `validation/query_context.json`

**How data was collected**

- Producer: `run_query_workloads.sh`.
- Direct source: launcher configuration, the producer progress file, child
  process exit statuses, and line counts from the two query JSONL files.
- Collection class: local orchestration metadata; no SQL or REST call is made
  by this writer.

The file is written before query processes start and updated after they stop.
It records target/warehouse configuration, configured cadences, query-window
scope, start/final producer watermarks, post-ingest scheduling metadata,
iteration counts, and process statuses.

## `validation/validation_report_settled_<timestamp>.json`

**How data was collected**

- Producer: `validate_run.py`, invoked by the settled collection launcher.
- Direct source: none. Validation is an offline derivation and makes no
  Databricks request.
- Inputs: retained source manifest and evidence summary, packaged ingest
  progress/metrics and freshness history, and preferably the retained full
  dashboard and drill-down audit JSONL. It also consumes the settled cost
  summary generated from retained billing evidence; that standalone cost file
  is intentionally not part of the compact result package.

The validator checks input availability, source identity, producer completion,
ingest-rate intervals, query execution/timing/cache evidence, scheduled
cadence, monotonic query watermarks, freshness windows, row reconciliation,
provider evidence completeness, MV incrementality, warehouse configuration,
and Predictive Optimization status.

Query acceptance uses full per-query evidence: connector success, canonical
provider duration, statement ID, final noncached Query History, result row
count/hash, and no observation errors. Freshness acceptance uses only the
configured eligible window and treats raw/MV ages as observational proxies.

The report stores every gate, its evidence and reasons, accepted/rejected
sample indices, normalized window/configuration, and the overall acceptance
decision. The packaged query files may already be compact, so the retained
audit copies are required to reproduce all query gates.

## `validation/manifest.json`

**How data was collected**

- Producer: `finalize_results.py`.
- Direct source: files under the completed result root plus the retained source
  manifest and `ingest/source_hashes_post_run.json`.
- Collection class: offline filesystem derivation; no SQL or REST call is
  made.

The finalizer recursively enumerates packaged files, recording each relative
path, byte size, and SHA-256. It also records compact source identity, aggregate
package size, package status, and the classes of runtime state excluded from
publication.

For public packages, `sanitization` records the profile, confirms metrics were
not changed, and lists the redacted information classes.

The manifest excludes itself from enumeration and hashing, so it must be
generated last. This avoids an impossible self-referential digest.
