# Databricks full-path real-time benchmark

This directory contains the current Databricks implementation of the
113,219,565,734-row StockHouse quotes benchmark. The path is:

1. a same-region producer reads the canonical Parquet capture;
2. Zerobus writes Arrow `RecordBatch` payloads directly into a Unity Catalog
   managed Delta table;
3. a strict-incremental Delta materialized view maintains the dashboard
   summary; and
4. Serverless SQL X-Small serves the completed September baseline. Four dashboard
   queries read the materialized view and two drill-down queries read the raw
   table.

Zerobus is the write path. Lakehouse//RT remains a future serving comparison.
The completed run uses Serverless SQL.

The September full-run integration is documented in
[SEPTEMBER_INTEGRATION.md](SEPTEMBER_INTEGRATION.md). It uses the established
measured quantities → checked-in pricing → cost-summary scripts workflow.
**PR #42 allocations are integrated for the real-time benchmark window.**
All preparation components and matched query costs are priced. Full-path cost
is $698.25310220.
The existing `costs/mv_refresh.json` is a June summary.

### Why this architecture

[Structured Streaming real-time mode](https://docs.databricks.com/aws/en/structured-streaming/real-time/concepts)
is a separate millisecond-latency stream-processing trigger, not the
Lakehouse//RT serving engine. As documented in September 2026, its
[supported sources and sinks](https://docs.databricks.com/aws/en/structured-streaming/real-time/reference)
exclude Delta. It therefore cannot read the benchmark's raw Delta table or
write a Delta table, streaming table, or materialized view that Lakehouse//RT
could serve directly.

Bridging real-time mode through Kafka or a custom sink and then ingesting the
result into Delta would add another processing and ingestion hop, changing the
full-path workload and cost boundary. Direct Zerobus into managed Delta,
followed by a Delta materialized view and Lakehouse//RT reads, is the shortest
supported path that preserves the benchmark's queryable raw and pre-aggregated
layers.

## Status and release-stage disclosure

The completed Serverless SQL baseline is
`results/serverless_baseline_full_20260918T170453Z`, with 113,219,565,734 rows
and exact final row reconciliation. Its supplied validation passed. CostBench
integration prices PR #42
Zerobus, MV-refresh, and Predictive Optimization allocations through producer
completion, following the owner-defined real-time cost scope. Earlier June results and qualification runs are excluded from this
integration. Lakehouse//RT has no accepted full-run result here; the remaining
Lakehouse//RT setup and execution instructions describe that future path.

As documented in September 2026, Lakehouse//RT, Zerobus Arrow Flight, Zerobus
writes into liquid-clustered tables, and materialized-view `REFRESH POLICY` are
Beta features. The `System-Managed Job for Materialized Views & Streaming
Tables` preview was enabled while investigating scheduled-refresh performance
controls, but did not expose a toggle for the trigger-on-update MV. Zerobus
Ingest availability in other formats does not make this
complete architecture generally available. Any future result must be labeled
a Databricks Beta-path evaluation and must record the release stages,
workspace previews, and documentation date observed at execution time.
Behavior, SQL support, performance, and pricing can change.

The authoritative documents are:

- [INFRASTRUCTURE_SETUP.md](INFRASTRUCTURE_SETUP.md) for the reproducible AWS,
  Unity Catalog, producer, and source-staging setup;
- [INGESTION_ARCHITECTURE.md](INGESTION_ARCHITECTURE.md) for the ingestion
  alternatives, real-time terminology, and rationale for selecting Zerobus;
- [DATABRICKS_BENCHMARK_CONTRACT.md](DATABRICKS_BENCHMARK_CONTRACT.md) for
  workload identity, semantics, evidence, cost, and validity gates;
- [TUNING.md](TUNING.md) for clean-target qualification and frozen settings;
- [SIX_HOUR_INGEST_MV.md](SIX_HOUR_INGEST_MV.md) for the Lakehouse//RT-free
  six-hour write, maintenance, freshness, and cost characterization;
- [RECOVERY_40M.md](RECOVERY_40M.md) for the fresh-target journal-scaling and
  fail-closed stream-rotation qualification;
- [SIX_HOUR_INGEST_MV_R2.md](SIX_HOUR_INGEST_MV_R2.md) for the fresh six-hour
  retry using the settings accepted by the recovery qualification;
- [QUERY_BASELINE.md](QUERY_BASELINE.md) for the Serverless SQL X-Small
  baseline and the later warehouse-only Lakehouse//RT swap;
- [REAL_SERVERLESS_BASELINE.md](REAL_SERVERLESS_BASELINE.md) for the canonical
  complete-source baseline and three-hour post-ingest read window;
- [REAL_RUN.md](REAL_RUN.md) for the only supported full-run sequence.

The full run is prohibited unless the qualification report contains exact
JSON boolean `"qualified": true`.

Use one benchmark catalog with only two schema roles:

```text
costbench
├── rt_qualification
└── rt_full_<run_id>
```

Qualification cases run sequentially in `rt_qualification`; `create.sql`
recreates empty raw and MV objects before each case. Every full attempt gets a
new `rt_full_<run_id>` schema, which qualification must never touch.

## Prerequisites

- Python 3.10 or newer on a dedicated producer in the same cloud region as the
  Databricks workspace. Size it as an `m6i.8xlarge` equivalent: 32 vCPU,
  128 GiB memory, and comparable network capacity. Record the actual machine,
  region, storage, and network placement; this producer size is not evidence
  for Lakehouse//RT query CPU.
- The complete canonical source capture with a manifest total of exactly
  113,219,565,734 rows. Do not add `quotes_0.parquet`, omit files, loop input,
  or cap the full run.
- A supported Databricks region with Zerobus, serverless materialized-view
  pipelines, and Lakehouse//RT enabled. Predictive Optimization must be
  effectively `ENABLE` for both the raw table and materialized view, whether
  configured directly or inherited. Record the system-managed-job preview and
  the MV schedule's Performance optimized setting independently.
- A dedicated Unity Catalog `costbench` catalog with an explicit, non-default
  managed storage location. `rt_qualification` and each fresh
  `rt_full_<run_id>` schema may inherit that catalog location. The raw target
  must remain a managed Delta table. Unity Catalog metastore default storage,
  external/path-backed tables, and a table-level `LOCATION` are outside the
  contract.
- Two distinct warehouses:
  - a Lakehouse//RT warehouse for measured Statement Execution reads;
  - a standard control SQL warehouse for DDL, preflight, bounded metadata and
    system-table reads, evidence collection, and empty-table protection.
  Record both IDs, the Lakehouse//RT HTTP path, frozen query size, autoscaling
  limit, auto-stop behavior, and all infrastructure changes.
- A dedicated benchmark service principal with Workspace and Databricks SQL
  entitlements. This deployment reuses it for workspace/control and Zerobus
  access: standard workspace OAuth authorizes SQL and metadata requests, while
  a separate resource-bound OAuth token authorizes Zerobus direct writes. It
  requires `USE CATALOG`, `USE SCHEMA`, `SELECT`, and `MODIFY` on the target,
  `CAN MONITOR` on measured/control warehouses, MV ownership for direct event
  log reads, and bounded system-schema reads. A two-principal deployment is
  also valid and has a smaller security blast radius. Never persist secrets in
  environment files, reports, commands, or source control.
- Access to `system.query.history`, Unity Catalog metadata, warehouse
  configuration/history, materialized-view event logs, Zerobus system tables,
  predictive-optimization history, billing usage, and list prices needed by
  the bounded collector.

Create separate virtual environments for the checked-in dependency sets:

```sh
cd "$(git rev-parse --show-toplevel)/full-path-realtime/quotes/databricks"
python3.12 -m venv .venv-runner
python3.12 -m venv .venv-zerobus
.venv-runner/bin/python -m pip install -r requirements-runner.txt
.venv-zerobus/bin/python -m pip install -r requirements-zerobus.txt

export DATABRICKS_RUNNER_PYTHON="$PWD/.venv-runner/bin/python"
export DATABRICKS_ZEROBUS_PYTHON="$PWD/.venv-zerobus/bin/python"
```

`requirements-runner.txt` pins
`databricks-sql-connector[kernel]==4.5.0` and `databricks-sdk==0.133.0`;
the SDK is required for the connector's OAuth M2M credential provider.
`requirements-zerobus.txt` pins
`databricks-zerobus-ingest-sdk[arrow]==1.8.0`.
Keep these environments separate: the SQL kernel requires PyArrow 23.x while
Zerobus 1.8 requires PyArrow earlier than 22. `preflight.py` checks each
dependency using its role-specific interpreter.

Copy `databricks-host.env.example` outside the repository, replace its
placeholders, and source it before launching any shell harness. It contains
only non-secret deployment coordinates. Enter the service-principal secret
interactively when prompted; never add it to that file.

Stage the licensed source from an authorized private S3 prefix using an EC2
instance role or another non-persisted AWS credential source:

```sh
export S3_SOURCE_URI='s3://<licensed-source-bucket>/<source-prefix>'
export SOURCE_DIR=/data/quotes
./stage_source_from_s3.sh
```

The staging command resumes existing downloads, does not delete local files,
and fails unless the top-level `quotes_*.parquet` files contain exactly
113,219,565,734 rows with the required columns. Its inventory is not a
substitute for the producer's file hashes and canonical task manifest.

## Current implementation map

- `README.md`: architecture, prerequisites, evidence, and operational entry
  point.
- `databricks-host.env.example`: placeholder-only non-secret runtime
  configuration required by the shell harnesses.
- `INFRASTRUCTURE_SETUP.md`: exact successful AWS and Unity Catalog setup plus
  reusable producer and source-staging instructions.
- `INGESTION_ARCHITECTURE.md`: decision record comparing Zerobus, streaming
  tables, Structured Streaming, real-time mode, and staged SQL ingestion.
- `DATABRICKS_BENCHMARK_CONTRACT.md`: provider-specific benchmark contract.
- `TUNING.md`: offline planning, online tuning/endurance matrix, and
  qualification evaluator.
- `SIX_HOUR_INGEST_MV.md`: bounded six-hour Zerobus/MV characterization and
  settled-cost recollection procedure.
- `REAL_RUN.md`: qualification-gated, fresh-target full-run procedure.
- `create.sql`: destructive four-statement raw-table and materialized-view DDL;
  catalog and schema are administrator prerequisites.
- `queries_mv.sql`: the four canonical dashboard statements in fixed order.
- `queries_raw.sql`: the two canonical drill-down statements in fixed order.
- `export_allocation_details.sql`: detailed non-aggregated MV refresh,
  Predictive Optimization, and Zerobus allocation exports.
- `dbx_common.py`: standard-library REST, SQL rendering/splitting, bounded
  result retrieval, atomic evidence writes, configuration, and redaction.
- `apply_ddl.py`: standard-library destructive-confirmation wrapper that
  renders and applies all of `create.sql` on the control warehouse and records
  hashes and statement IDs.
- `preflight.py`: offline contract checks and fail-closed online workspace,
  table, effective Predictive Optimization inheritance, storage, warehouse,
  query, package, permission, and endpoint checks.
- `stage_source_from_s3.sh`: resumable private-S3 source staging and exact
  Parquet metadata validation.
- `run_capacity_1m.sh`: destructive-confirmation wrapper for the preliminary
  75M-row, 1M-EPS capacity characterization.
- `run_ingest_mv_10m.sh`: preliminary 600M-row sustained-ingest trial with
  concurrent metadata-only MV freshness monitoring.
- `run_ingest_mv_6h.sh`: duration-bounded six-hour write/maintenance
  characterization with compact long-run state, post-ingest catch-up, and
  initial evidence capture.
- `run_ingest_mv_40m.sh`: 40-minute validation of long-run journal scaling and
  proactive, zero-unacknowledged stream rotation.
- `run_ingest_mv_6h_r2.sh`: fresh six-hour retry frozen to the accepted
  recovery settings.
- `run_query_workloads.sh`: warehouse-parameterized dashboard and drill-down
  orchestration for Serverless SQL and Lakehouse//RT.
- `RESULT_COLLECTION_REFERENCE.md`: SQL, REST, SDK, host, and offline
  derivation provenance for every accepted result artifact.
- `RESULT_FIELD_GLOSSARY.md`: field-by-field glossary for every accepted
  JSON/JSONL artifact and the retained full query-audit sidecar.
- `collect_query_baseline_settled.sh`: target/query-window evidence and billing
  recollection for the Serverless SQL baseline.
- `run_full_serverless_baseline.sh`: canonical full-source orchestration for
  ingest, MV freshness, Serverless reads, hashing, compact evidence, and
  initial cost.
- `collect_full_serverless_settled.sh`: settled billing recollection and final
  validation for the canonical compact Serverless result package.
- `collect_ingest_mv_6h_settled.sh`: immutable-window billing recollection and
  cost summary after the provider's settlement delay.
- `ingest_zerobus.py`: canonical Parquet enumeration, direct Arrow Flight
  Zerobus ingestion, provider-durability checkpoints, pacing, recovery state,
  and source/ingest evidence.
- `runner_common.py`: Lakehouse//RT connector/session setup, fixed-rate
  scheduling, query-history collection, canonical timing, cache rejection,
  result hashing, and aligned JSONL records.
- `run_dashboard.py`: four-query materialized-view runner at 600-second
  cadence.
- `run_drilldown.py`: two-query raw-table runner at 3,600-second cadence.
- `monitor_freshness.py`: bounded 30–60-second metadata-only observations of
  producer progress, Zerobus commits, and materialized-view refresh events.
- `collect_evidence.py`: bounded read-only export of required Databricks system
  and billing datasets into transient runtime state.
- `compact_evidence.py`: normalized provider reconciliation, MV refresh, and
  clustering-status evidence for the reviewable result package.
- `compact_query_results.py`: schema-v3 reviewable query JSONL generation while
  preserving schema-v2 audit records in runtime state.
- `finalize_results.py`: compact source summary plus SHA-256 artifact manifest.
- `costs/summarize_run.py`: deterministic offline cost classification,
  subledgers, pricing completeness checks, and overlap-safe query allocation.
- `validate_run.py`: offline acceptance gates for source membership,
  durability, throughput, queries, freshness, infrastructure, evidence, and
  cost.
- `qualify.py`: deterministic qualification plan generator and offline
  evaluator.
- `publish_results.py`: fail-closed accepted-sample filtering, 100B
  presentation capping, monotonic ClickHouse matching, cost-summary
  construction, and optional global-manifest activation.
- `visualizations/plot_publication.py`: pairwise charts that verify the
  publisher's source and artifact hashes before rendering.
- `requirements-runner.txt` and `requirements-zerobus.txt`: exact online
  dependency sets.
- `tests/test_offline.py`: standard-library offline tests for SQL, REST,
  evidence, scheduling, producer state, cost, validation, qualification,
  publication, and DDL application.

`ingest.py`, `run_queries.py`, `run_dashboard.sh`, `run_drilldown.sh`,
`queries/`, `RUNNERS_SPEC.md`, historical `results/`, and the older shell/JSON
cost files are legacy or historical material. They are not part of this
implementation and cannot supply evidence for a current result.

## Evidence model

A run is evidence-ledger based; console output or a screenshot is not enough.

- The producer writes the source manifest, atomic progress journal, logical
  batch ledger, five-second provider-committed metrics, and terminal summary.
  Throughput counts only rows whose Zerobus offsets have been acknowledged as
  durable. Client byte telemetry uses `RecordBatch.nbytes`, an uncompressed
  in-memory estimate that does not serialize the batch a second time.
  `system.lakeflow.zerobus_ingest.committed_bytes` remains the authoritative
  provider byte count. Interval evidence also records whole-host CPU and
  receive/transmit throughput so qualification rejects a saturated producer.
- Dashboard and drill-down runners retain every Statement Execution ID,
  fixed-rate schedule timestamps, ordered result hashes and row counts,
  canonical timing, detailed query-history phases, queue/wait signals, result
  cache fields, IO-cache observations, and the contemporaneous producer and
  freshness checkpoints.
- The freshness monitor records bounded provider timestamps and
  materialized-view refresh status/method without querying raw or MV contents.
  MV row count is intentionally `null`; refresh output rows are not interpreted
  as total MV rows or source coverage.
- The collector exports the run-bounded Zerobus summaries, refresh event log,
  tagged query history, warehouse events/configuration, predictive operations,
  table detail/history and Unity Catalog metadata, billing usage, and list
  prices. Required dataset failures make evidence incomplete.
- `validate_run.py` consumes only preserved files and accepts a run only when
  every gate passes. Historical artifacts cannot be substituted or merged.

Raw and materialized-view ages are observational: they are the difference
between a sample time and provider-reported timestamps. They are not exact
per-record ingest lag or materialized-view lag. Post-run analysis uses Zerobus
commit metadata and successful refresh start/finish times to publish
metadata-only lag bounds. Dashboard answers are current as of a source snapshot
taken during the last completed materialized-view refresh; the exact snapshot
instant is not exposed in documented metadata.

## Canonical query timing and cache semantics

The only canonical elapsed time is
`system.query.history.total_duration_ms / 1000`, which excludes result fetch.
Client wall time, time to first row, compilation plus execution, and canonical
duration plus fetch are not substitutes.

Measured sessions set `use_cached_result=false`. Every accepted query must have
`from_result_cache=false`, no foreign cache-origin statement, complete
query-history telemetry, and its Statement Execution ID. A missing cache field
or a result-cache hit rejects the observation. Provider-managed data/IO caching
is separate; preserve `read_io_cache_percent` and disclose the observed
physical cache state rather than calling the run cache-free.

## Cost boundary and subledgers

For September CostBench integration, run `costs/_commands_september.txt` as
documented in [SEPTEMBER_INTEGRATION.md](SEPTEMBER_INTEGRATION.md). Normalized
query cost and measured write/maintenance quantities use checked-in regional
public pricing. Preparation cost is $695.60475043, and full-path cost including
the matched queries is $698.25310220. Use `ALLOCATIONS_ONLY=1` for a cost-only refresh
that validates and preserves the accepted query selections.

The broader provider-billing workflow below is a separate accounting model;
its generated ledger is not a prerequisite for the CostBench pricing scripts.

Use one declared UTC window. Preserve resource usage before applying any price:
producer compute/storage reads/network egress; Zerobus ingest; managed raw
Delta storage and maintenance; materialized-view refresh compute and storage;
Lakehouse//RT billed minimum/idle/autoscaled usage; cloud storage/requests; and
separately billed services.

`costs/summarize_run.py` builds the Databricks-billed portion of the canonical
undiscounted primary ledger and separates:

- fresh-data-path cost: Zerobus ingestion, materialized-view refresh, and
  predictive optimization;
- measured-query serving: Lakehouse//RT usage allocated by the union overlap
  of successful tagged query intervals, avoiding double-counting shared
  warehouse time.

Storage, control-warehouse work, idle/minimum intervals that cannot be
attributed, failed work, setup, DDL, preflight, correctness, tuning, and
post-ingest analysis remain itemized excluded categories. Unknown, ambiguous,
unpriced, or multi-currency primary usage fails cost completeness. Credits,
commitments, negotiated prices, promotions, taxes, and any Beta discount are
separate scenarios, never the canonical total. No price is asserted in this
README.

Producer compute, source-storage reads, network egress, and cloud
storage/request charges are outside Databricks system billing. Preserve and
report them as a separate external subledger over the same UTC boundaries; do
not silently treat their absence as zero or splice them into provider exports.

Databricks billing usage can lag by as much as 24 hours. Preserve an initial
bounded collection, then repeat the same bounded collection after settlement;
run cost summary and final validation only against the settled collection.

## Local and online gates

Run the complete offline suite without credentials:

```sh
cd "$(git rev-parse --show-toplevel)/full-path-realtime/quotes/databricks"
python3 -m unittest discover -s tests -p 'test_*.py'
python3 -m py_compile \
  apply_ddl.py collect_evidence.py dbx_common.py ingest_zerobus.py \
  monitor_freshness.py preflight.py publish_results.py qualify.py \
  runner_common.py run_dashboard.py run_drilldown.py validate_run.py \
  costs/summarize_run.py visualizations/plot_publication.py
python3 preflight.py --output /absolute/path/to/offline_preflight.json
```

Offline checks do not prove service availability or produce a benchmark
result. Complete the qualification matrix in [TUNING.md](TUNING.md), require
exact `"qualified": true`, and then follow [REAL_RUN.md](REAL_RUN.md). The
online preflight runs only after the fresh administrator-managed namespace and
checked-in DDL exist; any online error stops the run.
