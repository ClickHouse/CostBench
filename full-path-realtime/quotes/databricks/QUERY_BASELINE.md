# Serverless SQL query baseline

This baseline runs the canonical read workload on a dedicated Databricks
Serverless SQL warehouse while Lakehouse//RT is unavailable. The later
Lakehouse//RT run must use the same SQL, cadence, ordering, cache policy,
timing source, runner version, and result schema. Only the query warehouse ID,
declared warehouse mode, and provider size change.

## Cross-provider workload contract

The accepted ClickHouse, Snowflake, BigQuery, and Redshift implementations all
use the same cadence:

- dashboard: four statements against the persisted daily aggregate, every 600
  seconds;
- drill-down: two statements against the raw table, every 3,600 seconds.
- canonical post-ingest window: three hours, adding 18 dashboard and three
  drill-down schedules after ingestion completes.

Both schedules are fixed-rate from their initial scheduled start. Statements
within one iteration execute serially in canonical file order. A runner never
overlaps itself; if an iteration exceeds its cadence, the next starts
immediately and records the overrun. Dashboard and drill-down runners may run
concurrently.

## Create the query warehouse

Create a dedicated SQL warehouse named `<query-warehouse-name>` with:

- type: Serverless;
- cluster size: `X-Small`;
- minimum clusters: 1;
- maximum clusters: 1; and
- auto-stop: 80 minutes.

Grant `<benchmark-service-principal>` **CAN MONITOR**. This permits query execution and
query/warehouse monitoring without warehouse administration.

Record the warehouse ID, channel/runtime, actual autoscaling values, auto-stop,
creation time, and owner. Do not use `<control-warehouse-name>` for measured reads.
Start the warehouse before launching the runners. The same warm-start and
auto-stop policy must be used for Lakehouse//RT.

### Prepared warehouse

The September 17, 2026 baseline warehouse is:

- name: `<query-warehouse-name>`;
- ID: `<query-warehouse-id>`;
- type: Serverless;
- size: `X-Small`;
- scaling: one cluster minimum and maximum;
- channel: Current, version `2026.36`;
- owner: the benchmark administrator; and
- `<benchmark-service-principal>` permission: `CAN MONITOR`.

The 80-minute auto-stop was intentionally retained. It exceeds both the
ten-minute dashboard cadence and the one-hour drill-down cadence. Dashboard
activity should keep the warehouse running during a healthy workload, but the
long timeout protects the hourly-only path if dashboard execution stops. Stop
the warehouse explicitly when the baseline ends; otherwise up to 80 minutes of
post-run idle usage must be excluded and disclosed. Preserve the same policy
for the Lakehouse//RT comparison or report the difference.

## Identical execution semantics

Both Serverless SQL and Lakehouse//RT use the Statement Execution kernel path:

- `databricks-sql-connector[kernel]==4.5.0`;
- `use_kernel=True`;
- `use_cached_result=false`;
- ANSI mode enabled;
- UTC session time zone;
- one Statement Execution ID per statement;
- canonical latency from
  `system.query.history.total_duration_ms`, excluding result fetch;
- compilation, execution, queue, result-fetch, IO-cache, bytes/files/rows, and
  result-cache evidence retained;
- full result draining and stable result hashing outside canonical provider
  timing; and
- producer progress plus the latest completed MV source-row watermark attached
  to every iteration.

The query files contain table-name placeholders, allowing the same SQL to
target immutable qualification objects without editing the query text.

The first online attempt on September 17 failed before executing SQL because
the runner supplied the connector's Thrift-style custom
`credentials_provider` while `use_kernel=True`. The Statement Execution kernel
explicitly rejects that combination. The runner now passes
`oauth_client_id` and `oauth_client_secret`, allowing the kernel to manage M2M
token lifecycle directly. The failed attempt is retained as invalid setup
evidence and is not a latency observation.

The second attempt authenticated and executed all six SQL statements, but the
kernel cursor requires `fetchmany(size)` while the initial drain loop called
`fetchmany()` without a size. Provider history was captured, but the runner
correctly nullified canonical results because full result draining and hashing
failed. The drain loop now uses explicit 1,000-row batches. This failed attempt
is also excluded; server timings visible in its diagnostics are not promoted
to benchmark observations.

The third attempt started at 9,891,162,743 provider-acknowledged raw rows and
successfully produced the first complete partial-baseline iteration. Canonical
provider durations were:

- dashboard Q1–Q4: 1.038, 0.734, 0.840, and 0.852 seconds;
- drill-down Q1–Q2: 10.074 and 9.906 seconds.

All statements reported `FINISHED`, `result_from_cache=false`, complete result
hashes, and no errors. Client wall times were retained only as diagnostics and
were not used as latency. This attempt began after ingestion was underway and
remains a partial-window characterization.

The operator stopped ingestion after 4h08m, so the valid query attempt ended
after approximately 1h24m. It produced nine dashboard and two drill-down
iterations spanning roughly 9.89B–14.69B raw rows. Every observation remained
valid and uncached.

Provider-duration ranges and medians were:

- dashboard Q1: 0.743–1.066 seconds, median 0.968;
- dashboard Q2: 0.411–0.734 seconds, median 0.610;
- dashboard Q3: 0.384–0.855 seconds, median 0.723;
- dashboard Q4: 0.448–0.852 seconds, median 0.720;
- drill-down Q1: 10.074 and 18.794 seconds, median 14.434; and
- drill-down Q2: 9.906 and 16.661 seconds, median 13.284.

The two earlier setup attempts are excluded. This partial baseline validates
the query harness but does not replace a from-time-zero canonical baseline.

## Launch against an active ingest run

Set only the new query warehouse ID:

```sh
export DATABRICKS_QUERY_WAREHOUSE_ID='<X_SMALL_SERVERLESS_WAREHOUSE_ID>'
export DATABRICKS_WAREHOUSE_MODE='serverless-baseline'
export DATABRICKS_QUERY_SIZE='X-Small'

tmux new -As query-baseline
/home/ubuntu/costbench/full-path-realtime/quotes/databricks/run_query_workloads.sh
```

The wrapper reads the active ingest context, prompts once for the benchmark
service-principal secret, and launches both runners. It stops them after the
producer stops, allowing an in-flight iteration to complete cleanly.

If the baseline starts after ingestion has already begun, its context records
the exact starting provider row count and marks the result
`partial_ingest_window=true`. Such a result is useful characterization but is
not a complete six-hour baseline.

## Lakehouse//RT swap

For the later run, create a fresh ingest target and use the same wrapper:

```sh
export DATABRICKS_QUERY_WAREHOUSE_ID='<LAKEHOUSE_RT_WAREHOUSE_ID>'
export DATABRICKS_WAREHOUSE_MODE='lakehouse-rt'
export DATABRICKS_QUERY_SIZE='<QUALIFIED_QUERY_SIZE>'

/home/ubuntu/costbench/full-path-realtime/quotes/databricks/run_query_workloads.sh
```

No query SQL or cadence may change. Generate a new output directory and run
ID; never compare two warehouses using cached results or a shared mutable
target.

## Cost treatment

Attribute query-warehouse billing separately from:

- Zerobus ingestion;
- MV refresh;
- Predictive Optimization;
- storage;
- the excluded `<control-warehouse-name>` instrumentation warehouse; and
- the producer.

The Serverless SQL baseline uses its account-effective SKU and list-price
history. The later Lakehouse//RT result uses the Lakehouse Serverless SKU and
is reported independently. Query cost allocation uses the union of
run-tagged successful query intervals so concurrent dashboard and drill-down
queries do not double-count shared warehouse uptime.

After billing has settled for at least 24 hours, collect the partial baseline's
query-warehouse cost with:

```sh
/home/ubuntu/costbench/full-path-realtime/quotes/databricks/collect_query_baseline_settled.sh \
  /data/databricks-qualification/ingest_mv_6h_r2_20260917T104305Z/queries/serverless-baseline_<query-warehouse-id>_20260917T132826Z
```

The existing cost summarizer uses the legacy internal category name
`primary_rt_serving_compute` for the selected query warehouse; the wrapper
relabels it `primary_query_serving_compute` for this Serverless baseline.
