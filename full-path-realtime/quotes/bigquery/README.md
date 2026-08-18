# BigQuery full-path real-time quotes benchmark

This directory is the BigQuery counterpart to the accepted ClickHouse Cloud T2 run. It
uses the BigQuery Storage Write API rather than load jobs, keeps the same raw and
dashboard query shapes, and emits BigQuery-native job cost evidence.

Nothing is executed merely by installing or inspecting these files. The actual
cloud calls begin only when you run `setup.py`, an online preflight, an ingester,
runner, monitor, or evidence collector.

## What is included

- `create.sql`: raw table clustered by `(sym, t)` and incremental daily MV.
- `setup.py`: creates the dataset and applies the idempotent DDL.
- `ingest_parquet_rows.py`: Parquet to Arrow to long-lived committed Storage
  Write API streams, with offsets, retries, request-size splitting, and live
  acknowledged-EPS metrics.
- `run_dashboard.sh`: four MV queries every ten minutes.
- `run_drilldown.sh`: two raw queries every hour.
- `monitor_mv.py`: one-minute MV refresh-watermark samples.
- `collect_evidence.py`: exports query/MV jobs, Write API timeline, raw/MV
  storage fields, and final table metadata over an explicit UTC window.
- `capture_query_results.py`: untimed canonical result rows and hashes for
  correctness checks at selected row-progress checkpoints.
- `preflight.py`: offline structure checks plus optional online schema/query dry
  runs.
- `cleanup_streams.py`: finalizes streams recorded before a hard interruption.
- `_commands.txt`: copy/paste tmux flow corresponding to the ClickHouse T2
  terminals.
- `SMOKE_TEST.md`: bounded end-to-end “does everything work?” validation with
  explicit pass/fail gates.
- `TUNING.md`: short Storage Write API capacity trials and the selected ingest
  settings.
- `REAL_RUN.md`: canonical fresh-run setup and the exact four-terminal launch,
  monitoring, and shutdown sequence.
- `SLOTS_AND_RESERVATIONS.md`: interpretation of the current on-demand slot
  evidence and a reversible, programmatic fixed-N Enterprise PAYG reservation
  workflow for a separate controlled-capacity run.
- `MATERIALIZED_VIEW_FRESHNESS.md`: automatic refresh semantics, interpretation
  of the observed watermark waveform, the current always-current query
  contract, BigQuery's hard one-minute `refresh_interval_minutes` minimum, and
  why `max_staleness` is out of scope for this workload.
- `COST_ACCOUNTING.md`: the separate ingestion, raw/MV storage, MV refresh,
  query, and clustering cost ledgers for on-demand and capacity pricing.
- `BIGQUERY_BENCHMARK_CONTRACT.md`: exact equivalence and accounting rules.

## Installation and authentication

Use a dedicated virtual environment and Application Default Credentials:

```bash
# From the CostBench repository root:
cd full-path-realtime/quotes/bigquery
python3 --version  # use Python 3.10+; 3.11 or 3.12 is preferable
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
gcloud auth application-default login
gcloud auth application-default set-quota-project YOUR_BILLING_PROJECT
export GOOGLE_CLOUD_PROJECT=YOUR_PROJECT
export BQ_LOCATION=US
export BQ_DATASET=quotes_streaming_t1
export BQ_RUN_ID=bq_t1
gcloud services enable \
  bigquery.googleapis.com \
  bigquerystorage.googleapis.com \
  --project "$GOOGLE_CLOUD_PROJECT"
```

Confirm which principal is active before granting access:

```bash
gcloud auth list --filter=status:ACTIVE --format='value(account)'
```

The execution identity needs four distinct capabilities. Grant them at the
smallest practical scope:

| Purpose | Required permission | Typical predefined role and scope |
|---|---|---|
| Run query jobs and create the fresh dataset | `bigquery.jobs.create` | `roles/bigquery.user` on the project |
| Export all benchmark and automatic MV-refresh jobs | `bigquery.jobs.listAll` | `roles/bigquery.resourceViewer` on the project |
| Read `WRITE_API_TIMELINE_BY_PROJECT` | `bigquery.tables.list` | `roles/bigquery.metadataViewer` on the project |
| Append rows through Storage Write API | `bigquery.tables.updateData` | `roles/bigquery.dataEditor` or `roles/bigquery.dataOwner` on the dataset |

When the same user creates a fresh dataset while holding
`roles/bigquery.user`, BigQuery makes that user the dataset owner. If an
administrator pre-creates the dataset, grant the dataset write role explicitly.
For a user-based ADC setup, an administrator can initialize the project roles
with:

```bash
export BQ_USER_EMAIL=YOUR_USER_EMAIL
gcloud projects add-iam-policy-binding "$GOOGLE_CLOUD_PROJECT" \
  --member="user:$BQ_USER_EMAIL" \
  --role="roles/bigquery.user" \
  --condition=None
gcloud projects add-iam-policy-binding "$GOOGLE_CLOUD_PROJECT" \
  --member="user:$BQ_USER_EMAIL" \
  --role="roles/bigquery.resourceViewer" \
  --condition=None
gcloud projects add-iam-policy-binding "$GOOGLE_CLOUD_PROJECT" \
  --member="user:$BQ_USER_EMAIL" \
  --role="roles/bigquery.metadataViewer" \
  --condition=None
```

`--condition=None` explicitly creates unconditional bindings and is required by
`gcloud` when the existing project policy contains any conditional binding. For
a service account, replace the member with
`serviceAccount:SERVICE_ACCOUNT_EMAIL`. Do not commit service-account keys. The
existing `_credentials.txt` is ignored and these scripts do not read it.

## Recommended run flow

Use `REAL_RUN.md` as the canonical launch sequence and `_commands.txt` for the
post-run evidence commands. At a high level:

1. choose a fresh dataset and a fixed BigQuery location;
2. run `setup.py`, then `preflight.py --online`;
3. start ingest, dashboard, drill-down, and freshness in separate tmux sessions;
4. stop all workloads consistently and export provider-side evidence;
5. capture correctness results at declared checkpoints;
6. reconcile expected input, acknowledged rows, provider timeline rows, and
   table metadata before analysis.

The ingester's conservative defaults are not the benchmark configuration.
Preparation documented in `TUNING.md` selected 40 committed streams with a
global `--target-eps 1000000` cap, 131,000-row Arrow batches, and a
16,000,000-byte serialized ceiling. The short
ten-row-group-per-writer trials were useful capacity tests, but understated
steady-state throughput: longer full-input diagnostics showed 40 and 30 writers
running materially above the 1M-EPS target, while a later 28-writer endurance
validation fell below it. The explicit source-rate cap makes the benchmark
target reproducible while 40 streams retain capacity headroom. Use
`--max-row-groups` to create clean,
repeatable tuning runs when a single source file is too large for `--max-files`
to be useful. For long runs, pass `--quiet-worker-logs`; aggregate acknowledged
throughput remains visible in a boxed display with prominent average and
instantaneous EPS values. The canonical command also enables a 60-second
Python-GC/glibc trim cycle and a 16 GiB system-memory safety floor. Every metric
records RSS, Linux `MemAvailable`, Arrow allocation, trim count, reclaimed RSS,
and trim duration; see `TUNING.md` for the superseded T1 failure analysis.

Offsets make a retry idempotent, but a successful append can lose its client
response. In that case BigQuery returns `ALREADY_EXISTS` when the client
replays the same offset. The ingester accepts that response only when the
server-reported received offset equals the requested offset and its expected
offset equals `offset + batch_rows`. It counts the batch once and records
`recovered_already_exists_appends` and `recovered_already_exists_rows` in the
progress, metrics, and summary evidence. A recovered replay is not a duplicate
or a failed append. The client similarly records
`recovered_server_row_count_appends` when stream metadata proves the batch was
committed before a replay was necessary. Both paths count the rows once, reopen
the append connection, and still require final provider-row reconciliation.

## Why Storage Write API, and the quota math

Load jobs cannot reproduce a continuously arriving, roughly once-per-second
event path because their per-table job quota would become the benchmark's hard
ceiling. The Storage Write API is BigQuery's streaming counterpart to Snowpipe
Streaming and ClickHouse asynchronous inserts.

The verified 5M-row smoke run recorded 457,832,696 provider input bytes, or
91.57 bytes/event. At that observed density, 1M events/second is approximately
91.6 MB/s: 30.5% of a 300 MB/s regional project quota or 3.1% of a 3 GB/s
US/EU multi-region quota. The aggregate quota therefore should not be the first
bottleneck, but the smoke ratio remains a planning input rather than evidence
for the final run. Arrow encoding size, retries, regional scope, other project
traffic, stream-level throughput, source decoding, and client networking all
matter. Use both:

- `ingest_metrics.jsonl` for client acknowledgments and serialized Arrow bytes;
- `WRITE_API_TIMELINE_BY_PROJECT` for provider-observed rows, input bytes,
  requests, stream type, and error code.

The gRPC `AppendRows` request limit is 20 MB; the separate HTTP request-size
limit is 10 MB. The ingester recursively splits serialized Arrow record batches
above `--max-request-bytes`. Its conservative default is 8 MB; the selected
benchmark configuration explicitly uses a 16 MB ceiling while retaining
headroom for request overhead.

## Output schema

Dashboard and drill-down files are newline-delimited JSON. They preserve the
existing benchmark fields and add the requested BigQuery arrays:

```json
"result": [[0.123], [0.456]],
"billed_slot_sec": [[2.4], [8.1]],
"billed_bytes": [[10485760], [52428800]]
```

All three arrays have exactly the same shape. Query 1 is outer index 0, query 2
is index 1, and one scheduled iteration contains one trial. For dashboard lines
there are four outer entries; for drill-down lines there are two. `query_jobs`
contains the supporting job ID, timestamps, runtime source, client wall time,
slot milliseconds, processed and billed bytes, cache flag, reservation usage,
and error.

`result` follows the prior BigQuery ClickBench implementation by preferring the
server's `statistics.finalExecutionDurationMs`. If unavailable, it falls back
to job start/end timestamps. It does not use the time spent printing or
downloading all result rows. Cache is explicitly disabled.

The name `billed_slot_sec` is retained for compatibility, but technically it is
`totalSlotMs / 1000`: slot-seconds consumed. See the contract before converting
these measurements to cost.

## Row progress and freshness

Running `COUNT(*)` on the 100B-row raw table every ten minutes would add an
extra workload and cost. Instead, the ingester atomically publishes acknowledged
row progress; runners use that as `raw_rows` and also record the BigQuery table
metadata count as `raw_rows_metadata`. If no progress file is supplied, metadata
is the fallback and `raw_rows_source` makes this explicit.

The MV table's metadata row count is recorded without adding a measured query.
`monitor_mv.py` separately queries `INFORMATION_SCHEMA.MATERIALIZED_VIEWS` for
the refresh watermark. BigQuery direct MV queries remain logically current even
when the cached watermark lags because BigQuery can read the base-table delta;
watermark lag is therefore a maintenance/cost signal, not automatically result
staleness.

That statement applies to this benchmark, where `max_staleness` is unset. This
is required by the current-result contract: the workload ingests approximately
one million events per second and queries newly ingested data immediately.
`max_staleness` is out of scope because it permits stale answers.

## Provider system-table evidence

`collect_evidence.py` reads three project-scoped system views:

- `JOBS_BY_PROJECT` for measured queries, automatic MV refreshes, cache status,
  slot time, processed bytes, and billed bytes;
- `WRITE_API_TIMELINE_BY_PROJECT` for provider-observed requests, rows, input
  bytes, stream type, and error code in one-minute buckets;
- `TABLE_STORAGE_BY_PROJECT` for a snapshot of raw-table and MV logical,
  physical, time-travel, and fail-safe bytes.

Automatic MV-refresh jobs have `destination_table = NULL`. The collector
identifies the target MV through the job's `referenced_tables` entries, which
contain its project, dataset, and table IDs; do not filter refresh jobs through
`destination_table`.

For an offset-based run, provider error buckets must normally be `OK`. A
matching `ALREADY_EXISTS` bucket is permitted only when the ingester recorded a
corresponding recovered exact-offset replay. Treat every other error code as a
run error. Accepted-row reconciliation uses the provider's successful rows,
not the number of retry requests.

Use an explicit UTC `--since` at least one minute before the ingestion start.
The one-minute margin is important because the timeline timestamp is the start
of its minute bucket. A relative `--hours` window can silently miss the run when
evidence is collected later.

```bash
jq -r '.started_at' results_t1/ingest/ingest_progress.json
# Choose a timestamp at least one minute earlier than the printed value.
export BQ_EVIDENCE_SINCE=YYYY-MM-DDTHH:MM:00Z
export BQ_EVIDENCE_UNTIL=YYYY-MM-DDTHH:MM:SSZ

python3 collect_evidence.py \
  --project "$GOOGLE_CLOUD_PROJECT" \
  --dataset "$BQ_DATASET" \
  --location "$BQ_LOCATION" \
  --since "$BQ_EVIDENCE_SINCE" \
  --until "$BQ_EVIDENCE_UNTIL" \
  --run-id "$BQ_RUN_ID" \
  --output-dir results_t1/evidence
```

Validate that collection was complete and reconcile provider totals with the
ingester's acknowledged rows:

```bash
jq '.errors' results_t1/evidence/evidence_summary.json
test -s results_t1/evidence/write_api_timeline.jsonl
test -s results_t1/evidence/table_storage.jsonl
jq -s '{
  minute_buckets: length,
  total_requests: (map(.total_requests) | add),
  successful_rows:
    (map(select(.error_code == "OK") | .total_rows) | add // 0),
  successful_input_bytes:
    (map(select(.error_code == "OK") | .total_input_bytes) | add // 0),
  already_exists_requests:
    (map(select(.error_code == "ALREADY_EXISTS") | .total_requests) | add // 0),
  unexpected_error_buckets:
    (map(select(.error_code != "OK" and .error_code != "ALREADY_EXISTS")) | length)
}' results_t1/evidence/write_api_timeline.jsonl
```

Require `errors = []`, a non-empty timeline, provider successful rows equal to
`acknowledged_rows` and expected input rows, and zero unexpected error buckets.
Any `ALREADY_EXISTS` requests must equal client-recorded recovered exact-offset
appends. The timeline's successful input bytes are provider-observed Write API
input, not table storage size or query billed bytes.

If the filtered timeline is unexpectedly empty, first discover all recent
project writes without dataset/table filters (replace `region-us` for a
different location):

```bash
bq query --use_legacy_sql=false --location="$BQ_LOCATION" --max_rows=100 \
  "SELECT start_timestamp, dataset_id, table_id, stream_type, error_code,
          total_requests, total_rows, total_input_bytes
   FROM \`$GOOGLE_CLOUD_PROJECT\`.\`region-us\`.INFORMATION_SCHEMA.WRITE_API_TIMELINE_BY_PROJECT
   WHERE start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
   ORDER BY start_timestamp DESC, dataset_id, table_id
   LIMIT 100"
```

## Important comparability caveats

- BigQuery clustering is managed and can recluster asynchronously; it is the
  closest layout counterpart, not an identical sorted MergeTree.
- Automatic MV refresh is best effort. A one-minute refresh interval is a cap,
  not a guaranteed schedule.
- BigQuery `APPROX_QUANTILES` is not ClickHouse TDigest. BigQuery returns p95
  and p99 as a named struct because output arrays cannot contain `NULL`
  elements. Validate percentile answers within a declared tolerance.
- BigQuery Q2 implements population skew and raw population kurtosis from
  moments to match ClickHouse. The existing Snowflake SQL uses sample
  estimators and must be reconciled before strict three-way claims.
- The run is exactly-once for in-process retries, not source-checkpoint-resumable
  after host failure. Start a new run in a fresh dataset after cleanup.
- On-demand and capacity pricing require different cost interpretation. Keep
  query, MV refresh, ingestion, and storage cost components separate.
- Materialized-view automatic refresh is billable maintenance, despite being
  automatic. Automatic reclustering has no separate compute charge, but the
  clustered table still incurs normal storage, ingestion, and query charges.

Current official references:

- [Storage Write API](https://docs.cloud.google.com/bigquery/docs/write-api)
- [Streaming with committed streams and Arrow](https://docs.cloud.google.com/bigquery/docs/write-api-streaming)
- [Storage Write API quotas](https://docs.cloud.google.com/bigquery/quotas#write-api-limits)
- [Materialized view management and refresh behavior](https://docs.cloud.google.com/bigquery/docs/materialized-views-manage)
- [Materialized-view pricing](https://docs.cloud.google.com/bigquery/docs/materialized-views-intro#materialized_views_pricing)
- [Materialized-view information schema](https://docs.cloud.google.com/bigquery/docs/information-schema-materialized-views)
- [Clustered-table pricing and automatic reclustering](https://docs.cloud.google.com/bigquery/docs/clustered-tables)
- [Write API timeline](https://docs.cloud.google.com/bigquery/docs/information-schema-write-api)
- [BigQuery jobs information schema](https://docs.cloud.google.com/bigquery/docs/information-schema-jobs)
- [BigQuery IAM roles and permissions](https://docs.cloud.google.com/bigquery/docs/access-control)
- [BigQuery API service dependencies](https://docs.cloud.google.com/bigquery/docs/service-dependencies)
