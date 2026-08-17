# BigQuery “does everything work?” smoke test

Use this sequence after installing or updating the BigQuery benchmark files and
before tuning or starting a real run. It validates the complete mechanics with
a small, bounded input slice. It is not a performance benchmark.

The sequence checks:

1. authentication, dataset location, schema, and query compilation;
2. Storage Write API ingestion and exact row reconciliation;
3. direct raw-table and materialized-view visibility;
4. MV freshness metadata collection;
5. all four dashboard and both drill-down queries;
6. runtime, slot-second, billed-byte, job-ID, and cache evidence;
7. provider-side Write API rows, requests, bytes, and errors.

## 1. Enter the environment

Complete `_prep.txt` first, including its project-scoped IAM roles. Then:

```bash
cd ~/bigquery
source .venv/bin/activate

python3 --version
python3 preflight.py

gcloud auth list \
  --filter=status:ACTIVE \
  --format='value(account)'
```

The active CLI account and Application Default Credentials should represent the
same intended principal. The smoke test must not use credentials committed to
the repository.

## 2. Define a fresh smoke run

Never reuse a partially populated dataset.

```bash
export GOOGLE_CLOUD_PROJECT=YOUR_PROJECT
export BQ_LOCATION=US
export BQ_SMOKE_STAMP="$(date -u +%Y%m%d_%H%M%S)"
export BQ_DATASET="quotes_smoke_${BQ_SMOKE_STAMP}"
export BQ_RUN_ID="bq-smoke-${BQ_SMOKE_STAMP}"
export BQ_SMOKE_ROOT="results_smoke/${BQ_RUN_ID}"

mkdir -p "$BQ_SMOKE_ROOT"
```

`US` is the intended multi-region for this benchmark. A different location is
valid only when it is a deliberate benchmark choice and every command uses it
consistently.

## 3. Create and preflight the objects

```bash
python3 setup.py \
  --project "$GOOGLE_CLOUD_PROJECT" \
  --dataset "$BQ_DATASET" \
  --location "$BQ_LOCATION" \
  --run-id "$BQ_RUN_ID"

python3 preflight.py \
  --project "$GOOGLE_CLOUD_PROJECT" \
  --dataset "$BQ_DATASET" \
  --location "$BQ_LOCATION" \
  --online \
  --output "$BQ_SMOKE_ROOT/preflight.json"

jq -e '
  (.offline.errors // []) == []
  and (.online.errors // []) == []
' "$BQ_SMOKE_ROOT/preflight.json"
```

Confirm the dataset's actual location rather than relying only on the shell
variable:

```bash
bq show \
  --format=json \
  "$GOOGLE_CLOUD_PROJECT:$BQ_DATASET" \
  | jq -r '.location'
```

Expected output: `US`.

## 4. Ingest a bounded slice

This uses the first 40 whole row groups from the first sorted Parquet file. With
the currently inspected StockHouse source, that is 5,232,720 rows. The summary
file remains the source of truth if the input files change.

```bash
python3 ingest_parquet_rows.py \
  --dir ~/data/stockhouse \
  --pattern '*.parquet' \
  --max-files 1 \
  --max-row-groups 40 \
  --project "$GOOGLE_CLOUD_PROJECT" \
  --dataset "$BQ_DATASET" \
  --location "$BQ_LOCATION" \
  --table quotes \
  --create-sql create.sql \
  --parallel 4 \
  --batch-size 131000 \
  --max-request-bytes 16000000 \
  --metrics-interval 5 \
  --output-dir "$BQ_SMOKE_ROOT/ingest" \
  --progress-file "$BQ_SMOKE_ROOT/ingest/ingest_progress.json" \
  --run-id "$BQ_RUN_ID"
```

Require clean client-side completion:

```bash
jq -e '
  .finished == true
  and .acknowledged_rows == .expected_input_rows
  and .retries == 0
  and (.errors | length) == 0
  and (.stream_finalize_errors | length) == 0
' "$BQ_SMOKE_ROOT/ingest/ingest_summary.json"
```

## 5. Verify raw and MV query visibility

Use `row_count` as the alias. `ROWS` is a GoogleSQL keyword and caused the
earlier `Unexpected keyword ROWS` error.

```bash
export EXPECTED_ROWS="$(
  jq -r '.expected_input_rows' \
    "$BQ_SMOKE_ROOT/ingest/ingest_summary.json"
)"

export ACKNOWLEDGED_ROWS="$(
  jq -r '.acknowledged_rows' \
    "$BQ_SMOKE_ROOT/ingest/ingest_summary.json"
)"

export RAW_QUERY_ROWS="$(
  bq query \
    --use_legacy_sql=false \
    --location="$BQ_LOCATION" \
    --format=json \
    "SELECT COUNT(*) AS row_count
     FROM \`$GOOGLE_CLOUD_PROJECT.$BQ_DATASET.quotes\`" \
  | jq -r '.[0].row_count'
)"

printf 'expected=%s acknowledged=%s raw_query=%s\n' \
  "$EXPECTED_ROWS" "$ACKNOWLEDGED_ROWS" "$RAW_QUERY_ROWS"

test "$EXPECTED_ROWS" -eq "$ACKNOWLEDGED_ROWS"
test "$ACKNOWLEDGED_ROWS" -eq "$RAW_QUERY_ROWS"
```

Verify that a direct query of the materialized view returns rows:

```bash
export MV_QUERY_ROWS="$(
  bq query \
    --use_legacy_sql=false \
    --location="$BQ_LOCATION" \
    --format=json \
    "SELECT COUNT(*) AS mv_row_count
     FROM \`$GOOGLE_CLOUD_PROJECT.$BQ_DATASET.quotes_daily\`" \
  | jq -r '.[0].mv_row_count'
)"

printf 'mv_query_rows=%s\n' "$MV_QUERY_ROWS"
test "$MV_QUERY_ROWS" -gt 0
```

The `tables.get` MV row count shown by a runner can temporarily be zero or lag
behind. A direct MV query remains logically current because BigQuery can combine
persisted MV state with the unmaterialized base-table delta. Keep the metadata
count and direct-query result conceptually separate.

## 6. Exercise the freshness monitor

One iteration validates the metadata query and JSONL schema without waiting for
a cadence interval:

```bash
python3 monitor_mv.py \
  --project "$GOOGLE_CLOUD_PROJECT" \
  --dataset "$BQ_DATASET" \
  --location "$BQ_LOCATION" \
  --progress-file "$BQ_SMOKE_ROOT/ingest/ingest_progress.json" \
  --output-dir "$BQ_SMOKE_ROOT/freshness" \
  --iterations 1 \
  --run-id "$BQ_RUN_ID"

export FRESHNESS_JSON="$(
  find "$BQ_SMOKE_ROOT/freshness" -type f -name 'mv_freshness_*.jsonl' \
    | sort | tail -n 1
)"

jq -e '.metadata_query_job.error == null' "$FRESHNESS_JSON"
```

An initially null refresh watermark is not itself a smoke-test failure. A
metadata-query error is a failure.

## 7. Run one dashboard iteration

```bash
./run_dashboard.sh \
  --project "$GOOGLE_CLOUD_PROJECT" \
  --dataset "$BQ_DATASET" \
  --location "$BQ_LOCATION" \
  --output-dir "$BQ_SMOKE_ROOT/mv" \
  --progress-file "$BQ_SMOKE_ROOT/ingest/ingest_progress.json" \
  --iterations 1 \
  --run-id "$BQ_RUN_ID" \
  "BigQuery (GCP)" "serverless" "serverless" "smoke test" 0

export DASHBOARD_JSON="$(
  find "$BQ_SMOKE_ROOT/mv" -type f -name 'dashboard_*.jsonl' \
    | sort | tail -n 1
)"

jq -e '
  .runner == "dashboard"
  and (.result | length) == 4
  and (.billed_slot_sec | length) == 4
  and (.billed_bytes | length) == 4
  and all(.result[]; .[0] != null)
  and all(.billed_slot_sec[]; .[0] != null)
  and all(.billed_bytes[]; .[0] != null)
  and all(.query_jobs[];
    .error == null
    and .job_id != null
    and .cache_hit == false
    and .total_bytes_billed != null)
' "$DASHBOARD_JSON"
```

## 8. Run one drill-down iteration

```bash
./run_drilldown.sh \
  --project "$GOOGLE_CLOUD_PROJECT" \
  --dataset "$BQ_DATASET" \
  --location "$BQ_LOCATION" \
  --output-dir "$BQ_SMOKE_ROOT/raw" \
  --progress-file "$BQ_SMOKE_ROOT/ingest/ingest_progress.json" \
  --iterations 1 \
  --run-id "$BQ_RUN_ID" \
  "BigQuery (GCP)" "serverless" "serverless" "smoke test" 0

export DRILLDOWN_JSON="$(
  find "$BQ_SMOKE_ROOT/raw" -type f -name 'drilldown_*.jsonl' \
    | sort | tail -n 1
)"

jq -e '
  .runner == "drilldown"
  and (.result | length) == 2
  and (.billed_slot_sec | length) == 2
  and (.billed_bytes | length) == 2
  and all(.result[]; .[0] != null)
  and all(.billed_slot_sec[]; .[0] != null)
  and all(.billed_bytes[]; .[0] != null)
  and all(.query_jobs[];
    .error == null
    and .job_id != null
    and .cache_hit == false
    and .total_bytes_billed != null)
' "$DRILLDOWN_JSON"
```

## 9. Capture untimed correctness results

```bash
python3 capture_query_results.py \
  --project "$GOOGLE_CLOUD_PROJECT" \
  --dataset "$BQ_DATASET" \
  --location "$BQ_LOCATION" \
  --run-id "$BQ_RUN_ID" \
  --output-dir "$BQ_SMOKE_ROOT/validation"

jq -e '
  (.queries | length) == 6
  and all(.queries[];
    .query_job.error == null
    and .query_job.job_id != null
    and .canonical_rows_sha256 != null)
' "$BQ_SMOKE_ROOT/validation/manifest.json"
```

This is deliberately outside the timed runners.

## 10. Export and reconcile provider evidence

Derive a timestamp one minute before the ingester's start so the first
minute-truncated Write API timeline bucket is included:

```bash
export BQ_EVIDENCE_SINCE="$(
python3 - <<'PY'
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

root = Path(os.environ["BQ_SMOKE_ROOT"])
progress = json.loads((root / "ingest/ingest_progress.json").read_text())
started = datetime.fromisoformat(progress["started_at"].replace("Z", "+00:00"))
since = started.replace(second=0, microsecond=0) - timedelta(minutes=1)
print(since.isoformat().replace("+00:00", "Z"))
PY
)"

printf 'evidence_since=%s\n' "$BQ_EVIDENCE_SINCE"

python3 collect_evidence.py \
  --project "$GOOGLE_CLOUD_PROJECT" \
  --dataset "$BQ_DATASET" \
  --location "$BQ_LOCATION" \
  --since "$BQ_EVIDENCE_SINCE" \
  --run-id "$BQ_RUN_ID" \
  --output-dir "$BQ_SMOKE_ROOT/evidence"
```

Require all three system-table exports and no collector errors:

```bash
jq -e '.errors == []' \
  "$BQ_SMOKE_ROOT/evidence/evidence_summary.json"

test -s "$BQ_SMOKE_ROOT/evidence/query_and_mv_jobs.jsonl"
test -s "$BQ_SMOKE_ROOT/evidence/write_api_timeline.jsonl"
test -s "$BQ_SMOKE_ROOT/evidence/table_storage.jsonl"
```

Reconcile provider and client totals:

```bash
export CLIENT_REQUESTS="$(
  jq -r '.append_requests' \
    "$BQ_SMOKE_ROOT/ingest/ingest_summary.json"
)"

export PROVIDER_REQUESTS="$(
  jq -s 'map(.total_requests) | add // 0' \
    "$BQ_SMOKE_ROOT/evidence/write_api_timeline.jsonl"
)"

export PROVIDER_ROWS="$(
  jq -s 'map(select(.error_code == "OK") | .total_rows) | add // 0' \
    "$BQ_SMOKE_ROOT/evidence/write_api_timeline.jsonl"
)"

export PROVIDER_BYTES="$(
  jq -s 'map(select(.error_code == "OK") | .total_input_bytes) | add // 0' \
    "$BQ_SMOKE_ROOT/evidence/write_api_timeline.jsonl"
)"

export ALREADY_EXISTS_REQUESTS="$(
  jq -s 'map(select(.error_code == "ALREADY_EXISTS") | .total_requests) | add // 0' \
    "$BQ_SMOKE_ROOT/evidence/write_api_timeline.jsonl"
)"

export RECOVERED_APPENDS="$(
  jq -r '.recovered_already_exists_appends // 0' \
    "$BQ_SMOKE_ROOT/ingest/ingest_summary.json"
)"

export UNEXPECTED_ERROR_BUCKETS="$(
  jq -s 'map(select(.error_code != "OK" and .error_code != "ALREADY_EXISTS")) | length' \
    "$BQ_SMOKE_ROOT/evidence/write_api_timeline.jsonl"
)"

printf 'client_requests=%s provider_requests=%s acknowledged=%s provider_rows=%s provider_bytes=%s already_exists=%s recovered=%s unexpected_errors=%s\n' \
  "$CLIENT_REQUESTS" "$PROVIDER_REQUESTS" "$ACKNOWLEDGED_ROWS" \
  "$PROVIDER_ROWS" "$PROVIDER_BYTES" "$ALREADY_EXISTS_REQUESTS" \
  "$RECOVERED_APPENDS" "$UNEXPECTED_ERROR_BUCKETS"

test "$CLIENT_REQUESTS" -eq "$PROVIDER_REQUESTS"
test "$ACKNOWLEDGED_ROWS" -eq "$PROVIDER_ROWS"
test "$PROVIDER_BYTES" -gt 0
test "$ALREADY_EXISTS_REQUESTS" -eq "$RECOVERED_APPENDS"
test "$UNEXPECTED_ERROR_BUCKETS" -eq 0
```

If the timeline file is empty immediately after ingestion, do not interpret
that as zero provider rows. Re-run the collector after the next provider minute
bucket becomes visible.

### System-table permission failures

`WRITE_API_TIMELINE_BY_PROJECT` requires `bigquery.tables.list` at project
scope. `JOBS_BY_PROJECT` requires access to all project jobs. For a user-based
ADC principal, an administrator can grant the standard roles with:

```bash
export BQ_USER_EMAIL="$(
  gcloud auth list --filter=status:ACTIVE --format='value(account)'
)"

gcloud projects add-iam-policy-binding "$GOOGLE_CLOUD_PROJECT" \
  --member="user:$BQ_USER_EMAIL" \
  --role="roles/bigquery.resourceViewer" \
  --condition=None

gcloud projects add-iam-policy-binding "$GOOGLE_CLOUD_PROJECT" \
  --member="user:$BQ_USER_EMAIL" \
  --role="roles/bigquery.metadataViewer" \
  --condition=None
```

`--condition=None` is necessary when the existing project policy contains
conditional bindings. Use `serviceAccount:...` instead for a service-account
principal.

## Pass criteria

The smoke test passes only when all of the following are true:

- offline and online preflight report no errors;
- the actual dataset location matches `BQ_LOCATION`;
- expected, acknowledged, raw-query, and provider row counts match exactly;
- ingestion has zero errors and stream-finalization errors; retries and
  recovered `ALREADY_EXISTS` appends, if any, remain explicit and reconcile to
  provider rows;
- a direct query of `quotes_daily` returns rows;
- the freshness metadata query succeeds;
- all four dashboard and both drill-down queries succeed;
- every measured query has a job ID, cache disabled, and non-null runtime,
  slot-second, and billed-byte values;
- correctness capture produces six non-error hashes;
- provider requests match client appends, input bytes are positive, and every
  Write API timeline bucket is either `OK` or a client-reconciled
  `ALREADY_EXISTS` exact-offset replay.

Do not delete the smoke dataset until its evidence files have been retained or
the validation is intentionally discarded.

## Resolved issues represented by the current files

- A lost success response followed by an exact-offset retry is recognized as a
  recovered append when BigQuery returns `ALREADY_EXISTS` with matching
  expected/received offsets; older copies retried that proof of success until
  exhausting the retry budget and stopped the run.
- The ingester closes the Storage Write client compatibly across client-library
  versions; older copies raised `BigQueryWriteClient has no attribute close`.
- Query jobs are explicitly reloaded after timing completes, so short queries
  retain `totalBytesBilled`; older copies emitted `bytes=None`.
- Drill-down Q2 returns p95/p99 as a nullable struct instead of an array that
  could contain a forbidden null element.
- Raw-count examples use `row_count`, not the reserved `ROWS` keyword.
- Project-wide system-table IAM and the conditional-policy
  `--condition=None` requirement are part of initialization rather than a
  post-run repair.
