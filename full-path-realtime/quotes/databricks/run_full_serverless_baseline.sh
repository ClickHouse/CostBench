#!/usr/bin/env bash
set -euo pipefail

# Canonical 113,219,565,734-row Serverless SQL baseline. Lakehouse//RT is not
# used. Run only against the fresh schema named below.

umask 077

DBX_BENCH_DIR="/home/ubuntu/costbench/full-path-realtime/quotes/databricks"
SOURCE_DIR="/data/quotes"
SOURCE_INVENTORY_OUTPUT="/data/databricks-qualification/source_inventory.json"
RUNNER_PYTHON="$DBX_BENCH_DIR/.venv-runner/bin/python"
ZEROBUS_PYTHON="$DBX_BENCH_DIR/.venv-zerobus/bin/python"
: "${DATABRICKS_HOST:?Set the Databricks workspace HTTPS origin}"
: "${DATABRICKS_WORKSPACE_ID:?Set the decimal Databricks workspace ID}"
: "${DATABRICKS_CLIENT_ID:?Set the benchmark service-principal application ID}"
: "${DATABRICKS_REGION:=eu-west-1}"
DATABRICKS_CATALOG="costbench"
DATABRICKS_SCHEMA="rt_full_serverless_baseline_r3_20260918"
DATABRICKS_RAW_TABLE="quotes"
DATABRICKS_MV_TABLE="quotes_daily"
: "${DATABRICKS_CONTROL_WAREHOUSE_ID:?Set the control warehouse ID}"
: "${DATABRICKS_QUERY_WAREHOUSE_ID:?Set the query warehouse ID}"
DATABRICKS_WAREHOUSE_MODE="serverless-baseline"
DATABRICKS_QUERY_SIZE="X-Small"
: "${ZEROBUS_ENDPOINT:=https://${DATABRICKS_WORKSPACE_ID}.zerobus.${DATABRICKS_REGION}.cloud.databricks.com}"
EXPECTED_ROWS=113219565734
TARGET_EPS=1000000
MANUAL_STREAM_ROTATION_SECONDS=600
FRESHNESS_INTERVAL_SECONDS=60
MAX_CATCHUP_SECONDS=900
POST_INGEST_QUERY_SECONDS=10800
RUN_ID="serverless_baseline_full_$(date -u +%Y%m%dT%H%M%SZ)"
RUN_STAMP="${RUN_ID#serverless_baseline_full_}"
RUN_ROOT="/data/databricks-runs/${RUN_ID}"
RUN_STATE="/data/databricks-state/${RUN_ID}"
RUN_CONTEXT="$RUN_ROOT/run_context.json"
LATEST_CONTEXT="/data/databricks-runs/latest_serverless_baseline.json"
SINCE_UTC="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
QUERY_OUTPUT_DIR="$RUN_ROOT"
QUERY_CONTEXT_PATH="$RUN_ROOT/validation/query_context.json"
DASHBOARD_OUTPUT="$RUN_ROOT/mv/dashboard_${RUN_STAMP}.jsonl"
DRILLDOWN_OUTPUT="$RUN_ROOT/raw/drilldown_${RUN_STAMP}.jsonl"
QUERY_LOG_DIR="$RUN_STATE/logs"
INGEST_STATE_PROGRESS="$RUN_STATE/ingest_progress.json"
INGEST_STATE_MANIFEST="$RUN_STATE/source_manifest.json"
INGEST_STATE_LEDGER="$RUN_STATE/ingest_events.jsonl"
INGEST_METRICS="$RUN_ROOT/ingest/ingest_metrics.jsonl"
FRESHNESS_OUTPUT="$RUN_ROOT/freshness/mv_freshness_${RUN_STAMP}.jsonl"
EVIDENCE_STATE_DIR="$RUN_STATE/evidence-initial"
QUERY_AUDIT_DIR="$RUN_STATE/query-evidence"
INGEST_PID=""
MONITOR_PID=""
QUERY_PID=""

cleanup() {
  unset DATABRICKS_CLIENT_SECRET
  for pid in "$INGEST_PID" "$MONITOR_PID" "$QUERY_PID"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
    fi
  done
  for pid in "$INGEST_PID" "$MONITOR_PID" "$QUERY_PID"; do
    if [[ -n "$pid" ]]; then
      wait "$pid" 2>/dev/null || true
    fi
  done
}

handle_interrupt() {
  trap - EXIT INT TERM
  unset DATABRICKS_CLIENT_SECRET
  echo "Interrupt received; waiting for child processes to checkpoint..." >&2
  for pid in "$INGEST_PID" "$MONITOR_PID" "$QUERY_PID"; do
    if [[ -n "$pid" ]]; then
      wait "$pid" 2>/dev/null || true
    fi
  done
  exit 130
}

handle_termination() {
  trap - EXIT INT TERM
  cleanup
  exit 143
}

trap cleanup EXIT
trap handle_interrupt INT
trap handle_termination TERM

TARGET="${DATABRICKS_CATALOG}.${DATABRICKS_SCHEMA}.${DATABRICKS_RAW_TABLE}"
EXPECTED_TARGET="costbench.rt_full_serverless_baseline_r3_20260918.quotes"
if [[ "$TARGET" != "$EXPECTED_TARGET" ]]; then
  printf 'Refusing unexpected target: %s\n' "$TARGET" >&2
  exit 1
fi
if [[ "$DATABRICKS_QUERY_WAREHOUSE_ID" == "$DATABRICKS_CONTROL_WAREHOUSE_ID" ]]; then
  echo "Query and control warehouses must be distinct." >&2
  exit 1
fi
if [[ -e "$RUN_ROOT" ]]; then
  printf 'Refusing existing output directory: %s\n' "$RUN_ROOT" >&2
  exit 1
fi
if [[ -e "$RUN_STATE" ]]; then
  printf 'Refusing existing runtime-state directory: %s\n' "$RUN_STATE" >&2
  exit 1
fi
for python in "$RUNNER_PYTHON" "$ZEROBUS_PYTHON"; do
  if [[ ! -x "$python" ]]; then
    printf 'Missing Python environment: %s\n' "$python" >&2
    exit 1
  fi
done
jq -e '
  .status == "complete"
  and .actual_rows == 113219565734
  and .file_count == 232
' "$SOURCE_INVENTORY_OUTPUT" >/dev/null

printf 'Canonical target: %s\n' "$TARGET"
printf 'Rows:             %d (entire source)\n' "$EXPECTED_ROWS"
printf 'Target EPS:       %d\n' "$TARGET_EPS"
printf 'Expected ingest:  approximately 31h27m\n'
printf 'Query warehouse:  %s (Serverless X-Small)\n' \
  "$DATABRICKS_QUERY_WAREHOUSE_ID"
printf 'Control warehouse:%s\n' "$DATABRICKS_CONTROL_WAREHOUSE_ID"
printf 'Dashboard:        4 queries every 600 seconds\n'
printf 'Drill-down:       2 queries every 3600 seconds\n'
printf 'Run root:         %s\n' "$RUN_ROOT"
printf '\nThis permanently consumes the fresh canonical target.\n'
read -r -p "Type '$EXPECTED_TARGET' to continue: " confirmation
if [[ "$confirmation" != "$EXPECTED_TARGET" ]]; then
  echo "Confirmation did not match; nothing was run." >&2
  exit 1
fi

read -rsp 'Benchmark service-principal secret: ' DATABRICKS_CLIENT_SECRET
echo
export DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET
export DATABRICKS_HOST DATABRICKS_CONTROL_WAREHOUSE_ID
export DATABRICKS_QUERY_WAREHOUSE_ID DATABRICKS_WAREHOUSE_MODE
export DATABRICKS_QUERY_SIZE
export POST_INGEST_QUERY_SECONDS
unset DATABRICKS_TOKEN

mkdir -p \
  "$RUN_ROOT/ingest" \
  "$RUN_ROOT/freshness" \
  "$RUN_ROOT/mv" \
  "$RUN_ROOT/raw" \
  "$RUN_ROOT/evidence" \
  "$RUN_ROOT/costs" \
  "$RUN_ROOT/validation" \
  "$RUN_ROOT/charts" \
  "$QUERY_LOG_DIR"
jq -n \
  --arg run_id "$RUN_ID" \
  --arg run_root "$RUN_ROOT" \
  --arg since "$SINCE_UTC" \
  --arg catalog "$DATABRICKS_CATALOG" \
  --arg schema "$DATABRICKS_SCHEMA" \
  --arg raw_table "$DATABRICKS_RAW_TABLE" \
  --arg mv_table "$DATABRICKS_MV_TABLE" \
  --arg query_warehouse_id "$DATABRICKS_QUERY_WAREHOUSE_ID" \
  --arg control_warehouse_id "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --argjson expected_rows "$EXPECTED_ROWS" \
  --argjson target_eps "$TARGET_EPS" \
  --argjson post_ingest_query_seconds "$POST_INGEST_QUERY_SECONDS" \
  '{
    schema_version: 1,
    status: "running",
    run_id: $run_id,
    run_root: $run_root,
    since: $since,
    catalog: $catalog,
    schema: $schema,
    raw_table: $raw_table,
    mv_table: $mv_table,
    query_warehouse_id: $query_warehouse_id,
    query_warehouse_mode: "serverless-baseline",
    query_size: "X-Small",
    control_warehouse_id: $control_warehouse_id,
    expected_rows: $expected_rows,
    target_eps: $target_eps,
    dashboard_interval_seconds: 600,
    drilldown_interval_seconds: 3600,
    post_ingest_query_seconds: $post_ingest_query_seconds,
    result_profile: "compact",
    ingest_metrics_interval_seconds: 60,
    freshness_interval_seconds: 60,
    automatic_stream_recovery: true,
    recovery_timeout_ms: 15000,
    recovery_backoff_ms: 2000,
    recovery_retries: 4,
    manual_stream_rotation_seconds: 600
  }' > "$RUN_CONTEXT"
cp "$RUN_CONTEXT" "$LATEST_CONTEXT"

echo "Running metadata-only Databricks preflight (no Zerobus streams yet)..."
if ! "$RUNNER_PYTHON" "$DBX_BENCH_DIR/preflight.py" \
  --online \
  --host "$DATABRICKS_HOST" \
  --cloud aws \
  --region eu-west-1 \
  --workspace-id "$DATABRICKS_WORKSPACE_ID" \
  --catalog "$DATABRICKS_CATALOG" \
  --schema "$DATABRICKS_SCHEMA" \
  --raw-table "$DATABRICKS_RAW_TABLE" \
  --mv-table "$DATABRICKS_MV_TABLE" \
  --rt-warehouse "$DATABRICKS_QUERY_WAREHOUSE_ID" \
  --query-warehouse-mode serverless-baseline \
  --control-warehouse "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --zerobus-endpoint "$ZEROBUS_ENDPOINT" \
  --runner-python "$RUNNER_PYTHON" \
  --zerobus-python "$ZEROBUS_PYTHON" \
  --output "$RUN_ROOT/validation/preflight.json" \
  > "$QUERY_LOG_DIR/preflight.stdout.json"; then
  jq '{
    summary,
    online_status: .online.status,
    errors: .online.errors,
    warnings: .online.warnings,
    target_contract: .online.target_contract
  }' "$RUN_ROOT/validation/preflight.json" >&2 || true
  jq '.status = "preflight_failed"' \
    "$RUN_CONTEXT" > "$RUN_CONTEXT.tmp"
  mv "$RUN_CONTEXT.tmp" "$RUN_CONTEXT"
  cp "$RUN_CONTEXT" "$LATEST_CONTEXT"
  echo "Canonical preflight failed; no Zerobus stream was opened." >&2
  exit 1
fi
echo "Preflight passed. Preparing the full source manifest..."

"$ZEROBUS_PYTHON" "$DBX_BENCH_DIR/ingest_zerobus.py" \
  --dir "$SOURCE_DIR" \
  --pattern 'quotes_*.parquet' \
  --host "$DATABRICKS_HOST" \
  --endpoint "$ZEROBUS_ENDPOINT" \
  --catalog "$DATABRICKS_CATALOG" \
  --schema "$DATABRICKS_SCHEMA" \
  --table "$DATABRICKS_RAW_TABLE" \
  --count-warehouse "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --workers 16 \
  --batch-size 50000 \
  --queue-capacity 64 \
  --target-eps "$TARGET_EPS" \
  --compression NONE \
  --checkpoint-batches 1 \
  --checkpoint-seconds 1 \
  --checkpoint-method wait \
  --recovery-timeout-ms 15000 \
  --recovery-backoff-ms 2000 \
  --recovery-retries 4 \
  --recovery-log-threshold-seconds 2 \
  --manual-stream-rotation-seconds "$MANUAL_STREAM_ROTATION_SECONDS" \
  --metrics-interval 5 \
  --metrics-output-interval 60 \
  --compact-metrics \
  --compact-evidence \
  --memory-trim-interval 60 \
  --min-system-available-gib 16 \
  --compact-progress \
  --defer-source-hashes \
  --expected-rows "$EXPECTED_ROWS" \
  --run-id "$RUN_ID" \
  --output-dir "$RUN_ROOT/ingest" \
  --manifest-output "$INGEST_STATE_MANIFEST" \
  --progress-file "$INGEST_STATE_PROGRESS" \
  --ledger-file "$INGEST_STATE_LEDGER" \
  --metrics-file "$INGEST_METRICS" \
  --summary-file "$RUN_ROOT/ingest/ingest_summary.json" \
  --quiet-worker-logs \
  > >(tee "$QUERY_LOG_DIR/ingest.log") 2>&1 &
INGEST_PID=$!

for ((attempt = 1; attempt <= 1800; attempt++)); do
  if [[ -s "$INGEST_STATE_PROGRESS" ]]; then
    break
  fi
  if ! kill -0 "$INGEST_PID" 2>/dev/null; then
    wait "$INGEST_PID"
    echo "Ingest exited before creating its progress journal." >&2
    exit 1
  fi
  sleep 1
done
if [[ ! -s "$INGEST_STATE_PROGRESS" ]]; then
  echo "Timed out preparing the canonical source manifest." >&2
  exit 1
fi

"$RUNNER_PYTHON" "$DBX_BENCH_DIR/monitor_freshness.py" \
  --host "$DATABRICKS_HOST" \
  --control-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --catalog "$DATABRICKS_CATALOG" \
  --schema "$DATABRICKS_SCHEMA" \
  --raw-table "$DATABRICKS_RAW_TABLE" \
  --mv-table "$DATABRICKS_MV_TABLE" \
  --producer-progress "$INGEST_STATE_PROGRESS" \
  --since "$SINCE_UTC" \
  --interval "$FRESHNESS_INTERVAL_SECONDS" \
  --iterations 0 \
  --compact-event-log \
  --compact-output \
  --output "$FRESHNESS_OUTPUT" \
  --progress-json "$RUN_ROOT/freshness/freshness_progress.json" \
  --run-id "$RUN_ID" \
  > >(tee "$QUERY_LOG_DIR/freshness.log") 2>&1 &
MONITOR_PID=$!

for ((attempt = 1; attempt <= 300; attempt++)); do
  if [[ -s "$RUN_ROOT/freshness/freshness_progress.json" ]]; then
    break
  fi
  if ! kill -0 "$MONITOR_PID" 2>/dev/null; then
    wait "$MONITOR_PID"
    echo "Freshness monitor exited before its first observation." >&2
    exit 1
  fi
  sleep 1
done
if [[ ! -s "$RUN_ROOT/freshness/freshness_progress.json" ]]; then
  echo "Timed out waiting for the first freshness observation." >&2
  exit 1
fi

export INGEST_CONTEXT="$RUN_CONTEXT"
export DATABRICKS_QUERY_RUN_ID="$RUN_ID"
export QUERY_OUTPUT_DIR QUERY_CONTEXT_PATH
export DASHBOARD_OUTPUT DRILLDOWN_OUTPUT QUERY_LOG_DIR
PRODUCER_PROGRESS="$INGEST_STATE_PROGRESS"
FRESHNESS_PROGRESS="$RUN_ROOT/freshness/freshness_progress.json"
export PRODUCER_PROGRESS FRESHNESS_PROGRESS
export QUERY_WINDOW_SCOPE="canonical-full"
"$DBX_BENCH_DIR/run_query_workloads.sh" \
  > >(tee "$QUERY_LOG_DIR/query_workloads.log") 2>&1 &
QUERY_PID=$!

while kill -0 "$INGEST_PID" 2>/dev/null; do
  if ! kill -0 "$QUERY_PID" 2>/dev/null; then
    set +e
    wait "$QUERY_PID"
    QUERY_STATUS=$?
    set -e
    QUERY_PID=""
    echo "Query workload exited while canonical ingestion was active." >&2
    if [[ "$QUERY_STATUS" -eq 0 ]]; then
      exit 1
    fi
    exit "$QUERY_STATUS"
  fi
  if ! kill -0 "$MONITOR_PID" 2>/dev/null; then
    set +e
    wait "$MONITOR_PID"
    MONITOR_STATUS=$?
    set -e
    MONITOR_PID=""
    echo "Freshness monitor exited while canonical ingestion was active." >&2
    if [[ "$MONITOR_STATUS" -eq 0 ]]; then
      exit 1
    fi
    exit "$MONITOR_STATUS"
  fi
  sleep 30
done

set +e
wait "$INGEST_PID"
INGEST_STATUS=$?
set -e
INGEST_PID=""

if [[ "$INGEST_STATUS" -ne 0 ]]; then
  cp "$INGEST_STATE_PROGRESS" "$RUN_ROOT/ingest/ingest_progress.json"
  FAILED_AT="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
  jq \
    --arg failed_at "$FAILED_AT" \
    --argjson ingest_status "$INGEST_STATUS" \
    --slurpfile ingest "$RUN_ROOT/ingest/ingest_summary.json" \
    '.status = "failed"
     | .publishable = false
     | .failed_at = $failed_at
     | .failure = {
         ingest_process_status: $ingest_status,
         elapsed_sec: $ingest[0].elapsed_sec,
         client_durable_rows: $ingest[0].provider_committed_rows,
         submitted_rows: $ingest[0].submitted_rows,
         pending_rows: $ingest[0].pending_rows,
         terminal_error: $ingest[0].terminal_error,
         ambiguous: $ingest[0].ambiguous,
         transport_recovery: $ingest[0].transport_recovery
       }' \
    "$RUN_CONTEXT" > "$RUN_CONTEXT.tmp"
  mv "$RUN_CONTEXT.tmp" "$RUN_CONTEXT"
  cp "$RUN_CONTEXT" "$LATEST_CONTEXT"
  echo "Canonical ingest failed; stopping query and freshness processes." >&2
  exit "$INGEST_STATUS"
fi
cp "$INGEST_STATE_PROGRESS" "$RUN_ROOT/ingest/ingest_progress.json"

set +e
wait "$QUERY_PID"
QUERY_STATUS=$?
set -e
QUERY_PID=""
if [[ "$QUERY_STATUS" -ne 0 ]]; then
  echo "Query workload failed validation." >&2
  exit "$QUERY_STATUS"
fi

PRODUCER_FINISHED_AT="$(
  jq -er '.updated_at' "$RUN_ROOT/ingest/ingest_summary.json"
)"
caught_up=false
for ((attempt = 1; attempt <= MAX_CATCHUP_SECONDS / 30; attempt++)); do
  if jq -e --arg finished "$PRODUCER_FINISHED_AT" '
    .mv_rows_behind == 0
    and (.errors | length) == 0
    and .observed_at >= $finished
  ' "$RUN_ROOT/freshness/freshness_progress.json" >/dev/null; then
    caught_up=true
    break
  fi
  sleep 30
done
if [[ "$caught_up" != true ]]; then
  echo "MV did not prove final catch-up within 15 minutes." >&2
  exit 1
fi

kill -TERM "$MONITOR_PID"
set +e
wait "$MONITOR_PID"
MONITOR_STATUS=$?
set -e
MONITOR_PID=""
if [[ "$MONITOR_STATUS" -ne 0 ]]; then
  echo "Freshness monitor failed." >&2
  exit "$MONITOR_STATUS"
fi

UNTIL_UTC="$(
  jq -er '.observed_at' "$RUN_ROOT/freshness/freshness_progress.json"
)"
jq \
  --arg until "$UNTIL_UTC" \
  --arg producer_finished_at "$PRODUCER_FINISHED_AT" \
  '.status = "measurement_complete"
   | .until = $until
   | .producer_finished_at = $producer_finished_at' \
  "$RUN_CONTEXT" > "$RUN_CONTEXT.tmp"
mv "$RUN_CONTEXT.tmp" "$RUN_CONTEXT"
cp "$RUN_CONTEXT" "$LATEST_CONTEXT"

"$RUNNER_PYTHON" "$DBX_BENCH_DIR/hash_source_manifest.py" \
  --manifest "$INGEST_STATE_MANIFEST" \
  --output "$RUN_ROOT/ingest/source_hashes_post_run.json"

"$RUNNER_PYTHON" "$DBX_BENCH_DIR/collect_evidence.py" \
  --host "$DATABRICKS_HOST" \
  --control-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --rt-warehouse-id "$DATABRICKS_QUERY_WAREHOUSE_ID" \
  --catalog "$DATABRICKS_CATALOG" \
  --schema "$DATABRICKS_SCHEMA" \
  --raw-table "$DATABRICKS_RAW_TABLE" \
  --mv-table "$DATABRICKS_MV_TABLE" \
  --since "$SINCE_UTC" \
  --until "$UNTIL_UTC" \
  --max-window-hours 36 \
  --history-limit 30000 \
  --statement-timeout 600 \
  --run-id "$RUN_ID" \
  --output-dir "$EVIDENCE_STATE_DIR"

"$RUNNER_PYTHON" "$DBX_BENCH_DIR/compact_evidence.py" \
  --evidence-dir "$EVIDENCE_STATE_DIR" \
  --preflight "$RUN_ROOT/validation/preflight.json" \
  --output-dir "$RUN_ROOT/evidence"

set +e
"$RUNNER_PYTHON" "$DBX_BENCH_DIR/costs/summarize_run.py" \
  --evidence-dir "$EVIDENCE_STATE_DIR" \
  --since "$SINCE_UTC" \
  --until "$UNTIL_UTC" \
  --producer-finished-at "$PRODUCER_FINISHED_AT" \
  --rt-warehouse-id "$DATABRICKS_QUERY_WAREHOUSE_ID" \
  --control-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --output "$RUN_ROOT/costs/cost_summary_initial.json"
COST_STATUS=$?
set -e

MIN_ROTATIONS=187
if ! jq -e --argjson expected "$EXPECTED_ROWS" --argjson rotations "$MIN_ROTATIONS" '
  .finished == true
  and .stopped_early == false
  and .provider_committed_rows == $expected
  and .submitted_rows == $expected
  and .pending_rows == 0
  and .pending_batches == 0
  and .terminal_error == null
  and .ambiguous == false
  and .clean_checkpoint == true
  and all(
    .stream_status[];
    .automatic_recovery == true
    and .rotation_count >= $rotations
    and .last_rotation_unacked_batches == 0
    and .close_succeeded == true
    and .unacked_inspection_succeeded == true
    and .unacked_batches == 0
  )
' "$RUN_ROOT/ingest/ingest_summary.json" >/dev/null; then
  echo "Canonical ingest gate failed." >&2
  exit 1
fi

if ! jq -s -e 'all(.[]; (.errors | length) == 0)' \
  "$FRESHNESS_OUTPUT" >/dev/null; then
  echo "Freshness evidence contains errors." >&2
  exit 1
fi

if ! jq -e '
  .complete == true
  and .provider_errors == 0
  and .zerobus_stream_error_count == 0
  and .mv_maintenance.full_refresh_count == 0
' "$EVIDENCE_STATE_DIR/evidence_summary.json" >/dev/null; then
  echo "Provider evidence completeness/error gate failed." >&2
  exit 1
fi

CLIENT_DURABLE_ROWS="$(
  jq -er '.provider_committed_rows' "$RUN_ROOT/ingest/ingest_summary.json"
)"
PROVIDER_COMMITTED_ROWS="$(
  jq -er '.provider_committed_records' \
    "$EVIDENCE_STATE_DIR/evidence_summary.json"
)"
if (( PROVIDER_COMMITTED_ROWS < CLIENT_DURABLE_ROWS )); then
  printf 'Provider committed rows (%s) are below client durable rows (%s).\n' \
    "$PROVIDER_COMMITTED_ROWS" "$CLIENT_DURABLE_ROWS" >&2
  exit 1
fi
DUPLICATE_ROWS=$((PROVIDER_COMMITTED_ROWS - CLIENT_DURABLE_ROWS))
if (( DUPLICATE_ROWS > 0 )); then
  printf 'WARNING: provider reports %d replay/duplicate surplus rows.\n' \
    "$DUPLICATE_ROWS" >&2
fi

if ! jq -e --argjson provider_rows "$PROVIDER_COMMITTED_ROWS" '
  .mv_source_rows == $provider_rows
  and .mv_rows_behind == 0
  and (.errors | length) == 0
' "$RUN_ROOT/freshness/freshness_progress.json" >/dev/null; then
  echo "Final MV watermark gate failed." >&2
  exit 1
fi

if ! jq -e '
  .dashboard_iterations >= 200
  and .drilldown_iterations >= 33
  and .dashboard_process_status == 0
  and .drilldown_process_status == 0
  and .partial_ingest_window == false
' "$QUERY_CONTEXT_PATH" >/dev/null; then
  echo "Canonical query-workload gate failed." >&2
  exit 1
fi

HASHED_SOURCE_FILE_COUNT="$(jq -er '.selected_file_count' "$INGEST_STATE_MANIFEST")"
if ! jq -e --argjson expected_files "$HASHED_SOURCE_FILE_COUNT" '
  .status == "complete"
  and .file_count == $expected_files
' "$RUN_ROOT/ingest/source_hashes_post_run.json" >/dev/null; then
  echo "Post-run source hash gate failed." >&2
  exit 1
fi

"$RUNNER_PYTHON" "$DBX_BENCH_DIR/validate_run.py" \
  --source-manifest "$INGEST_STATE_MANIFEST" \
  --ingest-progress "$RUN_ROOT/ingest/ingest_progress.json" \
  --ingest-metrics "$INGEST_METRICS" \
  --dashboard "$DASHBOARD_OUTPUT" \
  --drilldown "$DRILLDOWN_OUTPUT" \
  --freshness "$FRESHNESS_OUTPUT" \
  --evidence-summary "$EVIDENCE_STATE_DIR/evidence_summary.json" \
  --cost-summary "$RUN_ROOT/costs/cost_summary_initial.json" \
  --output "$RUN_ROOT/validation/validation_report_initial.json" \
  --expected-rows "$EXPECTED_ROWS" || VALIDATION_STATUS=$?
VALIDATION_STATUS="${VALIDATION_STATUS:-0}"

if ! jq -e --argjson duplicate_rows "$DUPLICATE_ROWS" '
  all(
    .gates[];
    .passed == true
    or .id == "cost_completeness"
    or ($duplicate_rows > 0 and .id == "row_reconciliation")
  )
' "$RUN_ROOT/validation/validation_report_initial.json" >/dev/null; then
  echo "Canonical non-cost validation gate failed." >&2
  exit 1
fi

jq \
  --argjson cost_status "$COST_STATUS" \
  --argjson validation_status "$VALIDATION_STATUS" \
  --argjson client_durable_rows "$CLIENT_DURABLE_ROWS" \
  --argjson provider_committed_rows "$PROVIDER_COMMITTED_ROWS" \
  --argjson duplicate_rows "$DUPLICATE_ROWS" \
  '.status = (
      if $duplicate_rows == 0
      then "complete"
      else "complete_with_duplicates"
      end
    )
   | .initial_cost_process_status = $cost_status
   | .initial_validation_process_status = $validation_status
   | .reconciliation = {
       client_durable_rows: $client_durable_rows,
       provider_committed_rows: $provider_committed_rows,
       duplicate_surplus_rows: $duplicate_rows,
       exact: ($duplicate_rows == 0)
     }' \
  "$RUN_CONTEXT" > "$RUN_CONTEXT.tmp"
mv "$RUN_CONTEXT.tmp" "$RUN_CONTEXT"
cp "$RUN_CONTEXT" "$LATEST_CONTEXT"

"$RUNNER_PYTHON" "$DBX_BENCH_DIR/compact_query_results.py" \
  --input "$DASHBOARD_OUTPUT" \
  --output "$DASHBOARD_OUTPUT" \
  --audit-output "$QUERY_AUDIT_DIR/$(basename "$DASHBOARD_OUTPUT")" \
  >/dev/null
"$RUNNER_PYTHON" "$DBX_BENCH_DIR/compact_query_results.py" \
  --input "$DRILLDOWN_OUTPUT" \
  --output "$DRILLDOWN_OUTPUT" \
  --audit-output "$QUERY_AUDIT_DIR/$(basename "$DRILLDOWN_OUTPUT")" \
  >/dev/null

"$RUNNER_PYTHON" "$DBX_BENCH_DIR/finalize_results.py" \
  --run-root "$RUN_ROOT" \
  --run-id "$RUN_ID" \
  --source-manifest "$INGEST_STATE_MANIFEST" \
  --source-hashes "$RUN_ROOT/ingest/source_hashes_post_run.json" \
  --status initial_cost_unsettled \
  >/dev/null

printf 'Canonical baseline completed: %s\n' "$RUN_ROOT"
printf 'Initial cost status: %d (billing may not be settled)\n' "$COST_STATUS"
printf 'Initial validation status: %d (cost may not be settled)\n' \
  "$VALIDATION_STATUS"
printf 'Provider replay/duplicate surplus rows: %d\n' "$DUPLICATE_ROWS"
printf 'Runtime diagnostics retained outside results: %s\n' "$RUN_STATE"
printf 'Stop query warehouse %s manually now.\n' "$DATABRICKS_QUERY_WAREHOUSE_ID"
