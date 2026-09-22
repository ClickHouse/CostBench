#!/usr/bin/env bash
set -euo pipefail

# Six-hour, 1M-EPS ingest + managed Delta maintenance + MV freshness
# characterization. Lakehouse//RT reads are intentionally absent.

umask 077

: "${DBX_BENCH_DIR:=/home/ubuntu/costbench/full-path-realtime/quotes/databricks}"
: "${SOURCE_DIR:=/data/quotes}"
: "${SOURCE_INVENTORY_OUTPUT:=/data/databricks-qualification/source_inventory.json}"
: "${DATABRICKS_RUNNER_PYTHON:=$DBX_BENCH_DIR/.venv-runner/bin/python}"
: "${DATABRICKS_ZEROBUS_PYTHON:=$DBX_BENCH_DIR/.venv-zerobus/bin/python}"
: "${DATABRICKS_HOST:?Set the Databricks workspace HTTPS origin}"
: "${DATABRICKS_WORKSPACE_ID:?Set the decimal Databricks workspace ID}"
: "${DATABRICKS_CLIENT_ID:?Set the benchmark service-principal application ID}"
: "${DATABRICKS_REGION:=eu-west-1}"
: "${DATABRICKS_CATALOG:=costbench}"
: "${DATABRICKS_SCHEMA:=rt_qualification}"
: "${DATABRICKS_RAW_TABLE:=quotes_ingest_mv_6h_20260916}"
: "${DATABRICKS_MV_TABLE:=quotes_daily_ingest_mv_6h_20260916}"
: "${DATABRICKS_CONTROL_WAREHOUSE_ID:?Set the control warehouse ID}"
: "${ZEROBUS_ENDPOINT:=https://${DATABRICKS_WORKSPACE_ID}.zerobus.${DATABRICKS_REGION}.cloud.databricks.com}"

TARGET="${DATABRICKS_CATALOG}.${DATABRICKS_SCHEMA}.${DATABRICKS_RAW_TABLE}"
: "${EXPECTED_TARGET:=costbench.rt_qualification.quotes_ingest_mv_6h_20260916}"
: "${INGEST_DURATION_SECONDS:=21600}"
: "${TARGET_EPS:=1000000}"
: "${NOMINAL_ROWS:=$((INGEST_DURATION_SECONDS * TARGET_EPS))}"
: "${SOURCE_SELECTION_ROWS:=22000000000}"
: "${POST_INGEST_MONITOR_SECONDS:=900}"
: "${MONITOR_INTERVAL_SECONDS:=60}"
: "${MANUAL_STREAM_ROTATION_SECONDS:=600}"
: "${RUN_KIND:=ingest_mv_6h}"
: "${LATEST_RUN_CONTEXT:=/data/databricks-qualification/latest_ingest_mv_6h_run.json}"
MONITOR_ITERATIONS=$(((
  INGEST_DURATION_SECONDS + POST_INGEST_MONITOR_SECONDS
) / MONITOR_INTERVAL_SECONDS + 1))
RUN_ID="${RUN_KIND}_$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ROOT="/data/databricks-qualification/${RUN_ID}"
SINCE_UTC="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
INGEST_PID=""
MONITOR_PID=""
TIMER_PID=""

cleanup() {
  unset DATABRICKS_CLIENT_SECRET
  if [[ -n "$INGEST_PID" ]] && kill -0 "$INGEST_PID" 2>/dev/null; then
    kill -TERM "$INGEST_PID" 2>/dev/null || true
  fi
  if [[ -n "$MONITOR_PID" ]] && kill -0 "$MONITOR_PID" 2>/dev/null; then
    kill -TERM "$MONITOR_PID" 2>/dev/null || true
  fi
  if [[ -n "$TIMER_PID" ]] && kill -0 "$TIMER_PID" 2>/dev/null; then
    kill -TERM "$TIMER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

if [[ "$TARGET" != "$EXPECTED_TARGET" ]]; then
  printf 'Refusing unexpected target: %s\n' "$TARGET" >&2
  exit 1
fi
if [[ -e "$RUN_ROOT" ]]; then
  printf 'Refusing existing output directory: %s\n' "$RUN_ROOT" >&2
  exit 1
fi
for python in "$DATABRICKS_RUNNER_PYTHON" "$DATABRICKS_ZEROBUS_PYTHON"; do
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

printf 'Raw target:    %s\n' "$TARGET"
printf 'MV target:     %s.%s.%s\n' \
  "$DATABRICKS_CATALOG" "$DATABRICKS_SCHEMA" "$DATABRICKS_MV_TABLE"
printf 'Nominal rows:  %d (actual result is duration-bounded)\n' "$NOMINAL_ROWS"
printf 'Target EPS:    %d\n' "$TARGET_EPS"
printf 'Ingest time:   %d seconds\n' "$INGEST_DURATION_SECONDS"
printf 'Monitor time:  %d seconds\n' \
  "$((INGEST_DURATION_SECONDS + POST_INGEST_MONITOR_SECONDS))"
printf 'Output:        %s\n' "$RUN_ROOT"
printf '\nThis permanently consumes the fresh Zerobus target name.\n'
read -r -p "Type '$EXPECTED_TARGET' to continue: " confirmation
if [[ "$confirmation" != "$EXPECTED_TARGET" ]]; then
  echo "Confirmation did not match; nothing was run." >&2
  exit 1
fi

read -rsp 'Benchmark service-principal secret: ' DATABRICKS_CLIENT_SECRET
echo
export DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET
unset DATABRICKS_TOKEN

mkdir -p "$RUN_ROOT/ingest" "$RUN_ROOT/freshness"
jq -n \
  --arg run_id "$RUN_ID" \
  --arg run_root "$RUN_ROOT" \
  --arg since "$SINCE_UTC" \
  --arg raw_table "$DATABRICKS_RAW_TABLE" \
  --arg mv_table "$DATABRICKS_MV_TABLE" \
  --argjson duration_seconds "$INGEST_DURATION_SECONDS" \
  --argjson nominal_rows "$NOMINAL_ROWS" \
  --argjson source_selection_rows "$SOURCE_SELECTION_ROWS" \
  --argjson target_eps "$TARGET_EPS" \
  --argjson manual_stream_rotation_seconds "$MANUAL_STREAM_ROTATION_SECONDS" \
  '{
    schema_version: 1,
    status: "running",
    run_id: $run_id,
    run_root: $run_root,
    since: $since,
    raw_table: $raw_table,
    mv_table: $mv_table,
    duration_seconds: $duration_seconds,
    nominal_rows: $nominal_rows,
    source_selection_rows: $source_selection_rows,
    target_eps: $target_eps,
    automatic_stream_recovery: false,
    manual_stream_rotation_seconds: $manual_stream_rotation_seconds
  }' > "$RUN_ROOT/run_context.json"
cp "$RUN_ROOT/run_context.json" \
  "$LATEST_RUN_CONTEXT"

"$DATABRICKS_ZEROBUS_PYTHON" "$DBX_BENCH_DIR/ingest_zerobus.py" \
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
  --disable-automatic-recovery \
  --manual-stream-rotation-seconds "$MANUAL_STREAM_ROTATION_SECONDS" \
  --metrics-interval 5 \
  --memory-trim-interval 60 \
  --min-system-available-gib 16 \
  --compact-progress \
  --defer-source-hashes \
  --max-rows "$SOURCE_SELECTION_ROWS" \
  --expected-rows "$SOURCE_SELECTION_ROWS" \
  --allow-partial \
  --run-id "$RUN_ID" \
  --output-dir "$RUN_ROOT/ingest" \
  --quiet-worker-logs \
  > >(tee "$RUN_ROOT/ingest.log") 2>&1 &
INGEST_PID=$!

for ((attempt = 1; attempt <= 1200; attempt++)); do
  if [[ -s "$RUN_ROOT/ingest/ingest_progress.json" ]]; then
    break
  fi
  if ! kill -0 "$INGEST_PID" 2>/dev/null; then
    wait "$INGEST_PID"
    echo "Ingest exited before creating its progress journal." >&2
    exit 1
  fi
  sleep 1
done
if [[ ! -s "$RUN_ROOT/ingest/ingest_progress.json" ]]; then
  echo "Timed out after 20 minutes waiting for source manifest preparation." >&2
  exit 1
fi

(
  sleep "$INGEST_DURATION_SECONDS"
  if kill -0 "$INGEST_PID" 2>/dev/null; then
    kill -TERM "$INGEST_PID"
  fi
) &
TIMER_PID=$!

"$DATABRICKS_RUNNER_PYTHON" "$DBX_BENCH_DIR/monitor_freshness.py" \
  --host "$DATABRICKS_HOST" \
  --control-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --catalog "$DATABRICKS_CATALOG" \
  --schema "$DATABRICKS_SCHEMA" \
  --raw-table "$DATABRICKS_RAW_TABLE" \
  --mv-table "$DATABRICKS_MV_TABLE" \
  --producer-progress "$RUN_ROOT/ingest/ingest_progress.json" \
  --since "$SINCE_UTC" \
  --interval "$MONITOR_INTERVAL_SECONDS" \
  --iterations "$MONITOR_ITERATIONS" \
  --compact-event-log \
  --output "$RUN_ROOT/freshness/freshness.jsonl" \
  --progress-json "$RUN_ROOT/freshness/freshness_progress.json" \
  --run-id "$RUN_ID" \
  > >(tee "$RUN_ROOT/freshness.log") 2>&1 &
MONITOR_PID=$!

set +e
wait "$INGEST_PID"
INGEST_STATUS=$?
if kill -0 "$TIMER_PID" 2>/dev/null; then
  kill -TERM "$TIMER_PID" 2>/dev/null
fi
wait "$TIMER_PID" 2>/dev/null
TIMER_PID=""
wait "$MONITOR_PID"
MONITOR_STATUS=$?
set -e
INGEST_PID=""
MONITOR_PID=""

UNTIL_UTC="$(
  jq -er '.observed_at' "$RUN_ROOT/freshness/freshness_progress.json"
)"
PRODUCER_FINISHED_AT="$(
  jq -er '.updated_at' "$RUN_ROOT/ingest/ingest_summary.json"
)"
jq \
  --arg until "$UNTIL_UTC" \
  --arg producer_finished_at "$PRODUCER_FINISHED_AT" \
  '.status = "complete"
   | .until = $until
   | .producer_finished_at = $producer_finished_at' \
  "$RUN_ROOT/run_context.json" > "$RUN_ROOT/run_context.json.tmp"
mv "$RUN_ROOT/run_context.json.tmp" "$RUN_ROOT/run_context.json"
cp "$RUN_ROOT/run_context.json" \
  "$LATEST_RUN_CONTEXT"

"$DATABRICKS_RUNNER_PYTHON" "$DBX_BENCH_DIR/hash_source_manifest.py" \
  --manifest "$RUN_ROOT/ingest/source_manifest.json" \
  --output "$RUN_ROOT/ingest/source_hashes_post_run.json"

set +e
"$DATABRICKS_RUNNER_PYTHON" "$DBX_BENCH_DIR/collect_evidence.py" \
  --host "$DATABRICKS_HOST" \
  --control-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --rt-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --catalog "$DATABRICKS_CATALOG" \
  --schema "$DATABRICKS_SCHEMA" \
  --raw-table "$DATABRICKS_RAW_TABLE" \
  --mv-table "$DATABRICKS_MV_TABLE" \
  --since "$SINCE_UTC" \
  --until "$UNTIL_UTC" \
  --max-window-hours 8 \
  --run-id "$RUN_ID" \
  --output-dir "$RUN_ROOT/evidence-initial"
EVIDENCE_STATUS=$?

"$DATABRICKS_RUNNER_PYTHON" \
  "$DBX_BENCH_DIR/costs/summarize_run.py" \
  --evidence-dir "$RUN_ROOT/evidence-initial" \
  --since "$SINCE_UTC" \
  --until "$UNTIL_UTC" \
  --producer-finished-at "$PRODUCER_FINISHED_AT" \
  --rt-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --control-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --output "$RUN_ROOT/cost_summary_initial.json"
COST_STATUS=$?
set -e

jq '{
  run_id,
  finished,
  terminal_error,
  provider_committed_rows,
  source_selection_rows: .source.total_rows,
  elapsed_sec,
  average_provider_committed_eps:
    .eps.average_provider_committed_rows_per_sec,
  clean_checkpoint,
  safe_to_resume,
  table_protection
}' "$RUN_ROOT/ingest/ingest_summary.json"

jq '{
  run_id,
  observed_at,
  latest_successful_refresh,
  mv_source_rows,
  mv_rows_behind,
  raw_commit_age_sec,
  mv_refresh_age_sec,
  errors
}' "$RUN_ROOT/freshness/freshness_progress.json"

printf 'Ingest status:           %d\n' "$INGEST_STATUS"
printf 'Monitor status:          %d\n' "$MONITOR_STATUS"
printf 'Initial evidence status: %d\n' "$EVIDENCE_STATUS"
printf 'Initial cost status:     %d (billing may not be settled)\n' "$COST_STATUS"
printf 'Run artifacts:           %s\n' "$RUN_ROOT"
printf 'Settled recollection:    %s/collect_ingest_mv_6h_settled.sh %s\n' \
  "$DBX_BENCH_DIR" "$RUN_ROOT"

if [[ "$INGEST_STATUS" -ne 0 && "$INGEST_STATUS" -ne 130 ]]; then
  exit 1
fi
if [[ "$MONITOR_STATUS" -ne 0 ]]; then
  exit 1
fi
MIN_ELAPSED=$((INGEST_DURATION_SECONDS - 10))
MAX_ELAPSED=$((INGEST_DURATION_SECONDS + 60))
MIN_ROWS=$((NOMINAL_ROWS * 90 / 100))
MAX_ROWS=$((NOMINAL_ROWS * 110 / 100))
MIN_ROTATIONS=$((INGEST_DURATION_SECONDS / MANUAL_STREAM_ROTATION_SECONDS - 1))
if ! jq -e \
  --argjson min_elapsed "$MIN_ELAPSED" \
  --argjson max_elapsed "$MAX_ELAPSED" \
  --argjson min_rows "$MIN_ROWS" \
  --argjson max_rows "$MAX_ROWS" \
  --argjson min_rotations "$MIN_ROTATIONS" '
  .stopped_early == true
  and .clean_checkpoint == true
  and .safe_to_resume == true
  and .elapsed_sec >= $min_elapsed
  and .elapsed_sec <= $max_elapsed
  and .eps.average_provider_committed_rows_per_sec >= 900000
  and .eps.average_provider_committed_rows_per_sec <= 1100000
  and .provider_committed_rows >= $min_rows
  and .provider_committed_rows <= $max_rows
  and .submitted_rows == .provider_committed_rows
  and .pending_rows == 0
  and all(
    .stream_status[];
    .automatic_recovery == false
    and .rotation_count >= $min_rotations
    and .last_rotation_unacked_batches == 0
  )
  and .terminal_error == null
  and .ambiguous == false
' "$RUN_ROOT/ingest/ingest_summary.json" >/dev/null; then
  echo "Ingest completion gate failed." >&2
  exit 1
fi
if ! jq -e '
  .mv_rows_behind == 0
  and (.errors | length) == 0
' "$RUN_ROOT/freshness/freshness_progress.json" >/dev/null; then
  echo "Final MV catch-up was not proven within the monitor window." >&2
  exit 1
fi
if ! jq -s 'all(.[]; (.errors | length) == 0)' \
  "$RUN_ROOT/freshness/freshness.jsonl" >/dev/null; then
  echo "One or more live freshness observations contain errors." >&2
  exit 1
fi
if [[ "$EVIDENCE_STATUS" -eq 0 ]]; then
  ACTUAL_ROWS="$(jq -er '.provider_committed_rows' \
    "$RUN_ROOT/ingest/ingest_summary.json")"
  if ! jq -e --argjson actual_rows "$ACTUAL_ROWS" '
    .provider_committed_records == $actual_rows
    and .provider_errors == 0
    and .mv_maintenance.full_refresh_count == 0
  ' "$RUN_ROOT/evidence-initial/evidence_summary.json" >/dev/null; then
    echo "Initial provider evidence reconciliation failed." >&2
    exit 1
  fi
fi
