#!/usr/bin/env bash
set -euo pipefail

# Ten-minute preliminary 1M-EPS ingest and MV-freshness trial. The target
# objects must be created fresh before this script is launched.

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
: "${DATABRICKS_RAW_TABLE:=quotes_ingest_mv_10m_20260915}"
: "${DATABRICKS_MV_TABLE:=quotes_daily_ingest_mv_10m_20260915}"
: "${DATABRICKS_CONTROL_WAREHOUSE_ID:?Set the control warehouse ID}"
: "${ZEROBUS_ENDPOINT:=https://${DATABRICKS_WORKSPACE_ID}.zerobus.${DATABRICKS_REGION}.cloud.databricks.com}"

TARGET="${DATABRICKS_CATALOG}.${DATABRICKS_SCHEMA}.${DATABRICKS_RAW_TABLE}"
EXPECTED_TARGET="costbench.rt_qualification.quotes_ingest_mv_10m_20260915"
EXPECTED_ROWS=600000000
TARGET_EPS=1000000
RUN_ID="ingest_mv_10m_$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ROOT="/data/databricks-qualification/${RUN_ID}"
SINCE_UTC="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
INGEST_PID=""
MONITOR_PID=""

cleanup() {
  unset DATABRICKS_CLIENT_SECRET
  if [[ -n "$INGEST_PID" ]] && kill -0 "$INGEST_PID" 2>/dev/null; then
    kill -TERM "$INGEST_PID" 2>/dev/null || true
  fi
  if [[ -n "$MONITOR_PID" ]] && kill -0 "$MONITOR_PID" 2>/dev/null; then
    kill -TERM "$MONITOR_PID" 2>/dev/null || true
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
printf 'Rows:          %d\n' "$EXPECTED_ROWS"
printf 'Target EPS:    %d\n' "$TARGET_EPS"
printf 'Duration:      10 minutes at target rate\n'
printf 'Output:        %s\n' "$RUN_ROOT"
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
  --metrics-interval 5 \
  --memory-trim-interval 60 \
  --min-system-available-gib 16 \
  --max-rows "$EXPECTED_ROWS" \
  --expected-rows "$EXPECTED_ROWS" \
  --allow-partial \
  --run-id "$RUN_ID" \
  --output-dir "$RUN_ROOT/ingest" \
  --quiet-worker-logs \
  > >(tee "$RUN_ROOT/ingest.log") 2>&1 &
INGEST_PID=$!

for ((attempt = 1; attempt <= 120; attempt++)); do
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
  echo "Timed out waiting for the ingest progress journal." >&2
  exit 1
fi

"$DATABRICKS_RUNNER_PYTHON" "$DBX_BENCH_DIR/monitor_freshness.py" \
  --host "$DATABRICKS_HOST" \
  --control-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --catalog "$DATABRICKS_CATALOG" \
  --schema "$DATABRICKS_SCHEMA" \
  --raw-table "$DATABRICKS_RAW_TABLE" \
  --mv-table "$DATABRICKS_MV_TABLE" \
  --producer-progress "$RUN_ROOT/ingest/ingest_progress.json" \
  --since "$SINCE_UTC" \
  --interval 30 \
  --iterations 24 \
  --output "$RUN_ROOT/freshness/freshness.jsonl" \
  --progress-json "$RUN_ROOT/freshness/freshness_progress.json" \
  --run-id "$RUN_ID" \
  > >(tee "$RUN_ROOT/freshness.log") 2>&1 &
MONITOR_PID=$!

set +e
wait "$INGEST_PID"
INGEST_STATUS=$?
wait "$MONITOR_PID"
MONITOR_STATUS=$?
set -e
INGEST_PID=""
MONITOR_PID=""

jq '{
  run_id,
  finished,
  terminal_error,
  provider_committed_rows,
  expected_rows: .source.total_rows,
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
  raw_commit_age_sec,
  mv_refresh_age_sec,
  errors
}' "$RUN_ROOT/freshness/freshness_progress.json"

printf 'Ingest status:  %d\n' "$INGEST_STATUS"
printf 'Monitor status: %d\n' "$MONITOR_STATUS"
printf 'Trial artifacts: %s\n' "$RUN_ROOT"

if [[ "$INGEST_STATUS" -ne 0 || "$MONITOR_STATUS" -ne 0 ]]; then
  exit 1
fi
