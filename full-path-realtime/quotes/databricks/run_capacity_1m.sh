#!/usr/bin/env bash
set -euo pipefail

# Bounded preliminary capacity trial for the prepared eu-west-1 qualification
# target. This is not a full qualification result.

umask 077

: "${DBX_BENCH_DIR:=/home/ubuntu/costbench/full-path-realtime/quotes/databricks}"
: "${SOURCE_DIR:=/data/quotes}"
: "${SOURCE_INVENTORY_OUTPUT:=/data/databricks-qualification/source_inventory.json}"
: "${DATABRICKS_ZEROBUS_PYTHON:=$DBX_BENCH_DIR/.venv-zerobus/bin/python}"
: "${DATABRICKS_HOST:?Set the Databricks workspace HTTPS origin}"
: "${DATABRICKS_WORKSPACE_ID:?Set the decimal Databricks workspace ID}"
: "${DATABRICKS_CLIENT_ID:?Set the benchmark service-principal application ID}"
: "${DATABRICKS_REGION:=eu-west-1}"
: "${DATABRICKS_CATALOG:=costbench}"
: "${DATABRICKS_SCHEMA:=rt_qualification}"
: "${DATABRICKS_RAW_TABLE:=quotes}"
: "${DATABRICKS_MV_TABLE:=quotes_daily}"
: "${DATABRICKS_CONTROL_WAREHOUSE_ID:?Set the control warehouse ID}"
: "${ZEROBUS_ENDPOINT:=https://${DATABRICKS_WORKSPACE_ID}.zerobus.${DATABRICKS_REGION}.cloud.databricks.com}"

TARGET="${DATABRICKS_CATALOG}.${DATABRICKS_SCHEMA}.${DATABRICKS_RAW_TABLE}"
EXPECTED_TARGET="costbench.rt_qualification.quotes"
EXPECTED_ROWS=75000000
TARGET_EPS=1000000
RUN_ID="capacity_1m_$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ROOT="/data/databricks-qualification/${RUN_ID}"

if [[ "$TARGET" != "$EXPECTED_TARGET" ]]; then
  printf 'Refusing unexpected target: %s\n' "$TARGET" >&2
  exit 1
fi
if [[ -e "$RUN_ROOT" ]]; then
  printf 'Refusing existing output directory: %s\n' "$RUN_ROOT" >&2
  exit 1
fi
if [[ ! -x "$DATABRICKS_ZEROBUS_PYTHON" ]]; then
  printf 'Missing Zerobus Python: %s\n' "$DATABRICKS_ZEROBUS_PYTHON" >&2
  exit 1
fi
if [[ ! -s "$SOURCE_INVENTORY_OUTPUT" ]]; then
  printf 'Missing source inventory: %s\n' "$SOURCE_INVENTORY_OUTPUT" >&2
  exit 1
fi

jq -e '
  .status == "complete"
  and .actual_rows == 113219565734
  and .file_count == 232
' "$SOURCE_INVENTORY_OUTPUT" >/dev/null

printf 'Target:       %s\n' "$TARGET"
printf 'Rows:         %d\n' "$EXPECTED_ROWS"
printf 'Target EPS:   %d\n' "$TARGET_EPS"
printf 'Workers:      16\n'
printf 'Output:       %s\n' "$RUN_ROOT"
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
trap 'unset DATABRICKS_CLIENT_SECRET' EXIT

mkdir -p "$RUN_ROOT"

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
  --checkpoint-batches 32 \
  --checkpoint-seconds 5 \
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
  2>&1 | tee "$RUN_ROOT/ingest.log"

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

printf 'Capacity trial artifacts: %s\n' "$RUN_ROOT"
