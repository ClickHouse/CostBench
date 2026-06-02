#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------
# Parallel TSV ingester for ClickHouse
#
# Behavior:
#   - Ensures target database exists
#   - Ensures target table DATABASE.hits exists
#     (creates it from CREATE_SQL if missing)
#   - Spawns N parallel workers
#   - Distributes FULL_INGESTS across the workers
#   - Each worker repeatedly runs:
#
#       INSERT INTO hits
#       FORMAT TSV
#
#     with the TSV file streamed via stdin
#
# Env:
#   export FQDN="your-service-fqdn.clickhouse.cloud"
#   export PASSWORD="your_password"
#
# Usage:
#   ./parallel_file_ingest.sh \
#       --file hits.tsv \
#       --database hits_100b \
#       --parallel 20 \
#       --full-ingests 1000 \
#       [--create-sql create.sql]
#
# Notes:
#   - Table name is fixed to: hits
#   - CREATE_SQL must create table `hits`
#   - With async_insert=1 and wait_for_async_insert=0, the script measures
#     client dispatch time, not guaranteed server-side completion time
# ------------------------------------------------------------

DATABASE="default"
TABLE="hits"
FILE=""
PARALLEL="1"
FULL_INGESTS=""
CREATE_SQL="create.sql"

START_TS="$(date -u +"%Y-%m-%d %H:%M:%S UTC")"
START_EPOCH="$(date +%s)"

# --- Parse CLI flags ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --file)
      FILE="$2"
      shift 2
      ;;
    --database)
      DATABASE="$2"
      shift 2
      ;;
    --parallel)
      PARALLEL="$2"
      shift 2
      ;;
    --full-ingests)
      FULL_INGESTS="$2"
      shift 2
      ;;
    --create-sql)
      CREATE_SQL="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$FILE" ]]; then
  echo "ERROR: --file is required" >&2
  exit 1
fi

if [[ -z "$FULL_INGESTS" ]]; then
  echo "ERROR: --full-ingests is required" >&2
  exit 1
fi

: "${FQDN:?ERROR: please export FQDN}"
: "${PASSWORD:?ERROR: please export PASSWORD}"

if [[ ! -f "$CREATE_SQL" ]]; then
  echo "ERROR: create SQL file '$CREATE_SQL' does not exist." >&2
  exit 1
fi

if [[ ! -f "$FILE" ]]; then
  echo "ERROR: input file '$FILE' does not exist." >&2
  exit 1
fi

if ! [[ "$PARALLEL" =~ ^[1-9][0-9]*$ ]]; then
  echo "ERROR: --parallel must be a positive integer" >&2
  exit 1
fi

if ! [[ "$FULL_INGESTS" =~ ^[0-9]+$ ]]; then
  echo "ERROR: --full-ingests must be a non-negative integer" >&2
  exit 1
fi

if (( FULL_INGESTS == 0 )); then
  echo "Nothing to do: --full-ingests = 0"
  exit 0
fi

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "ERROR: need '$1' in PATH" >&2
    exit 1
  }
}

need clickhouse-client

# --- ClickHouse client wrappers ---
cli() {
  clickhouse-client \
    --host "$FQDN" \
    --secure \
    --password "$PASSWORD" \
    --database "$DATABASE" \
    "$@"
}

admin_cli() {
  clickhouse-client \
    --host "$FQDN" \
    --secure \
    --password "$PASSWORD" \
    "$@"
}

# --- Ensure target database exists ---
admin_cli --query "CREATE DATABASE IF NOT EXISTS \`$DATABASE\`"

# --- Helpers ---
target_table_exists() {
  cli --query "EXISTS TABLE $TABLE" --format=TSV | tr -d '[:space:]'
}

target_row_count() {
  cli --query "SELECT toUInt64(count()) FROM $TABLE" --format=TSV | tr -d '[:space:]'
}

# --- Ensure target table exists ---
if [[ "$(target_table_exists)" != "1" ]]; then
  echo "Table $DATABASE.$TABLE does not exist — creating from $CREATE_SQL ..."
  cli < "$CREATE_SQL"
fi

echo "File:          $FILE"
echo "Target DB:     $DATABASE"
echo "Target table:  $TABLE"
echo "Parallel:      $PARALLEL"
echo "Full ingests:  $FULL_INGESTS"
echo "Create SQL:    $CREATE_SQL"
echo "Start time:    $START_TS"

current="$(target_row_count)"
echo "Starting rows in $DATABASE.$TABLE: $current"

# --- Work distribution ---
BASE=$(( FULL_INGESTS / PARALLEL ))
REMAINDER=$(( FULL_INGESTS % PARALLEL ))

echo "Distribution:  $BASE per worker, first $REMAINDER workers get +1"
echo

pids=()

cleanup() {
  local rc=$?
  echo "Stopping all workers..." >&2
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait || true
  exit "$rc"
}

trap cleanup SIGINT SIGTERM

run_worker() {
  local worker_id="$1"
  local assigned="$2"
  local i
  local w_start_epoch
  local w_end_epoch
  local duration

  w_start_epoch="$(date +%s)"

  if (( assigned == 0 )); then
    echo "[worker $worker_id] no work assigned"
    return 0
  fi

  echo "[worker $worker_id] starting with $assigned full ingests"

  for (( i=1; i<=assigned; i++ )); do
    echo "[worker $worker_id] ingest $i/$assigned"

    cli \
      --time \
      --async_insert 1 \
      --wait_for_async_insert 0 \
      --async_insert_deduplicate 0 \
      --async_insert_use_adaptive_busy_timeout 0 \
      --async_insert_busy_timeout_max_ms 60000 \
      --async_insert_max_data_size 2147483648 \
      --max_insert_block_size 1000000 \
      --query "INSERT INTO hits SETTINGS insert_deduplicate=0 FORMAT TSV" < "$FILE"
  done

  w_end_epoch="$(date +%s)"
  duration=$(( w_end_epoch - w_start_epoch ))

  echo "[worker $worker_id] done in ${duration}s"
}

for (( worker=1; worker<=PARALLEL; worker++ )); do
  assigned="$BASE"
  if (( worker <= REMAINDER )); then
    assigned=$(( assigned + 1 ))
  fi

  run_worker "$worker" "$assigned" &
  pids+=("$!")
done

for pid in "${pids[@]}"; do
  wait "$pid"
done

trap - EXIT SIGINT SIGTERM

END_TS="$(date -u +"%Y-%m-%d %H:%M:%S UTC")"
END_EPOCH="$(date +%s)"
TOTAL_DURATION=$(( END_EPOCH - START_EPOCH ))

final_rows="$(target_row_count)"

echo
echo "==================== SUMMARY ===================="
echo "Start time:   $START_TS"
echo "End time:     $END_TS"
echo "Duration:     ${TOTAL_DURATION}s (~$((TOTAL_DURATION / 60)) min)"
echo "Final rows:   $final_rows"
echo "================================================="