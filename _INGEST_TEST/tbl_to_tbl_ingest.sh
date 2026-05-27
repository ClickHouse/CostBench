#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------
# One-shot INSERT INTO ... SELECT ... runner for ClickHouse
#
# Behavior:
#   - Ensures target database exists
#   - Ensures target table TARGET_DB.hits exists
#     (creates it from CREATE_SQL if missing)
#   - Runs one full:
#
#       INSERT INTO TARGET_DB.hits
#       SELECT * FROM SOURCE_DB.hits
#
# Env:
#   export FQDN="your-service-fqdn.clickhouse.cloud"
#   export PASSWORD="your_password"
#
# Usage:
#   ./tbl_to_tbl_ingest.sh \
#       --source-db hits_100b_8c \
#       --target-db test1 \
#       --max-insert-threads 1 \
#       --min-insert-block-size-rows 10000000 \
#       [--create-sql create.sql]
# ------------------------------------------------------------

SOURCE_DB=""
TARGET_DB=""
TABLE="hits"
CREATE_SQL="create.sql"

MAX_INSERT_THREADS="1"
MIN_INSERT_BLOCK_SIZE_ROWS="10000000"

START_TS="$(date -u +"%Y-%m-%d %H:%M:%S UTC")"
START_EPOCH="$(date +%s)"

# --- Parse CLI flags ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --source-db)
      SOURCE_DB="$2"
      shift 2
      ;;
    --target-db)
      TARGET_DB="$2"
      shift 2
      ;;
    --create-sql)
      CREATE_SQL="$2"
      shift 2
      ;;
    --max-insert-threads)
      MAX_INSERT_THREADS="$2"
      shift 2
      ;;
    --min-insert-block-size-rows)
      MIN_INSERT_BLOCK_SIZE_ROWS="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$SOURCE_DB" ]]; then
  echo "ERROR: --source-db is required" >&2
  exit 1
fi

if [[ -z "$TARGET_DB" ]]; then
  echo "ERROR: --target-db is required" >&2
  exit 1
fi

: "${FQDN:?ERROR: please export FQDN}"
: "${PASSWORD:?ERROR: please export PASSWORD}"

if [[ ! -f "$CREATE_SQL" ]]; then
  echo "ERROR: create SQL file '$CREATE_SQL' does not exist." >&2
  exit 1
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
    --database "$TARGET_DB" \
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
admin_cli --query "CREATE DATABASE IF NOT EXISTS \`$TARGET_DB\`"

# --- Ensure target table exists ---
TABLE_EXISTS=$(cli --query "EXISTS TABLE $TABLE" --format=TSV | tr -d '[:space:]')

if [[ "$TABLE_EXISTS" != "1" ]]; then
  echo "Table $TARGET_DB.$TABLE does not exist — creating from $CREATE_SQL ..."
  cli < "$CREATE_SQL"
fi

echo "Source DB:     $SOURCE_DB"
echo "Target DB:     $TARGET_DB"
echo "Table:         $TABLE"
echo "Max threads:   $MAX_INSERT_THREADS"
echo "Block size:    $MIN_INSERT_BLOCK_SIZE_ROWS rows"
echo "Start time:    $START_TS"
echo

# --- Run INSERT SELECT ---
cli --time --query "
INSERT INTO $TABLE
SELECT *
FROM $SOURCE_DB.$TABLE
SETTINGS
    max_insert_threads = $MAX_INSERT_THREADS,
    min_insert_block_size_rows = $MIN_INSERT_BLOCK_SIZE_ROWS,
    min_insert_block_size_bytes = 0,
    parallel_distributed_insert_select = 2;
"

END_TS="$(date -u +"%Y-%m-%d %H:%M:%S UTC")"
END_EPOCH="$(date +%s)"
TOTAL_DURATION=$(( END_EPOCH - START_EPOCH ))

echo
echo "==================== SUMMARY ===================="
echo "Start time:   $START_TS"
echo "End time:     $END_TS"
echo "Duration:     ${TOTAL_DURATION}s (~$((TOTAL_DURATION / 60)) min)"
echo "================================================="