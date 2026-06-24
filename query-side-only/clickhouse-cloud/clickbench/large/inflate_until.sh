#!/usr/bin/env bash
set -euo pipefail
#
# Exponentially inflate a ClickHouse table by inserting from itself:
#   - First: seed target DB by copying from SEED_DB.hits into DATABASE.hits
#   - Then:  while rows*2 <= TARGET:  INSERT INTO hits SELECT * FROM hits;
#            final top-off:          INSERT INTO hits SELECT * FROM hits LIMIT remaining;
#
# Env:
#   export FQDN="your-service-fqdn.clickhouse.cloud"
#   export PASSWORD="your_password"
#
# Usage:
#   ./inflate_until.sh --seed-db SEED [--database DB] [--target ROWS] [--create-sql FILE]
#
# Examples:
#   ./inflate_until.sh --seed-db seed
#   ./inflate_until.sh --seed-db seed --database work --target 1000000000000
#   ./inflate_until.sh --seed-db seed --database work --create-sql schema.sql
#
# Defaults:
#   database     = default
#   target_rows  = 1,000,000,000
#   create_sql   = create.sql
#
# Notes:
#   - SEED_DB.hits must already exist and be non-empty.
#   - DATABASE.hits will be created from create SQL file if missing.
# ------------------------------------------------------------

DATABASE="default"
SEED_DB=""
TARGET="1000000000"
TABLE="hits"
CREATE_SQL="create.sql"

# --- Parse CLI flags ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --database)
      DATABASE="$2"
      shift 2
      ;;
    --seed-db)
      SEED_DB="$2"
      shift 2
      ;;
    --target)
      TARGET="$2"
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

if [[ -z "$SEED_DB" ]]; then
  echo "ERROR: --seed-db is required" >&2
  exit 1
fi

: "${FQDN:?ERROR: please export FQDN}"
: "${PASSWORD:?ERROR: please export PASSWORD}"

if [[ ! -f "$CREATE_SQL" ]]; then
  echo "ERROR: create SQL file '$CREATE_SQL' does not exist." >&2
  exit 1
fi

need() { command -v "$1" >/dev/null 2>&1 || { echo "ERROR: need '$1' in PATH" >&2; exit 1; }; }
need clickhouse-client

# Target-db client wrapper
cli() {
  clickhouse-client \
    --host "$FQDN" \
    --secure \
    --password "$PASSWORD" \
    --database "$DATABASE" \
    "$@"
}

# Seed-db client wrapper
seed_cli() {
  clickhouse-client \
    --host "$FQDN" \
    --secure \
    --password "$PASSWORD" \
    --database "$SEED_DB" \
    "$@"
}

# --- Ensure target database exists ---
clickhouse-client \
  --host "$FQDN" \
  --secure \
  --password "$PASSWORD" \
  --query "CREATE DATABASE IF NOT EXISTS \`$DATABASE\`"

# --- Helpers (target) ---
target_table_exists() {
  cli --query "EXISTS TABLE $TABLE" --format=TSV | tr -d '[:space:]'
}

target_row_count() {
  cli --query "SELECT toUInt64(count()) FROM $TABLE" --format=TSV | tr -d '[:space:]'
}

# --- Helpers (seed) ---
seed_table_exists() {
  seed_cli --query "EXISTS TABLE $TABLE" --format=TSV | tr -d '[:space:]'
}

seed_row_count() {
  seed_cli --query "SELECT toUInt64(count()) FROM $TABLE" --format=TSV | tr -d '[:space:]'
}

# --- Validate seed table exists and is non-empty ---
if [[ "$(seed_table_exists)" != "1" ]]; then
  echo "ERROR: Seed table $SEED_DB.$TABLE does not exist. Create/seed it first." >&2
  exit 1
fi

seed_rows="$(seed_row_count)"
if ! [[ "$seed_rows" =~ ^[0-9]+$ ]] || (( seed_rows == 0 )); then
  echo "ERROR: Seed table $SEED_DB.$TABLE is empty. Seed it with at least 1 row, then rerun." >&2
  exit 1
fi

# --- Ensure target table exists (create from CREATE_SQL if needed) ---
if [[ "$(target_table_exists)" != "1" ]]; then
  echo "Table $DATABASE.$TABLE does not exist — creating from $CREATE_SQL ..."
  cli < "$CREATE_SQL"
fi

echo "Seed DB:     $SEED_DB"
echo "Target DB:   $DATABASE"
echo "Target rows: $TARGET"
echo "Create SQL:  $CREATE_SQL"

current="$(target_row_count)"
echo "Starting rows in $DATABASE.$TABLE: $current"

# --- Initial seeding copy (only if target empty) ---
if (( current == 0 )); then
  echo "Target table is empty — seeding from $SEED_DB.$TABLE → $DATABASE.$TABLE ..."
  cli --time --query "
    INSERT INTO $TABLE
    SELECT *
    FROM \`$SEED_DB\`.$TABLE
    LIMIT $TARGET
    SETTINGS max_insert_threads=10, min_insert_block_size_rows = 10_000_000, min_insert_block_size_bytes = 0, parallel_distributed_insert_select = 2
  "
  current="$(target_row_count)"
  echo "Rows after seed copy: $current"
fi

if (( current == 0 )); then
  echo "ERROR: After seed copy, $DATABASE.$TABLE is still empty. Aborting." >&2
  exit 1
fi

if (( current >= TARGET )); then
  echo "✅ Target reached (>= $TARGET) after initial seed copy. Final row count: $current"
  exit 0
fi

# 1) Doubling phase
dbl_iter=0
while (( current * 2 <= TARGET )); do
  dbl_iter=$((dbl_iter+1))
  echo "===== Doubling #$dbl_iter: ${current} → $((current*2)) (target ${TARGET}) ====="
  cli --time --query "
    INSERT INTO $TABLE
    SELECT * FROM $TABLE
    SETTINGS max_insert_threads=10, min_insert_block_size_rows = 10_000_000, min_insert_block_size_bytes = 0, parallel_distributed_insert_select = 2
  "
  current="$(target_row_count)"
  echo "Rows now: $current"
done

# 2) Final top-off
if (( current < TARGET )); then
  remaining=$(( TARGET - current ))
  echo "===== Final top-off: inserting exactly ${remaining} rows into ${DATABASE}.${TABLE} ====="
  cli --time --query "
    INSERT INTO $TABLE
    SELECT * FROM $TABLE
    LIMIT $remaining
    SETTINGS max_insert_threads=10, min_insert_block_size_rows = 10_000_000, min_insert_block_size_bytes = 0, parallel_distributed_insert_select = 2
  "
  current="$(target_row_count)"
fi

echo "✅ Target reached (>= $TARGET). Final row count: $current"