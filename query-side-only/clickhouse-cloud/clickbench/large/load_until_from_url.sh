#!/usr/bin/env bash
set -euo pipefail
#
# Repeatedly load hits_{0..99}.parquet into ClickHouse Cloud
# until TARGET rows are reached. If DATABASE.hits does not exist,
# it is created from the provided create SQL file.
#
# Environment variables required:
#   export FQDN="your-service-fqdn.clickhouse.cloud"
#   export PASSWORD="your_password"
#
# Usage:
#   ./load_until_from_url.sh [--database DB] [--target ROWS] [--create-sql FILE]
#
# Examples:
#   ./load_until_from_url.sh
#   ./load_until_from_url.sh --database mydb
#   ./load_until_from_url.sh --database mydb --target 2000000000
#   ./load_until_from_url.sh --database mydb --create-sql schema.sql
#
# Defaults:
#   database     = default
#   target_rows  = 1,000,000,000
#   create_sql   = create.sql
# -------------------------------------------------------------

DATABASE="default"
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

: "${FQDN:?ERROR: please export FQDN}"
: "${PASSWORD:?ERROR: please export PASSWORD}"

if [[ ! -f "$CREATE_SQL" ]]; then
  echo "ERROR: create SQL file '$CREATE_SQL' does not exist." >&2
  exit 1
fi

need() { command -v "$1" >/dev/null 2>&1 || { echo "ERROR: need '$1' in PATH" >&2; exit 1; }; }
need clickhouse-client

cli() {
  clickhouse-client \
    --host "$FQDN" \
    --secure \
    --password "$PASSWORD" \
    --database "$DATABASE" \
    "$@"
}

# --- Ensure database exists ---
clickhouse-client \
  --host "$FQDN" \
  --secure \
  --password "$PASSWORD" \
  --query "CREATE DATABASE IF NOT EXISTS \`$DATABASE\`"

table_exists() {
  cli --query "EXISTS TABLE $TABLE" --format=TSV | tr -d '[:space:]'
}

row_count() {
  cli --query "SELECT toUInt64(count()) FROM $TABLE" --format=TSV | tr -d '[:space:]'
}

# --- Create table if needed ---
if [[ "$(table_exists)" != "1" ]]; then
  echo "Table $DATABASE.$TABLE does not exist — creating from $CREATE_SQL ..."
  cli < "$CREATE_SQL"
fi

echo "Database: $DATABASE"
echo "Target rows: $TARGET"

before="$(row_count)"
echo "Current rows in $DATABASE.$TABLE: $before"

iter=0
while :; do
  current="$(row_count)"
  printf "Rows: %s\r" "$current"

  if [[ "$current" =~ ^[0-9]+$ ]] && (( current >= TARGET )); then
    echo
    echo "Target reached (>= $TARGET). Done."
    break
  fi

  iter=$((iter+1))
  echo
  echo "===== Iteration #$iter: loading rows into $DATABASE.$TABLE ====="

  cli --time --query "
    INSERT INTO $TABLE
    SELECT *
    FROM urlCluster(default, 'https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{0..99}.parquet')
  "
done

final="$(row_count)"
echo "Final rows in $DATABASE.$TABLE: $final"