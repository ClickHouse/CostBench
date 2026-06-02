#!/usr/bin/env bash
set -euo pipefail
#
# Perform a single load iteration from hits_{0..99}.parquet
# into DATABASE.hits.
#
# Environment variables required:
#   export FQDN="your-service-fqdn.clickhouse.cloud"
#   export PASSWORD="your_password"
#
# Usage:
#   ./load_once_from_url.sh [--database DB] [--create-sql FILE]
#
# Examples:
#   ./load_once_from_url.sh
#   ./load_once_from_url.sh --database mydb
#   ./load_once_from_url.sh --database mydb --create-sql schema.sql
#
# Defaults:
#   database     = default
#   create_sql   = create.sql
# -------------------------------------------------------------

DATABASE="default"
TABLE="hits"
CREATE_SQL="create.sql"

# --- Parse CLI flags ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --database)
      DATABASE="$2"
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

before="$(row_count)"
echo "Rows before load: $before"

echo "===== Running single load iteration ====="

cli --time --query "
  INSERT INTO $TABLE
  SELECT *
  FROM urlCluster(default, 'https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{0..99}.parquet')
"

after="$(row_count)"
echo "Rows after load:  $after"

delta=$(( after - before ))
echo "Rows inserted:    $delta"
echo "Done."