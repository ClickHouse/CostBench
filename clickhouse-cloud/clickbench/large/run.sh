#!/usr/bin/env bash
set -euo pipefail
#
# Run ClickHouse queries 3x each and emit a JSON doc with results.
#
# Usage:
#   ./run.sh --database DB <system> <machine_desc> <cluster_size> <base_comment> <parallel_replicas_flag> [data_size]
#
# Example:
#   ./run.sh --database hits_10b "ClickHouse Cloud (AWS)" "236GiB" 3 "10B rows" 0 123456789
# ---------------------------------------------------------------------------

# --- Parse --database first ---
DATABASE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --database)
      DATABASE="$2"
      shift 2
      ;;
    *)
      break
      ;;
  esac
done

if [[ -z "$DATABASE" ]]; then
  echo "ERROR: --database is required" >&2
  exit 1
fi

if [[ $# -lt 5 ]]; then
  echo "Usage: $0 --database DB <system> <machine_desc> <cluster_size> <base_comment> <parallel_replicas_flag> [data_size]" >&2
  exit 1
fi

SYSTEM="$1"
MACHINE="$2"
CLUSTER_SIZE="$3"
BASE_COMMENT="$4"
PARALLEL_FLAG="$5"
DATA_SIZE="${6:-0}"

COMMENT="${BASE_COMMENT} (enable_parallel_replicas=${PARALLEL_FLAG})"
PROPRIETARY="yes"
TUNED="no"
TAGS='["C++","column-oriented","ClickHouse derivative","managed","aws"]'
LOAD_TIME=0

# Client env
FQDN="${FQDN:=localhost}"
PASSWORD="${PASSWORD:=}"
EXTRA_SETTINGS="--enable_parallel_replicas=${PARALLEL_FLAG} --max_parallel_replicas=${CLUSTER_SIZE}"

TRIES=3

# --- Fetch ClickHouse version once ---
VERSION="$(
  clickhouse-client \
    --host "$FQDN" \
    ${PASSWORD:+--secure} \
    ${PASSWORD:+--password "$PASSWORD"} \
    --database "$DATABASE" \
    --format=TSV \
    --query="SELECT version()" 2>/dev/null | tr -d '[:space:]'
)"

if [[ -z "$VERSION" ]]; then
  VERSION="unknown"
fi

# --- Parse queries.sql by semicolons (trimmed, non-empty) ---
mapfile -t QUERIES < <(
  awk '
    BEGIN { RS=";"; ORS="" }
    {
      q=$0
      gsub(/^[ \t\r\n]+|[ \t\r\n]+$/, "", q)
      if (length(q) > 0) print q "\n"
    }
  ' queries.sql
)

TOTAL=${#QUERIES[@]}
echo "Parsed queries: ${TOTAL}" >&2
if (( TOTAL == 0 )); then
  echo "ERROR: No queries found in queries.sql" >&2
  exit 1
fi

# --- Collect results ---
RESULT_RAW="$(
QUERY_NUM=1
for query in "${QUERIES[@]}"; do
    echo "Running query #$QUERY_NUM..." >&2
    echo -n "["
    ARRAY_VALUES=()
    for i in $(seq 1 $TRIES); do
        val=$(
          (clickhouse-client \
            --host "$FQDN" \
            ${PASSWORD:+--secure} \
            ${PASSWORD:+--password "$PASSWORD"} \
            --database "$DATABASE" \
            --time \
            --format=Null \
            --query="$query" \
            --progress 0 \
            ${EXTRA_SETTINGS} 2>&1 |
            grep -o -P '^\d+\.\d+$' || echo -n "null") | tr -d '\n'
        )
        ARRAY_VALUES+=("$val")
        echo -n "$val"
        [[ "$i" != $TRIES ]] && echo -n ", "
    done
    echo "],"
    echo "→ [${ARRAY_VALUES[*]}]" >&2
    QUERY_NUM=$((QUERY_NUM + 1))
done
)"

# Make valid JSON arrays (drop trailing comma)
RESULT_CLEAN="$(printf "%s\n" "$RESULT_RAW" | sed '$ s/,\s*$//')"

DATE_ISO="$(date -u +%F)"

cat <<JSON
{
    "system": "$SYSTEM",
    "version": "$VERSION",
    "date": "$DATE_ISO",
    "machine": "$MACHINE",
    "cluster_size": $CLUSTER_SIZE,
    "proprietary": "$PROPRIETARY",
    "tuned": "$TUNED",
    "comment": "$COMMENT",
    "tags": $TAGS,
    "load_time": $LOAD_TIME,
    "data_size": $DATA_SIZE,
    "result": [
$RESULT_CLEAN
    ]
}
JSON