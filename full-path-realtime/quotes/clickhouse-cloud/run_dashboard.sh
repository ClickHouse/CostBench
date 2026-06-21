#!/usr/bin/env bash
set -uo pipefail
# -----------------------------------------------------------------------------
# Dashboard runner — loops the MV queries every --interval seconds, appending
# one JSONL record per iteration to --output. Runs in parallel to the ingest
# script; Ctrl-C when ingest finishes.
#
# Each line is a self-contained ClickBench-style record augmented with
# iteration metadata and row counts of both raw and MV tables at the moment
# the iteration started. `result` is an array of single-element arrays
# (one try per query) in queries-file order. Failed queries log `null`.
#
# Usage:
#   ./run_dashboard.sh --database DB [--queries FILE] [--interval SEC] [--output FILE] \
#       <system> <machine_desc> <cluster_size> <base_comment> <parallel_replicas_flag>
#
# Example:
#   FQDN=spaeib65wx.us-east-2.aws.clickhouse-staging.com PASSWORD=xxx \
#   ./run_dashboard.sh --database test1 \
#       "ClickHouse Cloud (AWS)" "236GiB" 3 "10B rows" 0
# -----------------------------------------------------------------------------

DATABASE=""
QUERIES_FILE="queries_mv.sql"
INTERVAL=600
OUTPUT=""
OUTPUT_DIR="."

while [[ $# -gt 0 ]]; do
  case "$1" in
    --database)   DATABASE="$2"; shift 2 ;;
    --queries)    QUERIES_FILE="$2"; shift 2 ;;
    --interval)   INTERVAL="$2"; shift 2 ;;
    --output)     OUTPUT="$2"; shift 2 ;;
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    *) break ;;
  esac
done

if [[ -z "$DATABASE" ]]; then
  echo "ERROR: --database is required" >&2; exit 1
fi
if [[ $# -lt 5 ]]; then
  echo "Usage: $0 --database DB [--queries FILE] [--interval SEC] [--output FILE] [--output-dir DIR] <system> <machine_desc> <cluster_size> <base_comment> <parallel_replicas_flag>" >&2
  exit 1
fi

SYSTEM="$1"
MACHINE="$2"
CLUSTER_SIZE="$3"
BASE_COMMENT="$4"
PARALLEL_FLAG="$5"

[[ -z "$OUTPUT" ]] && OUTPUT="${OUTPUT_DIR}/dashboard_$(date -u +%Y%m%dT%H%M%SZ).jsonl"
mkdir -p "$(dirname "$OUTPUT")"

COMMENT="${BASE_COMMENT} (dashboard, enable_parallel_replicas=${PARALLEL_FLAG})"
TAGS='["C++","column-oriented","ClickHouse derivative","managed","aws"]'

FQDN="${FQDN:=localhost}"
PASSWORD="${PASSWORD:=}"
EXTRA_SETTINGS="--enable_parallel_replicas=${PARALLEL_FLAG} --max_parallel_replicas=${CLUSTER_SIZE}"

# --- Fetch server version once ---
VERSION="$(
  clickhouse-client --host "$FQDN" \
    ${PASSWORD:+--secure} ${PASSWORD:+--password "$PASSWORD"} \
    --database "$DATABASE" --format=TSV --query="SELECT version()" 2>/dev/null \
    | tr -d '[:space:]'
)"
[[ -z "$VERSION" ]] && VERSION="unknown"

# --- Parse queries.sql by semicolons (trimmed, non-empty) ---
mapfile -t QUERIES < <(
  # 1. sed strips `--` line comments (a `;` inside a comment would otherwise
  #    split a statement mid-text).
  # 2. awk splits records on `;`, trims surrounding whitespace, and collapses
  #    internal newlines to spaces — `mapfile -t` reads ONE LINE per array
  #    element, so a multi-line SQL statement must be flattened first.
  sed 's|--.*$||' "$QUERIES_FILE" |
  awk 'BEGIN { RS=";"; ORS="" }
       {
         q=$0
         gsub(/^[ \t\r\n]+|[ \t\r\n]+$/, "", q)
         gsub(/\r?\n/, " ", q)
         if (length(q) > 0) print q "\n"
       }
  '
)
TOTAL=${#QUERIES[@]}
if (( TOTAL == 0 )); then
  echo "ERROR: No queries found in $QUERIES_FILE" >&2; exit 1
fi

echo "Parsed ${TOTAL} queries from ${QUERIES_FILE}" >&2
echo "Writing JSONL to ${OUTPUT}" >&2
echo "Interval ${INTERVAL}s. Ctrl-C to stop." >&2

ITERATION=0
trap 'echo "" >&2; echo "Stopped after ${ITERATION} iterations." >&2; exit 0' INT TERM

# --- Helpers ---
time_query() {
  local query="$1"
  (clickhouse-client --host "$FQDN" \
    ${PASSWORD:+--secure} ${PASSWORD:+--password "$PASSWORD"} \
    --database "$DATABASE" --time --format=Null \
    --query="$query" --progress 0 ${EXTRA_SETTINGS} 2>&1 \
    | grep -o -P '^\d+\.\d+$' || echo -n "null") | tr -d '\n'
}

scalar_query() {
  local query="$1"
  local val
  val=$(clickhouse-client --host "$FQDN" \
    ${PASSWORD:+--secure} ${PASSWORD:+--password "$PASSWORD"} \
    --database "$DATABASE" --format=TSV --query="$query" 2>/dev/null \
    | tr -d '[:space:]')
  echo "${val:-0}"
}

# --- Main loop ---
while true; do
  ITERATION=$((ITERATION + 1))
  TS_START="$(date -u +%FT%TZ)"
  echo "[$(date -u +%T)] iter ${ITERATION} starting..." >&2

  RAW_ROWS="$(scalar_query "SELECT count() FROM ${DATABASE}.quotes")"
  MV_ROWS="$(scalar_query "SELECT count() FROM ${DATABASE}.quotes_daily")"
  echo "  raw_rows=${RAW_ROWS}  mv_rows=${MV_ROWS}" >&2

  RESULTS=()
  for ((i=0; i<TOTAL; i++)); do
    t=$(time_query "${QUERIES[$i]}")
    RESULTS+=("[${t}]")
    echo "  q$((i+1))/${TOTAL}: ${t}s" >&2
  done
  RESULTS_JSON=$(IFS=,; echo "${RESULTS[*]}")

  TS_END="$(date -u +%FT%TZ)"

  printf '{"iteration":%d,"iteration_started_at":"%s","iteration_finished_at":"%s","raw_rows":%s,"mv_rows":%s,"system":"%s","version":"%s","machine":"%s","cluster_size":%s,"comment":"%s","tags":%s,"result":[%s]}\n' \
    "$ITERATION" "$TS_START" "$TS_END" "$RAW_ROWS" "$MV_ROWS" \
    "$SYSTEM" "$VERSION" "$MACHINE" "$CLUSTER_SIZE" "$COMMENT" "$TAGS" \
    "$RESULTS_JSON" >> "$OUTPUT"

  echo "  done. sleeping ${INTERVAL}s..." >&2
  sleep "$INTERVAL"
done
