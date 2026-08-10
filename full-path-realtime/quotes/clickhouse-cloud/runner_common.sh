#!/usr/bin/env bash
# Shared implementation for the ClickHouse dashboard and drill-down runners.
#
# The thin wrappers define:
#   RUNNER_NAME, DEFAULT_QUERIES_FILE, DEFAULT_INTERVAL, COMMENT_FLAVOR
# and then call runner_main.

set -uo pipefail

TAGS='["C++","column-oriented","ClickHouse derivative","managed","aws"]'

STOP_REQUESTED=0
ITERATION=0

handle_stop() {
  STOP_REQUESTED=1
}

usage() {
  echo "Usage: $0 --database DB [--queries FILE] [--interval SEC] [--output FILE] [--output-dir DIR] <system> <machine_desc> <cluster_size> <base_comment> <parallel_replicas_flag>" >&2
}

scalar_query() {
  local query="$1"
  local val
  val=$(clickhouse-client "${CONNECTION_ARGS[@]}" \
    --database "$DATABASE" --format=TSV --query="$query" 2>/dev/null \
    | tr -d '[:space:]')
  echo "${val:-0}"
}

time_query() {
  local query="$1"
  local value
  value=$(
    clickhouse-client "${CONNECTION_ARGS[@]}" \
      --database "$DATABASE" --time --format=Null \
      --query="$query" --progress 0 "${QUERY_SETTINGS[@]}" 2>&1 \
      | awk '/^[0-9]+([.][0-9]+)?$/ { value=$0 }
             END { if (value != "") print value }'
  )
  echo "${value:-null}"
}

runner_main() {
  if [[ -z "${RUNNER_NAME:-}" || -z "${DEFAULT_QUERIES_FILE:-}" || \
        -z "${DEFAULT_INTERVAL:-}" || -z "${COMMENT_FLAVOR:-}" ]]; then
    echo "ERROR: runner wrapper did not configure runner_common.sh" >&2
    return 1
  fi

  DATABASE=""
  QUERIES_FILE="$DEFAULT_QUERIES_FILE"
  INTERVAL="$DEFAULT_INTERVAL"
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
    echo "ERROR: --database is required" >&2
    usage
    return 1
  fi
  if [[ ! "$INTERVAL" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: --interval must be a positive integer number of seconds" >&2
    return 1
  fi
  if [[ $# -lt 5 ]]; then
    usage
    return 1
  fi

  SYSTEM="$1"
  MACHINE="$2"
  CLUSTER_SIZE="$3"
  BASE_COMMENT="$4"
  PARALLEL_FLAG="$5"

  if [[ -z "$OUTPUT" ]]; then
    OUTPUT="${OUTPUT_DIR}/${RUNNER_NAME}_$(date -u +%Y%m%dT%H%M%SZ).jsonl"
  fi
  mkdir -p "$(dirname "$OUTPUT")"

  COMMENT="${BASE_COMMENT} (${COMMENT_FLAVOR}, enable_parallel_replicas=${PARALLEL_FLAG})"

  FQDN="${FQDN:-localhost}"
  PASSWORD="${PASSWORD:-}"
  CONNECTION_ARGS=(--host "$FQDN")
  if [[ -n "$PASSWORD" ]]; then
    CONNECTION_ARGS+=(--secure --password "$PASSWORD")
  fi
  QUERY_SETTINGS=(
    --enable_parallel_replicas="$PARALLEL_FLAG"
    --max_parallel_replicas="$CLUSTER_SIZE"
    --use_query_cache=0
  )

  VERSION=$(
    clickhouse-client "${CONNECTION_ARGS[@]}" \
      --database "$DATABASE" --format=TSV --query="SELECT version()" 2>/dev/null \
      | tr -d '[:space:]'
  )
  [[ -z "$VERSION" ]] && VERSION="unknown"

  QUERIES=()
  while IFS= read -r query; do
    QUERIES+=("$query")
  done < <(
    sed 's|--.*$||' "$QUERIES_FILE" |
    awk 'BEGIN { RS=";"; ORS="" }
         {
           q=$0
           gsub(/^[ \t\r\n]+|[ \t\r\n]+$/, "", q)
           gsub(/\r?\n/, " ", q)
           if (length(q) > 0) print q "\n"
         }'
  )
  TOTAL=${#QUERIES[@]}
  if (( TOTAL == 0 )); then
    echo "ERROR: No queries found in $QUERIES_FILE" >&2
    return 1
  fi

  echo "Parsed ${TOTAL} queries from ${QUERIES_FILE}" >&2
  echo "Writing JSONL to ${OUTPUT}" >&2
  echo "Interval ${INTERVAL}s, fixed-rate from scheduled starts. Ctrl-C to stop." >&2

  STOP_REQUESTED=0
  ITERATION=0
  trap handle_stop INT TERM

  # Bash's SECONDS counter is monotonic for this process and avoids wall-clock/NTP
  # adjustments. Each iteration is anchored to the previous SCHEDULED start, not
  # to the time the previous iteration finished.
  local next_fire=$SECONDS
  local delay now behind

  while (( ! STOP_REQUESTED )); do
    delay=$((next_fire - SECONDS))
    if (( delay > 0 )); then
      sleep "$delay" || true
    fi
    if (( STOP_REQUESTED )); then
      break
    fi

    ITERATION=$((ITERATION + 1))
    TS_START="$(date -u +%FT%TZ)"
    echo "[$(date -u +%T)] iter ${ITERATION} starting..." >&2

    RAW_ROWS="$(scalar_query "SELECT count() FROM ${DATABASE}.quotes")"
    MV_ROWS="$(scalar_query "SELECT count() FROM ${DATABASE}.quotes_daily")"
    echo "  raw_rows=${RAW_ROWS}  mv_rows=${MV_ROWS}" >&2

    RESULTS=()
    ITERATION_INTERRUPTED=0
    for ((i=0; i<TOTAL; i++)); do
      t=$(time_query "${QUERIES[$i]}")
      if (( STOP_REQUESTED )); then
        ITERATION_INTERRUPTED=1
        break
      fi
      t="${t:-null}"
      RESULTS+=("[${t}]")
      echo "  q$((i+1))/${TOTAL}: ${t}s" >&2
    done
    if (( ITERATION_INTERRUPTED )); then
      echo "  interrupted; discarding incomplete iteration ${ITERATION}." >&2
      break
    fi
    RESULTS_JSON=$(IFS=,; echo "${RESULTS[*]}")

    TS_END="$(date -u +%FT%TZ)"
    printf '{"iteration":%d,"iteration_started_at":"%s","iteration_finished_at":"%s","raw_rows":%s,"mv_rows":%s,"system":"%s","version":"%s","machine":"%s","cluster_size":%s,"comment":"%s","tags":%s,"result":[%s]}\n' \
      "$ITERATION" "$TS_START" "$TS_END" "$RAW_ROWS" "$MV_ROWS" \
      "$SYSTEM" "$VERSION" "$MACHINE" "$CLUSTER_SIZE" "$COMMENT" "$TAGS" \
      "$RESULTS_JSON" >> "$OUTPUT"

    # Fixed-rate scheduling: advance from this iteration's scheduled start.
    next_fire=$((next_fire + INTERVAL))
    now=$SECONDS
    if (( next_fire <= now )); then
      behind=$((now - next_fire))
      echo "  WARN: iteration exceeded the ${INTERVAL}s cadence by ${behind}s; next fires immediately (one runner cannot overlap iterations)." >&2
      next_fire=$now
    else
      echo "  done. next iteration in $((next_fire - now))s (fixed ${INTERVAL}s cadence)." >&2
    fi
  done

  echo "" >&2
  echo "Stopped after ${ITERATION} iterations." >&2
}
