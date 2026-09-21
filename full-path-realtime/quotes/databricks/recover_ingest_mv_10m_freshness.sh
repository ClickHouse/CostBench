#!/usr/bin/env bash
set -euo pipefail

# Recover metadata-only MV refresh evidence for the completed ten-minute trial
# after correcting materialized-view ownership.

umask 077

DBX_BENCH_DIR="/home/ubuntu/costbench/full-path-realtime/quotes/databricks"
RUN_ROOT="/data/databricks-qualification/ingest_mv_10m_20260915T143250Z"
OUTPUT_DIR="$RUN_ROOT/freshness-recovered"
RUN_ID="ingest_mv_10m_20260915T143250Z"
: "${DATABRICKS_HOST:?Set the Databricks workspace HTTPS origin}"
: "${DATABRICKS_CLIENT_ID:?Set the benchmark service-principal application ID}"
: "${DATABRICKS_CONTROL_WAREHOUSE_ID:?Set the control warehouse ID}"

if [[ -e "$OUTPUT_DIR" ]]; then
  printf 'Refusing existing recovery directory: %s\n' "$OUTPUT_DIR" >&2
  exit 1
fi
if [[ ! -s "$RUN_ROOT/ingest/ingest_progress.json" ]]; then
  echo "Completed ingest progress is missing." >&2
  exit 1
fi

read -rsp 'Benchmark service-principal secret: ' DATABRICKS_CLIENT_SECRET
echo
export DATABRICKS_CLIENT_SECRET
export DATABRICKS_CLIENT_ID
unset DATABRICKS_TOKEN
trap 'unset DATABRICKS_CLIENT_SECRET' EXIT

mkdir -p "$OUTPUT_DIR"

"$DBX_BENCH_DIR/.venv-runner/bin/python" \
  "$DBX_BENCH_DIR/monitor_freshness.py" \
  --host "$DATABRICKS_HOST" \
  --control-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --catalog costbench \
  --schema rt_qualification \
  --raw-table quotes_ingest_mv_10m_20260915 \
  --mv-table quotes_daily_ingest_mv_10m_20260915 \
  --producer-progress "$RUN_ROOT/ingest/ingest_progress.json" \
  --since 2026-09-15T14:32:50.000Z \
  --interval 1 \
  --iterations 1 \
  --output "$OUTPUT_DIR/freshness.jsonl" \
  --progress-json "$OUTPUT_DIR/freshness_progress.json" \
  --run-id "$RUN_ID"

tail -n 1 "$OUTPUT_DIR/freshness.jsonl" | jq '{
  run_id,
  observed_at,
  zerobus_ingest,
  latest_successful_refresh,
  raw_commit_age_sec,
  mv_refresh_age_sec,
  event_log_rows: .event_log.bounded_rows,
  errors
}'
