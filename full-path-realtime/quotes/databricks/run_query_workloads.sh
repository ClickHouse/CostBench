#!/usr/bin/env bash
set -euo pipefail

# Launch the canonical dashboard and drill-down workloads against one query
# warehouse while a prepared ingest/MV run is active.

umask 077

: "${DBX_BENCH_DIR:=/home/ubuntu/costbench/full-path-realtime/quotes/databricks}"
: "${DATABRICKS_RUNNER_PYTHON:=$DBX_BENCH_DIR/.venv-runner/bin/python}"
: "${DATABRICKS_HOST:?Set the Databricks workspace HTTPS origin}"
: "${DATABRICKS_CLIENT_ID:?Set the benchmark service-principal application ID}"
: "${DATABRICKS_CONTROL_WAREHOUSE_ID:?Set the control warehouse ID}"
: "${DATABRICKS_QUERY_WAREHOUSE_ID:?Set the dedicated query warehouse ID}"
: "${DATABRICKS_WAREHOUSE_MODE:=serverless-baseline}"
: "${DATABRICKS_QUERY_SIZE:=X-Small}"
: "${QUERY_WINDOW_SCOPE:=auto}"
: "${POST_INGEST_QUERY_SECONDS:=0}"
: "${INGEST_CONTEXT:=/data/databricks-qualification/latest_ingest_mv_6h_r2_run.json}"

if [[ "$DATABRICKS_QUERY_WAREHOUSE_ID" == "$DATABRICKS_CONTROL_WAREHOUSE_ID" ]]; then
  echo "Query and control warehouses must be different." >&2
  exit 1
fi
if [[ ! "$POST_INGEST_QUERY_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "POST_INGEST_QUERY_SECONDS must be a nonnegative integer." >&2
  exit 1
fi
if [[ ! -s "$INGEST_CONTEXT" ]]; then
  printf 'Missing ingest context: %s\n' "$INGEST_CONTEXT" >&2
  exit 1
fi

INGEST_RUN_ROOT="$(jq -er '.run_root' "$INGEST_CONTEXT")"
INGEST_RUN_ID="$(jq -er '.run_id' "$INGEST_CONTEXT")"
CATALOG="$(jq -er '.catalog // "costbench"' "$INGEST_CONTEXT")"
SCHEMA="$(jq -er '.schema // "rt_qualification"' "$INGEST_CONTEXT")"
RAW_TABLE="$(jq -er '.raw_table' "$INGEST_CONTEXT")"
MV_TABLE="$(jq -er '.mv_table' "$INGEST_CONTEXT")"
: "${PRODUCER_PROGRESS:=$INGEST_RUN_ROOT/ingest/ingest_progress.json}"
: "${FRESHNESS_PROGRESS:=$INGEST_RUN_ROOT/freshness/freshness_progress.json}"
if [[ ! -s "$PRODUCER_PROGRESS" ]]; then
  echo "Producer progress is unavailable." >&2
  exit 1
fi
if [[ "$(jq -r '.running' "$PRODUCER_PROGRESS")" != "true" ]]; then
  echo "The selected producer is not currently running." >&2
  exit 1
fi

START_ROWS="$(jq -er '.provider_committed_rows' "$PRODUCER_PROGRESS")"
STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
: "${DATABRICKS_QUERY_RUN_ID:=${INGEST_RUN_ID}_query_${DATABRICKS_WAREHOUSE_MODE}}"
QUERY_RUN_ID="$DATABRICKS_QUERY_RUN_ID"
: "${QUERY_OUTPUT_DIR:=$INGEST_RUN_ROOT/queries/${DATABRICKS_WAREHOUSE_MODE}_${DATABRICKS_QUERY_WAREHOUSE_ID}_${STAMP}}"
OUTPUT_DIR="$QUERY_OUTPUT_DIR"
: "${QUERY_CONTEXT_PATH:=$OUTPUT_DIR/query_context.json}"
: "${DASHBOARD_OUTPUT:=$OUTPUT_DIR/dashboard.jsonl}"
: "${DRILLDOWN_OUTPUT:=$OUTPUT_DIR/drilldown.jsonl}"
: "${QUERY_LOG_DIR:=$OUTPUT_DIR}"
DASHBOARD_PID=""
DRILLDOWN_PID=""

cleanup() {
  unset DATABRICKS_CLIENT_SECRET
  for pid in "$DASHBOARD_PID" "$DRILLDOWN_PID"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT INT TERM

printf 'Ingest run:      %s\n' "$INGEST_RUN_ID"
printf 'Starting rows:   %s\n' "$START_ROWS"
printf 'Raw/MV:          %s / %s\n' "$RAW_TABLE" "$MV_TABLE"
printf 'Query warehouse: %s (%s, %s)\n' \
  "$DATABRICKS_QUERY_WAREHOUSE_ID" "$DATABRICKS_WAREHOUSE_MODE" "$DATABRICKS_QUERY_SIZE"
printf 'Dashboard:       4 queries every 600 seconds\n'
printf 'Drill-down:      2 queries every 3600 seconds\n'
printf 'Output:          %s\n' "$OUTPUT_DIR"

if [[ -z "${DATABRICKS_CLIENT_SECRET:-}" ]]; then
  read -rsp 'Benchmark service-principal secret: ' DATABRICKS_CLIENT_SECRET
  echo
fi
export DATABRICKS_CLIENT_ID DATABRICKS_CLIENT_SECRET
unset DATABRICKS_TOKEN

mkdir -p \
  "$OUTPUT_DIR" \
  "$(dirname "$QUERY_CONTEXT_PATH")" \
  "$(dirname "$DASHBOARD_OUTPUT")" \
  "$(dirname "$DRILLDOWN_OUTPUT")" \
  "$QUERY_LOG_DIR"
jq -n \
  --arg query_run_id "$QUERY_RUN_ID" \
  --arg ingest_run_id "$INGEST_RUN_ID" \
  --arg started_at "$STARTED_AT" \
  --argjson starting_rows "$START_ROWS" \
  --arg warehouse_id "$DATABRICKS_QUERY_WAREHOUSE_ID" \
  --arg warehouse_mode "$DATABRICKS_WAREHOUSE_MODE" \
  --arg query_size "$DATABRICKS_QUERY_SIZE" \
  --arg catalog "$CATALOG" \
  --arg schema "$SCHEMA" \
  --arg raw_table "$RAW_TABLE" \
  --arg mv_table "$MV_TABLE" \
  --arg query_window_scope "$QUERY_WINDOW_SCOPE" \
  --argjson post_ingest_query_seconds "$POST_INGEST_QUERY_SECONDS" \
  '{
    schema_version: 1,
    status: "running",
    query_run_id: $query_run_id,
    ingest_run_id: $ingest_run_id,
    started_at: $started_at,
    starting_provider_committed_rows: $starting_rows,
    query_warehouse_id: $warehouse_id,
    warehouse_mode: $warehouse_mode,
    query_size: $query_size,
    catalog: $catalog,
    schema: $schema,
    raw_table: $raw_table,
    mv_table: $mv_table,
    dashboard_interval_seconds: 600,
    drilldown_interval_seconds: 3600,
    query_window_scope: $query_window_scope,
    post_ingest_query_seconds: $post_ingest_query_seconds,
    partial_ingest_window: (
      if $query_window_scope == "canonical-full"
      then false
      else $starting_rows > 0
      end
    )
  }' > "$QUERY_CONTEXT_PATH"

COMMON=(
  --host "$DATABRICKS_HOST"
  --query-warehouse-id "$DATABRICKS_QUERY_WAREHOUSE_ID"
  --http-path "/sql/1.0/warehouses/$DATABRICKS_QUERY_WAREHOUSE_ID"
  --warehouse-mode "$DATABRICKS_WAREHOUSE_MODE"
  --control-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID"
  --catalog "$CATALOG"
  --schema "$SCHEMA"
  --raw-table "$RAW_TABLE"
  --mv-table "$MV_TABLE"
  --producer-progress "$PRODUCER_PROGRESS"
  --freshness-monitor "$FRESHNESS_PROGRESS"
  --run-id "$QUERY_RUN_ID"
  --system "Databricks (AWS)"
  --machine "Databricks SQL warehouse"
  --cluster-size "$DATABRICKS_QUERY_SIZE"
  --tags "Databricks,managed,$DATABRICKS_WAREHOUSE_MODE,baseline"
  --comment "Warehouse-swap baseline; Statement Execution API; uncached results"
)

"$DATABRICKS_RUNNER_PYTHON" "$DBX_BENCH_DIR/run_dashboard.py" \
  "${COMMON[@]}" \
  --interval 600 \
  --output "$DASHBOARD_OUTPUT" \
  > >(tee "$QUERY_LOG_DIR/dashboard.log") 2>&1 &
DASHBOARD_PID=$!

"$DATABRICKS_RUNNER_PYTHON" "$DBX_BENCH_DIR/run_drilldown.py" \
  "${COMMON[@]}" \
  --interval 3600 \
  --output "$DRILLDOWN_OUTPUT" \
  > >(tee "$QUERY_LOG_DIR/drilldown.log") 2>&1 &
DRILLDOWN_PID=$!

POST_INGEST_STARTED_AT=""
POST_INGEST_STARTED_SECONDS=""
while kill -0 "$DASHBOARD_PID" 2>/dev/null &&
      kill -0 "$DRILLDOWN_PID" 2>/dev/null; do
  if [[ "$(jq -r '.running' "$PRODUCER_PROGRESS")" != "true" ]]; then
    if [[ "$POST_INGEST_QUERY_SECONDS" -le 0 ]]; then
      break
    fi
    if [[ -z "$POST_INGEST_STARTED_SECONDS" ]]; then
      POST_INGEST_STARTED_SECONDS=$SECONDS
      POST_INGEST_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
      printf 'Producer stopped; continuing queries for %d seconds.\n' \
        "$POST_INGEST_QUERY_SECONDS"
    elif (( SECONDS - POST_INGEST_STARTED_SECONDS >= POST_INGEST_QUERY_SECONDS )); then
      break
    fi
  fi
  sleep 30
done

for pid in "$DASHBOARD_PID" "$DRILLDOWN_PID"; do
  if kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid"
  fi
done

set +e
wait "$DASHBOARD_PID"
DASHBOARD_STATUS=$?
wait "$DRILLDOWN_PID"
DRILLDOWN_STATUS=$?
set -e
DASHBOARD_PID=""
DRILLDOWN_PID=""

FINISHED_AT="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
FINAL_ROWS="$(jq -er '.provider_committed_rows' "$PRODUCER_PROGRESS")"
DASHBOARD_ITERATIONS="$(test -s "$DASHBOARD_OUTPUT" && wc -l < "$DASHBOARD_OUTPUT" || echo 0)"
DRILLDOWN_ITERATIONS="$(test -s "$DRILLDOWN_OUTPUT" && wc -l < "$DRILLDOWN_OUTPUT" || echo 0)"

jq \
  --arg finished_at "$FINISHED_AT" \
  --argjson final_rows "$FINAL_ROWS" \
  --argjson dashboard_iterations "$DASHBOARD_ITERATIONS" \
  --argjson drilldown_iterations "$DRILLDOWN_ITERATIONS" \
  --argjson dashboard_status "$DASHBOARD_STATUS" \
  --argjson drilldown_status "$DRILLDOWN_STATUS" \
  --arg post_ingest_started_at "$POST_INGEST_STARTED_AT" \
  '.status = "complete"
   | .finished_at = $finished_at
   | .final_provider_committed_rows = $final_rows
   | .dashboard_iterations = $dashboard_iterations
   | .drilldown_iterations = $drilldown_iterations
   | .dashboard_process_status = $dashboard_status
   | .drilldown_process_status = $drilldown_status
   | .post_ingest_started_at = (
       if $post_ingest_started_at == ""
       then null
       else $post_ingest_started_at
       end
     )' \
  "$QUERY_CONTEXT_PATH" > "$QUERY_CONTEXT_PATH.tmp"
mv "$QUERY_CONTEXT_PATH.tmp" "$QUERY_CONTEXT_PATH"

jq . "$QUERY_CONTEXT_PATH"

if [[ "$DASHBOARD_STATUS" -ne 0 || "$DRILLDOWN_STATUS" -ne 0 ]]; then
  exit 1
fi
for output in "$DASHBOARD_OUTPUT" "$DRILLDOWN_OUTPUT"; do
  if [[ ! -s "$output" ]] || ! jq -s -e '
    length > 0
    and all(
      .[];
      all(
        .query_evidence[];
        .execution_succeeded == true
        and .canonical_duration_sec != null
        and (.errors | length) == 0
        and .metrics.result_from_cache == false
      )
    )
  ' "$output" >/dev/null; then
    printf 'Query evidence validation failed: %s\n' "$output" >&2
    exit 1
  fi
done
