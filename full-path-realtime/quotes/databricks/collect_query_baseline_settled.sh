#!/usr/bin/env bash
set -euo pipefail

# Recollect provider and billing evidence for one completed Serverless SQL
# baseline query window. Run at least 24 hours after the final query.

umask 077

DBX_BENCH_DIR="/home/ubuntu/costbench/full-path-realtime/quotes/databricks"
: "${DATABRICKS_HOST:?Set the Databricks workspace HTTPS origin}"
: "${DATABRICKS_CLIENT_ID:?Set the benchmark service-principal application ID}"
: "${DATABRICKS_CONTROL_WAREHOUSE_ID:?Set the control warehouse ID}"
QUERY_DIR="${1:?Usage: collect_query_baseline_settled.sh QUERY_OUTPUT_DIR}"
CONTEXT="$QUERY_DIR/query_context.json"
if [[ ! -s "$CONTEXT" ]]; then
  printf 'Missing query context: %s\n' "$CONTEXT" >&2
  exit 1
fi
jq -e '.status == "complete" and .finished_at and .query_warehouse_id' \
  "$CONTEXT" >/dev/null

INGEST_RUN_ID="$(jq -er '.ingest_run_id' "$CONTEXT")"
QUERY_RUN_ID="$(jq -er '.query_run_id' "$CONTEXT")"
SINCE_UTC="$(jq -er '.started_at' "$CONTEXT")"
UNTIL_UTC="$(jq -er '.finished_at' "$CONTEXT")"
QUERY_WAREHOUSE_ID="$(jq -er '.query_warehouse_id' "$CONTEXT")"
RAW_TABLE="$(jq -er '.raw_table' "$CONTEXT")"
MV_TABLE="$(jq -er '.mv_table' "$CONTEXT")"
INGEST_CONTEXT="/data/databricks-qualification/${INGEST_RUN_ID}/run_context.json"
PRODUCER_FINISHED_AT="$(jq -er '.producer_finished_at' "$INGEST_CONTEXT")"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE_DIR="$QUERY_DIR/evidence-settled-$STAMP"
COST_OUTPUT="$QUERY_DIR/cost_summary_settled-$STAMP.json"

read -rsp 'Benchmark service-principal secret: ' DATABRICKS_CLIENT_SECRET
echo
export DATABRICKS_CLIENT_SECRET
export DATABRICKS_CLIENT_ID
unset DATABRICKS_TOKEN
trap 'unset DATABRICKS_CLIENT_SECRET' EXIT

"$DBX_BENCH_DIR/.venv-runner/bin/python" \
  "$DBX_BENCH_DIR/collect_evidence.py" \
  --host "$DATABRICKS_HOST" \
  --control-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --rt-warehouse-id "$QUERY_WAREHOUSE_ID" \
  --catalog costbench \
  --schema rt_qualification \
  --raw-table "$RAW_TABLE" \
  --mv-table "$MV_TABLE" \
  --since "$SINCE_UTC" \
  --until "$UNTIL_UTC" \
  --max-window-hours 8 \
  --run-id "$QUERY_RUN_ID" \
  --output-dir "$EVIDENCE_DIR"

"$DBX_BENCH_DIR/.venv-runner/bin/python" \
  "$DBX_BENCH_DIR/costs/summarize_run.py" \
  --evidence-dir "$EVIDENCE_DIR" \
  --since "$SINCE_UTC" \
  --until "$UNTIL_UTC" \
  --producer-finished-at "$PRODUCER_FINISHED_AT" \
  --rt-warehouse-id "$QUERY_WAREHOUSE_ID" \
  --control-warehouse-id "$DATABRICKS_CONTROL_WAREHOUSE_ID" \
  --output "$COST_OUTPUT"

jq '{
  complete,
  measurement_window,
  primary_query_serving_compute:
    .primary_category_totals.primary_rt_serving_compute,
  fresh_path_during_query_window:
    .canonical_undiscounted_fresh_path_total,
  excluded_category_totals,
  unknown_total,
  completeness,
  errors
}' "$COST_OUTPUT"

printf 'Query evidence: %s\n' "$EVIDENCE_DIR"
printf 'Query cost:     %s\n' "$COST_OUTPUT"
