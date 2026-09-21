#!/usr/bin/env bash
set -euo pipefail

# Recollect settled Databricks billing and provider evidence for a completed
# ingest/MV or full baseline run. Run at least 24 hours after its window.

umask 077

DBX_BENCH_DIR="/home/ubuntu/costbench/full-path-realtime/quotes/databricks"
: "${DATABRICKS_HOST:?Set the Databricks workspace HTTPS origin}"
: "${DATABRICKS_CLIENT_ID:?Set the benchmark service-principal application ID}"
: "${DATABRICKS_CONTROL_WAREHOUSE_ID:?Set the control warehouse ID}"
LATEST="/data/databricks-qualification/latest_ingest_mv_6h_run.json"
RUN_ROOT="${1:-}"
if [[ -z "$RUN_ROOT" ]]; then
  RUN_ROOT="$(jq -er '.run_root' "$LATEST")"
fi
CONTEXT="$RUN_ROOT/run_context.json"
if [[ ! -s "$CONTEXT" ]]; then
  printf 'Missing run context: %s\n' "$CONTEXT" >&2
  exit 1
fi
jq -e '.status == "complete" and .until and .producer_finished_at' \
  "$CONTEXT" >/dev/null

RUN_ID="$(jq -er '.run_id' "$CONTEXT")"
SINCE_UTC="$(jq -er '.since' "$CONTEXT")"
UNTIL_UTC="$(jq -er '.until' "$CONTEXT")"
PRODUCER_FINISHED_AT="$(jq -er '.producer_finished_at' "$CONTEXT")"
RAW_TABLE="$(jq -er '.raw_table' "$CONTEXT")"
MV_TABLE="$(jq -er '.mv_table' "$CONTEXT")"
CATALOG="$(jq -r '.catalog // "costbench"' "$CONTEXT")"
SCHEMA="$(jq -r '.schema // "rt_qualification"' "$CONTEXT")"
CONTROL_WAREHOUSE_ID="$(
  jq -r '.control_warehouse_id // empty' "$CONTEXT"
)"
: "${CONTROL_WAREHOUSE_ID:=$DATABRICKS_CONTROL_WAREHOUSE_ID}"
QUERY_WAREHOUSE_ID="$(
  jq -r '.query_warehouse_id // .control_warehouse_id // empty' \
    "$CONTEXT"
)"
: "${QUERY_WAREHOUSE_ID:=$CONTROL_WAREHOUSE_ID}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE_DIR="$RUN_ROOT/evidence/settled-$STAMP"
COST_OUTPUT="$RUN_ROOT/costs/cost_summary_settled-$STAMP.json"
mkdir -p "$(dirname "$EVIDENCE_DIR")" "$(dirname "$COST_OUTPUT")"

read -rsp 'Benchmark service-principal secret: ' DATABRICKS_CLIENT_SECRET
echo
export DATABRICKS_CLIENT_SECRET
export DATABRICKS_CLIENT_ID
unset DATABRICKS_TOKEN
trap 'unset DATABRICKS_CLIENT_SECRET' EXIT

"$DBX_BENCH_DIR/.venv-runner/bin/python" \
  "$DBX_BENCH_DIR/collect_evidence.py" \
  --host "$DATABRICKS_HOST" \
  --control-warehouse-id "$CONTROL_WAREHOUSE_ID" \
  --rt-warehouse-id "$QUERY_WAREHOUSE_ID" \
  --catalog "$CATALOG" \
  --schema "$SCHEMA" \
  --raw-table "$RAW_TABLE" \
  --mv-table "$MV_TABLE" \
  --since "$SINCE_UTC" \
  --until "$UNTIL_UTC" \
  --max-window-hours 72 \
  --history-limit 100000 \
  --run-id "$RUN_ID" \
  --output-dir "$EVIDENCE_DIR"

"$DBX_BENCH_DIR/.venv-runner/bin/python" \
  "$DBX_BENCH_DIR/costs/summarize_run.py" \
  --evidence-dir "$EVIDENCE_DIR" \
  --since "$SINCE_UTC" \
  --until "$UNTIL_UTC" \
  --producer-finished-at "$PRODUCER_FINISHED_AT" \
  --rt-warehouse-id "$QUERY_WAREHOUSE_ID" \
  --control-warehouse-id "$CONTROL_WAREHOUSE_ID" \
  --output "$COST_OUTPUT"

jq '{
  complete,
  measurement_window,
  primary_category_totals,
  canonical_undiscounted_fresh_path_total,
  excluded_category_totals,
  unknown_total,
  completeness,
  errors
}' "$COST_OUTPUT"

printf 'Settled evidence: %s\n' "$EVIDENCE_DIR"
printf 'Settled cost:     %s\n' "$COST_OUTPUT"
