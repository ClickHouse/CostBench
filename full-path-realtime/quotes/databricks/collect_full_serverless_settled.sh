#!/usr/bin/env bash
set -euo pipefail

# Recollect settled billing for one completed canonical compact-result run.

umask 077

DBX_BENCH_DIR="/home/ubuntu/costbench/full-path-realtime/quotes/databricks"
: "${DATABRICKS_HOST:?Set the Databricks workspace HTTPS origin}"
: "${DATABRICKS_CLIENT_ID:?Set the benchmark service-principal application ID}"
RUN_ROOT="${1:?Usage: collect_full_serverless_settled.sh RUN_ROOT}"
RUN_CONTEXT="$RUN_ROOT/run_context.json"
if [[ ! -s "$RUN_CONTEXT" ]]; then
  printf 'Missing run context: %s\n' "$RUN_CONTEXT" >&2
  exit 1
fi
jq -e '
  (
    .status == "measurement_complete"
    or .status == "complete"
    or .status == "complete_with_duplicates"
  )
  and .run_id
  and .since
  and .until
  and .producer_finished_at
' "$RUN_CONTEXT" >/dev/null

RUN_ID="$(jq -er '.run_id' "$RUN_CONTEXT")"
RUN_STATE="/data/databricks-state/${RUN_ID}"
SOURCE_MANIFEST="$RUN_STATE/source_manifest.json"
SINCE_UTC="$(jq -er '.since' "$RUN_CONTEXT")"
UNTIL_UTC="$(jq -er '.until' "$RUN_CONTEXT")"
PRODUCER_FINISHED_AT="$(jq -er '.producer_finished_at' "$RUN_CONTEXT")"
CATALOG="$(jq -er '.catalog' "$RUN_CONTEXT")"
SCHEMA="$(jq -er '.schema' "$RUN_CONTEXT")"
RAW_TABLE="$(jq -er '.raw_table' "$RUN_CONTEXT")"
MV_TABLE="$(jq -er '.mv_table' "$RUN_CONTEXT")"
QUERY_WAREHOUSE_ID="$(jq -er '.query_warehouse_id' "$RUN_CONTEXT")"
CONTROL_WAREHOUSE_ID="$(jq -er '.control_warehouse_id' "$RUN_CONTEXT")"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
EVIDENCE_DIR="$RUN_STATE/evidence-settled-$STAMP"
COST_OUTPUT="$RUN_ROOT/costs/cost_summary_settled_$STAMP.json"
VALIDATION_OUTPUT="$RUN_ROOT/validation/validation_report_settled_$STAMP.json"

shopt -s nullglob
dashboards=("$RUN_ROOT"/mv/dashboard_*.jsonl)
drilldowns=("$RUN_ROOT"/raw/drilldown_*.jsonl)
freshness=("$RUN_ROOT"/freshness/mv_freshness_*.jsonl)
shopt -u nullglob
if [[ "${#dashboards[@]}" -ne 1 || "${#drilldowns[@]}" -ne 1 || \
      "${#freshness[@]}" -ne 1 ]]; then
  echo "Expected exactly one dashboard, drill-down, and freshness JSONL." >&2
  exit 1
fi
QUERY_AUDIT_DIR="$RUN_STATE/query-evidence"
DASHBOARD_AUDIT="$QUERY_AUDIT_DIR/$(basename "${dashboards[0]}")"
DRILLDOWN_AUDIT="$QUERY_AUDIT_DIR/$(basename "${drilldowns[0]}")"
DASHBOARD_VALIDATION="${dashboards[0]}"
DRILLDOWN_VALIDATION="${drilldowns[0]}"
if [[ -s "$DASHBOARD_AUDIT" ]]; then
  DASHBOARD_VALIDATION="$DASHBOARD_AUDIT"
fi
if [[ -s "$DRILLDOWN_AUDIT" ]]; then
  DRILLDOWN_VALIDATION="$DRILLDOWN_AUDIT"
fi
if [[ ! -s "$SOURCE_MANIFEST" ]]; then
  printf 'Missing retained runtime source manifest: %s\n' "$SOURCE_MANIFEST" >&2
  exit 1
fi

read -rsp 'Benchmark service-principal secret: ' DATABRICKS_CLIENT_SECRET
echo
export DATABRICKS_CLIENT_SECRET
export DATABRICKS_CLIENT_ID
unset DATABRICKS_TOKEN
trap 'unset DATABRICKS_CLIENT_SECRET' EXIT

RUNNER_PYTHON="$DBX_BENCH_DIR/.venv-runner/bin/python"
"$RUNNER_PYTHON" "$DBX_BENCH_DIR/collect_evidence.py" \
  --host "$DATABRICKS_HOST" \
  --control-warehouse-id "$CONTROL_WAREHOUSE_ID" \
  --rt-warehouse-id "$QUERY_WAREHOUSE_ID" \
  --catalog "$CATALOG" \
  --schema "$SCHEMA" \
  --raw-table "$RAW_TABLE" \
  --mv-table "$MV_TABLE" \
  --since "$SINCE_UTC" \
  --until "$UNTIL_UTC" \
  --max-window-hours 36 \
  --history-limit 30000 \
  --statement-timeout 600 \
  --run-id "$RUN_ID" \
  --output-dir "$EVIDENCE_DIR"

"$RUNNER_PYTHON" "$DBX_BENCH_DIR/compact_evidence.py" \
  --evidence-dir "$EVIDENCE_DIR" \
  --preflight "$RUN_ROOT/validation/preflight.json" \
  --output-dir "$RUN_ROOT/evidence"

"$RUNNER_PYTHON" "$DBX_BENCH_DIR/costs/summarize_run.py" \
  --evidence-dir "$EVIDENCE_DIR" \
  --since "$SINCE_UTC" \
  --until "$UNTIL_UTC" \
  --producer-finished-at "$PRODUCER_FINISHED_AT" \
  --rt-warehouse-id "$QUERY_WAREHOUSE_ID" \
  --control-warehouse-id "$CONTROL_WAREHOUSE_ID" \
  --output "$COST_OUTPUT"

CLIENT_DURABLE_ROWS="$(
  jq -er '.provider_committed_rows' "$RUN_ROOT/ingest/ingest_summary.json"
)"
PROVIDER_COMMITTED_ROWS="$(
  jq -er '.provider_committed_records' "$EVIDENCE_DIR/evidence_summary.json"
)"
if (( PROVIDER_COMMITTED_ROWS < CLIENT_DURABLE_ROWS )); then
  echo "Settled provider rows are below the client durability watermark." >&2
  exit 1
fi
DUPLICATE_ROWS=$((PROVIDER_COMMITTED_ROWS - CLIENT_DURABLE_ROWS))

set +e
"$RUNNER_PYTHON" "$DBX_BENCH_DIR/validate_run.py" \
  --source-manifest "$SOURCE_MANIFEST" \
  --ingest-progress "$RUN_ROOT/ingest/ingest_progress.json" \
  --ingest-metrics "$RUN_ROOT/ingest/ingest_metrics.jsonl" \
  --dashboard "$DASHBOARD_VALIDATION" \
  --drilldown "$DRILLDOWN_VALIDATION" \
  --freshness "${freshness[0]}" \
  --evidence-summary "$EVIDENCE_DIR/evidence_summary.json" \
  --cost-summary "$COST_OUTPUT" \
  --output "$VALIDATION_OUTPUT"
VALIDATION_STATUS=$?
set -e

if ! jq -e --argjson duplicate_rows "$DUPLICATE_ROWS" '
  all(
    .gates[];
    .passed == true
    or ($duplicate_rows > 0 and .id == "row_reconciliation")
  )
' "$VALIDATION_OUTPUT" >/dev/null; then
  echo "Settled validation has failures beyond the accepted duplicate surplus." >&2
  if [[ "$VALIDATION_STATUS" -ne 0 ]]; then
    exit "$VALIDATION_STATUS"
  fi
  exit 1
fi

"$RUNNER_PYTHON" "$DBX_BENCH_DIR/compact_query_results.py" \
  --input "$DASHBOARD_VALIDATION" \
  --output "${dashboards[0]}" \
  --audit-output "$DASHBOARD_AUDIT" \
  >/dev/null
"$RUNNER_PYTHON" "$DBX_BENCH_DIR/compact_query_results.py" \
  --input "$DRILLDOWN_VALIDATION" \
  --output "${drilldowns[0]}" \
  --audit-output "$DRILLDOWN_AUDIT" \
  >/dev/null

SETTLED_AT="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
jq \
  --arg settled_at "$SETTLED_AT" \
  --arg settled_cost "$(basename "$COST_OUTPUT")" \
  --arg settled_validation "$(basename "$VALIDATION_OUTPUT")" \
  --argjson validation_status "$VALIDATION_STATUS" \
  --argjson client_durable_rows "$CLIENT_DURABLE_ROWS" \
  --argjson provider_committed_rows "$PROVIDER_COMMITTED_ROWS" \
  --argjson duplicate_rows "$DUPLICATE_ROWS" \
  '.status = (
      if $duplicate_rows == 0
      then "complete"
      else "complete_with_duplicates"
      end
    )
   | .publishable = ($duplicate_rows == 0 and $validation_status == 0)
   | .reconciliation = {
       client_durable_rows: $client_durable_rows,
       provider_committed_rows: $provider_committed_rows,
       duplicate_surplus_rows: $duplicate_rows,
       exact: ($duplicate_rows == 0)
     }
   | .settled_validation_process_status = $validation_status
   | .settled_cost_collected_at = $settled_at
   | .settled_cost_file = $settled_cost
   | .settled_validation_file = $settled_validation' \
  "$RUN_CONTEXT" > "$RUN_CONTEXT.tmp"
mv "$RUN_CONTEXT.tmp" "$RUN_CONTEXT"
LATEST_CONTEXT="/data/databricks-runs/latest_serverless_baseline.json"
if [[ "$(jq -r '.run_id' "$LATEST_CONTEXT" 2>/dev/null || true)" == "$RUN_ID" ]]; then
  cp "$RUN_CONTEXT" "$LATEST_CONTEXT"
fi

"$RUNNER_PYTHON" "$DBX_BENCH_DIR/finalize_results.py" \
  --run-root "$RUN_ROOT" \
  --run-id "$RUN_ID" \
  --source-manifest "$SOURCE_MANIFEST" \
  --source-hashes "$RUN_ROOT/ingest/source_hashes_post_run.json" \
  --status settled \
  >/dev/null

printf 'Settled cost:       %s\n' "$COST_OUTPUT"
printf 'Settled validation: %s\n' "$VALIDATION_OUTPUT"
printf 'Compact run root:   %s\n' "$RUN_ROOT"
