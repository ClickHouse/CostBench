#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_path_utils.sh
source "$SCRIPT_DIR/_path_utils.sh"

usage() {
  cat >&2 <<'EOF'
Usage:
  summarize_queries.sh <jsonl-file-or-directory> <serverless_pricing.json> \
    <output.json> --runner dashboard|drilldown --iterations N \
    [--run-id ID] [--region us]

When the input is a directory, every *.jsonl file in that directory is read.
Records are filtered by --runner and, when supplied, --run-id.
Exactly the first N matching iteration records are included. The command fails
if fewer than N matching iterations are available.
EOF
  exit 1
}

[[ $# -ge 5 ]] || usage

INPUT=$1
PRICING_FILE=$2
OUT_FILE=$3
RUNNER=
RUN_ID=
REGION=us
MAX_ITERATIONS=
shift 3

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runner) RUNNER=$2; shift 2 ;;
    --run-id) RUN_ID=$2; shift 2 ;;
    --region) REGION=$2; shift 2 ;;
    --iterations) MAX_ITERATIONS=$2; shift 2 ;;
    *) echo "ERROR: unknown option: $1" >&2; usage ;;
  esac
done

[[ "$RUNNER" == "dashboard" || "$RUNNER" == "drilldown" ]] \
  || { echo "ERROR: --runner must be dashboard or drilldown" >&2; exit 1; }
[[ "$MAX_ITERATIONS" =~ ^[1-9][0-9]*$ ]] \
  || { echo "ERROR: --iterations is required and must be a positive integer" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "ERROR: jq is required." >&2; exit 1; }
[[ -f "$PRICING_FILE" ]] || { echo "ERROR: pricing file not found: $PRICING_FILE" >&2; exit 1; }
jq -e --arg region "$REGION" '.regions[$region].pricing_compute != null' "$PRICING_FILE" >/dev/null \
  || { echo "ERROR: no compute pricing for region $REGION in $PRICING_FILE" >&2; exit 1; }

FILES=()
if [[ -d "$INPUT" ]]; then
  while IFS= read -r file; do
    FILES+=("$file")
  done < <(find "$INPUT" -maxdepth 1 -type f -name '*.jsonl' | LC_ALL=C sort)
elif [[ -f "$INPUT" ]]; then
  FILES+=("$INPUT")
else
  echo "ERROR: input is neither a file nor a directory: $INPUT" >&2
  exit 1
fi

[[ ${#FILES[@]} -gt 0 ]] || { echo "ERROR: no JSONL input files found under $INPUT" >&2; exit 1; }

PORTABLE_INPUT="$(portable_path "$INPUT")"
PORTABLE_PRICING="$(portable_path "$PRICING_FILE")"

mkdir -p "$(dirname "$OUT_FILE")"
TMP_OUT="${OUT_FILE}.tmp"

jq -s \
  --arg runner "$RUNNER" \
  --arg run_id "$RUN_ID" \
  --arg region "$REGION" \
  --arg input_path "$PORTABLE_INPUT" \
  --arg pricing_file "$PORTABLE_PRICING" \
  --argjson max_iterations "$MAX_ITERATIONS" \
  --slurpfile pricing "$PRICING_FILE" '
  def round8: . * 100000000 | round / 100000000;

  ($pricing[0]) as $p
  | ($p.regions[$region].pricing_compute) as $pc
  | map(select(.runner == $runner))
  | if $run_id == "" then . else map(select(.run_id == $run_id)) end
  | sort_by(.iteration_started_at, .iteration)
  | .[:$max_iterations]
  | . as $records
  | [$records[].query_jobs[]?] as $jobs
  | ([$jobs[] | (.runtime_sec // 0)] | add // 0) as $runtime
  | ([$jobs[] | (.total_bytes_processed // 0)] | add // 0) as $processed_bytes
  | ([$jobs[] | (.total_bytes_billed // 0)] | add // 0) as $billed_bytes
  | ([$jobs[] | (.billed_slot_sec // ((.total_slot_ms // 0) / 1000))] | add // 0) as $slot_seconds
  | ($pc.on_demand.monthly) as $od
  | [
      $pc.capacity
      | to_entries[] as $variant
      | $variant.value
      | to_entries[] as $period
      | $period.value.tiers[]
      | {
          tier: .name,
          compute_model: "capacity",
          pricing_variant: $variant.key,
          billing_period: $period.key,
          usage_metric: "billed_slot_sec",
          billed_slot_sec: $slot_seconds,
          price_usd: .price_usd,
          price_unit: .price_unit,
          price_unit_seconds: .price_unit_seconds,
          total_compute_cost_usd:
            (($slot_seconds * .price_usd / .price_unit_seconds) | round8)
        }
    ] as $capacity_costs
  | ($capacity_costs + [
      {
        tier: "OnDemand",
        compute_model: "on_demand",
        billing_period: "monthly",
        usage_metric: "billed_bytes",
        billed_bytes: $billed_bytes,
        price_usd: $od.price_usd,
        price_unit: $od.price_unit,
        price_unit_bytes: $od.price_unit_bytes,
        total_compute_cost_usd:
          (($billed_bytes * $od.price_usd / $od.price_unit_bytes) | round8)
      }
    ]) as $costs
  | {
      schema_version: 1,
      system: "BigQuery",
      component: $runner,
      region: $region,
      run_id: (if $run_id == "" then ($records[0].run_id // null) else $run_id end),
      source_path: $input_path,
      pricing_file: $pricing_file,
      iterations_included: ($records | length),
      first_iteration_started_at: ($records | map(.iteration_started_at) | min),
      last_iteration_finished_at: ($records | map(.iteration_finished_at) | max),
      query_jobs: ($jobs | length),
      failed_jobs: ($jobs | map(select(.error != null)) | length),
      cache_hits: ($jobs | map(select(.cache_hit == true)) | length),
      missing_billed_bytes: ($jobs | map(select(.total_bytes_billed == null)) | length),
      missing_slot_seconds:
        ($jobs | map(select(.billed_slot_sec == null and .total_slot_ms == null)) | length),
      total_runtime_seconds: $runtime,
      total_bytes_processed: $processed_bytes,
      total_bytes_billed: $billed_bytes,
      total_billed_gib: ($billed_bytes / 1073741824),
      total_billed_slot_sec: $slot_seconds,
      costs: $costs,
      sources: ($p.sources // [])
    }
' "${FILES[@]}" > "$TMP_OUT"

if ! jq -e \
  --argjson expected_iterations "$MAX_ITERATIONS" \
  '.iterations_included == $expected_iterations and .query_jobs > 0' \
  "$TMP_OUT" \
  >/dev/null; then
  ACTUAL_ITERATIONS="$(jq -r '.iterations_included' "$TMP_OUT")"
  rm -f "$TMP_OUT"
  echo \
    "ERROR: requested $MAX_ITERATIONS $RUNNER iterations, but found $ACTUAL_ITERATIONS with query jobs in $INPUT" \
    >&2
  exit 1
fi

mv "$TMP_OUT" "$OUT_FILE"

echo "Written: $OUT_FILE"
jq '{component, iterations_included, query_jobs, total_billed_gib, total_billed_slot_sec, costs}' "$OUT_FILE"
