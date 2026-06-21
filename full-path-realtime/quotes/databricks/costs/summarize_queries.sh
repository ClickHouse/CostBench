#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------
# Summarize Databricks query JSONL results with cost info.
#
# Usage:
#   ./summarize_queries_databricks.sh <input.jsonl> <pricing_json> <output.json> \
#       [--cloud <val>] [--region <val>] [--plan <val>] [--iterations N]
#
# Example:
#   ./summarize_queries_databricks.sh dashboard_20260611.jsonl \
#       pricings/sql_serverless_compute.json \
#       results/dashboard_summary.json \
#       --cloud aws --region us-east-1 --plan premium --iterations 30
#
# cluster_size in the JSONL is the warehouse size name (e.g. "X-Small")
# and is used directly to look up DBU/hour in the pricing file.
# ---------------------------------------------

CLOUD="aws"
REGION="us-east-1"
PLAN="premium"
MAX_ITER=""

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <input.jsonl> <pricing_json> <output.json> [--cloud <val>] [--region <val>] [--plan <val>] [--iterations N]" >&2
  exit 1
fi

INPUT="$1"
PRICING_FILE="$2"
OUT_FILE="$3"
shift 3

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cloud)       CLOUD="$2";    shift 2 ;;
    --region)      REGION="$2";   shift 2 ;;
    --plan)        PLAN="$2";     shift 2 ;;
    --iterations)  MAX_ITER="$2"; shift 2 ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required." >&2
  exit 1
fi

if [[ ! -f "$INPUT" ]]; then
  echo "Error: input file not found: $INPUT" >&2
  exit 1
fi

if [[ ! -f "$PRICING_FILE" ]]; then
  echo "Error: pricing file not found: $PRICING_FILE" >&2
  exit 1
fi

mkdir -p "$(dirname "$OUT_FILE")"

echo "→ Summarizing Databricks query results"
echo "  Input  : $INPUT"
echo "  Pricing: $PRICING_FILE"
echo "  Cloud  : $CLOUD / $REGION"
echo "  Plan   : $PLAN"
if [[ -n "$MAX_ITER" ]]; then
  echo "  Iterations: first $MAX_ITER"
  DATA=$(head -n "$MAX_ITER" "$INPUT")
else
  echo "  Iterations: all"
  DATA=$(cat "$INPUT")
fi
echo

TMP_OUT="${OUT_FILE}.tmp"

jq -s \
  --arg cloud    "$CLOUD" \
  --arg region   "$REGION" \
  --arg plan     "$PLAN" \
  --slurpfile pricing "$PRICING_FILE" '

  .[0] as $first
  | length as $iter_count
  | ($first.cluster_size) as $warehouse_size

  # Sum every runtime across all iterations and all queries
  | ([.[].result[][]] | add) as $total_runtime

  # Find the matching pricing block and instance
  | (
      $pricing[0].pricing[]
      | select(.cloud == $cloud and .region == $region and .plan == $plan)
    ) as $pricing_block

  | (
      $pricing_block.instances[]
      | select(.name == $warehouse_size)
    ) as $instance

  | ($pricing_block.dbu_price_per_hour) as $dbu_price
  | ($instance.dbu_per_hour)            as $dbu_per_hour

  | ($total_runtime / 3600.0 * $dbu_per_hour * $dbu_price) as $compute_cost

  | {
      source_file:           ($first | input_filename? // ""),
      system:                $first.system,
      version:               $first.version,
      warehouse_size:        $warehouse_size,
      cloud:                 $cloud,
      region:                $region,
      plan:                  $plan,
      iterations_included:   $iter_count,
      queries_per_iteration: ($first.result | length),
      total_runtime_seconds: ($total_runtime * 1000 | round / 1000),
      costs: [
        {
          tier:                   $plan,
          warehouse_size:         $warehouse_size,
          dbu_per_hour:           $dbu_per_hour,
          dbu_price_per_hour:     $dbu_price,
          total_compute_cost_usd: ($compute_cost * 100000 | round / 100000)
        }
      ]
    }
' <(echo "$DATA") > "$TMP_OUT"

if [[ ! -s "$TMP_OUT" ]]; then
  rm -f "$TMP_OUT"
  echo "Error: no matching pricing entry found." >&2
  echo "  cloud=$CLOUD, region=$REGION, plan=$PLAN, warehouse_size=$(head -1 "$INPUT" | jq -r '.cluster_size')" >&2
  exit 1
fi

mv "$TMP_OUT" "$OUT_FILE"

echo "✅ Written to $OUT_FILE"
echo "⏱  Total runtime: $(jq '.total_runtime_seconds' "$OUT_FILE")s"
echo "💰 Total compute cost:"
jq -r '.costs[] | "  \(.tier) (\(.warehouse_size), \(.dbu_per_hour) DBU/h): $\(.total_compute_cost_usd)"' "$OUT_FILE"
