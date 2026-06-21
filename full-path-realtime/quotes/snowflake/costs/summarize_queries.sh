#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------
# Summarize Snowflake query JSONL results with cost info.
#
# Usage:
#   ./summarize_queries_snowflake.sh <input.jsonl> <pricing_json> <output.json> \
#       [--cloud <val>] [--region <val>] [--iterations N]
#
# Example:
#   ./summarize_queries_snowflake.sh dashboard_20260609T154019Z.jsonl \
#       pricings/gen2_warehouse.json \
#       results/dashboard_summary.json \
#       --cloud aws --region us-east-1 --iterations 50
#
# Input JSONL: one entry per iteration, result field is [[t1],[t2],...]
#              with a single runtime per query (not three runs).
# cluster_size in the JSONL is used as the credits_per_hour lookup key.
# ---------------------------------------------

CLOUD="aws"
REGION="us-east-1"
MAX_ITER=""

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <input.jsonl> <pricing_json> <output.json> [--cloud <val>] [--region <val>] [--iterations N]" >&2
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

mkdir -p "$(dirname "$OUT_FILE")"

echo "→ Summarizing Snowflake query results"
echo "  Input  : $INPUT"
echo "  Pricing: $PRICING_FILE"
echo "  Cloud  : $CLOUD / $REGION"
if [[ -n "$MAX_ITER" ]]; then
  echo "  Iterations: first $MAX_ITER"
  DATA=$(head -n "$MAX_ITER" "$INPUT")
else
  echo "  Iterations: all"
  DATA=$(cat "$INPUT")
fi
echo

jq -s \
  --arg cloud  "$CLOUD" \
  --arg region "$REGION" \
  --arg input_file "$(basename "$INPUT")" \
  --slurpfile pricing "$PRICING_FILE" '

  .[0] as $first
  | length as $iter_count
  | ($first.cluster_size | tonumber) as $cluster_credits

  # Sum every runtime across all iterations and all queries
  # result[][] flattens [[t1],[t2],...] directly to scalars
  | ([.[].result[][]] | add) as $total_runtime

  # Find all matching pricing blocks for this cloud/region
  | [
      $pricing[0].pricing[]
      | select(.cloud == $cloud and .region == $region)
      | . as $block
      | ($block.warehouses[] | select(.credits_per_hour == $cluster_credits)) as $wh
      | ($block.credit_price_per_hour) as $credit_price
      | ($total_runtime * ($wh.credits_per_hour * $credit_price / 3600.0)) as $compute_cost
      | {
          tier:                   $block.plan,
          warehouse_size:         $wh.name,
          credits_per_hour:       $wh.credits_per_hour,
          credit_price_per_hour:  $credit_price,
          total_compute_cost_usd: ($compute_cost * 100000 | round / 100000)
        }
    ] as $costs

  | {
      source_file:           $input_file,
      system:                $first.system,
      version:               $first.version,
      machine:               $first.machine,
      cluster_size:          $cluster_credits,
      cloud:                 $cloud,
      region:                $region,
      iterations_included:   $iter_count,
      queries_per_iteration: ($first.result | length),
      total_runtime_seconds: ($total_runtime * 1000 | round / 1000),
      costs:                 $costs
    }
' <(echo "$DATA") > "$OUT_FILE"

echo "✅ Written to $OUT_FILE"
echo "⏱  Total runtime: $(jq '.total_runtime_seconds' "$OUT_FILE")s"
echo "💰 Total compute cost per tier:"
jq -r '.costs[] | "  \(.tier) (\(.warehouse_size)): $\(.total_compute_cost_usd)"' "$OUT_FILE"
