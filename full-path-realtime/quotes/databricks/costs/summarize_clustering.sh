#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------
# Summarize Databricks clustering cost from a
# predictive_optimization_operations_history markdown export.
#
# Sums all DBUs across all operation types (CLUSTERING, ANALYZE, COMPACTION).
# usage_quantity in the table = total DBUs consumed per operation.
# Cost = total_dbus * dbu_price_per_dbu (from pricing JSON).
#
# Usage:
#   ./summarize_clustering_databricks.sh <clustering.md> <pricing_json> <output.json> \
#       [--cloud <val>] [--region <val>] [--plan <val>]
#
# Example:
#   ./summarize_clustering_databricks.sh \
#       clustering-raw_table-details.md \
#       pricings/sql_serverless_compute.json \
#       results/clustering_raw_table.json \
#       --cloud aws --region us-east-1 --plan premium
# ---------------------------------------------

CLOUD="aws"
REGION="us-east-1"
PLAN="premium"

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <clustering.md> <pricing_json> <output.json> [--cloud val] [--region val] [--plan val]" >&2
  exit 1
fi

INPUT="$1"
PRICING_FILE="$2"
OUT_FILE="$3"
shift 3

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cloud)   CLOUD="$2";  shift 2 ;;
    --region)  REGION="$2"; shift 2 ;;
    --plan)    PLAN="$2";   shift 2 ;;
    *)
      echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required." >&2; exit 1
fi
if [[ ! -f "$INPUT" ]]; then
  echo "Error: input file not found: $INPUT" >&2; exit 1
fi
if [[ ! -f "$PRICING_FILE" ]]; then
  echo "Error: pricing file not found: $PRICING_FILE" >&2; exit 1
fi

mkdir -p "$(dirname "$OUT_FILE")"

echo "→ Summarizing Databricks clustering cost"
echo "  Input          : $INPUT"
echo "  Pricing        : $PRICING_FILE ($CLOUD / $REGION / $PLAN)"
echo

# Parse the markdown table: extract operation_type and usage_quantity.
# Skips header row, separator row, rows with null/empty usage_quantity.
# Outputs tab-separated: operation_type <TAB> usage_quantity
PARSED=$(awk -F'|' '
  /^\|[-]+\|/ { next }                        # skip separator rows
  /^\| *operation_type/ { next }              # skip header row
  /^\|/ {
    op  = $2; gsub(/^[[:space:]]+|[[:space:]]+$/, "", op)
    qty = $7; gsub(/^[[:space:]]+|[[:space:]]+$/, "", qty)
    if (op == "" || qty == "" || qty == "null") next
    printf "%s\t%s\n", op, qty
  }
' "$INPUT")

if [[ -z "$PARSED" ]]; then
  echo "Error: no usable rows found in $INPUT" >&2
  exit 1
fi

# Convert parsed rows to a JSON array, then compute summary with jq
ROWS_JSON=$(echo "$PARSED" | awk -F'\t' 'BEGIN{print "["} {
  if (NR > 1) printf ","
  printf "{\"op\":\"%s\",\"dbu\":%s}", $1, $2
} END{print "]"}')

TMP_OUT="${OUT_FILE}.tmp"

jq -n \
  --arg cloud      "$CLOUD" \
  --arg region     "$REGION" \
  --arg plan       "$PLAN" \
  --arg input_file "$(basename "$INPUT")" \
  --argjson rows   "$ROWS_JSON" \
  --slurpfile pricing "$PRICING_FILE" '

  (
    $pricing[0].pricing[]
    | select(.cloud == $cloud and .region == $region and .plan == $plan)
  ) as $pricing_block

  | ($pricing_block.dbu_price_per_hour) as $dbu_price

  # Total DBUs across all rows
  | ([$rows[].dbu] | add) as $total_dbu

  # Break down by operation type for informational purposes
  | ([$rows[].op] | unique) as $op_types
  | ($op_types | map(
      . as $op
      | { operation_type: $op,
          operations: ([$rows[] | select(.op == $op)] | length),
          total_dbu: ([$rows[] | select(.op == $op) | .dbu] | add)
        }
    )) as $breakdown

  | {
      source_file:        $input_file,
      system:             "Databricks",
      cloud:              $cloud,
      region:             $region,
      plan:               $plan,
      total_operations:   ($rows | length),
      total_dbu:          ($total_dbu * 100000 | round / 100000),
      breakdown:          $breakdown,
      costs: [
        {
          tier:               $plan,
          dbu_price_per_dbu:  $dbu_price,
          total_dbu:          ($total_dbu * 100000 | round / 100000),
          total_cost_usd:     ($total_dbu * $dbu_price * 100000 | round / 100000)
        }
      ]
    }
' > "$TMP_OUT"

if [[ ! -s "$TMP_OUT" ]]; then
  rm -f "$TMP_OUT"
  echo "Error: no matching pricing entry found (cloud=$CLOUD, region=$REGION, plan=$PLAN)." >&2
  exit 1
fi

mv "$TMP_OUT" "$OUT_FILE"

# Count rows skipped due to null usage_quantity and report
NULL_COUNT=$(awk -F'|' '
  /^\|[-]+\|/ || /^\| *operation_type/ { next }
  /^\|/ {
    qty = $7; gsub(/^[[:space:]]+|[[:space:]]+$/, "", qty)
    if (qty == "" || qty == "null") print
  }
' "$INPUT" | wc -l | tr -d ' ')

echo "✅ Written to $OUT_FILE"
[[ "$NULL_COUNT" -gt 0 ]] && echo "⚠️  Skipped $NULL_COUNT row(s) with null usage_quantity"
echo "📊 Breakdown:"
jq -r '.breakdown[] | "  \(.operation_type): \(.operations) ops, \(.total_dbu) DBU"' "$OUT_FILE"
echo "💰 Total cost:"
jq -r '.costs[] | "  \(.tier): \(.total_dbu) DBU × $\(.dbu_price_per_dbu)/DBU = $\(.total_cost_usd)"' "$OUT_FILE"
