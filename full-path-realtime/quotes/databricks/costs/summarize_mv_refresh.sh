#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------
# Summarize Databricks MV refresh cost from a
# system.billing.usage markdown export.
#
# dbus column = total DBUs consumed per day.
# Cost = total_dbus * dbu_price_per_dbu (from pricing JSON).
#
# Usage:
#   ./summarize_mv_refresh_databricks.sh <refresh.md> <pricing_json> <output.json> \
#       [--cloud <val>] [--region <val>] [--plan <val>]
#
# Example:
#   ./summarize_mv_refresh_databricks.sh \
#       refresh-mv_table-details.md \
#       pricings/sql_serverless_compute.json \
#       results/mv_refresh.json \
#       --cloud aws --region us-east-1 --plan premium
# ---------------------------------------------

CLOUD="aws"
REGION="us-east-1"
PLAN="premium"

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <refresh.md> <pricing_json> <output.json> [--cloud val] [--region val] [--plan val]" >&2
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

echo "→ Summarizing Databricks MV refresh cost"
echo "  Input  : $INPUT"
echo "  Pricing: $PRICING_FILE ($CLOUD / $REGION / $PLAN)"
echo

# Parse the markdown table: extract dbus (column 3).
# Skips SQL block, header row, separator row, and any null/empty values.
PARSED=$(awk -F'|' '
  /^\|[-]+\|/ { next }
  /^\| *usage_date/ { next }
  /^\|/ {
    dbus = $4; gsub(/^[[:space:]]+|[[:space:]]+$/, "", dbus)
    if (dbus == "" || dbus == "null") next
    print dbus
  }
' "$INPUT")

if [[ -z "$PARSED" ]]; then
  echo "Error: no usable rows found in $INPUT" >&2
  exit 1
fi

TOTAL_DBUS=$(echo "$PARSED" | awk '{s += $1} END {printf "%.15f", s}')
ROW_COUNT=$(echo "$PARSED" | wc -l | tr -d ' ')

TMP_OUT="${OUT_FILE}.tmp"

jq -n \
  --arg cloud        "$CLOUD" \
  --arg region       "$REGION" \
  --arg plan         "$PLAN" \
  --arg input_file   "$(basename "$INPUT")" \
  --argjson total_dbus "$TOTAL_DBUS" \
  --argjson row_count  "$ROW_COUNT" \
  --slurpfile pricing  "$PRICING_FILE" '

  (
    $pricing[0].pricing[]
    | select(.cloud == $cloud and .region == $region and .plan == $plan)
  ) as $pricing_block

  | ($pricing_block.dbu_price_per_hour) as $dbu_price

  | {
      source_file:     $input_file,
      system:          "Databricks",
      cloud:           $cloud,
      region:          $region,
      plan:            $plan,
      total_days:      $row_count,
      total_dbu:       ($total_dbus * 100000 | round / 100000),
      costs: [
        {
          tier:               $plan,
          dbu_price_per_dbu:  $dbu_price,
          total_dbu:          ($total_dbus * 100000 | round / 100000),
          total_cost_usd:     ($total_dbus * $dbu_price * 100000 | round / 100000)
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

echo "✅ Written to $OUT_FILE"
echo "💰 Total cost:"
jq -r '.costs[] | "  \(.tier): \(.total_dbu) DBU × $\(.dbu_price_per_dbu)/DBU = $\(.total_cost_usd)"' "$OUT_FILE"
