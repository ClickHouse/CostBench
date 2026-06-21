#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------
# Summarize Snowflake MV refresh cost from an
# MATERIALIZED_VIEW_REFRESH_HISTORY markdown export.
#
# CREDITS_USED per row = credits consumed for that clustering period.
# Cost = total_credits * credit_price_per_hour (from pricing JSON).
# All matching plans (standard, enterprise, business_critical) are output.
#
# Usage:
#   ./summarize_mv_refresh_snowflake.sh <clustering.md> <pricing_json> <output.json> \
#       [--cloud <val>] [--region <val>]
#
# Example:
#   ./summarize_mv_refresh_snowflake.sh \
#       refresh-mv_table-details.md \
#       pricings/gen2_warehouse.json \
#       results/mv_refresh.json \
#       --cloud aws --region us-east-1
# ---------------------------------------------

CLOUD="aws"
REGION="us-east-1"

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <clustering.md> <pricing_json> <output.json> [--cloud val] [--region val]" >&2
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

echo "→ Summarizing Snowflake MV refresh cost"
echo "  Input  : $INPUT"
echo "  Pricing: $PRICING_FILE ($CLOUD / $REGION)"
echo

# Parse the markdown table: extract CREDITS_USED (column 3).
# Skips SQL block, header row, separator row, and any null/empty values.
PARSED=$(awk -F'|' '
  /^\|[-]+\|/ { next }
  /^\| *START_TIME/ { next }
  /^\|/ {
    credits = $4; gsub(/^[[:space:]]+|[[:space:]]+$/, "", credits)
    if (credits == "" || credits == "null") next
    print credits
  }
' "$INPUT")

if [[ -z "$PARSED" ]]; then
  echo "Error: no usable rows found in $INPUT" >&2
  exit 1
fi

TOTAL_CREDITS=$(echo "$PARSED" | awk '{s += $1} END {printf "%.15f", s}')
ROW_COUNT=$(echo "$PARSED" | wc -l | tr -d ' ')

TMP_OUT="${OUT_FILE}.tmp"

jq -n \
  --arg cloud          "$CLOUD" \
  --arg region         "$REGION" \
  --arg input_file     "$(basename "$INPUT")" \
  --argjson total_credits "$TOTAL_CREDITS" \
  --argjson row_count  "$ROW_COUNT" \
  --slurpfile pricing  "$PRICING_FILE" '

  [
    $pricing[0].pricing[]
    | select(.cloud == $cloud and .region == $region)
    | ($total_credits * .credit_price_per_hour) as $cost
    | {
        tier:                   .plan,
        credit_price_per_credit: .credit_price_per_hour,
        total_credits:          ($total_credits * 100000 | round / 100000),
        total_cost_usd:         ($cost * 100000 | round / 100000)
      }
  ] as $costs

  | {
      source_file:      $input_file,
      system:           "Snowflake",
      cloud:            $cloud,
      region:           $region,
      total_periods:    $row_count,
      total_credits:    ($total_credits * 100000 | round / 100000),
      costs:            $costs
    }
' > "$TMP_OUT"

if [[ ! -s "$TMP_OUT" ]]; then
  rm -f "$TMP_OUT"
  echo "Error: no matching pricing entry found (cloud=$CLOUD, region=$REGION)." >&2
  exit 1
fi

mv "$TMP_OUT" "$OUT_FILE"

echo "✅ Written to $OUT_FILE"
echo "💰 Total cost per tier:"
jq -r '.costs[] | "  \(.tier): \(.total_credits) credits × $\(.credit_price_per_credit)/credit = $\(.total_cost_usd)"' "$OUT_FILE"
