#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------
# Summarize Snowflake Snowpipe Streaming cost from a
# METERING_HISTORY (service_type = SNOWPIPE_STREAMING) CSV export.
#
# CREDITS_USED per row = credits consumed for that metering period.
# Cost = total_credits * credit_price_per_hour (from pricing JSON).
# All matching plans (standard, enterprise, business_critical) are output.
# Also sums BYTES for convenience.
#
# Usage:
#   ./summarize_snowpipe_streaming.sh <streaming.csv> <pricing_json> <output.json> \
#       [--cloud <val>] [--region <val>]
#
# Example:
#   ./summarize_snowpipe_streaming.sh \
#       snowpipe_streaming.csv \
#       pricings/gen2_warehouse.json \
#       results/snowpipe_streaming.json \
#       --cloud aws --region us-east-1
# ---------------------------------------------

CLOUD="aws"
REGION="us-east-1"

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <streaming.csv> <pricing_json> <output.json> [--cloud val] [--region val]" >&2
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
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required (CSV contains multi-line quoted fields)." >&2; exit 1
fi
if [[ ! -f "$INPUT" ]]; then
  echo "Error: input file not found: $INPUT" >&2; exit 1
fi
if [[ ! -f "$PRICING_FILE" ]]; then
  echo "Error: pricing file not found: $PRICING_FILE" >&2; exit 1
fi

mkdir -p "$(dirname "$OUT_FILE")"

echo "→ Summarizing Snowflake Snowpipe Streaming cost"
echo "  Input  : $INPUT"
echo "  Pricing: $PRICING_FILE ($CLOUD / $REGION)"
echo

# Parse the CSV with a real CSV parser (fields like DEFINITION contain
# embedded commas and newlines). Sums CREDITS_USED and BYTES;
# empty/null values count as 0. Prints: total_credits total_bytes row_count
SUMS=$(python3 - "$INPUT" <<'PY'
import csv, sys

def num(v):
    v = (v or "").strip()
    return float(v) if v and v.lower() != "null" else 0.0

credits = bytes_ = 0.0
count = 0
with open(sys.argv[1], newline="") as f:
    for row in csv.DictReader(f):
        credits += num(row.get("CREDITS_USED"))
        bytes_  += num(row.get("BYTES"))
        count   += 1

if count == 0:
    sys.exit(2)
print(f"{credits:.15f} {bytes_:.0f} {count}")
PY
) || {
  echo "Error: no usable rows found in $INPUT" >&2
  exit 1
}

read -r TOTAL_CREDITS TOTAL_BYTES ROW_COUNT <<< "$SUMS"

TMP_OUT="${OUT_FILE}.tmp"

jq -n \
  --arg cloud          "$CLOUD" \
  --arg region         "$REGION" \
  --arg input_file     "$(basename "$INPUT")" \
  --argjson total_credits "$TOTAL_CREDITS" \
  --argjson total_bytes   "$TOTAL_BYTES" \
  --argjson row_count     "$ROW_COUNT" \
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
      total_bytes:      $total_bytes,
      total_gib:        ($total_bytes / 1073741824 * 100 | round / 100),
      total_tib:        ($total_bytes / 1099511627776 * 10000 | round / 10000),
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
echo "📦 Ingested: $(jq -r '"\(.total_gib) GiB"' "$OUT_FILE")"
echo "💰 Total cost per tier:"
jq -r '.costs[] | "  \(.tier): \(.total_credits) credits × $\(.credit_price_per_credit)/credit = $\(.total_cost_usd)"' "$OUT_FILE"
