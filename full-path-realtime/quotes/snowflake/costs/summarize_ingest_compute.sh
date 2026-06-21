#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------
# Summarize Snowflake ingest compute cost.
#
# Usage:
#   ./summarize_ingest_compute_snowflake.sh <pricing_json> <output_json> \
#       --warehouse-size <name> --hours <n> \
#       [--rows <n>] [--rows-per-sec <n>] \
#       [--cloud <val>] [--region <val>]
#
# Example:
#   ./summarize_ingest_compute_snowflake.sh \
#       pricings/gen2_warehouse.json results/ingest_snowflake.json \
#       --warehouse-size "Gen2 X-Small" --hours 27 \
#       --rows 100000000000 --rows-per-sec 1000000 \
#       --cloud aws --region us-east-1
# ---------------------------------------------

CLOUD="aws"
REGION="us-east-1"
WAREHOUSE_SIZE=""
HOURS=""
ROWS=""
ROWS_PER_SEC=""

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <pricing_json> <output_json> --warehouse-size <name> --hours <n> [options]" >&2
  exit 1
fi

PRICING_FILE="$1"
OUT_FILE="$2"
shift 2

while [[ $# -gt 0 ]]; do
  case "$1" in
    --warehouse-size) WAREHOUSE_SIZE="$2"; shift 2 ;;
    --hours)          HOURS="$2";          shift 2 ;;
    --rows)           ROWS="$2";           shift 2 ;;
    --rows-per-sec)   ROWS_PER_SEC="$2";   shift 2 ;;
    --cloud)          CLOUD="$2";          shift 2 ;;
    --region)         REGION="$2";         shift 2 ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$WAREHOUSE_SIZE" || -z "$HOURS" ]]; then
  echo "Error: --warehouse-size and --hours are required." >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required." >&2; exit 1
fi

if [[ ! -f "$PRICING_FILE" ]]; then
  echo "Error: pricing file not found: $PRICING_FILE" >&2; exit 1
fi

mkdir -p "$(dirname "$OUT_FILE")"

echo "→ Summarizing Snowflake ingest compute cost"
echo "  Warehouse : $WAREHOUSE_SIZE"
echo "  Duration  : ${HOURS}h"
echo "  Pricing   : $PRICING_FILE ($CLOUD / $REGION)"
echo

TMP_OUT="${OUT_FILE}.tmp"

jq -n \
  --arg cloud           "$CLOUD" \
  --arg region          "$REGION" \
  --arg warehouse_size  "$WAREHOUSE_SIZE" \
  --argjson hours       "$HOURS" \
  --argjson rows        "${ROWS:-null}" \
  --argjson rows_per_sec "${ROWS_PER_SEC:-null}" \
  --slurpfile pricing   "$PRICING_FILE" '

  [
    $pricing[0].pricing[]
    | select(.cloud == $cloud and .region == $region)
    | . as $block
    | ($block.warehouses[] | select(.name == $warehouse_size)) as $wh
    | ($block.credit_price_per_hour) as $credit_price
    | ($hours * $wh.credits_per_hour * $credit_price) as $compute_cost
    | {
        tier:                   $block.plan,
        warehouse_size:         $wh.name,
        credits_per_hour:       $wh.credits_per_hour,
        credit_price_per_hour:  $credit_price,
        total_compute_cost_usd: ($compute_cost * 100000 | round / 100000)
      }
  ] as $costs

  | {
      system:           "Snowflake",
      warehouse_size:   $warehouse_size,
      cloud:            $cloud,
      region:           $region,
      duration_hours:   $hours,
      rows_ingested:    $rows,
      rows_per_sec:     $rows_per_sec,
      costs:            $costs
    }
' > "$TMP_OUT"

if [[ ! -s "$TMP_OUT" ]]; then
  rm -f "$TMP_OUT"
  echo "Error: no matching pricing entry found (cloud=$CLOUD, region=$REGION, warehouse=$WAREHOUSE_SIZE)." >&2
  exit 1
fi

mv "$TMP_OUT" "$OUT_FILE"

echo "✅ Written to $OUT_FILE"
echo "💰 Total compute cost per tier:"
jq -r --argjson h "$HOURS" '.costs[] | "  \(.tier) (\(.warehouse_size), \(.credits_per_hour) credits/h × \(.credit_price_per_hour) $/credit × \($h)h): $\(.total_compute_cost_usd)"' "$OUT_FILE"
