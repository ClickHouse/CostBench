#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------
# Summarize Databricks ingest compute cost.
#
# Usage:
#   ./summarize_ingest_compute_databricks.sh <pricing_json> <output_json> \
#       --warehouse-size <name> --hours <n> \
#       [--rows <n>] [--rows-per-sec <n>] \
#       [--cloud <val>] [--region <val>] [--plan <val>]
#
# Example:
#   ./summarize_ingest_compute_databricks.sh \
#       pricings/sql_serverless_compute.json results/ingest_databricks.json \
#       --warehouse-size 2X-Small --hours 27 \
#       --rows 100000000000 --rows-per-sec 1000000 \
#       --cloud aws --region us-east-1 --plan premium
# ---------------------------------------------

CLOUD="aws"
REGION="us-east-1"
PLAN="premium"
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
    --plan)           PLAN="$2";           shift 2 ;;
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

echo "→ Summarizing Databricks ingest compute cost"
echo "  Warehouse : $WAREHOUSE_SIZE"
echo "  Duration  : ${HOURS}h"
echo "  Pricing   : $PRICING_FILE ($CLOUD / $REGION / $PLAN)"
echo

TMP_OUT="${OUT_FILE}.tmp"

jq -n \
  --arg cloud           "$CLOUD" \
  --arg region          "$REGION" \
  --arg plan            "$PLAN" \
  --arg warehouse_size  "$WAREHOUSE_SIZE" \
  --argjson hours       "$HOURS" \
  --argjson rows        "${ROWS:-null}" \
  --argjson rows_per_sec "${ROWS_PER_SEC:-null}" \
  --slurpfile pricing   "$PRICING_FILE" '

  (
    $pricing[0].pricing[]
    | select(.cloud == $cloud and .region == $region and .plan == $plan)
  ) as $pricing_block

  | (
      $pricing_block.instances[]
      | select(.name == $warehouse_size)
    ) as $instance

  | ($pricing_block.dbu_price_per_hour) as $dbu_price
  | ($instance.dbu_per_hour)            as $dbu_per_hour
  | ($hours * $dbu_per_hour * $dbu_price) as $compute_cost

  | {
      system:           "Databricks",
      warehouse_size:   $warehouse_size,
      cloud:            $cloud,
      region:           $region,
      plan:             $plan,
      duration_hours:   $hours,
      rows_ingested:    $rows,
      rows_per_sec:     $rows_per_sec,
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
' > "$TMP_OUT"

if [[ ! -s "$TMP_OUT" ]]; then
  rm -f "$TMP_OUT"
  echo "Error: no matching pricing entry found (cloud=$CLOUD, region=$REGION, plan=$PLAN, warehouse=$WAREHOUSE_SIZE)." >&2
  exit 1
fi

mv "$TMP_OUT" "$OUT_FILE"

echo "✅ Written to $OUT_FILE"
echo "💰 Total compute cost:"
jq -r --argjson h "$HOURS" '.costs[] | "  \(.tier) (\(.warehouse_size), \(.dbu_per_hour) DBU/h × \(.dbu_price_per_hour) $/DBU × \($h)h): $\(.total_compute_cost_usd)"' "$OUT_FILE"
