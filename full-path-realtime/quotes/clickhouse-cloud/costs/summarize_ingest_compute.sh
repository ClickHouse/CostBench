#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------
# Summarize ClickHouse Cloud ingest compute cost.
#
# Usage:
#   ./summarize_ingest_compute_clickhouse.sh <output_json> \
#       --nodes <n> --mem-gib <n> --hours <n> \
#       [--rows <n>] [--rows-per-sec <n>] \
#       [--cloud <val>] [--region <val>]
#
# Example:
#   ./summarize_ingest_compute_clickhouse.sh results/ingest_clickhouse.json \
#       --nodes 2 --mem-gib 8 --hours 27 \
#       --rows 100000000000 --rows-per-sec 1000000 \
#       --cloud aws --region us-east-1
#
# Pricing: ClickHouse Cloud (AWS us-east-1), per 8 GiB-hour.
# Cost = hours * (total_mem_gib / 8) * compute_price_per_8gib_hour * nodes
# ---------------------------------------------

CLOUD="aws"
REGION="us-east-1"
NODES=""
MEM_GIB=""
HOURS=""
ROWS=""
ROWS_PER_SEC=""

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <output_json> --nodes <n> --mem-gib <n> --hours <n> [options]" >&2
  exit 1
fi

OUT_FILE="$1"
shift

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes)        NODES="$2";        shift 2 ;;
    --mem-gib)      MEM_GIB="$2";      shift 2 ;;
    --hours)        HOURS="$2";        shift 2 ;;
    --rows)         ROWS="$2";         shift 2 ;;
    --rows-per-sec) ROWS_PER_SEC="$2"; shift 2 ;;
    --cloud)        CLOUD="$2";        shift 2 ;;
    --region)       REGION="$2";       shift 2 ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$NODES" || -z "$MEM_GIB" || -z "$HOURS" ]]; then
  echo "Error: --nodes, --mem-gib, and --hours are required." >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required." >&2; exit 1
fi

mkdir -p "$(dirname "$OUT_FILE")"

# Pricing: USD per 8 GiB-hour (AWS us-east-1)
TIERS_JSON='[
  {"name":"Basic",      "compute":0.2181100, "compute_price_unit":8},
  {"name":"Scale",      "compute":0.29846,   "compute_price_unit":8},
  {"name":"Enterprise", "compute":0.3903,    "compute_price_unit":8}
]'

echo "→ Summarizing ClickHouse Cloud ingest compute cost"
echo "  Cluster   : ${NODES} node(s) × ${MEM_GIB} GiB RAM"
echo "  Duration  : ${HOURS}h"
echo "  Cloud     : $CLOUD / $REGION"
echo

jq -n \
  --arg cloud        "$CLOUD" \
  --arg region       "$REGION" \
  --argjson nodes    "$NODES" \
  --argjson mem_gib  "$MEM_GIB" \
  --argjson hours    "$HOURS" \
  --argjson rows     "${ROWS:-null}" \
  --argjson rows_per_sec "${ROWS_PER_SEC:-null}" \
  --argjson tiers    "$TIERS_JSON" '

  ($nodes * $mem_gib) as $total_mem_gib

  | [
      $tiers[] | . as $tier
      | ($hours * ($total_mem_gib / $tier.compute_price_unit) * $tier.compute) as $compute_cost
      | {
          tier:                   $tier.name,
          nodes:                  $nodes,
          mem_gib_per_node:       $mem_gib,
          total_mem_gib:          $total_mem_gib,
          compute_price_per_8gib_hour: $tier.compute,
          total_compute_cost_usd: ($compute_cost * 100000 | round / 100000)
        }
    ] as $costs

  | {
      system:         "ClickHouse Cloud",
      cloud:          $cloud,
      region:         $region,
      nodes:          $nodes,
      mem_gib_per_node: $mem_gib,
      total_mem_gib:  $total_mem_gib,
      duration_hours: $hours,
      rows_ingested:  $rows,
      rows_per_sec:   $rows_per_sec,
      costs:          $costs
    }
' > "$OUT_FILE"

echo "✅ Written to $OUT_FILE"
echo "💰 Total compute cost per tier:"
jq -r --argjson h "$HOURS" \
  '.costs[] | "  \(.tier) (\(.total_mem_gib) GiB total, \(.compute_price_per_8gib_hour) $/8GiB-h × \($h)h): $\(.total_compute_cost_usd)"' \
  "$OUT_FILE"
