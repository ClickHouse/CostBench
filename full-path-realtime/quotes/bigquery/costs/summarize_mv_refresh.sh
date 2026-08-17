#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_path_utils.sh
source "$SCRIPT_DIR/_path_utils.sh"

usage() {
  cat >&2 <<'EOF'
Usage:
  summarize_mv_refresh.sh <mv_refresh_jobs.jsonl> <serverless_pricing.json> \
    <output.json> [--region us]
EOF
  exit 1
}

[[ $# -ge 3 ]] || usage

INPUT=$1
PRICING_FILE=$2
OUT_FILE=$3
REGION=us
shift 3

while [[ $# -gt 0 ]]; do
  case "$1" in
    --region) REGION=$2; shift 2 ;;
    *) echo "ERROR: unknown option: $1" >&2; usage ;;
  esac
done

command -v jq >/dev/null 2>&1 || { echo "ERROR: jq is required." >&2; exit 1; }
[[ -f "$INPUT" ]] || { echo "ERROR: input not found: $INPUT" >&2; exit 1; }
[[ -f "$PRICING_FILE" ]] || { echo "ERROR: pricing file not found: $PRICING_FILE" >&2; exit 1; }
jq -e --arg region "$REGION" '.regions[$region].pricing_compute != null' "$PRICING_FILE" >/dev/null \
  || { echo "ERROR: no compute pricing for region $REGION in $PRICING_FILE" >&2; exit 1; }

PORTABLE_INPUT="$(portable_path "$INPUT")"
PORTABLE_PRICING="$(portable_path "$PRICING_FILE")"

mkdir -p "$(dirname "$OUT_FILE")"
TMP_OUT="${OUT_FILE}.tmp"

jq -s \
  --arg region "$REGION" \
  --arg input_file "$PORTABLE_INPUT" \
  --arg pricing_file "$PORTABLE_PRICING" \
  --slurpfile pricing "$PRICING_FILE" '
  def round8: . * 100000000 | round / 100000000;

  ($pricing[0]) as $p
  | ($p.regions[$region].pricing_compute) as $pc
  | length as $job_count
  | ([.[] | (.total_bytes_processed // 0)] | add // 0) as $processed_bytes
  | ([.[] | (.total_bytes_billed // 0)] | add // 0) as $billed_bytes
  | ([.[] | (.total_slot_ms // 0)] | add // 0) as $slot_ms
  | ($slot_ms / 1000) as $slot_seconds
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
      component: "mv_refresh",
      region: $region,
      source_file: $input_file,
      pricing_file: $pricing_file,
      refresh_jobs: $job_count,
      failed_jobs: (map(select(.error_result != null)) | length),
      missing_billed_bytes: (map(select(.total_bytes_billed == null)) | length),
      missing_slot_ms: (map(select(.total_slot_ms == null)) | length),
      total_bytes_processed: $processed_bytes,
      total_bytes_billed: $billed_bytes,
      total_billed_gib: ($billed_bytes / 1073741824),
      total_billed_slot_sec: $slot_seconds,
      first_refresh: (map(.creation_time) | min),
      last_refresh: (map(.creation_time) | max),
      costs: $costs,
      sources: ($p.sources // [])
    }
' "$INPUT" > "$TMP_OUT"

mv "$TMP_OUT" "$OUT_FILE"

echo "Written: $OUT_FILE"
jq '{component, refresh_jobs, total_billed_gib, total_billed_slot_sec, costs}' "$OUT_FILE"
