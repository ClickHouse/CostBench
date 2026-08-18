#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_path_utils.sh
source "$SCRIPT_DIR/_path_utils.sh"

usage() {
  cat >&2 <<'EOF'
Usage:
  summarize_storage_write_api.sh <write_api_timeline.jsonl> \
    <storage_write_api_pricing.json> <output.json>
EOF
  exit 1
}

[[ $# -eq 3 ]] || usage

INPUT=$1
PRICING_FILE=$2
OUT_FILE=$3

command -v jq >/dev/null 2>&1 || { echo "ERROR: jq is required." >&2; exit 1; }
[[ -f "$INPUT" ]] || { echo "ERROR: input not found: $INPUT" >&2; exit 1; }
[[ -f "$PRICING_FILE" ]] || { echo "ERROR: pricing file not found: $PRICING_FILE" >&2; exit 1; }

PORTABLE_INPUT="$(portable_path "$INPUT")"
PORTABLE_PRICING="$(portable_path "$PRICING_FILE")"

mkdir -p "$(dirname "$OUT_FILE")"
TMP_OUT="${OUT_FILE}.tmp"

jq -s \
  --arg input_file "$PORTABLE_INPUT" \
  --arg pricing_file "$PORTABLE_PRICING" \
  --slurpfile pricing "$PRICING_FILE" '
  def round8: . * 100000000 | round / 100000000;

  ($pricing[0]) as $p
  | ($p.pricing) as $rate
  | map(select(.error_code == "OK")) as $ok
  | ([$ok[] | (.total_requests // 0)] | add // 0) as $requests
  | ([$ok[] | (.total_rows // 0)] | add // 0) as $rows
  | ([$ok[] | (.total_input_bytes // 0)] | add // 0) as $bytes
  | (($bytes * $rate.price_usd / $rate.price_unit_bytes) | round8) as $cost
  | {
      schema_version: 1,
      system: "BigQuery",
      component: "storage_write_api_ingest",
      source_file: $input_file,
      pricing_file: $pricing_file,
      successful_minute_buckets: ($ok | length),
      successful_requests: $requests,
      successful_rows: $rows,
      total_input_bytes: $bytes,
      total_input_gib: ($bytes / 1073741824),
      non_ok: {
        minute_buckets: (map(select(.error_code != "OK")) | length),
        requests: ([.[] | select(.error_code != "OK") | (.total_requests // 0)] | add // 0),
        rows: ([.[] | select(.error_code != "OK") | (.total_rows // 0)] | add // 0),
        by_error_code: (
          [group_by(.error_code)[]
           | select(.[0].error_code != "OK")
           | {
               error_code: .[0].error_code,
               minute_buckets: length,
               requests: ([.[] | (.total_requests // 0)] | add // 0),
               rows: ([.[] | (.total_rows // 0)] | add // 0),
               input_bytes: ([.[] | (.total_input_bytes // 0)] | add // 0)
             }]
        )
      },
      pricing: {
        compute_model: "storage_write_api",
        price_usd: $rate.price_usd,
        price_unit: $rate.price_unit,
        price_unit_bytes: $rate.price_unit_bytes,
        total_cost_usd: $cost
      },
      sources: ($p.sources // [])
    }
' "$INPUT" > "$TMP_OUT"

mv "$TMP_OUT" "$OUT_FILE"

echo "Written: $OUT_FILE"
jq '{component, successful_rows, total_input_gib, total_cost_usd: .pricing.total_cost_usd}' "$OUT_FILE"
