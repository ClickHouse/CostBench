#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_path_utils.sh
source "$SCRIPT_DIR/_path_utils.sh"

usage() {
  cat >&2 <<'EOF'
Usage:
  summarize_storage.sh <table_storage.jsonl> <serverless_pricing.json> \
    <output.json> [--region us] [--raw-table quotes] \
    [--mv-table quotes_daily]

Creates separate active logical- and active physical-storage price scenarios
from the final TABLE_STORAGE_BY_PROJECT snapshot. Exactly one row for each
requested object is required, and long-term bytes must be zero.
EOF
  exit 1
}

[[ $# -ge 3 ]] || usage

INPUT=$1
PRICING_FILE=$2
OUT_FILE=$3
REGION=us
RAW_TABLE=quotes
MV_TABLE=quotes_daily
shift 3

while [[ $# -gt 0 ]]; do
  case "$1" in
    --region) REGION=$2; shift 2 ;;
    --raw-table) RAW_TABLE=$2; shift 2 ;;
    --mv-table) MV_TABLE=$2; shift 2 ;;
    *) echo "ERROR: unknown option: $1" >&2; usage ;;
  esac
done

command -v jq >/dev/null 2>&1 || { echo "ERROR: jq is required." >&2; exit 1; }
[[ -f "$INPUT" ]] || { echo "ERROR: input not found: $INPUT" >&2; exit 1; }
[[ -f "$PRICING_FILE" ]] || { echo "ERROR: pricing file not found: $PRICING_FILE" >&2; exit 1; }
jq -e \
  --arg region "$REGION" \
  '.regions[$region].pricing_storage.logical.hourly.active != null and
   .regions[$region].pricing_storage.logical.monthly.active != null and
   .regions[$region].pricing_storage.physical.hourly.active != null and
   .regions[$region].pricing_storage.physical.monthly.active != null' \
  "$PRICING_FILE" \
  >/dev/null \
  || { echo "ERROR: incomplete active storage pricing for region $REGION in $PRICING_FILE" >&2; exit 1; }

PORTABLE_INPUT="$(portable_path "$INPUT")"
PORTABLE_PRICING="$(portable_path "$PRICING_FILE")"

mkdir -p "$(dirname "$OUT_FILE")"
TMP_OUT="${OUT_FILE}.tmp"

if ! jq -s \
  --arg region "$REGION" \
  --arg raw_table "$RAW_TABLE" \
  --arg mv_table "$MV_TABLE" \
  --arg input_file "$PORTABLE_INPUT" \
  --arg pricing_file "$PORTABLE_PRICING" \
  --slurpfile pricing "$PRICING_FILE" '
  def round8: . * 100000000 | round / 100000000;
  def gib($bytes): $bytes / 1073741824;

  ($pricing[0]) as $p
  | ($p.regions[$region].pricing_storage) as $ps
  | map(select(
      .deleted != true and
      (.table_name == $raw_table or .table_name == $mv_table)
    ))
  | sort_by(.table_name)
  | . as $rows
  | if ($rows | length) != 2 or ($rows | map(.table_name) | unique | length) != 2
    then error("expected exactly one storage row for each requested table")
    else .
    end
  | ([
      $rows[]
      | select(
          (.active_logical_bytes == null) or
          (.long_term_logical_bytes == null) or
          (.active_physical_bytes == null) or
          (.long_term_physical_bytes == null) or
          (.fail_safe_physical_bytes == null)
        )
    ] | length) as $missing_metric_rows
  | if $missing_metric_rows != 0
    then error("one or more storage rows are missing required byte metrics")
    else .
    end
  | ([
      $rows[]
      | (.long_term_logical_bytes + .long_term_physical_bytes)
    ] | add // 0) as $long_term_bytes
  | if $long_term_bytes != 0
    then error("long-term bytes are nonzero; this active-only benchmark summary refuses to reclassify them")
    else .
    end
  | [
      $rows[]
      | . as $row
      | .active_logical_bytes as $logical_bytes
      | (.active_physical_bytes + .fail_safe_physical_bytes) as $physical_bytes
      | {
          object_role:
            (if .table_name == $raw_table then "raw_table" else "materialized_view" end),
          table_name,
          table_type,
          total_rows,
          storage_last_modified_time,
          long_term_logical_bytes,
          long_term_physical_bytes,
          time_travel_physical_bytes,
          fail_safe_physical_bytes,
          logical: {
            active_bytes: $logical_bytes,
            active_gib: gib($logical_bytes),
            hourly_cost_usd:
              ((gib($logical_bytes) * $ps.logical.hourly.active.price_usd) | round8),
            monthly_cost_usd:
              ((gib($logical_bytes) * $ps.logical.monthly.active.price_usd) | round8)
          },
          physical: {
            active_billable_bytes: $physical_bytes,
            active_billable_gib: gib($physical_bytes),
            hourly_cost_usd:
              ((gib($physical_bytes) * $ps.physical.hourly.active.price_usd) | round8),
            monthly_cost_usd:
              ((gib($physical_bytes) * $ps.physical.monthly.active.price_usd) | round8)
          }
        }
    ] as $objects
  | ($objects | map(.logical.active_bytes) | add) as $logical_bytes
  | ($objects | map(.physical.active_billable_bytes) | add) as $physical_bytes
  | {
      schema_version: 1,
      system: "BigQuery",
      component: "storage",
      region: $region,
      source_file: $input_file,
      pricing_file: $pricing_file,
      snapshot_semantics:
        "Final TABLE_STORAGE_BY_PROJECT point-in-time snapshot; not byte-duration usage.",
      active_only: true,
      physical_billing_formula:
        "active_physical_bytes + fail_safe_physical_bytes",
      free_tier_applied: false,
      objects: $objects,
      pricing_scenarios: {
        logical: {
          active_bytes: $logical_bytes,
          active_gib: gib($logical_bytes),
          hourly_rate_usd_per_gib: $ps.logical.hourly.active.price_usd,
          monthly_rate_usd_per_gib: $ps.logical.monthly.active.price_usd,
          hourly_cost_usd:
            ((gib($logical_bytes) * $ps.logical.hourly.active.price_usd) | round8),
          monthly_cost_usd:
            ((gib($logical_bytes) * $ps.logical.monthly.active.price_usd) | round8)
        },
        physical: {
          active_billable_bytes: $physical_bytes,
          active_billable_gib: gib($physical_bytes),
          hourly_rate_usd_per_gib: $ps.physical.hourly.active.price_usd,
          monthly_rate_usd_per_gib: $ps.physical.monthly.active.price_usd,
          hourly_cost_usd:
            ((gib($physical_bytes) * $ps.physical.hourly.active.price_usd) | round8),
          monthly_cost_usd:
            ((gib($physical_bytes) * $ps.physical.monthly.active.price_usd) | round8)
        }
      },
      sources: ($p.sources // [])
    }
' "$INPUT" > "$TMP_OUT"; then
  rm -f "$TMP_OUT"
  echo "ERROR: unable to summarize storage evidence from $INPUT" >&2
  exit 1
fi

mv "$TMP_OUT" "$OUT_FILE"

echo "Written: $OUT_FILE"
jq '{objects, pricing_scenarios}' "$OUT_FILE"
