#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Args
# -----------------------------
usage() {
  echo "Usage: $0 <input.jsonl> <output.json> [--iterations N]"
  echo ""
  echo "  <input.jsonl>  : JSONL file where each line is one iteration"
  echo "                   result field: [[t1],[t2],...] — one runtime per query"
  echo "  <output.json>  : summary file to write"
  echo "  --iterations N : only include the first N iterations (default: all)"
  exit 1
}

if [[ $# -lt 2 ]]; then
  usage
fi

INPUT="$1"
OUTPUT="$2"
MAX_ITER=""

shift 2
while [[ $# -gt 0 ]]; do
  case "$1" in
    --iterations|-n)
      [[ $# -ge 2 ]] || { echo "Error: --iterations requires a value"; exit 1; }
      MAX_ITER="$2"
      shift 2
      ;;
    -h|--help)
      usage
      ;;
    *)
      echo "Unknown option: $1"
      usage
      ;;
  esac
done

# -----------------------------
# Pricing tiers (ClickHouse Cloud, AWS us-east-1)
# compute = USD per 8 GiB-hour
# -----------------------------
TIERS_JSON='[
  {"name":"Basic",      "compute":0.2181100, "compute_price_unit":8, "storage":25.30, "storage_price_unit":1000000000000},
  {"name":"Scale",      "compute":0.29846,   "compute_price_unit":8, "storage":25.30, "storage_price_unit":1000000000000},
  {"name":"Enterprise", "compute":0.3903,    "compute_price_unit":8, "storage":25.30, "storage_price_unit":1000000000000}
]'

# -----------------------------
# Slice to first N iterations if requested
# -----------------------------
if [[ -n "$MAX_ITER" ]]; then
  DATA=$(head -n "$MAX_ITER" "$INPUT")
  echo "Using first $MAX_ITER iteration(s) from $INPUT"
else
  DATA=$(cat "$INPUT")
  echo "Using all iterations from $INPUT"
fi

# -----------------------------
# Compute summary with jq
# -----------------------------
echo "$DATA" | jq -s \
  --argjson tiers "$TIERS_JSON" \
  --arg input_file "$(basename "$INPUT")" \
'
  def tosec:
    if type == "number" then . else (try tonumber catch 0) // 0 end;

  .[0] as $first
  | (if ($first | has("cluster_size")) then ($first.cluster_size | tonumber) else 1 end) as $cluster
  | ($first.machine | tostring | gsub("[^0-9\\.]"; "") | if length > 0 then tonumber else 0 end) as $mem_gib
  | length as $iter_count

  # Sum every single runtime value across all iterations and all queries
  | ([.[].result[][] | tosec] | add) as $total_runtime

  | {
      source_file:           $input_file,
      system:                $first.system,
      version:               $first.version,
      machine:               $first.machine,
      mem_gib:               $mem_gib,
      cluster_size:          $cluster,
      iterations_included:   $iter_count,
      queries_per_iteration: ([$first.result | length] | .[0]),
      total_runtime_seconds: ($total_runtime | . * 1000 | round / 1000),
      costs: (
        $tiers | map(
          . as $tier
          | ($total_runtime * ($tier.compute / 3600.0) * ($mem_gib / $tier.compute_price_unit) * $cluster) as $cost
          | {
              tier:                   $tier.name,
              total_compute_cost_usd: ($cost * 100000 | round / 100000)
            }
        )
      )
    }
' > "$OUTPUT"

echo "Summary written → $OUTPUT"
cat "$OUTPUT"
