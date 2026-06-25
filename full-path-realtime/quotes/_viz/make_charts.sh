#!/bin/bash
# =============================================================================
# Generate ALL charts for one benchmark (t0 | t1 | t2) into _out/<tn>.
# Stages the collected result files from ../snowflake/results/<tn>/ + the ClickHouse
# baseline (../clickhouse-cloud/results/) into _test/ under the names generate.py
# expects, then runs generate.py --out-dir _out/<tn>.
#
#   bash make_charts.sh t1
#
# Inputs expected in ../snowflake/results/<tn>/ (collect from the box first — see runbook §5/§5a):
#   drilldown_*.jsonl   query latency, raw IT / standard table       (REQUIRED)
#   dashboard_*.jsonl   query latency, rollup                        (optional)
#   it_refresh.csv      INTERACTIVE_TABLE_REFRESH_HISTORY dump        (t1/t2 freshness lag; ops/collect_it_refresh.sh)
#   storage.json        {raw:[{system,bytes,rows}], mv:[...]}         (storage chart; ops/collect_storage.sh)
# Optional inputs that are absent => those charts are skipped (reported below).
# =============================================================================
set -uo pipefail
TN="${1:-}"
case "$TN" in
  t0|t1|t2|t0v*|t1v*|t2v*) ;;
  *) echo "usage: bash make_charts.sh <t0|t1|t2|t1v2 ...>" >&2; exit 2 ;;
esac

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"     # _viz/
TEST="$HERE/_test"; OUT="$HERE/_out/$TN"
SF="$HERE/../snowflake/results/$TN"
CH="$HERE/../clickhouse-cloud/results"

# --- preflight checks --------------------------------------------------------
fail=0
command -v uv >/dev/null 2>&1 || {
  echo "ERROR: 'uv' is not on PATH — needed to run the renderers." >&2
  echo "       install it (https://docs.astral.sh/uv/), e.g.  pip install uv   or   brew install uv" >&2
  fail=1; }
[ -f "$HERE/generate.py" ] || { echo "ERROR: generate.py not found in $HERE — run this from the repo's quotes/_viz." >&2; fail=1; }
[ -d "$SF" ] || { echo "ERROR: results dir not found: $SF" >&2
  echo "       collect this benchmark's JSONL into quotes/snowflake/results/$TN/ first (runbook §5)." >&2; fail=1; }
[ -d "$CH" ] || { echo "ERROR: ClickHouse baseline dir not found: $CH" >&2; fail=1; }
[ "$fail" -eq 0 ] || exit 1
mkdir -p "$TEST" "$OUT"

newest() { ls -t $1 2>/dev/null | head -1; }   # newest file matching a glob (unquoted on purpose)
staged=0
# stage <glob> <dest> — copy newest match; charts whose inputs are absent are skipped by generate.py.
# Everything is optional so partially-collected benchmarks still render what's available (e.g. T0
# pre-populated with dashboard/mv_lag/storage while the new drilldown is still being re-run).
stage() {
  local glob="$1" dest="$2" src
  src="$(newest "$glob")"
  if [ -n "$src" ]; then
    cp "$src" "$dest"; echo "  ✓ ${src#$HERE/}  ->  $(basename "$dest")"; staged=$((staged+1))
  else
    echo "  – absent (chart skipped): $glob"
  fi
}

echo "staging inputs for $TN ..."
stage "$CH/raw/drilldown_*.jsonl" "$TEST/drilldown_clickhouse.jsonl"
stage "$CH/mv/dashboard_*.jsonl"  "$TEST/dashboard_clickhouse.jsonl"
case "$TN" in
  t0)
    stage "$SF/drilldown_*.jsonl" "$TEST/drilldown_snowflake.jsonl"
    stage "$SF/dashboard_*.jsonl" "$TEST/dashboard_snowflake.jsonl"
    stage "$SF/mv_latency*.jsonl" "$TEST/mv_latency_snowflake.jsonl"
    stage "$SF/storage.json"      "$TEST/storage.json"
    VENDORS="clickhouse snowflake databricks"
    CHARTS="query_latency dashboard dashboard_smooth drilldown drilldown_smooth mv_lag storage" ;;
  t1|t2|t1v*|t2v*)
    stage "$SF/drilldown_*.jsonl" "$TEST/drilldown_snowflake_it.jsonl"
    stage "$SF/dashboard_*.jsonl" "$TEST/dashboard_snowflake_it.jsonl"
    stage "$SF/it_refresh.csv"    "$TEST/it_refresh_snowflake_it.csv"
    stage "$SF/storage.json"      "$TEST/storage.json"
    VENDORS="clickhouse snowflake"
    CHARTS="it_query_latency it_dashboard_smooth it_drilldown_smooth it_lag storage" ;;
esac

VENDORS="${VENDORS:-clickhouse snowflake}"

if [ "$staged" -eq 0 ]; then
  echo "ERROR: no inputs found to stage — collect results into $SF (and the CH baseline into $CH) first." >&2
  exit 1
fi

if [ -f "$TEST/storage.json" ] && grep -q '"bytes": *null' "$TEST/storage.json"; then
  echo "  ⚠ storage.json has placeholder(s) with null bytes (e.g. ClickHouse) — those bars are SKIPPED."
  echo "    Fill the missing bytes/rows in quotes/snowflake/results/$TN/storage.json manually (runbook §5a),"
  echo "    e.g. ClickHouse: sum(bytes_on_disk) + sum(rows) from system.parts on quotes / quotes_daily."
fi

echo "rendering -> $OUT"
TIER_ARG=()
case "$TN" in
  t0) TIER_ARG=(--benchmark-tier T0) ;;
  t1|t2|t1v*|t2v*) TIER_ARG=(--benchmark-tier T1) ;;
esac
uv run generate.py --vendors $VENDORS --charts $CHARTS --out-dir "$OUT" "${TIER_ARG[@]}"

# Combined T0 vs T1 storage comparison (when both result dirs exist).
SF_T0="$HERE/../snowflake/results/t0/storage.json"
SF_T1="$HERE/../snowflake/results/t1/storage.json"
if [ -f "$SF_T0" ] && [ -f "$SF_T1" ]; then
  echo "rendering combined storage comparison -> $HERE/_out/storage_t0_t1.png"
  uv run render_storage.py --compare \
    "T0=$SF_T0" "T1=$SF_T1" \
    --out "$HERE/_out/storage_t0_t1.png"
fi

if [ "$TN" = "t0" ] && [ -f "$SF_T0" ]; then
  echo "rendering T0 ClickHouse vs Snowflake storage -> $OUT/storage_ch_sf.png"
  uv run render_storage.py "$SF_T0" --tier T0 --vendors clickhouse snowflake \
    --out "$OUT/storage_ch_sf.png" \
    --title "Storage — T0 standard MV (ClickHouse vs Snowflake)"
  echo "rendering T0 ClickHouse vs Databricks storage -> $OUT/storage_ch_db.png"
  uv run render_storage.py "$SF_T0" --tier T0 --vendors clickhouse databricks \
    --out "$OUT/storage_ch_db.png" \
    --title "Storage — T0 standard MV (ClickHouse vs Databricks)"
fi

if [[ "$TN" == t1* ]] && [ -f "$SF_T1" ]; then
  echo "rendering T1 ClickHouse vs Snowflake (split) storage -> $OUT/storage_ch_sf.png"
  uv run render_storage.py "$SF_T1" --tier T1 --vendors clickhouse snowflake \
    --split-snowflake \
    --out "$OUT/storage_ch_sf.png" \
    --title "Storage — T1 interactive tables (ClickHouse vs Snowflake)"
fi

echo "done: charts in $OUT  (any 'skip' lines above = optional input not collected yet)"
