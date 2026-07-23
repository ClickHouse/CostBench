#!/bin/bash
# =============================================================================
# Generate the T2 (streaming) comparison charts into _out/t2/ from collected results.
# Stages the Snowflake T2 result files + ClickHouse baselines into _test/, builds two derived
# inputs (the +5s interactive-timeout "fallback" dataset and the epoch-filtered MV-lag file),
# then renders the linear-axis chart set:
#   t2_dash_mvstd_vs_ch_linear        dashboard MV on the standard wh vs ClickHouse
#   t2_dash_mvstd_plus5_vs_ch_linear  + the 5s interactive timeout (fallback reality) vs ClickHouse
#   t2_drilldown_vs_ch_linear         drilldown (2-query) vs ClickHouse
#   t2_mv_lag_linear                  interactive-MV freshness lag vs ClickHouse (moving average)
#   t2_dashboard_fallback_linear      per-query: interactive execution vs 5s-timeout fallback vs CH
#   storage_ch_sf                     on-disk footprint: CH vs Snowflake IT (raw + interactive MV)
#
# Inputs (collect first): snowflake/results/t2/{dashboard_imv_iv,dashboard_raw_iv,dashboard_mv_std,
#   drilldown,mv_latency}_*.jsonl + storage.json  +  clickhouse-cloud/results/{mv/dashboard,raw/drilldown}_*.jsonl
# Run from quotes/_viz:   bash make_t2_charts.sh
# =============================================================================
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; cd "$HERE"
SF=../snowflake/results/t2
CH=../clickhouse-cloud/results_t2   # T2-specific CH baseline (co-scaled to ~113B rows)
TEST=_test; OUT=_out/t2
mkdir -p "$TEST" "$OUT"
DASH="Single-symbol summary;Watchlist summary;Top movers;Daily activity"
DRILL="Hourly OHLCV bars;Risk & liquidity (B7)"

command -v uv >/dev/null 2>&1 || { echo "ERROR: 'uv' not on PATH (needed by the renderers)." >&2; exit 1; }
newest(){ ls -t $1 2>/dev/null | head -1 || true; }
stage(){ local s; s="$(newest "$1")"; [ -n "$s" ] || { echo "ERROR: no input matches $1" >&2; exit 1; }; cp "$s" "$2"; echo "  staged $(basename "$2")  <- $(basename "$s")"; }

echo "staging inputs ..."
stage "$SF/dashboard_imv_iv_*.jsonl"  "$TEST/dash_mv_iv_snowflake.jsonl"
stage "$SF/dashboard_raw_iv_*.jsonl"  "$TEST/dash_raw_iv_snowflake.jsonl"
stage "$SF/dashboard_mv_std_*.jsonl"  "$TEST/dash_mv_std_snowflake.jsonl"
stage "$SF/drilldown_*.jsonl"         "$TEST/drill_iv_snowflake.jsonl"
stage "$CH/mv/dashboard_*.jsonl"      "$TEST/dashboard_clickhouse.jsonl"
stage "$CH/raw/drilldown_*.jsonl"     "$TEST/drilldown_clickhouse.jsonl"

echo "building derived inputs ..."
# (a) +5s per query = 5s interactive timeout, then the standard-wh execution (the fallback total)
python3 - "$TEST/dash_mv_std_snowflake.jsonl" "$TEST/dash_mv_std_plus5_snowflake.jsonl" <<'PY'
import json, sys
src, dst = sys.argv[1], sys.argv[2]
out = []
for l in open(src):
    if not l.strip():
        continue
    r = json.loads(l); r["machine"] = "MV std +5s fallback"
    r["result"] = [[(x[0] + 5.0) if (x and isinstance(x[0], (int, float))) else (x[0] if x else None)]
                   for x in r.get("result", [])]
    out.append(json.dumps(r, separators=(",", ":")))
open(dst, "w").write("\n".join(out) + "\n")
print("  built dash_mv_std_plus5_snowflake.jsonl")
PY
# (b) MV-lag: drop pre-first-refresh polls (rows==0 => behind_by is the ~56yr epoch artifact)
MVL="$(newest "$SF/mv_latency_*.jsonl")"; [ -n "$MVL" ] || { echo "ERROR: no mv_latency file" >&2; exit 1; }
python3 - "$MVL" "$TEST/mv_latency_snowflake.jsonl" <<'PY'
import json, sys
src, dst = sys.argv[1], sys.argv[2]
out = []
for l in open(src):
    if not l.strip():
        continue
    r = json.loads(l)
    try: rows = int(r.get("rows", 0))
    except Exception: rows = 0
    if rows > 0:
        out.append(l.rstrip("\n"))
open(dst, "w").write("\n".join(out) + "\n")
print(f"  built mv_latency_snowflake.jsonl ({len(out)} polls, rows>0)")
PY

echo "rendering -> $OUT"
uv run render_latency.py "$TEST/dashboard_clickhouse.jsonl" "$TEST/dash_mv_std_snowflake.jsonl" \
  --smooth 7 --no-raw --xscale linear --yscale linear --query-labels "$DASH" \
  --out "$OUT/t2_dash_mvstd_vs_ch_linear.png" \
  --title "Dashboard (vs MV) STANDARD wh vs ClickHouse — latency vs volume (linear axes)"

uv run render_latency.py "$TEST/dashboard_clickhouse.jsonl" "$TEST/dash_mv_std_plus5_snowflake.jsonl" \
  --smooth 7 --no-raw --xscale linear --yscale linear --query-labels "$DASH" \
  --out "$OUT/t2_dash_mvstd_plus5_vs_ch_linear.png" \
  --title "Dashboard MV via fallback (+5s) vs ClickHouse — latency vs volume (linear axes)"

uv run render_latency.py "$TEST/drilldown_clickhouse.jsonl" "$TEST/drill_iv_snowflake.jsonl" \
  --smooth 5 --no-raw --xscale linear --yscale linear --query-labels "$DRILL" \
  --out "$OUT/t2_drilldown_vs_ch_linear.png" \
  --title "Drilldown (vs raw) vs ClickHouse — latency vs volume (linear axes)"

uv run render_mv_lag.py "$TEST/mv_latency_snowflake.jsonl" --volume-from "$TEST/dash_mv_iv_snowflake.jsonl" \
  --smooth 61 --smooth-mode mean --no-raw --xscale linear --mv-kind "interactive MV" \
  --out "$OUT/t2_mv_lag_linear.png" \
  --title "Interactive-MV freshness lag vs data volume: Snowflake vs ClickHouse (T2 RUN8)"

uv run render_dashboard_fallback.py --interactive "$TEST/dash_mv_iv_snowflake.jsonl" \
  --standard "$TEST/dash_mv_std_snowflake.jsonl" --clickhouse "$TEST/dashboard_clickhouse.jsonl" \
  --timeout 5 --xscale linear --yscale linear --query-labels "$DASH" \
  --out "$OUT/t2_dashboard_fallback_linear.png" \
  --title "Dashboard MV: interactive execution vs 5s-timeout fallback vs ClickHouse (T2 RUN8, linear)"

# Storage — T2 is pure-streaming interactive tables (no standard-table split, unlike T1).
# ClickHouse bytes/rows are reused from the T1 run (same ~113B-row co-scaled dataset); see the
# note in snowflake/results/t2/storage.json.
if [ -f "$SF/storage.json" ]; then
  echo "rendering T2 ClickHouse vs Snowflake storage -> $OUT/storage_ch_sf.png"
  uv run render_storage.py "$SF/storage.json" --tier T2 --vendors clickhouse snowflake \
    --out "$OUT/storage_ch_sf.png" \
    --title "Storage — T2 streaming interactive tables (ClickHouse vs Snowflake)"
else
  echo "  - absent (storage chart skipped): $SF/storage.json"
fi

echo "done: charts in $OUT"
