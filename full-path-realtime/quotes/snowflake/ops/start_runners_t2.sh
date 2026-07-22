#!/bin/bash
# Start the T2 (streaming) READ runners, detached (survive logout). The T2 analogue of
# ops/start_runners_it.sh:
#   - dashboard #1 (600s)  -> 4 q vs QUOTES_DAILY_IMV @ interactive wh  (t2/queries_mv_imv.sql)
#   - drilldown #2 (3600s) -> 2 q vs QUOTES_IT        @ interactive wh  (t2/queries_raw_it.sql)
#   - dashboard #3 (600s)  -> 4 q vs QUOTES_IT (raw)  @ interactive wh  (t2/queries_dashboard_raw.sql)
#   - dashboard #4 (600s)  -> 4 q vs QUOTES_DAILY_IMV @ STANDARD wh BENCH2COST_GEN2_SMALL_DASH
#       (#1 vs #4 = same MV dashboard on interactive vs standard compute; #1 vs #3 = MV-backed vs
#        on-the-fly-from-raw on the same interactive wh. #1/#2/#3 SHARE the one interactive read wh,
#        so their latencies interfere — expected, they are one dashboard box.)
#
# Reads run on the interactive warehouse SNOWPIPES_IT_READ_SMALL; tracking (row counts / timing
# lookups) on BENCH. Output -> out_t2/.
#
# Ingest is SEPARATE and serverless: run t2/stream_quotes.py (the T2 analogue of run.sh). Instead of
# the it_refresh tracker (interactive tables), T2 runs the MV-lag poller (ops/mv_latency.sh) against
# the interactive MV QUOTES_DAILY_IMV — its live freshness signal is `behind_by` from SHOW
# MATERIALIZED VIEWS (metadata-only, ~free). Post-hoc credits/refresh history are still under
# MATERIALIZED_VIEW_REFRESH_HISTORY (metrics_reference.sql §4).
#
#   bash ops/start_runners_t2.sh [comment] [machine] [cluster_size]
#   e.g.  bash ops/start_runners_t2.sh "T2 streaming 1M EPS" "IT (stream)" 1
#   Stop with: bash ops/stop_experiment.sh  (and pkill -f stream_quotes.py for the streamer)
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench && source .venv/bin/activate
: "${SF_ACCOUNT:?set SF_ACCOUNT}"
: "${SF_USER:?set SF_USER}"
COMMENT="${1:-T2 streaming}"; MACHINE="${2:-IT (stream)}"; CLUSTER="${3:-1}"
OUTDIR="${OUTDIR:-out_t2}"; mkdir -p "$OUTDIR"

export SF_SCHEMA="${SF_SCHEMA:-STOCKHOUSE_T2}"
export SF_WAREHOUSE="${SF_WAREHOUSE:-SNOWPIPES_IT_READ_SMALL}"   # measured: interactive read path
export SF_TRACK_WAREHOUSE="${SF_TRACK_WAREHOUSE:-BENCH}"         # tracking: counts + timing lookups
export SF_RAW_TABLE=QUOTES_IT
export SF_MV_TABLE=QUOTES_DAILY_IMV

DTS="$(date -u +%Y%m%dT%H%M%SZ)"

# #1 dashboard vs MV (QUOTES_DAILY_IMV) on the interactive read wh
setsid nohup python run_dashboard.py --database BENCH2COST --output-dir "$OUTDIR" \
  --queries t2/queries_mv_imv.sql --output "$OUTDIR/dashboard_imv_iv_${DTS}.jsonl" \
  "Snowflake IT (AWS)" "MV iv" "$CLUSTER" "$COMMENT" 0 > "$OUTDIR/dashboard_imv_iv.log" 2>&1 < /dev/null &
echo "dashboard #1 pid $!  (600s)  QUOTES_DAILY_IMV @ $SF_WAREHOUSE (iv)"

# #3 dashboard computed on the fly from RAW QUOTES_IT on the interactive read wh
setsid nohup python run_dashboard.py --database BENCH2COST --output-dir "$OUTDIR" \
  --queries t2/queries_dashboard_raw.sql --output "$OUTDIR/dashboard_raw_iv_${DTS}.jsonl" \
  "Snowflake IT (AWS)" "raw iv" "$CLUSTER" "$COMMENT" 0 > "$OUTDIR/dashboard_raw_iv.log" 2>&1 < /dev/null &
echo "dashboard #3 pid $!  (600s)  QUOTES_IT raw @ $SF_WAREHOUSE (iv)"

# #4 dashboard vs MV (QUOTES_DAILY_IMV) on a STANDARD Gen2 Small wh (SF_WAREHOUSE override)
SF_WAREHOUSE=BENCH2COST_GEN2_SMALL_DASH setsid nohup python run_dashboard.py --database BENCH2COST --output-dir "$OUTDIR" \
  --queries t2/queries_mv_imv.sql --output "$OUTDIR/dashboard_mv_std_${DTS}.jsonl" \
  "Snowflake (AWS)" "MV std" "$CLUSTER" "$COMMENT" 0 > "$OUTDIR/dashboard_mv_std.log" 2>&1 < /dev/null &
echo "dashboard #4 pid $!  (600s)  QUOTES_DAILY_IMV @ BENCH2COST_GEN2_SMALL_DASH (std)"

setsid nohup python run_drilldown.py --database BENCH2COST --output-dir "$OUTDIR" \
  --queries t2/queries_raw_it.sql \
  "Snowflake IT (AWS)" "$MACHINE" "$CLUSTER" "$COMMENT" 0 > "$OUTDIR/drilldown_t2.log" 2>&1 < /dev/null &
echo "drilldown runner pid $!  (3600s)  QUOTES_IT @ $SF_WAREHOUSE        (track: $SF_TRACK_WAREHOUSE)"

# MV-lag tracker: poll QUOTES_DAILY_IMV behind_by every 60s (metadata-only, no warehouse)
MVTS="$(date -u +%Y%m%dT%H%M%SZ)"
setsid nohup bash ops/mv_latency.sh 60 "$OUTDIR/mv_latency_${MVTS}.jsonl" \
  > "$OUTDIR/mv_latency_${MVTS}.log" 2>&1 < /dev/null &
echo "mv_latency tracker pid $!  (60s)   $SF_MV_TABLE behind_by -> $OUTDIR/mv_latency_${MVTS}.jsonl"

# Clustering-depth tracker: SYSTEM$CLUSTERING_INFORMATION on QUOTES_IT (sym,t) AND the rollup
# QUOTES_DAILY_IMV (sym,day) every 300s. Point-in-time only (never historized) -> must sample live.
# Runs on the TRACKING wh (not the measured read wh) so it doesn't perturb read latency.
CLTS="$(date -u +%Y%m%dT%H%M%SZ)"
SF_WAREHOUSE="$SF_TRACK_WAREHOUSE" setsid nohup bash ops/clustering_lag.sh 300 "$OUTDIR/clustering_lag_${CLTS}.jsonl" \
  > "$OUTDIR/clustering_lag_${CLTS}.log" 2>&1 < /dev/null &
echo "clustering tracker pid $!  (300s)  $SF_RAW_TABLE + $SF_MV_TABLE avg_depth -> $OUTDIR/clustering_lag_${CLTS}.jsonl"

echo "JSONL -> $OUTDIR/{dashboard_imv_iv,dashboard_raw_iv,dashboard_mv_std,drilldown,mv_latency,clustering_lag}_<ts>.jsonl"
echo "Stop with: bash ops/stop_experiment.sh  (+ pkill -f stream_quotes.py to stop the streamer)"
