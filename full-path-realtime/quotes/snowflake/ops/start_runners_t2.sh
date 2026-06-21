#!/bin/bash
# Start the T2 (streaming) READ runners, detached (survive logout). The T2 analogue of
# ops/start_runners_it.sh:
#   - dashboard runner (600s)  -> 4 queries vs QUOTES_DAILY_IMV  (t2/queries_mv_imv.sql)
#   - drilldown runner (3600s) -> 2 queries vs QUOTES_IT         (t2/queries_raw_it.sql)
#
# Reads run on the interactive warehouse SNOWPIPES_IT_READ_SMALL; tracking (row counts / timing
# lookups) on BENCH. Output -> out_t2/.
#
# Ingest is SEPARATE and serverless: run t2/stream_quotes.py (the T2 analogue of run.sh). There's no
# it_refresh tracker here — the IMV's refresh history is under MATERIALIZED_VIEW_REFRESH_HISTORY
# (see metrics_reference.sql §4), not INTERACTIVE_TABLE_REFRESH_HISTORY.
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

setsid nohup python run_dashboard.py --database BENCH2COST --output-dir "$OUTDIR" \
  --queries t2/queries_mv_imv.sql \
  "Snowflake IT (AWS)" "$MACHINE" "$CLUSTER" "$COMMENT" 0 > "$OUTDIR/dashboard_t2.log" 2>&1 < /dev/null &
echo "dashboard runner pid $!  (600s)   QUOTES_DAILY_IMV @ $SF_WAREHOUSE  (track: $SF_TRACK_WAREHOUSE)"

setsid nohup python run_drilldown.py --database BENCH2COST --output-dir "$OUTDIR" \
  --queries t2/queries_raw_it.sql \
  "Snowflake IT (AWS)" "$MACHINE" "$CLUSTER" "$COMMENT" 0 > "$OUTDIR/drilldown_t2.log" 2>&1 < /dev/null &
echo "drilldown runner pid $!  (3600s)  QUOTES_IT @ $SF_WAREHOUSE        (track: $SF_TRACK_WAREHOUSE)"

echo "JSONL -> $OUTDIR/{dashboard,drilldown}_<ts>.jsonl"
echo "Stop with: bash ops/stop_experiment.sh  (+ pkill -f stream_quotes.py to stop the streamer)"
