#!/bin/bash
# Start the INTERACTIVE-TABLE monitoring suite for a benchmark, all detached (survive logout):
#   - dashboard runner (every 600s)  -> 4 queries vs QUOTES_DAILY_IT  (queries_mv_it.sql)
#   - drilldown runner (every 3600s) -> 2 queries vs QUOTES_IT        (queries_raw_it.sql)
#   - it_refresh tracker (every 60s) -> refresh duration/lag per IT
#
# Timed dashboard/drilldown queries run on the INTERACTIVE warehouse BENCH2COST_IT_SMALL (the
# measured read path, 5s query timeout -> recorded as "timeout"). ALL tracking (row counts,
# QUERY_HISTORY timing lookups, refresh-history polling) runs on BENCH so it adds no load/cost
# to the measured warehouses.
#
# Requires SF_SCHEMA (e.g. STOCKHOUSE_T1). Output dir is derived per-benchmark from the schema
# suffix (STOCKHOUSE_T1 -> out_t1), overridable with OUTDIR=...
#   SF_SCHEMA=STOCKHOUSE_T1 bash start_runners_it.sh [comment] [machine] [cluster_size]
#   e.g.  SF_SCHEMA=STOCKHOUSE_T1 bash start_runners_it.sh "T1 interactive" "IT Small" 1
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench && source .venv/bin/activate
: "${SF_ACCOUNT:?set SF_ACCOUNT, e.g. export SF_ACCOUNT=ORG-ACCT}"
: "${SF_USER:?set SF_USER, e.g. export SF_USER=MYUSER}"
: "${SF_SCHEMA:?set SF_SCHEMA, e.g. export SF_SCHEMA=STOCKHOUSE_T1}"
COMMENT="${1:-IT probe}"; MACHINE="${2:-IT Small}"; CLUSTER="${3:-1}"
# per-benchmark output dir from the schema suffix: STOCKHOUSE_T1 -> out_t1
OUTDIR="${OUTDIR:-out_$(printf '%s' "${SF_SCHEMA##*_}" | tr 'A-Z' 'a-z')}"
mkdir -p "$OUTDIR"

export SF_WAREHOUSE=BENCH2COST_IT_SMALL    # measured: timed dashboard/drilldown queries
export SF_TRACK_WAREHOUSE=BENCH            # tracking: counts + timing lookups (not measured)
export SF_MV_TABLE=QUOTES_DAILY_IT         # mv_rows volume count target
export SF_RAW_TABLE=QUOTES                 # raw_rows volume count target (base table)
TS=$(date -u +%Y%m%dT%H%M%SZ)

setsid nohup python run_dashboard.py --database BENCH2COST --output-dir "$OUTDIR" \
  --queries queries_mv_it.sql \
  "Snowflake IT (AWS)" "$MACHINE" "$CLUSTER" "$COMMENT" 0 > "$OUTDIR/dashboard_it.log" 2>&1 < /dev/null &
echo "dashboard runner   pid $!  (600s)   QUOTES_DAILY_IT @ BENCH2COST_IT_SMALL  (track: BENCH)  schema=$SF_SCHEMA"

setsid nohup python run_drilldown.py --database BENCH2COST --output-dir "$OUTDIR" \
  --queries queries_raw_it.sql \
  "Snowflake IT (AWS)" "$MACHINE" "$CLUSTER" "$COMMENT" 0 > "$OUTDIR/drilldown_it.log" 2>&1 < /dev/null &
echo "drilldown runner   pid $!  (3600s)  QUOTES_IT @ BENCH2COST_IT_SMALL        (track: BENCH)  schema=$SF_SCHEMA"

setsid nohup bash it_refresh.sh 60 "$OUTDIR/it_refresh_${TS}.jsonl" > "$OUTDIR/it_refresh_run.log" 2>&1 < /dev/null &
echo "it_refresh tracker pid $!  (60s)    refresh duration/lag per IT @ BENCH    -> $OUTDIR/it_refresh_${TS}.jsonl"

echo "JSONL -> $OUTDIR/{dashboard,drilldown}_<ts>.jsonl + it_refresh_${TS}.jsonl"
echo "Stop with: bash ops/stop_experiment.sh"
