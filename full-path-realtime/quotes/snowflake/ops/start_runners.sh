#!/bin/bash
# Start the monitoring suite, all detached (survive logout):
#   - dashboard query runner   (every 600s  -> out/dashboard_<ts>.jsonl)
#   - drilldown query runner   (every 3600s -> out/drilldown_<ts>.jsonl)
#   - MV latency tracker       (every 60s   -> out/mv_latency_<ts>.jsonl)
#   - clustering lag tracker   (every 300s  -> out/clustering_lag_<ts>.jsonl)  [clustered runs]
#   bash start_runners.sh [comment] [machine] [cluster_size]
#   e.g.  bash start_runners.sh "24h 1M EPS" "Small" 1
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench && source .venv/bin/activate
: "${SF_ACCOUNT:?set SF_ACCOUNT, e.g. export SF_ACCOUNT=ORG-ACCT}"
: "${SF_USER:?set SF_USER, e.g. export SF_USER=MYUSER}"
mkdir -p out
COMMENT="${1:-24h 1M EPS}"; MACHINE="${2:-Small}"; CLUSTER="${3:-1}"
export SF_WAREHOUSE=BENCH2COST_SMALL_GEN2
setsid nohup python run_dashboard.py --database BENCH2COST --output-dir out \
  "Snowflake (AWS)" "$MACHINE" "$CLUSTER" "$COMMENT" 0 > out/dashboard.log 2>&1 < /dev/null &
echo "dashboard runner   pid $!  (every 600s)   machine=$MACHINE cluster_size=$CLUSTER"
setsid nohup python run_drilldown.py --database BENCH2COST --output-dir out \
  "Snowflake (AWS)" "$MACHINE" "$CLUSTER" "$COMMENT" 0 > out/drilldown.log 2>&1 < /dev/null &
echo "drilldown runner   pid $!  (every 3600s)  machine=$MACHINE cluster_size=$CLUSTER"
TS=$(date -u +%Y%m%dT%H%M%SZ)
setsid nohup bash mv_latency.sh 60 "out/mv_latency_${TS}.jsonl" > out/mv_latency_run.log 2>&1 < /dev/null &
echo "mv_latency tracker pid $!  (every 60s)    -> out/mv_latency_${TS}.jsonl"
# Clustering-lag tracker: poll SYSTEM$CLUSTERING_INFORMATION depth/overlap over time.
# Run it WITHOUT a warehouse (env -u SF_WAREHOUSE) so it stays on cloud services and never
# competes with the reader warehouse — otherwise a poll could skew the dashboard latencies.
# Only meaningful on clustered runs; it just records an error per poll if no clustering key.
setsid nohup env -u SF_WAREHOUSE bash clustering_lag.sh 300 "out/clustering_lag_${TS}.jsonl" > out/clustering_lag_run.log 2>&1 < /dev/null &
echo "clustering tracker pid $!  (every 300s)   -> out/clustering_lag_${TS}.jsonl"
echo "JSONL -> /home/ubuntu/bench/out/{dashboard,drilldown,mv_latency,clustering_lag}_<ts>.jsonl"
