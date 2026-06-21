#!/bin/bash
# Launch the Snowflake ingester detached, surviving SSH disconnects.
# Usage: bash run.sh [parallel] [row_groups_per_insert] [max_files|all] [target_rps] [warehouse]
#   max_files: a number, or "all" (= every file in /data/quotes)
export PATH="$HOME/.local/bin:$PATH"
cd /home/ubuntu/bench
source .venv/bin/activate
: "${SF_ACCOUNT:?set SF_ACCOUNT, e.g. export SF_ACCOUNT=ORG-ACCT}"
: "${SF_USER:?set SF_USER, e.g. export SF_USER=MYUSER}"
pkill -9 -f ingest.py 2>/dev/null
sleep 2
rm -f /dev/shm/*.parquet 2>/dev/null
P=${1:-8}; RG=${2:-50}; MF=${3:-4}; TRPS=${4:-0}; WH=${5:-BENCH2COST_GEN2_LARGE}
SCHEMA="${SF_SCHEMA:-STOCKHOUSE}"   # target schema (export SF_SCHEMA=STOCKHOUSE_2 for the re-run)
# "all" (or 0) -> no --max-files cap (ingest every file in the dir)
MF_FLAG="--max-files $MF"
if [ "$MF" = "all" ] || [ "$MF" = "0" ]; then MF_FLAG=""; MF="all"; fi
echo "RUN START $(date) parallel=$P rgpi=$RG maxfiles=$MF target_rps=$TRPS wh=$WH schema=$SCHEMA" > /home/ubuntu/bench/ingest.log
setsid nohup python ingest.py --dir /data/quotes --schema "$SCHEMA" $MF_FLAG --parallel "$P" \
  --row-groups-per-insert "$RG" --target-rps "$TRPS" --warehouse "$WH" --live-eps-interval 15 >> /home/ubuntu/bench/ingest.log 2>&1 < /dev/null &
echo "launched pid $! (parallel=$P rgpi=$RG maxfiles=$MF target_rps=$TRPS wh=$WH schema=$SCHEMA)"
