#!/bin/bash
# =============================================================================
# Start a timed Redshift T2 run. Run reset_run.py FIRST (fresh topic + empty MVs + datashare).
#
# START ORDER MATTERS: controller and read-runners come up BEFORE the producer, so the very first
# rows are already being refreshed and queried — otherwise iteration 1 measures an idle system and
# the initial MV build lands inside the steady-state numbers.
#
#   writer  (128 RPU) : monitor_lag.py  — freshness sampling + both child refreshes
#   reader  ( 32 RPU) : runner_redshift.py x2 — dashboard, and the two drilldowns BACK-TO-BACK
#   producer          : produce_quotes.py -> MSK (24 partitions)
#
# The two drilldown suites run in ONE process (--role drilldown_typed,drilldown_super) so they
# execute sequentially inside an iteration: same data volume stamped on both, and they never contend
# on the reader. Do NOT split them into two processes with a delay — a staggered pair compares
# different volumes, and a concurrent pair contends.
#
# The reader is NOT publicly accessible (a public consumer cannot read datashare objects), so this
# must run from inside the VPC — i.e. on the producer box.
#
#   bash start_run.sh            # ~31 h for the full 113B-row dataset at 1M EPS
# Env: .env in this directory must define RS_USER / RS_PASSWORD (+ optional RS_DB).
# =============================================================================
set -u
cd "$(dirname "$0")" || exit 1
[ -f .env ] || { echo "ERROR: .env with RS_USER/RS_PASSWORD not found" >&2; exit 1; }
set -a; . ./.env; set +a

WRITER=cb-quotes-rt-wg.244449518788.eu-west-2.redshift-serverless.amazonaws.com
READER=cb-quotes-rt-reader-wg.244449518788.eu-west-2.redshift-serverless.amazonaws.com
BOOT="b-1.cbquotesrtmsk.3tt8fa.c2.kafka.eu-west-2.amazonaws.com:9092,b-2.cbquotesrtmsk.3tt8fa.c2.kafka.eu-west-2.amazonaws.com:9092,b-3.cbquotesrtmsk.3tt8fa.c2.kafka.eu-west-2.amazonaws.com:9092"
PY=$PWD/.venv/bin/python

DASH_INTERVAL=${DASH_INTERVAL:-600}     # dashboard: every 10 min (matches Snowflake T2)
DRILL_INTERVAL=${DRILL_INTERVAL:-3600}  # drilldowns: hourly    (matches Snowflake T2)
TYPED_DELAY=${TYPED_DELAY:-2}           # quotes_typed: continuous, this gap after each refresh
DAILY_INTERVAL=${DAILY_INTERVAL:-60}    # quotes_daily: fixed-rate
TARGET_RPS=${TARGET_RPS:-1000000}
PARALLEL=${PARALLEL:-16}

TS=$(date -u +%Y%m%dT%H%M%SZ)
OUT="run_t2/$TS"; mkdir -p "$OUT"
# labels land in every JSONL record (shared schema with the Snowflake/ClickHouse runners)
LBL=(redshift "Redshift Serverless w128r32" "w128/r32" "T2 quotes" "MSK-3x-m7g.xlarge-24part")

{
  echo "run          : $TS"
  echo "writer       : $WRITER (128 RPU base=max)"
  echo "reader       : $READER (32 RPU base=max)"
  echo "MSK topic    : quotes, 24 partitions, RF3, retention 30min/8GiB per partition"
  echo "streaming MV : quotes_streamed, SUPER payload + typed sym/t, DISTSTYLE EVEN SORTKEY (sym,t)"
  echo "children     : quotes_typed (continuous, ${TYPED_DELAY}s gap), quotes_daily (${DAILY_INTERVAL}s)"
  echo "reads        : dashboard ${DASH_INTERVAL}s; drilldown typed+super back-to-back ${DRILL_INTERVAL}s"
  echo "producer     : ${PARALLEL} procs, target ${TARGET_RPS} rows/s"
} > "$OUT/RUN_INFO.txt"
cat "$OUT/RUN_INFO.txt"

# ---- 1. writer: refresh controller + freshness monitor ----
RS_HOST=$WRITER nohup $PY monitor_lag.py \
    --typed-delay "$TYPED_DELAY" --daily-interval "$DAILY_INTERVAL" --lag-interval 60 \
    --output-dir "$OUT" > "$OUT/controller.log" 2>&1 &
echo "controller        pid=$!"

# ---- 2. reader: dashboard, and both drilldowns back-to-back ----
RS_HOST=$READER RS_SEARCH_PATH=shared_public nohup $PY runner_redshift.py \
    --role dashboard --interval "$DASH_INTERVAL" --output-dir "$OUT" \
    "${LBL[0]}" "${LBL[1]}" "${LBL[2]}" "${LBL[3]}" "${LBL[4]}" \
    > "$OUT/run_dashboard.log" 2>&1 &
echo "dashboard         pid=$!"

RS_HOST=$READER RS_SEARCH_PATH=shared_public nohup $PY runner_redshift.py \
    --role drilldown_typed,drilldown_super --interval "$DRILL_INTERVAL" --output-dir "$OUT" \
    "${LBL[0]}" "${LBL[1]}" "${LBL[2]}" "${LBL[3]}" "${LBL[4]}" \
    > "$OUT/run_drilldowns.log" 2>&1 &
echo "drilldowns (b2b)  pid=$!"

# ---- 3. producer LAST ----
nohup $PY produce_quotes.py --bootstrap "$BOOT" --topic quotes --dir /data/quotes \
    --parallel "$PARALLEL" --target-rps "$TARGET_RPS" > "$OUT/producer.log" 2>&1 &
echo "producer          pid=$!"

sleep 25
echo
echo "=== processes ==="
ps -eo pid,etime,args --no-headers | grep -E "monitor_lag\.py|runner_redshift\.py|produce_quotes\.py" \
  | grep -v "bash -c" | grep -v grep | sed 's/\(.\{110\}\).*/\1/'
echo
echo "RUN DIR: $OUT"
echo "watch:  tail -f $OUT/controller.log   |   tail -f $OUT/run_drilldowns.log"
echo "STOP:   bash stop_run.sh"
