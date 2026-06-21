#!/bin/bash
# Collect IT refresh lag from the bench box, copy back, and render the it_lag chart.
# Charts are saved to _viz/_out/t1_lag_snapshots/<timestamp>/ for tracking over time.
#
#   bash ops/collect_and_chart_lag.sh
set -uo pipefail

KEY="${SF_KEY:-$HOME/.ssh/ch_key}"
HOST="ubuntu@ec2-15-188-86-128.eu-west-3.compute.amazonaws.com"
SCHEMA="${SF_SCHEMA:-STOCKHOUSE_T1v2}"
TN="$(printf '%s' "${SCHEMA##*_}" | tr 'A-Z' 'a-z')"   # t1v2
RESULTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/results/$TN"
mkdir -p "$RESULTS_DIR"
VIZ_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../_viz" && pwd)"
TS="$(date -u +%Y%m%dT%H%M%SZ)"

echo "==> collecting IT refresh history on box (schema=$SCHEMA) ..."
ssh -i "$KEY" "$HOST" "source bench/.sfenv && SF_SCHEMA=$SCHEMA bash bench/ops/collect_it_refresh.sh"

echo "==> copying out_${TN}/it_refresh.csv -> results/${TN}/it_refresh.csv ..."
scp -i "$KEY" "$HOST:bench/out_${TN}/it_refresh.csv" "$RESULTS_DIR/it_refresh.csv"

echo "==> rendering charts ..."
cd "$VIZ_DIR"
bash make_charts.sh "$TN"

echo "==> snapshotting -> _out/${TN}_lag_snapshots/$TS ..."
SNAP="$VIZ_DIR/_out/${TN}_lag_snapshots/$TS"
mkdir -p "$SNAP"
cp "$VIZ_DIR/_out/$TN/"*.png "$SNAP/"

echo "done: $SNAP"
ls "$SNAP"
