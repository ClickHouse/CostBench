#!/usr/bin/env bash
set -euo pipefail

# Fresh-target qualification of long-run journal scaling and proactive,
# fail-closed Zerobus stream rotation.

export DATABRICKS_RAW_TABLE="quotes_ingest_mv_40m_20260917"
export DATABRICKS_MV_TABLE="quotes_daily_ingest_mv_40m_20260917"
export EXPECTED_TARGET="costbench.rt_qualification.${DATABRICKS_RAW_TABLE}"
export INGEST_DURATION_SECONDS=2400
export TARGET_EPS=1000000
export NOMINAL_ROWS=2400000000
export SOURCE_SELECTION_ROWS=2500000000
export POST_INGEST_MONITOR_SECONDS=600
export MONITOR_INTERVAL_SECONDS=60
export MANUAL_STREAM_ROTATION_SECONDS=600
export RUN_KIND="ingest_mv_40m"
export LATEST_RUN_CONTEXT="/data/databricks-qualification/latest_ingest_mv_40m_run.json"

exec /home/ubuntu/costbench/full-path-realtime/quotes/databricks/run_ingest_mv_6h.sh
