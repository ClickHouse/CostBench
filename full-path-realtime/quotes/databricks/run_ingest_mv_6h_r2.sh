#!/usr/bin/env bash
set -euo pipefail

# Six-hour retry using the journal and fail-closed stream-rotation settings
# accepted by ingest_mv_40m_20260917T090123Z.

export DATABRICKS_RAW_TABLE="quotes_ingest_mv_6h_r2_20260917"
export DATABRICKS_MV_TABLE="quotes_daily_ingest_mv_6h_r2_20260917"
export EXPECTED_TARGET="costbench.rt_qualification.${DATABRICKS_RAW_TABLE}"
export INGEST_DURATION_SECONDS=21600
export TARGET_EPS=1000000
export NOMINAL_ROWS=21600000000
export SOURCE_SELECTION_ROWS=22000000000
export POST_INGEST_MONITOR_SECONDS=900
export MONITOR_INTERVAL_SECONDS=60
export MANUAL_STREAM_ROTATION_SECONDS=600
export RUN_KIND="ingest_mv_6h_r2"
export LATEST_RUN_CONTEXT="/data/databricks-qualification/latest_ingest_mv_6h_r2_run.json"

exec /home/ubuntu/costbench/full-path-realtime/quotes/databricks/run_ingest_mv_6h.sh
