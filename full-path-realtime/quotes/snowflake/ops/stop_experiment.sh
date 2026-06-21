#!/bin/bash
# Stop ingest + both query runners + the MV latency + clustering lag trackers.
pkill -9 -f ingest.py 2>/dev/null && echo "stopped ingest"     || echo "ingest not running"
pkill -9 -f run_dashboard.py    2>/dev/null && echo "stopped dashboard"  || echo "dashboard not running"
pkill -9 -f run_drilldown.py    2>/dev/null && echo "stopped drilldown"  || echo "drilldown not running"
pkill -9 -f mv_latency          2>/dev/null && echo "stopped mv_latency" || echo "mv_latency not running"
pkill -9 -f clustering_lag      2>/dev/null && echo "stopped clustering_lag" || echo "clustering_lag not running"
pkill -9 -f it_refresh          2>/dev/null && echo "stopped it_refresh"  || echo "it_refresh not running"
