#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Plot ClickHouse and Snowflake drill-down query latency versus raw rows."""

from plot_aggregate_query_latency import main
import plot_aggregate_query_latency as chart

chart.QUERY_NAMES = ("Hourly OHLCV bars", "Risk & liquidity (B7)")
chart.DEFAULT_BASENAME = "drilldown_query_latency_clickhouse_vs_snowflake"
chart.CHART_KEY = "clickhouse_vs_snowflake_drilldown_query_latency"

if __name__ == "__main__":
    raise SystemExit(main())
