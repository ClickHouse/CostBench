#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Plot ClickHouse and BigQuery raw-table drill-down latency versus raw rows.

This dedicated entry point uses the same audited loading, validation, plotting,
CSV, and provenance machinery as the aggregate-query chart. The two drill-down
queries are rendered in their SQL-file order without smoothing or cross-system
iteration matching.
"""

from __future__ import annotations

import plot_aggregate_query_latency as renderer

renderer.QUERY_NAMES = (
    "Hourly OHLCV bars",
    "Risk & liquidity (B7)",
)
renderer.DEFAULT_BASENAME = "drilldown_query_latency_clickhouse_vs_bigquery"
renderer.CHART_KEY = "clickhouse_vs_bigquery_drilldown_query_latency"
renderer.CLICKHOUSE_QUERY_FILE = "queries_raw.sql"
renderer.BIGQUERY_QUERY_FILE = "queries_raw.sql"


if __name__ == "__main__":
    raise SystemExit(renderer.main())
