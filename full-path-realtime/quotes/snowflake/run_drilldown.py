#!/usr/bin/env python3
"""
Drilldown runner (Snowflake) — loops the raw (QUOTES) query every --interval
seconds (default 3600, hourly), one JSONL record per iteration. Snowflake
analogue of run_drilldown.sh. Shared loop/timing/schema in runner_common.py.

Usage:
  SF_WAREHOUSE=BENCH2COST_SMALL_GEN2 \
  python3 run_drilldown.py --database BENCH2COST \
      "Snowflake (AWS)" "Small" 1 "10B rows" 0
"""
import runner_common

if __name__ == "__main__":
    runner_common.run(
        runner_name="drilldown",
        default_queries="queries_raw.sql",
        default_interval=3600,
        comment_flavor="drilldown",
    )
