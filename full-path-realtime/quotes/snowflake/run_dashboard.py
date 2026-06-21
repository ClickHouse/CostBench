#!/usr/bin/env python3
"""
Dashboard runner (Snowflake) — loops the MV (QUOTES_DAILY) queries every
--interval seconds (default 600), one JSONL record per iteration. Snowflake
analogue of run_dashboard.sh. Shared loop/timing/schema in runner_common.py.

Usage:
  SF_WAREHOUSE=BENCH2COST_SMALL_GEN2 \
  python3 run_dashboard.py --database BENCH2COST \
      "Snowflake (AWS)" "Small" 1 "10B rows" 0
"""
import runner_common

if __name__ == "__main__":
    runner_common.run(
        runner_name="dashboard",
        default_queries="queries_mv.sql",
        default_interval=600,
        comment_flavor="dashboard",
    )
