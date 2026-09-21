#!/usr/bin/env python3
"""Thin entry point for the four-query Databricks dashboard workload."""

from pathlib import Path
from typing import Sequence

from runner_common import runner_main

WORKLOAD = "dashboard"
DEFAULT_INTERVAL_SECONDS = 600.0
DEFAULT_QUERY_FILE = Path(__file__).resolve().with_name("queries_mv.sql")


def main(argv: Sequence[str] | None = None) -> int:
    return runner_main(
        WORKLOAD,
        DEFAULT_QUERY_FILE,
        DEFAULT_INTERVAL_SECONDS,
        argv,
    )


if __name__ == "__main__":
    raise SystemExit(main())
