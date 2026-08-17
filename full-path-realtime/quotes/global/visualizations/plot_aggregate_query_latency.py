#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib==3.10.9"]
# ///
"""Convenience wrapper for the global aggregate-query chart."""
from __future__ import annotations
import sys
from plot_query_latency import main

if __name__ == "__main__":
    sys.argv[1:1] = ["--workload", "aggregate"]
    raise SystemExit(main())
