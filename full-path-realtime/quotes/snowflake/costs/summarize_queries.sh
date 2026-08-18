#!/usr/bin/env bash
set -euo pipefail

# Stable shell entry point for Snowflake normalized-query-cost summaries.
# The implementation lives in Python so mixed Interactive/fallback attribution
# can be validated and preserved in structured provenance.

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$SCRIPT_DIR/summarize_queries.py" "$@"
