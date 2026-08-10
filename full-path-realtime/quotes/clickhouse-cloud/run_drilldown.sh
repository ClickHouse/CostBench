#!/usr/bin/env bash
# ClickHouse drill-down query runner: raw-table queries every hour by default.
# Shared execution, JSONL, and fixed-rate scheduling logic lives in runner_common.sh.

set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
RUNNER_NAME="drilldown"
DEFAULT_QUERIES_FILE="${SCRIPT_DIR}/queries_raw.sql"
DEFAULT_INTERVAL=3600
COMMENT_FLAVOR="drilldown"

source "${SCRIPT_DIR}/runner_common.sh"
runner_main "$@"
