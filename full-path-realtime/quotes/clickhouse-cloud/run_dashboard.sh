#!/usr/bin/env bash
# ClickHouse dashboard query runner: MV queries every 10 minutes by default.
# Shared execution, JSONL, and fixed-rate scheduling logic lives in runner_common.sh.

set -uo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
RUNNER_NAME="dashboard"
DEFAULT_QUERIES_FILE="${SCRIPT_DIR}/queries_mv.sql"
DEFAULT_INTERVAL=600
COMMENT_FLAVOR="dashboard"

source "${SCRIPT_DIR}/runner_common.sh"
runner_main "$@"
