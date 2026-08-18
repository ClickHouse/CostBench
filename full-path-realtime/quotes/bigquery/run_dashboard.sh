#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "${SCRIPT_DIR}/run_queries.py" \
  --runner-name dashboard \
  --queries "${SCRIPT_DIR}/queries_mv.sql" \
  --interval 600 \
  "$@"
