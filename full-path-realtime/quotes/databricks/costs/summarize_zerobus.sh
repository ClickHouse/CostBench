#!/usr/bin/env bash
set -euo pipefail
exec "${PYTHON:-python3}" "$(dirname "$0")/summarize_components.py" zerobus "$@"
