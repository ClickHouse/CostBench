#!/usr/bin/env bash
set -euo pipefail
# Existing CLI preserved; strict runner validation and portable provenance.
exec "${PYTHON:-python3}" "$(dirname "$0")/summarize_queries.py" "$@"
