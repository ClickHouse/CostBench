#!/usr/bin/env bash

# Convert repository-local inputs to paths relative to full-path-realtime so
# committed summaries are portable across clones. External inputs retain the
# caller-supplied path.
BIGQUERY_COSTS_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
FULL_PATH_ROOT="$(cd -- "$BIGQUERY_COSTS_DIR/../../.." && pwd)"

portable_path() {
  local input=$1
  local absolute

  if [[ -d "$input" ]]; then
    absolute="$(cd -- "$input" && pwd)"
  else
    absolute="$(cd -- "$(dirname -- "$input")" && pwd)/$(basename -- "$input")"
  fi

  case "$absolute" in
    "$FULL_PATH_ROOT"/*) printf '%s' "${absolute#"$FULL_PATH_ROOT"/}" ;;
    *) printf '%s' "$input" ;;
  esac
}
