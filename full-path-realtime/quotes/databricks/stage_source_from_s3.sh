#!/usr/bin/env bash
set -euo pipefail

: "${S3_SOURCE_URI:?Set S3_SOURCE_URI to the private prefix containing quotes_*.parquet}"
: "${SOURCE_DIR:=/data/quotes}"
: "${DBX_BENCH_DIR:=/home/ubuntu/costbench/full-path-realtime/quotes/databricks}"
: "${DATABRICKS_ZEROBUS_PYTHON:=$DBX_BENCH_DIR/.venv-zerobus/bin/python}"
: "${SOURCE_INVENTORY_OUTPUT:=/data/databricks-qualification/source_inventory.json}"

command -v aws >/dev/null 2>&1 || {
  echo "aws CLI v2 is required" >&2
  exit 1
}
[ -x "$DATABRICKS_ZEROBUS_PYTHON" ] || {
  echo "Zerobus Python is not executable: $DATABRICKS_ZEROBUS_PYTHON" >&2
  exit 1
}

case "$S3_SOURCE_URI" in
  s3://*) ;;
  *)
    echo "S3_SOURCE_URI must start with s3://" >&2
    exit 1
    ;;
esac

mkdir -p "$SOURCE_DIR" "$(dirname "$SOURCE_INVENTORY_OUTPUT")"
export AWS_CLI_AUTO_PROMPT=off
export AWS_PAGER=
export AWS_MAX_ATTEMPTS="${AWS_MAX_ATTEMPTS:-10}"

# Prove credentials and prefix access before beginning a large transfer.
aws sts get-caller-identity >/dev/null
aws s3 ls "${S3_SOURCE_URI%/}/" >/dev/null

# `sync` is resumable and does not delete local files. S3/HTTPS transport
# integrity checks remain enabled by the AWS CLI.
aws s3 sync \
  "${S3_SOURCE_URI%/}/" \
  "${SOURCE_DIR%/}/" \
  --exclude '*' \
  --include 'quotes_*.parquet' \
  --no-follow-symlinks \
  --only-show-errors

"$DATABRICKS_ZEROBUS_PYTHON" - "$SOURCE_DIR" "$SOURCE_INVENTORY_OUTPUT" <<'PY'
import json
import os
import sys
import tempfile
from pathlib import Path

import pyarrow.parquet as pq

EXPECTED_ROWS = 113_219_565_734
REQUIRED_COLUMNS = (
    "sym", "bx", "bp", "bs", "ax", "ap", "as", "c", "i", "t", "q", "z"
)

source = Path(sys.argv[1]).expanduser().resolve()
output = Path(sys.argv[2]).expanduser().resolve()
files = sorted(source.glob("quotes_*.parquet"), key=lambda path: path.name.encode())
if not files:
    raise SystemExit(f"no quotes_*.parquet files found in {source}")
if any(path.name == "quotes_0.parquet" for path in files):
    raise SystemExit("quotes_0.parquet is outside the canonical full-run source")

rows = 0
row_groups = 0
schemas = set()
file_reports = []
for path in files:
    parquet = pq.ParquetFile(path)
    metadata = parquet.metadata
    columns = tuple(parquet.schema_arrow.names)
    missing = [name for name in REQUIRED_COLUMNS if name not in columns]
    if missing:
        raise SystemExit(f"{path.name} is missing required columns: {missing}")
    rows += int(metadata.num_rows)
    row_groups += int(metadata.num_row_groups)
    schemas.add(str(parquet.schema_arrow))
    file_reports.append(
        {
            "file": path.name,
            "size_bytes": path.stat().st_size,
            "rows": int(metadata.num_rows),
            "row_groups": int(metadata.num_row_groups),
        }
    )

report = {
    "schema_version": 1,
    "status": "complete" if rows == EXPECTED_ROWS else "incomplete",
    "source_uri": os.environ["S3_SOURCE_URI"].rstrip("/") + "/",
    "source_dir": str(source),
    "expected_rows": EXPECTED_ROWS,
    "actual_rows": rows,
    "row_delta": rows - EXPECTED_ROWS,
    "file_count": len(files),
    "row_group_count": row_groups,
    "schema_variant_count": len(schemas),
    "files": file_reports,
}

output.parent.mkdir(parents=True, exist_ok=True)
fd, temporary = tempfile.mkstemp(prefix=f".{output.name}.", dir=output.parent)
try:
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, output)
finally:
    try:
        os.unlink(temporary)
    except FileNotFoundError:
        pass

print(json.dumps({key: report[key] for key in (
    "status", "file_count", "row_group_count", "actual_rows", "row_delta"
)}, sort_keys=True))
if rows != EXPECTED_ROWS:
    raise SystemExit(
        f"source row count is {rows:,}; expected exactly {EXPECTED_ROWS:,}"
    )
PY
