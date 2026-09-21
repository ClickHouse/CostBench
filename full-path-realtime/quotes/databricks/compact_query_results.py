#!/usr/bin/env python3
"""Create reviewable query JSONL while preserving full audit evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence


REMOVED_TOP_LEVEL_FIELDS = {
    "mv_rows",
    "mv_rows_source",
    "result_row_count",
    "result_hash",
    "query_evidence",
    "rt_warehouse",
}
REMOVED_WAREHOUSE_API_FIELDS = {
    "creator_name",
    "creator_id",
    "channel",
    "jdbc_url",
    "odbc_params",
}


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def compact_record(record: Mapping[str, Any]) -> dict[str, Any]:
    compact = {
        key: value
        for key, value in record.items()
        if key not in REMOVED_TOP_LEVEL_FIELDS
    }
    compact["schema_version"] = 3
    warehouse = compact.get("query_warehouse")
    if isinstance(warehouse, Mapping):
        warehouse = dict(warehouse)
        metadata = warehouse.get("api_metadata")
        if isinstance(metadata, Mapping):
            warehouse["api_metadata"] = {
                key: value
                for key, value in metadata.items()
                if key not in REMOVED_WAREHOUSE_API_FIELDS
            }
        compact["query_warehouse"] = warehouse
    return compact


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def compact_file(
    source: Path,
    output: Path,
    *,
    audit_output: Path | None = None,
) -> dict[str, Any]:
    raw = source.read_bytes()
    records = [
        json.loads(line)
        for line in raw.decode("utf-8").splitlines()
        if line.strip()
    ]
    if not records:
        raise ValueError(f"{source} contains no records")
    if any(not isinstance(record, Mapping) for record in records):
        raise ValueError(f"{source} contains a non-object JSONL record")
    if audit_output is not None:
        if audit_output.exists():
            if audit_output.read_bytes() != raw:
                raise FileExistsError(
                    f"audit output exists with different content: {audit_output}"
                )
        else:
            _atomic_write(audit_output, raw)
    compact_records = [compact_record(record) for record in records]
    content = "".join(
        json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n"
        for record in compact_records
    ).encode("utf-8")
    _atomic_write(output, content)
    return {
        "source": str(source),
        "output": str(output),
        "audit_output": str(audit_output) if audit_output else None,
        "record_count": len(records),
        "source_size_bytes": len(raw),
        "output_size_bytes": len(content),
        "source_sha256": sha256_bytes(raw),
        "output_sha256": sha256_bytes(content),
        "removed_top_level_fields": sorted(REMOVED_TOP_LEVEL_FIELDS),
        "removed_warehouse_api_fields": sorted(
            REMOVED_WAREHOUSE_API_FIELDS
        ),
    }


def run(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--audit-output", type=Path)
    args = parser.parse_args(argv)
    report = compact_file(
        args.input.expanduser().resolve(),
        args.output.expanduser().resolve(),
        audit_output=(
            args.audit_output.expanduser().resolve()
            if args.audit_output is not None
            else None
        ),
    )
    print(json.dumps(report, sort_keys=True))
    return 0


def main() -> int:
    try:
        return run()
    except Exception as exc:
        print(f"FATAL: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
