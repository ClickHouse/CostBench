#!/usr/bin/env python3
"""Create the compact source and artifact manifest for one completed run."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from dbx_common import atomic_write_json


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} is not a JSON object")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def run(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--source-manifest", type=Path, required=True)
    parser.add_argument("--source-hashes", type=Path, required=True)
    parser.add_argument(
        "--status",
        default="initial_cost_unsettled",
        choices=("initial_cost_unsettled", "settled", "accepted"),
    )
    args = parser.parse_args(argv)
    run_root = args.run_root.expanduser().resolve()
    source_manifest = _read_json(args.source_manifest.expanduser().resolve())
    source_hashes = _read_json(args.source_hashes.expanduser().resolve())
    manifest_path = run_root / "validation" / "manifest.json"
    existing_sanitization = None
    if manifest_path.exists():
        existing_manifest = _read_json(manifest_path)
        candidate = existing_manifest.get("sanitization")
        if isinstance(candidate, dict):
            existing_sanitization = candidate
    artifacts = []
    for path in sorted(item for item in run_root.rglob("*") if item.is_file()):
        if path == manifest_path:
            continue
        artifacts.append(
            {
                "path": path.relative_to(run_root).as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
        )
    manifest = {
        "schema_version": 1,
        "generated_at": _utc_now(),
        "status": args.status,
        "run_id": args.run_id,
        "source": {
            "total_rows": source_manifest.get("total_rows"),
            "selected_file_count": source_manifest.get("selected_file_count"),
            "selected_row_group_count": source_manifest.get(
                "selected_row_group_count"
            ),
            "quotes_0_parquet_included": source_manifest.get(
                "quotes_0_parquet_included"
            ),
            "excluded_files": source_manifest.get("excluded_files"),
            "manifest_sha256": source_manifest.get("manifest_sha256"),
            "hash_validation_status": source_hashes.get("status"),
            "hash_validation_file_count": source_hashes.get("file_count"),
        },
        "runtime_state": {
            "included_in_results": False,
            "files_excluded": [
                "source_manifest.json",
                "ingest_events.jsonl",
                "full query audit JSONL",
                "raw provider exports",
                "process logs",
            ],
        },
        "artifact_count": len(artifacts),
        "result_size_bytes": sum(item["size_bytes"] for item in artifacts),
        "artifacts": artifacts,
    }
    if existing_sanitization is not None:
        manifest["sanitization"] = existing_sanitization
    atomic_write_json(manifest_path, manifest)
    print(json.dumps(manifest, sort_keys=True))
    return 0


def main() -> int:
    try:
        return run()
    except Exception as exc:
        print(f"FATAL: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
