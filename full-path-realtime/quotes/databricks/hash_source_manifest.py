#!/usr/bin/env python3
"""Hash deferred source files after a measured ingest window."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    manifest_path = args.manifest.expanduser().resolve()
    output = args.output.expanduser().resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    root = Path(manifest["root"]).expanduser().resolve()
    reports: list[dict[str, Any]] = []
    for index, source in enumerate(manifest["files"], start=1):
        path = (root / source["path"]).resolve()
        if root not in path.parents:
            raise RuntimeError(f"source path escapes manifest root: {path}")
        before = path.stat()
        if (
            before.st_size != int(source["source_size_bytes"])
            or before.st_mtime_ns != int(source["source_mtime_ns"])
        ):
            raise RuntimeError(f"source identity changed before hashing: {path}")
        print(
            f"SOURCE HASH {index}/{len(manifest['files'])} {source['path']}",
            flush=True,
        )
        digest = sha256_file(path)
        after = path.stat()
        if (
            after.st_size != before.st_size
            or after.st_mtime_ns != before.st_mtime_ns
        ):
            raise RuntimeError(f"source changed while hashing: {path}")
        reports.append(
            {
                "file_ordinal": source["file_ordinal"],
                "path": source["path"],
                "size_bytes": after.st_size,
                "mtime_ns": after.st_mtime_ns,
                "sha256": digest,
            }
        )

    atomic_json(
        output,
        {
            "schema_version": 1,
            "status": "complete",
            "generated_at": datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "source_manifest_path": str(manifest_path),
            "source_manifest_sha256": sha256_file(manifest_path),
            "file_count": len(reports),
            "total_size_bytes": sum(item["size_bytes"] for item in reports),
            "files": reports,
        },
    )
    print(f"Source hashes written to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
