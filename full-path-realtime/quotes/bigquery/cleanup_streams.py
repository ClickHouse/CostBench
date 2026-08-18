#!/usr/bin/env python3
"""Finalize committed streams left open after a hard-killed ingestion client."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from google.cloud import bigquery_storage_v1

from bq_common import atomic_write_json, close_google_client, iso_utc, utc_now


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    client = bigquery_storage_v1.BigQueryWriteClient()
    errors = []
    finalized = []
    for name in manifest.get("streams", []):
        try:
            response = client.finalize_write_stream(name=name)
            finalized.append({"name": name, "row_count": int(response.row_count)})
            print(f"finalized {name}: {response.row_count} rows")
        except Exception as exc:
            errors.append({"name": name, "error": str(exc)})
            print(f"failed {name}: {exc}", file=sys.stderr)
    try:
        close_google_client(client)
    except Exception as exc:
        print(f"warning: could not close the write client cleanly: {exc}", file=sys.stderr)
    manifest["cleanup_at"] = iso_utc(utc_now())
    manifest["cleanup_finalized"] = finalized
    manifest["cleanup_errors"] = errors
    manifest["finalized"] = not errors
    atomic_write_json(args.manifest, manifest)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
