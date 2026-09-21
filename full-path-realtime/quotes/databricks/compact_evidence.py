#!/usr/bin/env python3
"""Normalize provider exports into small, publishable benchmark evidence."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

from dbx_common import atomic_write_json


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} is not a JSON object")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError(f"{path}:{line_number} is not a JSON object")
            rows.append(value)
    return rows


def _decode(value: Any) -> Any:
    decoded = value
    for _ in range(3):
        if not isinstance(decoded, str):
            break
        text = decoded.strip()
        if not text or text[0] not in "[{":
            break
        try:
            decoded = json.loads(text)
        except (TypeError, ValueError, json.JSONDecodeError):
            break
    return decoded


def _first(mapping: Mapping[str, Any], *names: str) -> Any:
    lowered = {str(key).lower(): value for key, value in mapping.items()}
    for name in names:
        if name.lower() in lowered:
            return lowered[name.lower()]
    return None


def _normalize_clustering_columns(value: Any) -> Any:
    decoded = _decode(value)
    if not isinstance(decoded, list):
        return decoded
    normalized = []
    for item in decoded:
        if isinstance(item, list) and len(item) == 1:
            normalized.append(item[0])
        else:
            normalized.append(item)
    return normalized


def _relevant_properties(value: Any) -> Any:
    decoded = _decode(value)
    if not isinstance(decoded, Mapping):
        return decoded
    exact = {
        "delta.enablechangedatafeed",
        "delta.enabledeletionvectors",
        "delta.enablerowtracking",
        "delta.parquet.compression.codec",
    }
    return {
        str(key): item
        for key, item in decoded.items()
        if str(key).lower() in exact
        or "clustering" in str(key).lower()
        or "predictive" in str(key).lower()
    }


def _table_status(value: Any) -> dict[str, Any] | None:
    decoded = _decode(value)
    if not isinstance(decoded, Mapping):
        return None
    metadata = _decode(_first(decoded, "json_metadata"))
    if isinstance(metadata, Mapping):
        decoded = metadata
    properties = _relevant_properties(
        _first(decoded, "properties", "table_properties")
    )
    features = _decode(_first(decoded, "tableFeatures", "table_features"))
    clustering = _normalize_clustering_columns(
        _first(
            decoded,
            "clusteringColumns",
            "clustering_columns",
            "clusterBy",
            "cluster_by",
        )
    )
    if clustering is None and isinstance(properties, Mapping):
        clustering = _normalize_clustering_columns(
            _first(properties, "clusteringColumns", "clustering_columns")
        )
    return {
        "name": _first(decoded, "name", "table_name"),
        "catalog": _first(decoded, "catalog_name", "catalog"),
        "schema": _first(decoded, "schema_name", "schema"),
        "type": _first(decoded, "type", "table_type"),
        "format": _first(decoded, "format", "data_source_format"),
        "location": _first(decoded, "location", "storage_location"),
        "clustering_columns": clustering,
        "cluster_by_auto": _first(decoded, "clusterByAuto", "cluster_by_auto"),
        "partition_columns": _decode(
            _first(decoded, "partitionColumns", "partition_columns")
        ),
        "num_files": _first(decoded, "numFiles", "num_files"),
        "size_in_bytes": _first(decoded, "sizeInBytes", "size_in_bytes"),
        "table_features": features,
        "properties": properties,
        "enable_predictive_optimization": _first(
            decoded, "enable_predictive_optimization"
        ),
        "effective_predictive_optimization_flag": _decode(
            _first(decoded, "effective_predictive_optimization_flag")
        ),
    }


def build_compact_evidence(
    summary: Mapping[str, Any],
    predictive_rows: Sequence[Mapping[str, Any]],
    preflight: Mapping[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    operations = []
    for row in predictive_rows:
        operations.append(
            {
                "table_name": row.get("table_name"),
                "operation_id": row.get("operation_id"),
                "operation_type": row.get("operation_type"),
                "start_time": row.get("start_time"),
                "end_time": row.get("end_time"),
                "operation_status": row.get("operation_status"),
                "usage_unit": row.get("usage_unit"),
                "usage_quantity": row.get("usage_quantity"),
                "operation_metrics": _decode(row.get("operation_metrics_json")),
            }
        )
    clustering_operations = [
        row
        for row in operations
        if str(row.get("operation_type") or "").upper()
        in {"CLUSTERING", "OPTIMIZE"}
    ]
    table_details = summary.get("table_details", {})
    if not isinstance(table_details, Mapping):
        table_details = {}
    predictive = summary.get("predictive_optimization", {})
    if not isinstance(predictive, Mapping):
        predictive = {}
    maintenance = summary.get("mv_maintenance", {})
    if not isinstance(maintenance, Mapping):
        maintenance = {}
    maintenance_counts = maintenance.get("maintenance_type_counts", {})
    if not isinstance(maintenance_counts, Mapping):
        maintenance_counts = {}

    common = {
        "schema_version": 1,
        "run_id": summary.get("run_id"),
        "collected_at": summary.get("collected_at"),
        "collection_window": summary.get("collection_window"),
    }
    provider = {
        **common,
        "provider_committed_records": summary.get("provider_committed_records"),
        "provider_committed_bytes": summary.get("provider_committed_bytes"),
        "provider_errors": summary.get("provider_errors"),
        "zerobus_stream_error_count": summary.get("zerobus_stream_error_count"),
        "zerobus_ingest_bounds": summary.get("zerobus_ingest_bounds"),
        "table_num_rows": summary.get("table_num_rows"),
        "required_dataset_completeness": summary.get(
            "required_dataset_completeness"
        ),
        "optional_dataset_errors": summary.get("optional_dataset_errors"),
        "complete": summary.get("complete"),
    }
    table_statuses = {
        "raw": _table_status(table_details.get("raw")),
        "materialized_view": _table_status(table_details.get("materialized_view")),
    }
    preflight_predictive: Mapping[str, Any] = {}
    if isinstance(preflight, Mapping):
        online = preflight.get("online")
        if isinstance(online, Mapping):
            candidate = online.get("predictive_optimization")
            if isinstance(candidate, Mapping):
                preflight_predictive = candidate
    for role in ("raw", "materialized_view"):
        status = table_statuses.get(role)
        inherited = preflight_predictive.get(role)
        if isinstance(status, dict) and isinstance(inherited, Mapping):
            status["enable_predictive_optimization"] = inherited.get(
                "configured_value"
            )
            status["effective_predictive_optimization_flag"] = inherited.get(
                "effective_value"
            )
            status["predictive_optimization_effectively_enabled"] = inherited.get(
                "effectively_enabled"
            )

    clustering = {
        **common,
        "tables": table_statuses,
        "predictive_optimization": {
            "preflight_status": preflight_predictive.get("status"),
            "summary": dict(predictive),
            "clustering_operation_count": len(clustering_operations),
            "operations": operations,
        },
    }
    refresh = {
        **common,
        "maintenance_type_counts": dict(maintenance_counts),
        "maintenance_class_counts": maintenance.get(
            "maintenance_class_counts"
        ),
        "full_refresh_count": maintenance.get("full_refresh_count"),
    }
    return {
        "provider_reconciliation": provider,
        "clustering_status": clustering,
        "mv_refresh_summary": refresh,
    }


def run(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--preflight", type=Path)
    args = parser.parse_args(argv)
    evidence_dir = args.evidence_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    summary = _read_json(evidence_dir / "evidence_summary.json")
    predictive_rows = _read_jsonl(
        evidence_dir / "predictive_optimization_operations.jsonl"
    )
    preflight = (
        _read_json(args.preflight.expanduser().resolve())
        if args.preflight is not None
        else None
    )
    outputs = build_compact_evidence(summary, predictive_rows, preflight)
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, value in outputs.items():
        atomic_write_json(output_dir / f"{name}.json", value)
    print(
        json.dumps(
            {
                "output_dir": str(output_dir),
                "files": sorted(f"{name}.json" for name in outputs),
            },
            sort_keys=True,
        )
    )
    return 0


def main() -> int:
    try:
        return run()
    except Exception as exc:
        print(f"FATAL: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
