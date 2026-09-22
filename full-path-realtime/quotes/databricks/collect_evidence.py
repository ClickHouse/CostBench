#!/usr/bin/env python3
"""Collect bounded, read-only Databricks benchmark evidence."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import tempfile
import urllib.parse
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from dbx_common import (
    DatabricksRestClient,
    atomic_write_json,
    bounded_statement_rows,
    iso_utc,
    normalize_host,
    quote_identifier,
    redact_secrets,
    sql_string,
)
from monitor_freshness import (
    classify_maintenance_type,
    maintenance_type_from_detail,
    parse_utc_timestamp,
)

DEFAULT_MAX_WINDOW_HOURS = 72.0
DEFAULT_ROW_LIMIT = 100_000
DEFAULT_HISTORY_LIMIT = 10_000
DEFAULT_MAX_RESULT_BYTES = 256 * 1024 * 1024
DEFAULT_MAX_RESULT_CHUNKS = 1_000

REQUIRED_DATASETS = (
    "zerobus_ingest_summary",
    "zerobus_ingest_by_minute_stream",
    "mv_event_log",
    "query_history",
    "warehouse_events",
    "warehouse_config_snapshots",
    "predictive_optimization_operations",
    "raw_table_detail",
    "mv_table_detail",
    "raw_table_history",
    "uc_table_metadata",
    "billing_usage",
    "billing_list_prices",
)
OPTIONAL_DATASETS = (
    "zerobus_stream_schema",
    "zerobus_stream_events",
    "zerobus_stream_errors",
)


def _env_first(env: Mapping[str, str], *names: str) -> str | None:
    for name in names:
        value = env.get(name)
        if value is not None and value.strip():
            return value.strip()
    return None


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def validate_window(
    since: Any,
    until: Any,
    *,
    max_window_hours: float = DEFAULT_MAX_WINDOW_HOURS,
) -> tuple[datetime, datetime]:
    """Return one strictly bounded UTC collection window."""
    if not math.isfinite(max_window_hours) or max_window_hours <= 0:
        raise ValueError("maximum window must be a finite positive number of hours")
    start = parse_utc_timestamp(since)
    end = parse_utc_timestamp(until)
    if end <= start:
        raise ValueError("--until must be later than --since")
    if end - start > timedelta(hours=max_window_hours):
        raise ValueError(
            "collection window exceeds the maximum "
            f"of {max_window_hours:g} hours"
        )
    return start, end


def _sql_timestamp(value: datetime) -> str:
    return f"CAST({sql_string(iso_utc(value))} AS TIMESTAMP)"


def _qualified_name(catalog: str, schema: str, table: str) -> str:
    return ".".join(quote_identifier(part) for part in (catalog, schema, table))


def build_evidence_queries(
    *,
    catalog: str,
    schema: str,
    raw_table: str,
    mv_table: str,
    rt_warehouse_id: str,
    control_warehouse_id: str,
    run_id: str,
    since: Any,
    until: Any,
    row_limit: int = DEFAULT_ROW_LIMIT,
    history_limit: int = DEFAULT_HISTORY_LIMIT,
    max_window_hours: float = DEFAULT_MAX_WINDOW_HOURS,
) -> dict[str, str]:
    """Build fixed, target-scoped SQL for all non-dynamic evidence sources."""
    start, end = validate_window(
        since,
        until,
        max_window_hours=max_window_hours,
    )
    if row_limit <= 0 or history_limit <= 0:
        raise ValueError("row and history limits must be positive")
    start_sql = _sql_timestamp(start)
    end_sql = _sql_timestamp(end)
    target = ".".join((catalog, schema, raw_table))
    raw_name = _qualified_name(catalog, schema, raw_table)
    mv_name = _qualified_name(catalog, schema, mv_table)
    table_names = ", ".join(sql_string(value) for value in (raw_table, mv_table))
    warehouse_ids = ", ".join(
        sql_string(value) for value in (rt_warehouse_id, control_warehouse_id)
    )
    billing_products = (
        "UPPER(billing_origin_product) IN "
        "('ZEROBUS', 'ZEROBUS_INGEST', 'PREDICTIVE_OPTIMIZATION', "
        "'LAKEFLOW', 'LAKEFLOW_PIPELINES', 'PIPELINES', 'DLT') "
        "OR UPPER(billing_origin_product) LIKE 'ZEROBUS%' "
        "OR UPPER(billing_origin_product) LIKE 'LAKEFLOW%' "
        "OR UPPER(billing_origin_product) LIKE '%PIPELINE%'"
    )
    mv_pipeline_billing = (
        "(\n"
        "      UPPER(billing_origin_product) = 'SQL'\n"
        "      AND usage_metadata.dlt_pipeline_id = (\n"
        "        SELECT origin.pipeline_id\n"
        f"        FROM event_log(TABLE({mv_name}))\n"
        "        WHERE origin.pipeline_id IS NOT NULL\n"
        "        ORDER BY timestamp DESC\n"
        "        LIMIT 1\n"
        "      )\n"
        "    )"
    )
    billing_filter = (
        f"usage_start_time < {end_sql}\n"
        f"  AND usage_end_time > {start_sql}\n"
        "  AND (\n"
        f"    usage_metadata.warehouse_id IN ({warehouse_ids})\n"
        f"    OR {billing_products}\n"
        f"    OR {mv_pipeline_billing}\n"
        "  )"
    )

    return {
        "zerobus_ingest_summary": (
            "SELECT\n"
            "  COALESCE(SUM(committed_records), 0) AS provider_committed_records,\n"
            "  COALESCE(SUM(committed_bytes), 0) AS provider_committed_bytes,\n"
            "  COALESCE(SUM(COALESCE(size(errors), 0)), 0) AS provider_errors,\n"
            "  MIN(commit_version) AS min_commit_version,\n"
            "  MAX(commit_version) AS max_commit_version,\n"
            "  MIN(commit_time) AS min_commit_time,\n"
            "  MAX(commit_time) AS max_commit_time\n"
            "FROM system.lakeflow.zerobus_ingest\n"
            f"WHERE table_name = {sql_string(target)}\n"
            f"  AND commit_time >= {start_sql}\n"
            f"  AND commit_time < {end_sql}"
        ),
        "zerobus_ingest_by_minute_stream": (
            "SELECT\n"
            "  date_trunc('minute', commit_time) AS commit_minute,\n"
            "  stream_id,\n"
            "  SUM(committed_records) AS committed_records,\n"
            "  SUM(committed_bytes) AS committed_bytes,\n"
            "  SUM(COALESCE(size(errors), 0)) AS error_count,\n"
            "  MIN(commit_version) AS min_commit_version,\n"
            "  MAX(commit_version) AS max_commit_version,\n"
            "  MIN(commit_time) AS min_commit_time,\n"
            "  MAX(commit_time) AS max_commit_time\n"
            "FROM system.lakeflow.zerobus_ingest\n"
            f"WHERE table_name = {sql_string(target)}\n"
            f"  AND commit_time >= {start_sql}\n"
            f"  AND commit_time < {end_sql}\n"
            "GROUP BY date_trunc('minute', commit_time), stream_id\n"
            "ORDER BY commit_minute, stream_id\n"
            f"LIMIT {row_limit}"
        ),
        "mv_event_log": (
            "SELECT\n"
            "  id, timestamp, event_type, message, level,\n"
            "  details AS details_json,\n"
            "  origin AS origin_json\n"
            f"FROM event_log(TABLE({mv_name}))\n"
            f"WHERE timestamp >= {start_sql}\n"
            f"  AND timestamp < {end_sql}\n"
            "  AND event_type IN ('planning_information', 'update_progress')\n"
            "ORDER BY timestamp, id\n"
            f"LIMIT {row_limit}"
        ),
        "query_history": (
            "SELECT\n"
            "  account_id, workspace_id, statement_id, session_id,\n"
            "  execution_status, compute.warehouse_id AS warehouse_id,\n"
            "  executed_by_user_id, executed_by, statement_type, error_message,\n"
            "  client_application, client_driver, cache_origin_statement_id,\n"
            "  total_duration_ms, waiting_for_compute_duration_ms,\n"
            "  waiting_at_capacity_duration_ms, execution_duration_ms,\n"
            "  compilation_duration_ms, total_task_duration_ms,\n"
            "  result_fetch_duration_ms, start_time, end_time, update_time,\n"
            "  read_partitions, pruned_files, read_files, read_rows, produced_rows,\n"
            "  read_bytes, read_io_cache_percent, from_result_cache,\n"
            "  spilled_local_bytes, written_bytes, written_rows, written_files,\n"
            "  shuffle_read_bytes, to_json(query_source) AS query_source_json,\n"
            "  to_json(query_tags) AS query_tags_json\n"
            "FROM system.query.history\n"
            f"WHERE compute.warehouse_id = {sql_string(rt_warehouse_id)}\n"
            f"  AND query_tags['run_id'] = {sql_string(run_id)}\n"
            f"  AND start_time >= {start_sql}\n"
            f"  AND start_time < {end_sql}\n"
            "ORDER BY start_time, statement_id\n"
            f"LIMIT {row_limit}"
        ),
        "warehouse_events": (
            "SELECT account_id, workspace_id, warehouse_id, event_type,\n"
            "       cluster_count, event_time\n"
            "FROM system.compute.warehouse_events\n"
            f"WHERE warehouse_id IN ({warehouse_ids})\n"
            f"  AND event_time >= {start_sql}\n"
            f"  AND event_time < {end_sql}\n"
            "ORDER BY event_time, warehouse_id, event_type\n"
            f"LIMIT {row_limit}"
        ),
        "warehouse_config_snapshots": (
            "SELECT warehouse_id, workspace_id, warehouse_name, warehouse_type,\n"
            "       warehouse_channel, warehouse_size, min_clusters, max_clusters,\n"
            "       auto_stop_minutes, to_json(tags) AS tags_json,\n"
            "       change_time, delete_time\n"
            "FROM system.compute.warehouses\n"
            f"WHERE warehouse_id IN ({warehouse_ids})\n"
            f"  AND change_time < {end_sql}\n"
            "QUALIFY ROW_NUMBER() OVER (\n"
            "  PARTITION BY warehouse_id ORDER BY change_time DESC\n"
            ") = 1\n"
            "ORDER BY warehouse_id\n"
            "LIMIT 2"
        ),
        "predictive_optimization_operations": (
            "SELECT metastore_name, catalog_name, schema_name, table_name,\n"
            "       operation_id, operation_type, start_time, end_time,\n"
            "       operation_status, usage_unit, usage_quantity,\n"
            "       to_json(operation_metrics) AS operation_metrics_json\n"
            "FROM system.storage.predictive_optimization_operations_history\n"
            f"WHERE catalog_name = {sql_string(catalog)}\n"
            f"  AND schema_name = {sql_string(schema)}\n"
            f"  AND table_name IN ({table_names})\n"
            f"  AND start_time < {end_sql}\n"
            f"  AND COALESCE(end_time, {end_sql}) > {start_sql}\n"
            "ORDER BY start_time, operation_id\n"
            f"LIMIT {row_limit}"
        ),
        "raw_table_detail": f"DESCRIBE DETAIL {raw_name}",
        "mv_table_detail": f"DESCRIBE TABLE EXTENDED {mv_name} AS JSON",
        "raw_table_history": (
            f"DESCRIBE HISTORY {raw_name} LIMIT {history_limit}"
        ),
        "billing_usage": (
            "SELECT account_id, workspace_id, record_id, sku_name, cloud,\n"
            "       usage_start_time, usage_end_time, usage_date,\n"
            "       usage_unit, usage_quantity, record_type, ingestion_date,\n"
            "       billing_origin_product,\n"
            "       to_json(usage_metadata) AS usage_metadata_json,\n"
            "       to_json(product_features) AS product_features_json,\n"
            "       to_json(custom_tags) AS custom_tags_json,\n"
            "       to_json(identity_metadata) AS identity_metadata_json\n"
            "FROM system.billing.usage\n"
            f"WHERE {billing_filter}\n"
            "ORDER BY usage_start_time, record_id\n"
            f"LIMIT {row_limit}"
        ),
        "billing_list_prices": (
            "WITH observed_usage AS (\n"
            "  SELECT sku_name, cloud,\n"
            "         MIN(usage_start_time) AS first_usage_time,\n"
            "         MAX(usage_end_time) AS last_usage_time\n"
            "  FROM system.billing.usage\n"
            f"  WHERE {billing_filter}\n"
            "  GROUP BY sku_name, cloud\n"
            ")\n"
            "SELECT p.price_start_time, p.price_end_time, p.account_id,\n"
            "       p.sku_name, p.cloud, p.currency_code, p.usage_unit,\n"
            "       to_json(p.pricing) AS pricing_json\n"
            "FROM system.billing.list_prices AS p\n"
            "INNER JOIN observed_usage AS u\n"
            "  ON p.sku_name = u.sku_name AND p.cloud = u.cloud\n"
            " AND p.price_start_time < u.last_usage_time\n"
            " AND (p.price_end_time IS NULL OR p.price_end_time > u.first_usage_time)\n"
            "ORDER BY p.sku_name, p.cloud, p.price_start_time\n"
            f"LIMIT {row_limit}"
        ),
    }


def build_zerobus_stream_query(
    schema_rows: Sequence[Mapping[str, Any]],
    *,
    target: str,
    since: Any,
    until: Any,
    row_limit: int,
    max_window_hours: float = DEFAULT_MAX_WINDOW_HOURS,
) -> str:
    """Build a bounded stream query only after inspecting the live schema."""
    start, end = validate_window(
        since,
        until,
        max_window_hours=max_window_hours,
    )
    discovered: list[str] = []
    seen: set[str] = set()
    for row in schema_rows:
        name = _row_value(row, "col_name")
        if name in (None, ""):
            name = _row_value(row, "column_name")
        text = str(name or "").strip()
        if not text or text.startswith("#") or text.lower() in seen:
            continue
        quote_identifier(text)
        discovered.append(text)
        seen.add(text.lower())
    missing = [name for name in ("table_name", "event_time") if name not in seen]
    if missing:
        raise ValueError(
            "unsupported system.lakeflow.zerobus_stream schema; missing documented "
            "filter column(s): " + ", ".join(missing)
        )
    if row_limit <= 0:
        raise ValueError("row limit must be positive")
    selected = ", ".join(quote_identifier(name) for name in discovered)
    stream_order = ", `stream_id`" if "stream_id" in seen else ""
    return (
        f"SELECT {selected}\n"
        "FROM system.lakeflow.zerobus_stream\n"
        f"WHERE `table_name` = {sql_string(target)}\n"
        f"  AND `event_time` >= {_sql_timestamp(start)}\n"
        f"  AND `event_time` < {_sql_timestamp(end)}\n"
        f"ORDER BY `event_time`{stream_order}\n"
        f"LIMIT {row_limit}"
    )


def _atomic_write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            for row in rows:
                json.dump(row, handle, separators=(",", ":"), ensure_ascii=False)
                handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _row_value(row: Mapping[str, Any], name: str) -> Any:
    wanted = name.lower()
    for key, value in row.items():
        if str(key).lower() == wanted:
            return value
    return None


def _exact_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _json_value(value: Any) -> Any:
    decoded = value
    for _ in range(3):
        if not isinstance(decoded, str):
            return decoded
        text = decoded.strip()
        if not text or text[0] not in "[{\"":
            return decoded
        try:
            decoded = json.loads(text)
        except (TypeError, ValueError, json.JSONDecodeError):
            return decoded
    return decoded


def _stream_error_count(rows: Sequence[Mapping[str, Any]]) -> int:
    total = 0
    for row in rows:
        errors = _json_value(_row_value(row, "errors"))
        if isinstance(errors, list):
            total += len(errors)
        elif errors not in (None, "", []):
            total += 1
    return total


def _stream_error_rows(
    rows: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    return [
        row
        for row in rows
        if _stream_error_count((row,)) > 0
    ]


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _mv_maintenance_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    maintenance_types: list[str] = []
    for row in rows:
        if str(_row_value(row, "event_type") or "").lower() != "planning_information":
            continue
        detail = _row_value(row, "details_json")
        if detail is None:
            detail = _row_value(row, "details")
        maintenance_type = maintenance_type_from_detail(detail)
        if maintenance_type is not None:
            maintenance_types.append(maintenance_type)
    classes = Counter(classify_maintenance_type(value) for value in maintenance_types)
    return {
        "maintenance_types": maintenance_types,
        "maintenance_type_counts": dict(Counter(maintenance_types)),
        "maintenance_class_counts": dict(classes),
        "full_refresh_count": classes.get("full", 0),
    }


def _predictive_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    counts = Counter(str(_row_value(row, "operation_type") or "UNKNOWN") for row in rows)
    usage: dict[str, Decimal] = {}
    for row in rows:
        quantity = _decimal(_row_value(row, "usage_quantity"))
        if quantity is None:
            continue
        unit = str(_row_value(row, "usage_unit") or "UNKNOWN")
        usage[unit] = usage.get(unit, Decimal(0)) + quantity
    return {
        "operation_count": len(rows),
        "operation_counts_by_type": dict(counts),
        "usage_by_unit": {unit: format(value, "f") for unit, value in usage.items()},
    }


def _billing_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    usage: dict[str, Decimal] = {}
    skus: set[str] = set()
    for row in rows:
        sku = _row_value(row, "sku_name")
        if sku not in (None, ""):
            skus.add(str(sku))
        quantity = _decimal(_row_value(row, "usage_quantity"))
        if quantity is None:
            continue
        unit = str(_row_value(row, "usage_unit") or "UNKNOWN")
        usage[unit] = usage.get(unit, Decimal(0)) + quantity
    return {
        "row_count": len(rows),
        "observed_skus": sorted(skus),
        "usage_by_unit": {unit: format(value, "f") for unit, value in usage.items()},
        "settlement_warning": (
            "system.billing.usage can lag actual usage by up to 24 hours; "
            "rerun collection after settlement before final cost validation."
        ),
    }


def build_summary(
    *,
    since: Any,
    until: Any,
    run_id: str,
    targets: Mapping[str, Any],
    datasets: Mapping[str, Mapping[str, Any]],
    rows: Mapping[str, Sequence[Mapping[str, Any]]],
    collected_at: Any | None = None,
    max_window_hours: float = DEFAULT_MAX_WINDOW_HOURS,
) -> dict[str, Any]:
    """Build the deterministic evidence summary from exported rows."""
    start, end = validate_window(
        since,
        until,
        max_window_hours=max_window_hours,
    )
    ingest_rows = rows.get("zerobus_ingest_summary", ())
    ingest = ingest_rows[0] if ingest_rows else {}
    required_failed = [
        name
        for name in REQUIRED_DATASETS
        if name not in datasets or datasets[name].get("error")
    ]
    optional_failed = [
        name
        for name in OPTIONAL_DATASETS
        if name not in datasets or datasets[name].get("error")
    ]
    required_succeeded = [
        name for name in REQUIRED_DATASETS if name not in required_failed
    ]
    table_details = {
        "raw": (
            dict(rows["raw_table_detail"][0])
            if rows.get("raw_table_detail")
            else None
        ),
        "materialized_view": (
            dict(rows["mv_table_detail"][0])
            if rows.get("mv_table_detail")
            else None
        ),
    }
    return {
        "schema_version": 1,
        "collected_at": iso_utc(
            datetime.now(timezone.utc)
            if collected_at is None
            else parse_utc_timestamp(collected_at)
        ),
        "collection_window": {
            "since": iso_utc(start),
            "until": iso_utc(end),
            "duration_seconds": (end - start).total_seconds(),
        },
        "run_id": run_id,
        "targets": dict(targets),
        "datasets": {name: dict(value) for name, value in datasets.items()},
        "provider_committed_records": _exact_int(
            _row_value(ingest, "provider_committed_records")
        ),
        "provider_committed_bytes": _exact_int(
            _row_value(ingest, "provider_committed_bytes")
        ),
        "provider_errors": _exact_int(_row_value(ingest, "provider_errors")),
        "zerobus_ingest_bounds": {
            name: _row_value(ingest, name)
            for name in (
                "min_commit_version",
                "max_commit_version",
                "min_commit_time",
                "max_commit_time",
            )
        },
        "zerobus_stream_error_count": _stream_error_count(
            rows.get("zerobus_stream_errors", ())
        ),
        "mv_maintenance": _mv_maintenance_summary(rows.get("mv_event_log", ())),
        "frozen_warehouse_configs": [
            dict(row) for row in rows.get("warehouse_config_snapshots", ())
        ],
        "table_details": table_details,
        "table_num_rows": {
            "raw": _exact_int(_row_value(table_details["raw"] or {}, "numRows")),
            "materialized_view": _exact_int(
                _row_value(table_details["materialized_view"] or {}, "numRows")
            ),
        },
        "predictive_optimization": _predictive_summary(
            rows.get("predictive_optimization_operations", ())
        ),
        "billing": _billing_summary(rows.get("billing_usage", ())),
        "required_dataset_completeness": {
            "required": list(REQUIRED_DATASETS),
            "succeeded": required_succeeded,
            "failed": required_failed,
            "complete": not required_failed,
        },
        "optional_dataset_errors": optional_failed,
        "complete": not required_failed and not optional_failed,
    }


def _assert_read_only(sql: str) -> None:
    first = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""
    if first not in {"SELECT", "WITH", "DESCRIBE"}:
        raise ValueError(f"collector refused non-read-only SQL beginning with {first!r}")


class EvidenceCollector:
    def __init__(
        self,
        client: DatabricksRestClient,
        *,
        control_warehouse_id: str,
        catalog: str,
        schema: str,
        run_id: str,
        output_dir: Path,
        statement_timeout: float,
        poll_interval: float,
        max_rows: int,
        max_result_bytes: int,
        max_result_chunks: int,
        secrets: Sequence[str],
    ):
        self.client = client
        self.control_warehouse_id = control_warehouse_id
        self.catalog = catalog
        self.schema = schema
        self.run_id = run_id
        self.output_dir = output_dir
        self.statement_timeout = statement_timeout
        self.poll_interval = poll_interval
        self.max_rows = max_rows
        self.max_result_bytes = max_result_bytes
        self.max_result_chunks = max_result_chunks
        self.secrets = tuple(secrets)
        self.rows: dict[str, list[dict[str, Any]]] = {}
        self.datasets: dict[str, dict[str, Any]] = {}
        self.statements: list[dict[str, Any]] = []
        self.errors: list[dict[str, Any]] = []

    def _path(self, name: str) -> Path:
        return self.output_dir / f"{name}.jsonl"

    def _record(
        self,
        name: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        statement_id: str | None,
        error: str | None,
        required: bool,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        safe_rows = redact_secrets(list(rows), self.secrets)
        assert isinstance(safe_rows, list)
        path = self._path(name)
        write_error: str | None = None
        try:
            _atomic_write_jsonl(path, safe_rows)
        except Exception as exc:
            write_error = f"{type(exc).__name__}: {exc}"
        combined_error = error
        if write_error:
            combined_error = (
                f"{combined_error}; output write failed: {write_error}"
                if combined_error
                else f"output write failed: {write_error}"
            )
        self.rows[name] = safe_rows
        self.datasets[name] = {
            "required": required,
            "row_count": len(safe_rows),
            "path": str(path),
            "statement_id": statement_id,
            "error": combined_error,
        }
        if metadata:
            self.datasets[name]["result"] = dict(metadata)
        if combined_error:
            self.errors.append(
                {"dataset": name, "required": required, "error": combined_error}
            )

    def sql(
        self,
        name: str,
        sql: str,
        *,
        required: bool,
        max_rows: int | None = None,
    ) -> list[dict[str, Any]]:
        _assert_read_only(sql)
        response: Mapping[str, Any] | None = None
        statement_id: str | None = None
        metadata: dict[str, Any] | None = None
        error: str | None = None
        result_rows: list[dict[str, Any]] = []
        try:
            response = self.client.execute_statement(
                sql,
                self.control_warehouse_id,
                timeout=self.statement_timeout,
                poll_interval=self.poll_interval,
                catalog=self.catalog,
                schema=self.schema,
                query_tags={
                    "benchmark": "full-path-realtime",
                    "run_id": self.run_id,
                    "collector": "evidence",
                    "dataset": name,
                },
            )
            statement_id_value = response.get("statement_id")
            statement_id = (
                str(statement_id_value)
                if statement_id_value not in (None, "")
                else None
            )
            result_rows, metadata = bounded_statement_rows(
                self.client,
                response,
                max_rows=max_rows or self.max_rows,
                max_bytes=self.max_result_bytes,
                max_chunks=self.max_result_chunks,
            )
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
        statement = {
            "dataset": name,
            "statement_id": statement_id,
            "warehouse_id": self.control_warehouse_id,
            "sql": sql,
            "row_count": len(result_rows),
            "error": error,
        }
        if metadata:
            statement["result"] = metadata
        self.statements.append(redact_secrets(statement, self.secrets))
        self._record(
            name,
            result_rows,
            statement_id=statement_id,
            error=error,
            required=required,
            metadata=metadata,
        )
        return result_rows

    def rest_table_metadata(
        self,
        targets: Sequence[tuple[str, str]],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        item_errors: list[str] = []
        for role, full_name in targets:
            encoded = urllib.parse.quote(full_name, safe="")
            try:
                metadata = self.client.request(
                    "GET", f"/api/2.1/unity-catalog/tables/{encoded}"
                )
                rows.append({"role": role, **metadata})
            except Exception as exc:
                item_errors.append(f"{role}: {type(exc).__name__}: {exc}")
        error = "; ".join(item_errors) or None
        self._record(
            "uc_table_metadata",
            rows,
            statement_id=None,
            error=error,
            required=True,
            metadata={"method": "GET", "target_count": len(targets)},
        )
        return rows

    def finish_metadata(self) -> None:
        _atomic_write_jsonl(
            self.output_dir / "collector_statements.jsonl", self.statements
        )
        safe_errors = redact_secrets(self.errors, self.secrets)
        assert isinstance(safe_errors, list)
        _atomic_write_jsonl(self.output_dir / "collector_errors.jsonl", safe_errors)


def _validate_collected_targets(
    collector: EvidenceCollector,
    *,
    rt_warehouse_id: str,
    control_warehouse_id: str,
) -> None:
    checks: list[tuple[str, str | None]] = []
    if len(collector.rows.get("zerobus_ingest_summary", ())) != 1:
        checks.append(
            ("zerobus_ingest_summary", "aggregate query did not return exactly one row")
        )
    config_ids = {
        str(_row_value(row, "warehouse_id"))
        for row in collector.rows.get("warehouse_config_snapshots", ())
    }
    missing_ids = {rt_warehouse_id, control_warehouse_id} - config_ids
    if missing_ids:
        checks.append(
            (
                "warehouse_config_snapshots",
                "missing end-of-window warehouse snapshot(s): "
                + ", ".join(sorted(missing_ids)),
            )
        )
    if len(collector.rows.get("uc_table_metadata", ())) != 2:
        checks.append(
            ("uc_table_metadata", "Unity Catalog REST metadata omitted a target table")
        )
    for name, description in (
        ("raw_table_detail", "DESCRIBE DETAIL"),
        ("mv_table_detail", "DESCRIBE TABLE EXTENDED AS JSON"),
    ):
        if len(collector.rows.get(name, ())) != 1:
            checks.append(
                (name, f"{description} did not return exactly one row")
            )

    for name, error in checks:
        dataset = collector.datasets.get(name)
        if dataset is None or not error:
            continue
        old = dataset.get("error")
        dataset["error"] = f"{old}; {error}" if old else error
        collector.errors.append({"dataset": name, "required": True, "error": error})


def collect(
    client: DatabricksRestClient,
    *,
    control_warehouse_id: str,
    rt_warehouse_id: str,
    catalog: str,
    schema: str,
    raw_table: str,
    mv_table: str,
    since: datetime,
    until: datetime,
    run_id: str,
    output_dir: Path,
    row_limit: int,
    history_limit: int,
    statement_timeout: float,
    poll_interval: float,
    max_result_bytes: int,
    max_result_chunks: int,
    max_window_hours: float = DEFAULT_MAX_WINDOW_HOURS,
    secrets: Sequence[str] = (),
) -> tuple[dict[str, Any], bool]:
    """Collect all evidence and return ``(summary, required_sources_complete)``."""
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_full_name = ".".join((catalog, schema, raw_table))
    mv_full_name = ".".join((catalog, schema, mv_table))
    targets = {
        "catalog": catalog,
        "schema": schema,
        "raw_table": raw_table,
        "materialized_view": mv_table,
        "raw_full_name": raw_full_name,
        "materialized_view_full_name": mv_full_name,
        "rt_warehouse_id": rt_warehouse_id,
        "control_warehouse_id": control_warehouse_id,
    }
    collector = EvidenceCollector(
        client,
        control_warehouse_id=control_warehouse_id,
        catalog=catalog,
        schema=schema,
        run_id=run_id,
        output_dir=output_dir,
        statement_timeout=statement_timeout,
        poll_interval=poll_interval,
        max_rows=row_limit,
        max_result_bytes=max_result_bytes,
        max_result_chunks=max_result_chunks,
        secrets=secrets,
    )
    summary: dict[str, Any] | None = None
    try:
        queries = build_evidence_queries(
            catalog=catalog,
            schema=schema,
            raw_table=raw_table,
            mv_table=mv_table,
            rt_warehouse_id=rt_warehouse_id,
            control_warehouse_id=control_warehouse_id,
            run_id=run_id,
            since=since,
            until=until,
            row_limit=row_limit,
            history_limit=history_limit,
            max_window_hours=max_window_hours,
        )
        for name, sql in queries.items():
            if name in (
                "zerobus_ingest_summary",
                "raw_table_detail",
                "mv_table_detail",
            ):
                result_row_limit = 1
            elif name in ("raw_table_history", "mv_table_history"):
                result_row_limit = history_limit
            elif name == "warehouse_config_snapshots":
                result_row_limit = 2
            else:
                result_row_limit = row_limit
            collector.sql(
                name,
                sql,
                required=True,
                max_rows=result_row_limit,
            )

        schema_rows = collector.sql(
            "zerobus_stream_schema",
            "DESCRIBE TABLE system.lakeflow.zerobus_stream",
            required=False,
            max_rows=1_000,
        )
        if collector.datasets["zerobus_stream_schema"]["error"]:
            unsupported_error = (
                "unsupported schema: DESCRIBE TABLE "
                "system.lakeflow.zerobus_stream failed"
            )
            collector._record(
                "zerobus_stream_events",
                (),
                statement_id=None,
                error=unsupported_error,
                required=False,
            )
            collector._record(
                "zerobus_stream_errors",
                (),
                statement_id=None,
                error=unsupported_error,
                required=False,
            )
        else:
            try:
                stream_sql = build_zerobus_stream_query(
                    schema_rows,
                    target=raw_full_name,
                    since=since,
                    until=until,
                    row_limit=row_limit,
                    max_window_hours=max_window_hours,
                )
            except Exception as exc:
                unsupported_error = f"{type(exc).__name__}: {exc}"
                collector._record(
                    "zerobus_stream_events",
                    (),
                    statement_id=None,
                    error=unsupported_error,
                    required=False,
                )
                collector._record(
                    "zerobus_stream_errors",
                    (),
                    statement_id=None,
                    error=unsupported_error,
                    required=False,
                )
            else:
                stream_rows = collector.sql(
                    "zerobus_stream_events",
                    stream_sql,
                    required=False,
                )
                stream_dataset = collector.datasets["zerobus_stream_events"]
                collector._record(
                    "zerobus_stream_errors",
                    _stream_error_rows(stream_rows),
                    statement_id=stream_dataset.get("statement_id"),
                    error=stream_dataset.get("error"),
                    required=False,
                    metadata={"source_dataset": "zerobus_stream_events"},
                )

        collector.rest_table_metadata(
            (("raw", raw_full_name), ("materialized_view", mv_full_name))
        )
        _validate_collected_targets(
            collector,
            rt_warehouse_id=rt_warehouse_id,
            control_warehouse_id=control_warehouse_id,
        )
    except Exception as exc:
        collector.errors.append(
            {
                "dataset": "collector",
                "required": True,
                "error": f"{type(exc).__name__}: {exc}",
            }
        )
        for name in REQUIRED_DATASETS:
            if name not in collector.datasets:
                collector._record(
                    name,
                    (),
                    statement_id=None,
                    error="collection aborted before this required source was collected",
                    required=True,
                )
        for name in OPTIONAL_DATASETS:
            if name not in collector.datasets:
                collector._record(
                    name,
                    (),
                    statement_id=None,
                    error="collection aborted before this optional source was collected",
                    required=False,
                )
    finally:
        for name in REQUIRED_DATASETS:
            if name not in collector.datasets:
                collector._record(
                    name,
                    (),
                    statement_id=None,
                    error="required source was not collected",
                    required=True,
                )
        for name in OPTIONAL_DATASETS:
            if name not in collector.datasets:
                collector._record(
                    name,
                    (),
                    statement_id=None,
                    error="optional source was not collected",
                    required=False,
                )
        try:
            collector.finish_metadata()
        except Exception as exc:
            collector.errors.append(
                {
                    "dataset": "collector_metadata",
                    "required": True,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
        summary = build_summary(
            since=since,
            until=until,
            run_id=run_id,
            targets=targets,
            datasets=collector.datasets,
            rows=collector.rows,
            max_window_hours=max_window_hours,
        )
        summary["errors"] = list(collector.errors)
        summary["required_dataset_completeness"]["complete"] = (
            not summary["required_dataset_completeness"]["failed"]
            and not any(
                error.get("required")
                for error in collector.errors
                if error.get("dataset") not in collector.datasets
            )
        )
        if not summary["required_dataset_completeness"]["complete"]:
            summary["complete"] = False
        atomic_write_json(
            output_dir / "evidence_summary.json",
            redact_secrets(summary, secrets),
        )
    assert summary is not None
    return summary, bool(summary["required_dataset_completeness"]["complete"])


def build_parser(env: Mapping[str, str] | None = None) -> argparse.ArgumentParser:
    source = os.environ if env is None else env
    parser = argparse.ArgumentParser(
        description="Export bounded, read-only Databricks benchmark evidence"
    )
    parser.add_argument(
        "--host",
        default=_env_first(
            source,
            "DATABRICKS_HOST",
            "DATABRICKS_SERVER_HOSTNAME",
            "DATABRICKS_WORKSPACE_URL",
        ),
    )
    parser.add_argument(
        "--token",
        default=_env_first(source, "DATABRICKS_TOKEN", "DATABRICKS_ACCESS_TOKEN"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--client-id", default=_env_first(source, "DATABRICKS_CLIENT_ID"))
    parser.add_argument(
        "--client-secret",
        default=_env_first(source, "DATABRICKS_CLIENT_SECRET"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--control-warehouse-id",
        "--control-warehouse",
        dest="control_warehouse_id",
        default=_env_first(
            source,
            "DATABRICKS_CONTROL_WAREHOUSE_ID",
            "DBX_CONTROL_WAREHOUSE_ID",
            "CONTROL_WAREHOUSE_ID",
        ),
    )
    parser.add_argument(
        "--rt-warehouse-id",
        "--rt-warehouse",
        dest="rt_warehouse_id",
        default=_env_first(
            source,
            "DATABRICKS_RT_WAREHOUSE_ID",
            "DBX_RT_WAREHOUSE_ID",
            "RT_WAREHOUSE_ID",
        ),
    )
    parser.add_argument(
        "--catalog",
        default=_env_first(source, "DATABRICKS_CATALOG", "DBX_CATALOG") or "workspace",
    )
    parser.add_argument(
        "--schema",
        default=_env_first(source, "DATABRICKS_SCHEMA", "DBX_SCHEMA")
        or "benchmarking",
    )
    parser.add_argument(
        "--raw-table",
        default=_env_first(source, "DATABRICKS_RAW_TABLE", "DBX_RAW_TABLE") or "quotes",
    )
    parser.add_argument(
        "--mv-table",
        default=_env_first(source, "DATABRICKS_MV_TABLE", "DBX_MV_TABLE")
        or "quotes_daily",
    )
    parser.add_argument("--since", required=True, help="required UTC ISO-8601 start")
    parser.add_argument("--until", required=True, help="required UTC ISO-8601 end")
    parser.add_argument("--max-window-hours", type=float, default=DEFAULT_MAX_WINDOW_HOURS)
    parser.add_argument(
        "--run-id",
        required=not bool(_env_first(source, "BENCHMARK_RUN_ID", "DATABRICKS_RUN_ID")),
        default=_env_first(source, "BENCHMARK_RUN_ID", "DATABRICKS_RUN_ID"),
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--row-limit", type=_positive_int, default=DEFAULT_ROW_LIMIT)
    parser.add_argument(
        "--history-limit", type=_positive_int, default=DEFAULT_HISTORY_LIMIT
    )
    parser.add_argument(
        "--max-result-bytes",
        type=_positive_int,
        default=DEFAULT_MAX_RESULT_BYTES,
    )
    parser.add_argument(
        "--max-result-chunks",
        type=_positive_int,
        default=DEFAULT_MAX_RESULT_CHUNKS,
    )
    parser.add_argument("--statement-timeout", type=float, default=120.0)
    parser.add_argument("--statement-poll-interval", type=float, default=0.5)
    parser.add_argument("--network-timeout", type=float, default=30.0)
    return parser


def finalize_args(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
) -> argparse.Namespace:
    if (
        args.statement_timeout <= 0
        or args.statement_poll_interval <= 0
        or args.network_timeout <= 0
    ):
        parser.error("timeouts and poll interval must be greater than zero")
    try:
        args.since, args.until = validate_window(
            args.since,
            args.until,
            max_window_hours=args.max_window_hours,
        )
    except (TypeError, ValueError, OverflowError, OSError) as exc:
        parser.error(f"invalid bounded UTC collection window: {exc}")
    args.host = normalize_host(args.host)
    if not args.host:
        parser.error("--host or DATABRICKS_HOST is required")
    parsed_host = urllib.parse.urlparse(args.host)
    if (
        parsed_host.scheme != "https"
        or not parsed_host.hostname
        or parsed_host.username
        or parsed_host.password
        or parsed_host.query
        or parsed_host.fragment
        or parsed_host.path not in ("", "/")
    ):
        parser.error("--host must be an HTTPS workspace origin without a path")
    if not args.control_warehouse_id or not args.rt_warehouse_id:
        parser.error("--control-warehouse-id and --rt-warehouse-id are required")
    if bool(args.client_id) != bool(args.client_secret):
        parser.error("--client-id and --client-secret must be supplied together")
    if bool(args.token) == bool(args.client_id and args.client_secret):
        parser.error("configure exactly one of PAT or OAuth M2M credentials")
    for name in ("catalog", "schema", "raw_table", "mv_table"):
        try:
            quote_identifier(getattr(args, name))
        except ValueError as exc:
            parser.error(f"--{name.replace('_', '-')} is invalid: {exc}")
    if not str(args.run_id).strip():
        parser.error("--run-id must be non-empty")
    args.output_dir = args.output_dir.expanduser().resolve()
    return args


def run(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = finalize_args(parser, parser.parse_args(argv))
    client = DatabricksRestClient(
        args.host,
        token=args.token,
        client_id=args.client_id,
        client_secret=args.client_secret,
        timeout=args.network_timeout,
    )
    secrets = tuple(
        value for value in (args.token, args.client_secret) if isinstance(value, str)
    )
    summary, required_complete = collect(
        client,
        control_warehouse_id=args.control_warehouse_id,
        rt_warehouse_id=args.rt_warehouse_id,
        catalog=args.catalog,
        schema=args.schema,
        raw_table=args.raw_table,
        mv_table=args.mv_table,
        since=args.since,
        until=args.until,
        run_id=args.run_id,
        output_dir=args.output_dir,
        row_limit=args.row_limit,
        history_limit=args.history_limit,
        statement_timeout=args.statement_timeout,
        poll_interval=args.statement_poll_interval,
        max_result_bytes=args.max_result_bytes,
        max_result_chunks=args.max_result_chunks,
        max_window_hours=args.max_window_hours,
        secrets=secrets,
    )
    for error in summary.get("errors", []):
        print(
            f"{error.get('dataset')}: {error.get('error')}",
            file=sys.stderr,
        )
    print(f"Evidence written to {args.output_dir}")
    return 0 if required_complete else 1


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return run(argv)
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"FATAL: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
