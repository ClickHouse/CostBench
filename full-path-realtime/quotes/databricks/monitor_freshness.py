#!/usr/bin/env python3
"""Continuously record bounded metadata-only freshness evidence.

The monitor never queries the raw table or materialized-view contents. Raw
progress comes from Zerobus system telemetry and materialized-view progress
comes from the provider event log.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import signal
import sys
import threading
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from dbx_common import (
    DatabricksRestClient,
    atomic_append_jsonl,
    atomic_write_json,
    iso_utc,
    normalize_host,
    quote_identifier,
    redact_secrets,
    sql_string,
    statement_rows,
    utc_now,
)

DEFAULT_INTERVAL_SECONDS = 30.0
EVENT_LIMIT = 100
EVENT_LIMIT_PER_TYPE = EVENT_LIMIT // 2
AGE_SEMANTICS = (
    "These are observational ages from the sample time to provider timestamps; "
    "they are not exact per-record ingestion or materialized-view lag."
)


def _env_first(env: Mapping[str, str], *names: str) -> str | None:
    for name in names:
        value = env.get(name)
        if value is not None and value.strip():
            return value.strip()
    return None


def _path_default(value: str | None) -> Path | None:
    return Path(value).expanduser() if value else None


def parse_utc_timestamp(value: Any) -> datetime:
    """Parse an aware UTC timestamp and reject local or non-UTC input."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("timestamp must be finite")
        if abs(number) >= 100_000_000_000:
            number /= 1000.0
        parsed = datetime.fromtimestamp(number, tz=timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("timestamp must be non-empty")
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    else:
        raise ValueError("timestamp must be an ISO-8601 string or datetime")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include the UTC timezone")
    if parsed.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be UTC")
    return parsed.astimezone(timezone.utc)


def timestamp_age_sec(observed_at: Any, event_at: Any) -> float | None:
    """Return a non-negative observational timestamp age in seconds."""
    if event_at in (None, ""):
        return None
    try:
        observed = parse_utc_timestamp(observed_at)
        event = parse_utc_timestamp(event_at)
    except (TypeError, ValueError, OverflowError, OSError):
        return None
    return max(0.0, (observed - event).total_seconds())


def _sql_timestamp(value: Any) -> str:
    return f"CAST({sql_string(iso_utc(parse_utc_timestamp(value)))} AS TIMESTAMP)"


def _qualified_name(catalog: str, schema: str, table: str) -> str:
    return ".".join(quote_identifier(part) for part in (catalog, schema, table))


def build_control_queries(
    catalog: str,
    schema: str,
    raw_table: str,
    mv_table: str,
    since: Any,
    until: Any,
) -> dict[str, str]:
    """Build target-filtered monitor SQL for one required, bounded UTC window."""
    start = parse_utc_timestamp(since)
    end = parse_utc_timestamp(until)
    if end < start:
        raise ValueError("monitor query window ends before it starts")
    start_sql = _sql_timestamp(start)
    end_sql = _sql_timestamp(end)
    raw_name = _qualified_name(catalog, schema, raw_table)
    mv_name = _qualified_name(catalog, schema, mv_table)
    target_name = ".".join((catalog, schema, raw_table))
    event_types = ", ".join(
        sql_string(value) for value in ("update_progress", "planning_information")
    )
    return {
        "zerobus_ingest": (
            "SELECT\n"
            "  COALESCE(SUM(committed_records), 0) AS cumulative_committed_records,\n"
            "  COALESCE(SUM(committed_bytes), 0) AS cumulative_committed_bytes,\n"
            "  COALESCE(SUM(COALESCE(size(errors), 0)), 0) AS cumulative_error_count,\n"
            "  max_by(commit_version, commit_time) AS latest_commit_version,\n"
            "  MAX(commit_time) AS latest_commit_time\n"
            "FROM system.lakeflow.zerobus_ingest\n"
            f"WHERE table_name = {sql_string(target_name)}\n"
            f"  AND commit_time >= {start_sql}\n"
            f"  AND commit_time <= {end_sql}"
        ),
        "raw_history": f"DESCRIBE HISTORY {raw_name} LIMIT 1",
        "mv_events": (
            "SELECT id, timestamp, event_type, message, level, origin, details\n"
            f"FROM event_log(TABLE({mv_name}))\n"
            f"WHERE timestamp >= {start_sql}\n"
            f"  AND timestamp <= {end_sql}\n"
            f"  AND event_type IN ({event_types})\n"
            "QUALIFY row_number() OVER (\n"
            "  PARTITION BY event_type ORDER BY timestamp DESC\n"
            f") <= {EVENT_LIMIT_PER_TYPE}\n"
            "ORDER BY timestamp DESC\n"
            f"LIMIT {EVENT_LIMIT}"
        ),
    }


def _row_value(row: Mapping[str, Any], name: str) -> Any:
    wanted = name.lower()
    for key, value in row.items():
        if str(key).lower() == wanted:
            return value
    return None


def _nonnegative_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if number >= 0 else None


def _decode_json(value: Any) -> Any:
    decoded = value
    for _ in range(3):
        if not isinstance(decoded, str):
            break
        text = decoded.strip()
        if not text or text[0] not in "[{\"":
            break
        try:
            decoded = json.loads(text)
        except (TypeError, ValueError, json.JSONDecodeError):
            break
    return decoded


def _decode_event_row(row: Mapping[str, Any]) -> dict[str, Any]:
    decoded = dict(row)
    for name in ("details", "origin"):
        for key in tuple(decoded):
            if str(key).lower() == name:
                decoded[key] = _decode_json(decoded[key])
    return decoded


def _mapping_path(value: Any, *path: str) -> Any:
    current = _decode_json(value)
    for component in path:
        if not isinstance(current, Mapping):
            return None
        current = _decode_json(current.get(component))
    return current


def _recursive_value(value: Any, names: set[str]) -> Any:
    decoded = _decode_json(value)
    if isinstance(decoded, Mapping):
        for key, item in decoded.items():
            if str(key).lower() in names and item is not None:
                return _decode_json(item)
        for item in decoded.values():
            found = _recursive_value(item, names)
            if found is not None:
                return found
    elif isinstance(decoded, list):
        for item in decoded:
            found = _recursive_value(item, names)
            if found is not None:
                return found
    return None


def classify_maintenance_type(value: Any) -> str:
    """Classify a provider maintenance enum without treating NO_OP as incremental."""
    if value is None:
        return "unknown"
    normalized = str(value).strip().upper().replace("-", "_").replace(" ", "_")
    if not normalized:
        return "unknown"
    if "NO_OP" in normalized or normalized == "NOOP":
        return "neutral"
    if "COMPLETE_RECOMPUTE" in normalized or "FULL" in normalized:
        return "full"
    return "incremental"


def maintenance_type_from_detail(detail: Any) -> str | None:
    """Extract the chosen maintenance type from a planning-information detail."""
    decoded = _decode_json(detail)
    planning = _mapping_path(decoded, "planning_information")
    if planning is None:
        planning = decoded
    techniques = _mapping_path(planning, "technique_information")
    if isinstance(techniques, Mapping):
        techniques = [techniques]
    if isinstance(techniques, list):
        mappings = [item for item in techniques if isinstance(item, Mapping)]
        chosen = [
            item
            for item in mappings
            if item.get("is_chosen") is True
            or str(item.get("is_chosen", "")).strip().lower() == "true"
        ]
        for item in chosen or mappings:
            maintenance_type = item.get("maintenance_type")
            if maintenance_type not in (None, ""):
                return str(maintenance_type)
    direct = _recursive_value(planning, {"maintenance_type"})
    return None if direct in (None, "") else str(direct)


def _event_update_id(row: Mapping[str, Any]) -> str | None:
    value = _row_value(row, "update_id")
    if value in (None, ""):
        value = _mapping_path(_row_value(row, "origin"), "update_id")
    if value in (None, ""):
        value = _recursive_value(_row_value(row, "details"), {"update_id"})
    return None if value in (None, "") else str(value)


def _event_timestamp(row: Mapping[str, Any]) -> str | None:
    value = _row_value(row, "timestamp")
    if value in (None, ""):
        return None
    try:
        return iso_utc(parse_utc_timestamp(value))
    except (TypeError, ValueError, OverflowError, OSError):
        return str(value)


def _update_state(row: Mapping[str, Any]) -> str | None:
    detail = _row_value(row, "details")
    value = _mapping_path(detail, "update_progress", "state")
    if value in (None, ""):
        value = _mapping_path(detail, "state")
    return None if value in (None, "") else str(value).upper()


def _output_rows(row: Mapping[str, Any]) -> int | None:
    value = _recursive_value(
        _row_value(row, "details"),
        {"num_output_rows", "output_rows", "output_row_count"},
    )
    return _nonnegative_int(value)


def _source_snapshot(planning_row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if planning_row is None:
        return None
    details = _decode_json(_row_value(planning_row, "details"))
    planning = _mapping_path(details, "planning_information")
    sources = (
        _mapping_path(planning, "source_table_information")
        if planning is not None
        else None
    )
    if isinstance(sources, Mapping):
        sources = [sources]
    if not isinstance(sources, list) or len(sources) != 1:
        return None
    source = sources[0]
    if not isinstance(source, Mapping):
        return None
    origin = _decode_json(_row_value(planning_row, "origin"))
    return {
        "table_name": source.get("table_name"),
        "num_rows": _nonnegative_int(source.get("num_rows")),
        "num_files": _nonnegative_int(source.get("num_files")),
        "full_size_bytes": _nonnegative_int(source.get("full_size")),
        "changed_rows": _nonnegative_int(source.get("num_changed_rows")),
        "changed_files": _nonnegative_int(source.get("num_changed_files")),
        "change_size_bytes": _nonnegative_int(source.get("change_size")),
        "delta_version": _nonnegative_int(
            _mapping_path(origin, "ingestion_source_table_version")
        ),
    }


def _event_sort_key(row: Mapping[str, Any]) -> float:
    try:
        return parse_utc_timestamp(_row_value(row, "timestamp")).timestamp()
    except (TypeError, ValueError, OverflowError, OSError):
        return float("-inf")


def latest_successful_refresh(
    event_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Correlate the latest completed update with its chosen planning technique."""
    rows = [_decode_event_row(row) for row in event_rows]
    rows.sort(key=_event_sort_key, reverse=True)
    completed = next(
        (
            row
            for row in rows
            if str(_row_value(row, "event_type")).lower() == "update_progress"
            and _update_state(row) == "COMPLETED"
        ),
        None,
    )
    if completed is None:
        return None

    update_id = _event_update_id(completed)
    finished_at = _event_timestamp(completed)
    planning_rows = [
        row
        for row in rows
        if str(_row_value(row, "event_type")).lower() == "planning_information"
    ]
    planning = next(
        (
            row
            for row in planning_rows
            if update_id is not None and _event_update_id(row) == update_id
        ),
        None,
    )
    if planning is None and update_id is None and planning_rows:
        planning = planning_rows[0]
    maintenance_type = (
        maintenance_type_from_detail(_row_value(planning, "details"))
        if planning is not None
        else None
    )
    result: dict[str, Any] = {
        "finished_at": finished_at,
        "output_rows": _output_rows(completed),
        "maintenance_type": maintenance_type,
        "maintenance_class": classify_maintenance_type(maintenance_type),
    }
    if planning is not None:
        result["planned_at"] = _event_timestamp(planning)
        snapshot = _source_snapshot(planning)
        if snapshot is not None:
            result["source_snapshot"] = snapshot
    if update_id is not None:
        result["update_id"] = update_id
    return result


def read_producer_progress(path: Path) -> dict[str, Any]:
    """Read only the producer counters needed by the freshness observation."""
    result: dict[str, Any] = {
        "path": str(path),
        "provider_committed_rows": None,
        "logical_raw_rows": None,
        "updated_at": None,
        "error": None,
    }
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(loaded, Mapping):
            raise ValueError("top-level producer progress value is not an object")
        result["provider_committed_rows"] = _nonnegative_int(
            loaded.get("provider_committed_rows")
        )
        result["logical_raw_rows"] = _nonnegative_int(loaded.get("logical_raw_rows"))
        result["updated_at"] = loaded.get("updated_at")
        missing = [
            name
            for name in ("provider_committed_rows", "logical_raw_rows", "updated_at")
            if result[name] is None
        ]
        if missing:
            result["error"] = "producer progress omitted or invalid: " + ", ".join(missing)
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def _execute_query(
    client: DatabricksRestClient,
    warehouse_id: str,
    sql: str,
    *,
    timeout: float,
    poll_interval: float,
    catalog: str,
    schema: str,
    query_tags: Mapping[str, str],
) -> dict[str, Any]:
    try:
        response = client.execute_statement(
            sql,
            warehouse_id,
            timeout=timeout,
            poll_interval=poll_interval,
            catalog=catalog,
            schema=schema,
            query_tags=query_tags,
        )
        rows = statement_rows(response)
        return {
            "statement_id": response.get("statement_id"),
            "row_count": len(rows),
            "rows": rows,
            "error": None,
        }
    except Exception as exc:
        return {
            "statement_id": None,
            "row_count": 0,
            "rows": [],
            "error": f"{type(exc).__name__}: {exc}",
        }


def _zerobus_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    row = rows[0] if rows else {}
    return {
        "cumulative_committed_records": _nonnegative_int(
            _row_value(row, "cumulative_committed_records")
        ),
        "cumulative_committed_bytes": _nonnegative_int(
            _row_value(row, "cumulative_committed_bytes")
        ),
        "cumulative_error_count": _nonnegative_int(
            _row_value(row, "cumulative_error_count")
        ),
        "latest_commit_version": _nonnegative_int(
            _row_value(row, "latest_commit_version")
        ),
        "latest_commit_time": _row_value(row, "latest_commit_time"),
    }


def collect_observation(
    client: DatabricksRestClient,
    *,
    control_warehouse_id: str,
    catalog: str,
    schema: str,
    raw_table: str,
    mv_table: str,
    producer_progress_path: Path,
    since: datetime,
    run_id: str,
    iteration: int,
    timeout: float,
    poll_interval: float,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    iteration_started_at = iso_utc()
    producer = read_producer_progress(producer_progress_path)
    observation_time = utc_now() if observed_at is None else parse_utc_timestamp(observed_at)
    queries = build_control_queries(
        catalog,
        schema,
        raw_table,
        mv_table,
        since,
        observation_time,
    )
    results: dict[str, dict[str, Any]] = {}
    for name, sql in queries.items():
        results[name] = _execute_query(
            client,
            control_warehouse_id,
            sql,
            timeout=timeout,
            poll_interval=poll_interval,
            catalog=catalog,
            schema=schema,
            query_tags={
                "benchmark": "full-path-realtime",
                "run_id": run_id,
                "collector": "freshness",
                "query": name,
                "iteration": str(iteration),
            },
        )

    zerobus = _zerobus_summary(results["zerobus_ingest"]["rows"])
    raw_history_rows = results["raw_history"]["rows"]
    events = [
        _decode_event_row(row) for row in results["mv_events"]["rows"]
    ]
    latest_update = next(
        (
            row
            for row in events
            if str(_row_value(row, "event_type")).lower() == "update_progress"
        ),
        None,
    )
    latest_planning = next(
        (
            row
            for row in events
            if str(_row_value(row, "event_type")).lower() == "planning_information"
        ),
        None,
    )
    refresh = latest_successful_refresh(events)
    errors = [
        {"source": name, "error": result["error"]}
        for name, result in results.items()
        if result["error"]
    ]
    if producer["error"]:
        errors.insert(0, {"source": "producer_progress", "error": producer["error"]})

    observed = iso_utc(observation_time)
    refresh_finished_at = refresh.get("finished_at") if refresh else None
    source_snapshot = refresh.get("source_snapshot") if refresh else None
    mv_source_rows = (
        _nonnegative_int(source_snapshot.get("num_rows"))
        if isinstance(source_snapshot, Mapping)
        else None
    )
    raw_committed_rows = _nonnegative_int(
        zerobus.get("cumulative_committed_records")
    )
    mv_rows_behind = (
        max(0, raw_committed_rows - mv_source_rows)
        if raw_committed_rows is not None and mv_source_rows is not None
        else None
    )
    return {
        "schema_version": 1,
        "run_id": run_id,
        "iteration": iteration,
        "iteration_started_at": iteration_started_at,
        "observed_at": observed,
        "iteration_finished_at": iso_utc(),
        "since": iso_utc(since),
        "control_warehouse_id": control_warehouse_id,
        "target": {
            "catalog": catalog,
            "schema": schema,
            "raw_table": raw_table,
            "materialized_view": mv_table,
            "raw_full_name": ".".join((catalog, schema, raw_table)),
            "materialized_view_full_name": ".".join((catalog, schema, mv_table)),
        },
        "producer_progress": producer,
        "zerobus_ingest": zerobus,
        "raw_history": raw_history_rows[0] if raw_history_rows else None,
        # Deliberately unavailable: neither refresh output-row metrics nor the
        # aggregate MV row count measures raw-to-MV coverage. Querying the MV
        # would also perturb the data path under test.
        "mv_rows": None,
        "event_log": {
            "latest_update_progress": latest_update,
            "latest_planning_information": latest_planning,
            "bounded_rows": events,
            "limit": EVENT_LIMIT,
        },
        "latest_successful_refresh": refresh,
        "mv_source_rows": mv_source_rows,
        "mv_rows_behind": mv_rows_behind,
        "raw_commit_age_sec": timestamp_age_sec(
            observation_time, zerobus["latest_commit_time"]
        ),
        "producer_progress_age_sec": timestamp_age_sec(
            observation_time, producer["updated_at"]
        ),
        "mv_refresh_age_sec": timestamp_age_sec(
            observation_time, refresh_finished_at
        ),
        "age_semantics": AGE_SEMANTICS,
        "query_evidence": {
            name: {
                "statement_id": result["statement_id"],
                "row_count": result["row_count"],
                "error": result["error"],
            }
            for name, result in results.items()
        },
        "errors": errors,
    }


def progress_payload(observation: Mapping[str, Any]) -> dict[str, Any]:
    """Return the compact runner-facing and publishable observation."""
    producer = observation.get("producer_progress", {})
    zerobus = observation.get("zerobus_ingest", {})
    raw_history = observation.get("raw_history") or {}
    query_evidence = observation.get("query_evidence", {})
    return {
        "schema_version": observation.get("schema_version", 1),
        "run_id": observation.get("run_id"),
        "iteration": observation.get("iteration"),
        "updated_at": observation.get("iteration_finished_at")
        or observation.get("observed_at"),
        "observed_at": observation.get("observed_at"),
        "client_durable_rows": (
            producer.get("provider_committed_rows")
            if isinstance(producer, Mapping)
            else None
        ),
        "provider_committed_rows": (
            zerobus.get("cumulative_committed_records")
            if isinstance(zerobus, Mapping)
            else None
        ),
        "provider_committed_bytes": (
            zerobus.get("cumulative_committed_bytes")
            if isinstance(zerobus, Mapping)
            else None
        ),
        "provider_error_count": (
            zerobus.get("cumulative_error_count")
            if isinstance(zerobus, Mapping)
            else None
        ),
        "latest_commit_version": (
            zerobus.get("latest_commit_version")
            if isinstance(zerobus, Mapping)
            else None
        ),
        "latest_commit_time": (
            zerobus.get("latest_commit_time")
            if isinstance(zerobus, Mapping)
            else None
        ),
        "raw_delta_version": (
            _row_value(raw_history, "version")
            if isinstance(raw_history, Mapping)
            else None
        ),
        "raw_delta_timestamp": (
            _row_value(raw_history, "timestamp")
            if isinstance(raw_history, Mapping)
            else None
        ),
        "mv_rows": None,
        "latest_successful_refresh": observation.get("latest_successful_refresh"),
        "mv_source_rows": observation.get("mv_source_rows"),
        "mv_rows_behind": observation.get("mv_rows_behind"),
        "raw_commit_age_sec": observation.get("raw_commit_age_sec"),
        "producer_progress_age_sec": observation.get("producer_progress_age_sec"),
        "mv_refresh_age_sec": observation.get("mv_refresh_age_sec"),
        "age_semantics": observation.get("age_semantics", AGE_SEMANTICS),
        "statement_ids": {
            name: value.get("statement_id")
            for name, value in query_evidence.items()
            if isinstance(value, Mapping)
        }
        if isinstance(query_evidence, Mapping)
        else {},
        "errors": observation.get("errors", []),
    }


def build_parser(
    env: Mapping[str, str] | None = None,
) -> argparse.ArgumentParser:
    source = os.environ if env is None else env
    parser = argparse.ArgumentParser(
        description="Record bounded Zerobus and materialized-view freshness evidence"
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
    parser.add_argument(
        "--client-id", default=_env_first(source, "DATABRICKS_CLIENT_ID")
    )
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
        "--catalog",
        default=_env_first(source, "DATABRICKS_CATALOG", "DBX_CATALOG") or "workspace",
    )
    parser.add_argument(
        "--schema",
        default=_env_first(source, "DATABRICKS_SCHEMA", "DBX_SCHEMA") or "benchmarking",
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
    parser.add_argument(
        "--producer-progress",
        type=Path,
        default=_path_default(
            _env_first(
                source,
                "DATABRICKS_PRODUCER_PROGRESS_JSON",
                "DATABRICKS_PRODUCER_PROGRESS",
                "PRODUCER_PROGRESS_JSON",
            )
        ),
    )
    parser.add_argument(
        "--since",
        default=_env_first(
            source,
            "DATABRICKS_RUN_STARTED_AT",
            "BENCHMARK_RUN_STARTED_AT",
        ),
        help="required UTC ISO-8601 run start",
    )
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument(
        "--iterations", type=int, default=0, help="0 runs until signaled"
    )
    parser.add_argument(
        "--output",
        "--output-jsonl",
        dest="output",
        type=Path,
        default=_path_default(
            _env_first(source, "DATABRICKS_FRESHNESS_OUTPUT", "FRESHNESS_OUTPUT")
        ),
    )
    parser.add_argument(
        "--progress-json",
        "--progress",
        dest="progress_json",
        type=Path,
        default=_path_default(
            _env_first(
                source,
                "DATABRICKS_FRESHNESS_MONITOR_JSON",
                "DATABRICKS_FRESHNESS_PROGRESS",
            )
        ),
    )
    parser.add_argument(
        "--compact-event-log",
        action="store_true",
        help=(
            "retain only latest event summaries per sample; use the bounded "
            "collector for complete event-log export"
        ),
    )
    parser.add_argument(
        "--compact-output",
        action="store_true",
        help="persist only benchmark freshness and ingest-progress fields",
    )
    parser.add_argument(
        "--run-id",
        default=_env_first(source, "BENCHMARK_RUN_ID", "DATABRICKS_RUN_ID"),
    )
    parser.add_argument("--statement-timeout", type=float, default=120.0)
    parser.add_argument("--statement-poll-interval", type=float, default=0.5)
    parser.add_argument("--network-timeout", type=float, default=30.0)
    return parser


def finalize_args(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
) -> argparse.Namespace:
    if args.interval <= 0:
        parser.error("--interval must be greater than zero")
    if args.iterations < 0:
        parser.error("--iterations must be zero or greater")
    if args.statement_timeout <= 0 or args.statement_poll_interval <= 0:
        parser.error("statement timeout and poll interval must be greater than zero")
    if args.network_timeout <= 0:
        parser.error("--network-timeout must be greater than zero")
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
        parser.error("--host must be an HTTPS workspace origin without credentials or a path")
    if not args.control_warehouse_id:
        parser.error("--control-warehouse-id is required")
    if not args.producer_progress:
        parser.error("--producer-progress is required")
    if not args.output:
        parser.error("--output is required")
    if not args.progress_json:
        parser.error("--progress-json is required")
    if not args.run_id:
        parser.error("--run-id is required")
    if not args.since:
        parser.error("--since is required")
    try:
        args.since = parse_utc_timestamp(args.since)
    except (TypeError, ValueError, OverflowError, OSError) as exc:
        parser.error(f"--since must be an aware UTC ISO-8601 timestamp: {exc}")
    if bool(args.client_id) != bool(args.client_secret):
        parser.error("--client-id and --client-secret must be supplied together")
    if bool(args.token) == bool(args.client_id and args.client_secret):
        parser.error("configure exactly one of PAT or OAuth M2M credentials")
    for name in ("catalog", "schema", "raw_table", "mv_table"):
        try:
            quote_identifier(getattr(args, name))
        except ValueError as exc:
            parser.error(f"--{name.replace('_', '-')} is invalid: {exc}")
    args.producer_progress = args.producer_progress.expanduser().resolve()
    args.output = args.output.expanduser().resolve()
    args.progress_json = args.progress_json.expanduser().resolve()
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
    stop = threading.Event()
    signal_count = 0

    def request_stop(_signum: int, _frame: Any) -> None:
        nonlocal signal_count
        signal_count += 1
        stop.set()
        if signal_count >= 2:
            raise KeyboardInterrupt

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    print(
        f"Freshness run {args.run_id}; fixed-rate interval {args.interval:g}s; "
        f"writing {args.output}",
        file=sys.stderr,
    )
    anchor = time.monotonic()
    iteration = 0
    while not stop.is_set() and (args.iterations == 0 or iteration < args.iterations):
        delay = max(0.0, anchor + args.interval * iteration - time.monotonic())
        if delay and stop.wait(delay):
            break
        iteration += 1
        observation = collect_observation(
            client,
            control_warehouse_id=args.control_warehouse_id,
            catalog=args.catalog,
            schema=args.schema,
            raw_table=args.raw_table,
            mv_table=args.mv_table,
            producer_progress_path=args.producer_progress,
            since=args.since,
            run_id=args.run_id,
            iteration=iteration,
            timeout=args.statement_timeout,
            poll_interval=args.statement_poll_interval,
        )
        safe = redact_secrets(observation, secrets)
        if args.compact_event_log:
            event_log = safe.get("event_log")
            if isinstance(event_log, dict):
                event_log["bounded_rows"] = []
                event_log["compacted"] = True
        compact = progress_payload(safe)
        atomic_append_jsonl(args.output, compact if args.compact_output else safe)
        atomic_write_json(args.progress_json, compact)
        print(
            f"[{observation['observed_at']}] iteration={iteration} "
            f"producer_rows={observation['producer_progress']['provider_committed_rows']} "
            f"zerobus_rows={observation['zerobus_ingest']['cumulative_committed_records']} "
            f"errors={len(observation['errors'])}",
            file=sys.stderr,
        )

    print(f"Stopped after {iteration} iteration(s).", file=sys.stderr)
    return 0


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
