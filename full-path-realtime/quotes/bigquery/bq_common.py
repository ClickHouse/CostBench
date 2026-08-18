#!/usr/bin/env python3
"""Shared, deliberately small BigQuery benchmark helpers."""

from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

PLACEHOLDERS = ("__PROJECT_ID__", "__DATASET_ID__")
OFFSET_ALREADY_WRITTEN_RE = re.compile(
    r"offset is within stream, expected offset (?P<expected>\d+), received (?P<received>\d+)",
    re.IGNORECASE,
)


def offset_already_written_matches(message: str, offset: int, rows: int) -> bool:
    """Confirm that an ALREADY_EXISTS response describes this exact replayed batch."""
    match = OFFSET_ALREADY_WRITTEN_RE.search(message)
    if match is None:
        return False
    expected = int(match.group("expected"))
    received = int(match.group("received"))
    return received == offset and expected == offset + rows


def close_google_client(client: Any) -> None:
    """Close a Google client across library versions when a hook exists."""
    close = getattr(client, "close", None)
    if callable(close):
        close()
        return
    transport = getattr(client, "transport", None)
    transport_close = getattr(transport, "close", None)
    if callable(transport_close):
        transport_close()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime | None = None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return iso_utc(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    if hasattr(value, "items"):
        return dict(value)
    return str(value)


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, default=json_default, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        json.dump(payload, handle, default=json_default, separators=(",", ":"), sort_keys=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def render_sql(sql: str, project_id: str, dataset_id: str) -> str:
    rendered = sql.replace("__PROJECT_ID__", project_id).replace("__DATASET_ID__", dataset_id)
    missing = [token for token in PLACEHOLDERS if token in rendered]
    if missing:
        raise ValueError(f"unresolved SQL placeholders: {', '.join(missing)}")
    return rendered


def split_sql_statements(sql: str) -> list[str]:
    """Split GoogleSQL on semicolons outside strings/backticks and -- comments."""
    statements: list[str] = []
    current: list[str] = []
    quote: str | None = None
    i = 0
    while i < len(sql):
        char = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""

        if quote is None and char == "-" and nxt == "-":
            i += 2
            while i < len(sql) and sql[i] not in "\r\n":
                i += 1
            current.append("\n")
            continue

        if quote is None and char in ("'", '"', "`"):
            quote = char
            current.append(char)
            i += 1
            continue

        if quote is not None:
            current.append(char)
            if char == quote:
                if quote in ("'", '"') and nxt == quote:
                    current.append(nxt)
                    i += 2
                    continue
                quote = None
            elif char == "\\" and nxt:
                current.append(nxt)
                i += 2
                continue
            i += 1
            continue

        if char == ";":
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(char)
        i += 1

    if quote is not None:
        raise ValueError(f"unterminated SQL quote {quote!r}")
    statement = "".join(current).strip()
    if statement:
        statements.append(statement)
    return statements


def load_queries(path: Path, project_id: str, dataset_id: str) -> list[str]:
    return split_sql_statements(render_sql(path.read_text(encoding="utf-8"), project_id, dataset_id))


def label_value(value: str) -> str:
    value = re.sub(r"[^a-z0-9_-]+", "-", value.lower()).strip("-_")
    return (value or "unknown")[:63]


def job_labels(component: str, run_id: str) -> dict[str, str]:
    return {
        "benchmark": "full-path-realtime",
        "component": label_value(component),
        "run_id": label_value(run_id),
    }


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def query_job_stats(job: Any, client_wall_s: float | None = None, error: str | None = None) -> dict[str, Any]:
    """Extract the same metrics as Bench2Cost/run_bq_bench.sh plus evidence."""
    # QueryJob.to_api_repr() is the jobs.insert submission representation: it
    # intentionally contains only jobReference/configuration, not the
    # server-returned statistics. reload() stores the jobs.get resource in
    # _properties, which is the Python equivalent of Bench2Cost's `bq show -j`.
    server_resource = getattr(job, "_properties", None) if job is not None else None
    resource = server_resource if isinstance(server_resource, dict) else (job.to_api_repr() if job is not None else {})
    statistics = resource.get("statistics", {})
    query_stats = statistics.get("query", {})

    runtime_ms = statistics.get("finalExecutionDurationMs")
    if runtime_ms is not None:
        runtime_s = int(runtime_ms) / 1000.0
        runtime_source = "statistics.finalExecutionDurationMs"
    elif getattr(job, "started", None) and getattr(job, "ended", None):
        runtime_s = (job.ended - job.started).total_seconds()
        runtime_source = "job.ended-job.started"
    else:
        runtime_s = None
        runtime_source = None

    slot_ms = statistics.get("totalSlotMs", getattr(job, "slot_millis", None))
    billed_bytes = query_stats.get("totalBytesBilled", statistics.get("totalBytesBilled"))
    processed_bytes = query_stats.get("totalBytesProcessed")
    cache_hit = query_stats.get("cacheHit", getattr(job, "cache_hit", None))

    return {
        "job_id": getattr(job, "job_id", None),
        "location": getattr(job, "location", None),
        "state": getattr(job, "state", None),
        "statement_type": getattr(job, "statement_type", None),
        "created_at": iso_utc(getattr(job, "created", None)),
        "started_at": iso_utc(getattr(job, "started", None)),
        "ended_at": iso_utc(getattr(job, "ended", None)),
        "runtime_sec": runtime_s,
        "runtime_source": runtime_source,
        "client_wall_sec": client_wall_s,
        "total_slot_ms": _as_int(slot_ms),
        "billed_slot_sec": None if slot_ms is None else int(slot_ms) / 1000.0,
        "total_bytes_billed": _as_int(billed_bytes),
        "total_bytes_processed": _as_int(processed_bytes),
        "cache_hit": cache_hit,
        "reservation_usage": statistics.get("reservationUsage"),
        "referenced_tables": query_stats.get("referencedTables"),
        "materialized_view_statistics": query_stats.get("materializedViewStatistics"),
        "error": error,
    }


def aligned_metric_arrays(job_stats: list[dict[str, Any]]) -> dict[str, list[list[Any]]]:
    """Return query/trial arrays with the Bench2Cost BigQuery field names."""
    return {
        "result": [[item["runtime_sec"] if item.get("error") is None else None] for item in job_stats],
        "billed_slot_sec": [[item.get("billed_slot_sec")] for item in job_stats],
        "billed_bytes": [[item.get("total_bytes_billed")] for item in job_stats],
        "processed_bytes": [[item.get("total_bytes_processed")] for item in job_stats],
    }


def table_snapshot(client: Any, table_id: str) -> dict[str, Any]:
    table = client.get_table(table_id)
    materialized = table._properties.get("materializedView", {})  # API field not fully exposed by the client.
    return {
        "table_id": table_id,
        "table_type": getattr(table, "table_type", None),
        "num_rows": _as_int(getattr(table, "num_rows", None)),
        "num_bytes": _as_int(getattr(table, "num_bytes", None)),
        "modified_at": iso_utc(getattr(table, "modified", None)),
        "mview_last_refresh_time": iso_utc(getattr(table, "mview_last_refresh_time", None)),
        "mview_refresh_watermark": materialized.get("refreshWatermark"),
        "mview_enable_refresh": getattr(table, "mview_enable_refresh", None),
        "mview_refresh_interval_ms": (
            None
            if getattr(table, "mview_refresh_interval", None) is None
            else int(table.mview_refresh_interval.total_seconds() * 1000)
        ),
    }


def read_progress(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def rows_to_dicts(rows: Iterable[Any]) -> list[dict[str, Any]]:
    return [dict(row.items()) for row in rows]
