#!/usr/bin/env python3
"""Shared Lakehouse//RT dashboard and drill-down query runner.

The Databricks connector is deliberately imported only when an online run
opens a connection. This keeps parsing, validation, and unit tests usable with
``python3 -S``.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.metadata
import json
import math
import os
import signal
import sys
import threading
import time
import urllib.parse
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from dbx_common import (
    DatabricksRestClient,
    atomic_append_jsonl,
    iso_utc,
    json_default,
    load_queries,
    normalize_host,
    quote_identifier,
    redact_secrets,
    statement_rows,
    utc_now,
)

CONNECTOR_VERSION = "4.5.0"
RESULT_FETCH_BATCH_SIZE = 1_000
SESSION_CONFIGURATION = {
    "use_cached_result": "false",
    "ansi_mode": "true",
    "timezone": "UTC",
}
EXPECTED_QUERY_COUNTS = {"dashboard": 4, "drilldown": 2}
FINAL_HISTORY_STATUSES = {"FINISHED", "FAILED", "CANCELED"}
MAX_HISTORY_BYTES = 64 * 1024
ALIGNED_FIELDS = (
    "result",
    "compilation_time",
    "execution_time",
    "queue_time",
    "result_fetch_time",
    "client_wall_time",
    "statement_ids",
    "cache_hit",
    "read_io_cache_percent",
    "result_row_count",
    "result_hash",
)


def _env_first(env: Mapping[str, str], *names: str) -> str | None:
    for name in names:
        value = env.get(name)
        if value is not None and value.strip():
            return value.strip()
    return None


def _path_default(value: str | None) -> Path | None:
    return Path(value).expanduser() if value else None


def build_parser(
    workload: str,
    default_queries: Path,
    default_interval: float,
    *,
    env: Mapping[str, str] | None = None,
) -> argparse.ArgumentParser:
    if workload not in EXPECTED_QUERY_COUNTS:
        raise ValueError(f"unsupported workload: {workload}")
    source = os.environ if env is None else env
    parser = argparse.ArgumentParser(
        description=f"Run the Databricks Statement Execution {workload} workload"
    )
    parser.add_argument("--queries", type=Path, default=default_queries)
    parser.add_argument("--interval", type=float, default=default_interval)
    parser.add_argument("--iterations", type=int, default=0, help="0 runs until stopped")
    parser.add_argument(
        "--output",
        type=Path,
        default=_path_default(
            _env_first(source, "DATABRICKS_RUNNER_OUTPUT", "BENCHMARK_OUTPUT")
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=_path_default(
            _env_first(source, "DATABRICKS_RUNNER_OUTPUT_DIR", "BENCHMARK_OUTPUT_DIR")
        )
        or Path("."),
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
        "--client-id",
        default=_env_first(source, "DATABRICKS_CLIENT_ID"),
    )
    parser.add_argument(
        "--client-secret",
        default=_env_first(source, "DATABRICKS_CLIENT_SECRET"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--rt-warehouse-id",
        "--query-warehouse-id",
        "--warehouse",
        dest="rt_warehouse_id",
        default=_env_first(
            source,
            "DATABRICKS_QUERY_WAREHOUSE_ID",
            "DATABRICKS_RT_WAREHOUSE_ID",
            "DBX_RT_WAREHOUSE_ID",
            "RT_WAREHOUSE_ID",
        ),
    )
    parser.add_argument(
        "--warehouse-mode",
        choices=("serverless-baseline", "lakehouse-rt"),
        default=_env_first(source, "DATABRICKS_WAREHOUSE_MODE")
        or "lakehouse-rt",
    )
    parser.add_argument(
        "--http-path",
        default=_env_first(source, "DATABRICKS_HTTP_PATH", "DATABRICKS_RT_HTTP_PATH"),
    )
    parser.add_argument(
        "--control-warehouse-id",
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
        default=_env_first(source, "DATABRICKS_RAW_TABLE", "DBX_RAW_TABLE")
        or "quotes",
    )
    parser.add_argument(
        "--mv-table",
        default=_env_first(source, "DATABRICKS_MV_TABLE", "DBX_MV_TABLE") or "quotes_daily",
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
        "--freshness-monitor",
        type=Path,
        default=_path_default(
            _env_first(
                source,
                "DATABRICKS_FRESHNESS_MONITOR_JSON",
                "DATABRICKS_FRESHNESS_MONITOR",
                "FRESHNESS_MONITOR_JSON",
            )
        ),
    )
    parser.add_argument(
        "--system",
        default=_env_first(source, "BENCHMARK_SYSTEM", "DATABRICKS_SYSTEM")
        or "Databricks",
    )
    parser.add_argument(
        "--machine",
        default=_env_first(source, "BENCHMARK_MACHINE", "DATABRICKS_MACHINE") or "unknown",
    )
    parser.add_argument(
        "--cluster-size",
        default=_env_first(source, "BENCHMARK_CLUSTER_SIZE", "DATABRICKS_CLUSTER_SIZE")
        or "1",
    )
    parser.add_argument(
        "--comment",
        default=_env_first(source, "BENCHMARK_COMMENT", "DATABRICKS_COMMENT") or "",
    )
    parser.add_argument(
        "--run-id",
        default=_env_first(source, "BENCHMARK_RUN_ID", "DATABRICKS_RUN_ID"),
    )
    parser.add_argument(
        "--tags",
        default=_env_first(source, "BENCHMARK_TAGS", "DATABRICKS_TAGS")
        or "Databricks,managed,lakehouse-rt",
        help="Comma-separated report tags",
    )
    parser.add_argument("--history-timeout", type=float, default=120.0)
    parser.add_argument("--history-poll-interval", type=float, default=1.0)
    parser.add_argument("--control-timeout", type=float, default=120.0)
    parser.add_argument(
        "--print-queries",
        action="store_true",
        help="Render queries and exit without loading the connector or using the network",
    )
    return parser


def _warehouse_id_from_path(http_path: str | None) -> str | None:
    if not http_path:
        return None
    marker = "/sql/1.0/warehouses/"
    normalized = "/" + http_path.strip().lstrip("/").rstrip("/")
    if marker not in normalized:
        return None
    value = normalized.rsplit("/", 1)[-1]
    return value or None


def finalize_args(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
) -> argparse.Namespace:
    if args.interval <= 0:
        parser.error("--interval must be greater than zero")
    if args.iterations < 0:
        parser.error("--iterations must be zero or greater")
    if args.history_timeout < 0 or args.history_poll_interval <= 0:
        parser.error("--history-timeout must be >= 0 and --history-poll-interval must be > 0")
    if args.control_timeout <= 0:
        parser.error("--control-timeout must be greater than zero")

    args.host = normalize_host(args.host)
    path_warehouse_id = _warehouse_id_from_path(args.http_path)
    if args.rt_warehouse_id and path_warehouse_id:
        if args.rt_warehouse_id != path_warehouse_id:
            parser.error("--query-warehouse-id does not match --http-path")
    elif path_warehouse_id:
        args.rt_warehouse_id = path_warehouse_id
    elif args.rt_warehouse_id and not args.http_path:
        args.http_path = f"/sql/1.0/warehouses/{args.rt_warehouse_id}"

    if args.print_queries:
        return args
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
    if not args.rt_warehouse_id or not args.http_path:
        parser.error("--query-warehouse-id or a warehouse --http-path is required")
    if not args.control_warehouse_id:
        parser.error("--control-warehouse-id is required")
    if args.control_warehouse_id == args.rt_warehouse_id:
        parser.error("the control warehouse must be separate from the query warehouse")
    if not args.producer_progress:
        parser.error("--producer-progress is required; raw COUNT(*) is prohibited")
    if bool(args.client_id) != bool(args.client_secret):
        parser.error("--client-id and --client-secret must be supplied together")
    if bool(args.token) == bool(args.client_id and args.client_secret):
        parser.error("configure exactly one of PAT or OAuth M2M credentials")
    return args


def fixed_rate_next_fire(anchor: float, interval: float, completed_iterations: int) -> float:
    """Return the next anchored fire time after *completed_iterations*."""
    if interval <= 0 or completed_iterations < 0:
        raise ValueError("interval must be positive and completed_iterations non-negative")
    return anchor + interval * completed_iterations


def fixed_rate_delay(
    anchor: float,
    interval: float,
    completed_iterations: int,
    now: float,
) -> float:
    return max(0.0, fixed_rate_next_fire(anchor, interval, completed_iterations) - now)


def _stable_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return {"__float__": "nan"}
        if math.isinf(value):
            return {"__float__": "inf" if value > 0 else "-inf"}
        return value
    if isinstance(value, Decimal):
        return {"__decimal__": str(value)}
    if isinstance(value, datetime):
        return {"__datetime__": iso_utc(value)}
    if isinstance(value, date):
        return {"__date__": value.isoformat()}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"__bytes__": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, Mapping):
        return {
            str(key): _stable_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_stable_value(item) for item in value]
    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        return _stable_value(tolist())
    as_dict = getattr(value, "asDict", None)
    if callable(as_dict):
        return _stable_value(as_dict())
    return {"__python__": f"{type(value).__module__}.{type(value).__qualname__}", "value": str(value)}


def stable_result_hash(rows: Iterable[Any]) -> tuple[int, str]:
    """Hash ordered result rows with deterministic, type-aware JSON framing."""
    digest = hashlib.sha256()
    count = 0
    for row in rows:
        encoded = json.dumps(
            _stable_value(row),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        count += 1
    return count, digest.hexdigest()


def drain_cursor(cursor: Any) -> tuple[int, str]:
    digest = hashlib.sha256()
    count = 0
    while True:
        rows = cursor.fetchmany(RESULT_FETCH_BATCH_SIZE)
        if not rows:
            break
        for row in rows:
            encoded = json.dumps(
                _stable_value(row),
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
            count += 1
    return count, digest.hexdigest()


def _number(value: Any) -> int | float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(result):
        return None
    return int(result) if result.is_integer() else result


def _row_count(value: Any) -> int | None:
    number = _number(value)
    if number is None or number < 0 or int(number) != number:
        return None
    return int(number)


def _history_value(
    record: Mapping[str, Any],
    aliases: Sequence[str],
) -> tuple[Any, str | None]:
    metrics = record.get("metrics")
    containers = (
        ("history", record),
        ("history.metrics", metrics if isinstance(metrics, Mapping) else {}),
    )
    for prefix, container in containers:
        for alias in aliases:
            if alias in container and container[alias] is not None:
                return container[alias], f"{prefix}.{alias}"
    return None, None


def _history_number(
    record: Mapping[str, Any],
    aliases: Sequence[str],
) -> tuple[int | float | None, str | None]:
    value, source = _history_value(record, aliases)
    return _number(value), source


def _cache_evidence(record: Mapping[str, Any]) -> tuple[Any, str | None]:
    metrics = record.get("metrics")
    found: list[tuple[str, Any]] = []
    for prefix, container in (
        ("history", record),
        ("history.metrics", metrics if isinstance(metrics, Mapping) else {}),
    ):
        for name in ("result_from_cache", "from_result_cache"):
            if name in container:
                found.append((f"{prefix}.{name}", container[name]))
    if not found:
        return None, None
    source = ",".join(name for name, _value in found)
    if all(value is False for _name, value in found):
        return False, source
    if any(value is True for _name, value in found):
        return True, source
    return found[0][1], source


def normalize_query_history(record: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize current Query History API and system-table field aliases."""
    duration_ms, duration_source = _history_number(
        record,
        ("duration", "total_duration_ms", "total_time_ms"),
    )
    compilation_ms, compilation_source = _history_number(
        record,
        ("compilation_time_ms", "compilation_duration_ms"),
    )
    execution_ms, execution_source = _history_number(
        record,
        ("execution_time_ms", "execution_duration_ms"),
    )
    result_fetch_ms, result_fetch_source = _history_number(
        record,
        ("result_fetch_time_ms", "result_fetch_duration_ms"),
    )
    waiting_at_capacity_ms, waiting_at_capacity_source = _history_number(
        record,
        ("waiting_at_capacity_duration_ms", "waiting_at_capacity_ms"),
    )
    waiting_for_compute_ms, waiting_for_compute_source = _history_number(
        record,
        ("waiting_for_compute_duration_ms", "waiting_for_compute_ms"),
    )
    generic_queue_ms, generic_queue_source = _history_number(
        record,
        (
            "queue_duration_ms",
            "queue_time_ms",
            "queue_ms",
            "overloading_queue_duration_ms",
            "provisioning_queue_duration_ms",
        ),
    )
    queue_candidates = (
        (waiting_at_capacity_ms, waiting_at_capacity_source),
        (waiting_for_compute_ms, waiting_for_compute_source),
        (generic_queue_ms, generic_queue_source),
    )
    queue_ms, queue_source = next(
        ((value, source) for value, source in queue_candidates if value is not None),
        (None, None),
    )
    bytes_read, bytes_read_source = _history_number(
        record,
        ("read_bytes", "bytes_read", "read_files_bytes"),
    )
    files_read, files_read_source = _history_number(
        record,
        ("read_files_count", "files_read", "read_files"),
    )
    partitions_read, partitions_read_source = _history_number(
        record,
        ("read_partitions_count", "partitions_read", "read_partitions"),
    )
    rows_read, rows_read_source = _history_number(
        record,
        ("rows_read_count", "read_rows", "rows_read"),
    )
    rows_produced, rows_produced_source = _history_number(
        record,
        ("rows_produced", "rows_produced_count", "produced_rows"),
    )
    cache_value, cache_source = _cache_evidence(record)
    cache_origin, cache_origin_source = _history_value(
        record,
        ("cache_query_id", "cache_origin_statement_id"),
    )
    read_cache_bytes, read_cache_bytes_source = _history_number(
        record,
        ("read_cache_bytes", "read_io_cache_bytes"),
    )
    read_io_percent, read_io_percent_source = _history_number(
        record,
        ("read_io_cache_percent", "read_cache_percent"),
    )
    if read_io_percent is None and read_cache_bytes is not None and bytes_read:
        read_io_percent = 100.0 * float(read_cache_bytes) / float(bytes_read)
        read_io_percent_source = (
            f"derived:{read_cache_bytes_source}/{bytes_read_source}"
        )

    status = record.get("status", record.get("execution_status"))
    statement_id = record.get("statement_id", record.get("query_id"))
    is_final = (
        record.get("is_final") is True
        if "is_final" in record
        else str(status).upper() in FINAL_HISTORY_STATUSES
    )
    return {
        "statement_id": statement_id,
        "status": status,
        "is_final": is_final,
        "duration_ms": duration_ms,
        "duration_sec": None if duration_ms is None else float(duration_ms) / 1000.0,
        "duration_source": duration_source,
        "compilation_time_ms": compilation_ms,
        "compilation_source": compilation_source,
        "execution_time_ms": execution_ms,
        "execution_source": execution_source,
        "queue_time_ms": queue_ms,
        "queue_source": queue_source,
        "waiting_at_capacity_ms": waiting_at_capacity_ms,
        "waiting_at_capacity_source": waiting_at_capacity_source,
        "waiting_for_compute_ms": waiting_for_compute_ms,
        "waiting_for_compute_source": waiting_for_compute_source,
        "result_fetch_time_ms": result_fetch_ms,
        "result_fetch_source": result_fetch_source,
        "bytes_read": bytes_read,
        "bytes_read_source": bytes_read_source,
        "files_read": files_read,
        "files_read_source": files_read_source,
        "partitions_read": partitions_read,
        "partitions_read_source": partitions_read_source,
        "rows_read": rows_read,
        "rows_read_source": rows_read_source,
        "rows_produced": rows_produced,
        "rows_produced_source": rows_produced_source,
        "result_from_cache": cache_value,
        "result_from_cache_source": cache_source,
        "cache_origin_statement_id": cache_origin,
        "cache_origin_source": cache_origin_source,
        "read_cache_bytes": read_cache_bytes,
        "read_cache_bytes_source": read_cache_bytes_source,
        "read_io_cache_percent": read_io_percent,
        "read_io_cache_percent_source": read_io_percent_source,
    }


def _history_is_final(record: Mapping[str, Any]) -> bool:
    status = str(record.get("status", record.get("execution_status", ""))).upper()
    if "is_final" in record:
        return record.get("is_final") is True
    return status in FINAL_HISTORY_STATUSES


def poll_query_history(
    client: DatabricksRestClient,
    statement_id: str,
    *,
    timeout: float,
    poll_interval: float,
    clock: Any = time.monotonic,
    sleeper: Any = time.sleep,
) -> tuple[dict[str, Any] | None, str | None]:
    """Poll by Statement/Query ID until Query History reports a final record."""
    deadline = clock() + max(0.0, timeout)
    latest: dict[str, Any] | None = None
    last_error: str | None = None
    while True:
        try:
            candidate = client.query_history_by_statement_id(statement_id)
            if candidate is not None:
                latest = candidate
                if _history_is_final(candidate):
                    return candidate, None
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        now = clock()
        if now >= deadline:
            if latest is not None:
                return latest, "query history never became final before timeout"
            suffix = f"; last lookup error: {last_error}" if last_error else ""
            return None, f"query history was unavailable before timeout{suffix}"
        sleeper(min(poll_interval, deadline - now))


def bounded_history_record(
    record: Mapping[str, Any] | None,
    *,
    secrets: Sequence[str] = (),
    max_bytes: int = MAX_HISTORY_BYTES,
) -> Any:
    if record is None:
        return None
    safe = redact_secrets(record, secrets)
    encoded = json.dumps(
        safe,
        default=json_default,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if len(encoded) <= max_bytes:
        return safe
    return {
        "omitted": True,
        "reason": "redacted query-history record exceeded evidence bound",
        "encoded_bytes": len(encoded),
        "max_bytes": max_bytes,
        "sha256": hashlib.sha256(encoded).hexdigest(),
    }


def read_json_snapshot(path: Path | None) -> dict[str, Any]:
    loaded_at = iso_utc()
    if path is None:
        return {"path": None, "loaded_at": loaded_at, "data": None, "error": "not configured"}
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise ValueError("top-level JSON value is not an object")
        stat = path.stat()
        modified = datetime.fromtimestamp(stat.st_mtime, tz=utc_now().tzinfo)
        return {
            "path": str(path),
            "loaded_at": loaded_at,
            "file_modified_at": iso_utc(modified),
            "data": data,
            "error": None,
        }
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {
            "path": str(path),
            "loaded_at": loaded_at,
            "data": None,
            "error": f"{type(exc).__name__}: {exc}",
        }


def producer_progress_snapshot(path: Path | None) -> dict[str, Any]:
    snapshot = read_json_snapshot(path)
    data = snapshot.pop("data")
    rows = None
    rows_source = None
    progress_updated_at = None
    progress_run_id = None
    finished = None
    progress_fields: dict[str, Any] = {}
    if isinstance(data, Mapping):
        progress_updated_at = data.get("updated_at")
        progress_run_id = data.get("run_id")
        finished = data.get("finished")
        progress_fields = {
            name: data.get(name)
            for name in (
                "logical_raw_rows",
                "provider_committed_rows",
                "baseline_table_rows",
                "baseline_table_rows_unknown",
                "running",
                "finished",
            )
        }
        logical = _row_count(data.get("logical_raw_rows"))
        if logical is not None:
            rows = logical
            rows_source = "producer_progress.logical_raw_rows"
        else:
            committed = _row_count(data.get("provider_committed_rows"))
            baseline = _row_count(data.get("baseline_table_rows"))
            if committed is not None and baseline is not None:
                rows = committed + baseline
                rows_source = (
                    "producer_progress.provider_committed_rows"
                    "+baseline_table_rows"
                )
            else:
                snapshot["error"] = snapshot.get("error") or (
                    "producer progress omitted logical_raw_rows and the "
                    "provider_committed_rows+baseline_table_rows fallback"
                )
    return {
        **snapshot,
        "progress_updated_at": progress_updated_at,
        "progress_run_id": progress_run_id,
        "finished": finished,
        "raw_rows": rows,
        "raw_rows_source": rows_source,
        "progress_fields": progress_fields,
    }


def _nested_value(data: Mapping[str, Any], path: Sequence[str]) -> Any:
    current: Any = data
    for component in path:
        if not isinstance(current, Mapping) or component not in current:
            return None
        current = current[component]
    return current


def freshness_progress_snapshot(path: Path | None) -> dict[str, Any]:
    snapshot = read_json_snapshot(path)
    data = snapshot.pop("data")
    progress_updated_at = None
    refresh_finished_at = None
    mv_source_rows = None
    mv_rows_behind = None
    if isinstance(data, Mapping):
        progress_updated_at = next(
            (
                data.get(name)
                for name in ("updated_at", "observed_at", "sampled_at")
                if data.get(name) is not None
            ),
            None,
        )
        mv_source_rows = data.get("mv_source_rows")
        mv_rows_behind = data.get("mv_rows_behind")
        refresh_finished_at = next(
            (
                _nested_value(data, candidate_path)
                for candidate_path in (
                    ("latest_successful_refresh", "finished_at"),
                    ("latest_successful_refresh", "completed_at"),
                    ("latest_successful_refresh", "finish_time"),
                    ("latest_successful", "finished_at"),
                    ("latest_successful", "completed_at"),
                    ("latest_refresh", "finished_at"),
                    ("latest_refresh", "completed_at"),
                    ("refresh_finished_at",),
                )
                if _nested_value(data, candidate_path) is not None
            ),
            None,
        )
    return {
        **snapshot,
        "progress_updated_at": progress_updated_at,
        "refresh_finished_at": refresh_finished_at,
        "mv_source_rows": mv_source_rows,
        "mv_rows_behind": mv_rows_behind,
        # Kept nullable for the provider-neutral result schema. The harness
        # deliberately does not query MV contents, and flow output rows are
        # not the materialized view's total row count.
        "mv_rows": None,
        "mv_rows_source": "not_collected_auxiliary_data_queries_prohibited",
    }


def _history_seconds(normalized: Mapping[str, Any], key: str) -> float | None:
    value = normalized.get(key)
    return None if value is None else float(value) / 1000.0


def failed_observation(
    query_number: int,
    sql: str,
    error: str,
) -> dict[str, Any]:
    return {
        "query_number": query_number,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "statement_id": None,
        "query_tags": None,
        "execution_succeeded": False,
        "result_row_count": None,
        "result_hash_sha256": None,
        "client_wall_time_sec": None,
        "canonical_duration_sec": None,
        "metrics": {},
        "history_record": None,
        "errors": [error],
    }


def execute_measured_query(
    connection: Any,
    rest_client: DatabricksRestClient,
    sql: str,
    *,
    query_number: int,
    iteration: int,
    workload: str,
    run_id: str,
    history_timeout: float,
    history_poll_interval: float,
    secrets: Sequence[str] = (),
    clock: Any = time.monotonic,
) -> dict[str, Any]:
    tags = {
        "benchmark": "full-path-realtime",
        "run_id": run_id,
        "workload": workload,
        "query_number": str(query_number),
        "iteration": str(iteration),
    }
    errors: list[str] = []
    statement_id = None
    row_count = None
    result_hash = None
    execution_succeeded = False
    cursor = None
    started = clock()
    try:
        cursor = connection.cursor()
        cursor.execute(sql, query_tags=tags)
        statement_id = getattr(cursor, "query_id", None)
        row_count, result_hash = drain_cursor(cursor)
        execution_succeeded = True
    except Exception as exc:
        if cursor is not None and statement_id is None:
            statement_id = getattr(cursor, "query_id", None)
        errors.append(f"query execution failed: {type(exc).__name__}: {exc}")
    finally:
        client_wall_time = max(0.0, clock() - started)
        if cursor is not None:
            try:
                cursor.close()
            except Exception as exc:
                errors.append(f"cursor close failed: {type(exc).__name__}: {exc}")

    history = None
    history_error = None
    normalized: dict[str, Any] = {}
    if statement_id:
        history, history_error = poll_query_history(
            rest_client,
            str(statement_id),
            timeout=history_timeout,
            poll_interval=history_poll_interval,
        )
        if history is not None:
            normalized = normalize_query_history(history)
    else:
        errors.append("cursor.query_id was missing; query history cannot be correlated")
    if history_error:
        errors.append(history_error)

    canonical_duration = normalized.get("duration_sec")
    status = str(normalized.get("status", "")).upper()
    cache_value = normalized.get("result_from_cache")
    if history is None:
        errors.append("canonical query-history record is missing")
    elif not normalized.get("is_final"):
        errors.append("query-history record is not final")
    elif status and status != "FINISHED":
        errors.append(f"query-history status is {status}, not FINISHED")
    if canonical_duration is None:
        errors.append("canonical provider duration is missing from query history")
    if cache_value is True:
        errors.append("query result cache hit detected; observation rejected")
    elif cache_value is not False:
        errors.append(
            "query result cache evidence is missing or not exact false; observation rejected"
        )
    if normalized.get("cache_origin_statement_id") not in (None, ""):
        errors.append("query history reports a cache origin; observation rejected")
    if not execution_succeeded:
        canonical_duration = None
    if errors:
        canonical_duration = None

    safe_errors = [
        str(redact_secrets(error, secrets))
        for error in errors
    ]
    return {
        "query_number": query_number,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "statement_id": str(statement_id) if statement_id else None,
        "query_tags": tags,
        "execution_succeeded": execution_succeeded,
        "result_row_count": row_count,
        "result_hash_sha256": result_hash,
        "client_wall_time_sec": client_wall_time,
        "canonical_duration_sec": canonical_duration,
        "metrics": normalized,
        "history_record": bounded_history_record(history, secrets=secrets),
        "errors": safe_errors,
    }


def aligned_metric_arrays(
    observations: Sequence[Mapping[str, Any]],
) -> dict[str, list[list[Any]]]:
    def metric_seconds(observation: Mapping[str, Any], name: str) -> float | None:
        metrics = observation.get("metrics")
        if not isinstance(metrics, Mapping):
            return None
        return _history_seconds(metrics, name)

    return {
        "result": [[observation.get("canonical_duration_sec")] for observation in observations],
        "compilation_time": [
            [metric_seconds(observation, "compilation_time_ms")]
            for observation in observations
        ],
        "execution_time": [
            [metric_seconds(observation, "execution_time_ms")]
            for observation in observations
        ],
        "queue_time": [
            [metric_seconds(observation, "queue_time_ms")]
            for observation in observations
        ],
        "result_fetch_time": [
            [metric_seconds(observation, "result_fetch_time_ms")]
            for observation in observations
        ],
        "client_wall_time": [
            [observation.get("client_wall_time_sec")]
            for observation in observations
        ],
        "statement_ids": [
            [observation.get("statement_id")]
            for observation in observations
        ],
        "cache_hit": [
            [
                (
                    observation.get("metrics", {}).get("result_from_cache")
                    if isinstance(observation.get("metrics"), Mapping)
                    else None
                )
            ]
            for observation in observations
        ],
        "read_io_cache_percent": [
            [
                (
                    observation.get("metrics", {}).get("read_io_cache_percent")
                    if isinstance(observation.get("metrics"), Mapping)
                    else None
                )
            ]
            for observation in observations
        ],
        "result_row_count": [
            [observation.get("result_row_count")]
            for observation in observations
        ],
        "result_hash": [
            [observation.get("result_hash_sha256")]
            for observation in observations
        ],
    }


def validate_jsonl_record(record: Mapping[str, Any], query_count: int) -> None:
    required = (
        "iteration",
        "iteration_started_at",
        "iteration_finished_at",
        "raw_rows",
        "mv_rows",
        "system",
        "version",
        "machine",
        "cluster_size",
        "comment",
        "tags",
        "result",
        "query_evidence",
        "query_errors",
    )
    missing = [name for name in required if name not in record]
    if missing:
        raise ValueError(f"JSONL record omitted required keys: {', '.join(missing)}")
    if not isinstance(record["tags"], list):
        raise ValueError("tags must be an array")
    for name in ALIGNED_FIELDS:
        value = record.get(name)
        if not isinstance(value, list) or len(value) != query_count:
            raise ValueError(f"{name} must contain exactly {query_count} query entries")
        if any(not isinstance(entry, list) or len(entry) != 1 for entry in value):
            raise ValueError(f"{name} entries must be single-trial arrays")
    for name in ("query_evidence", "query_errors"):
        value = record[name]
        if not isinstance(value, list) or len(value) != query_count:
            raise ValueError(f"{name} must contain exactly {query_count} entries")
    for name in ("raw_rows", "mv_rows"):
        value = record[name]
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
            raise ValueError(f"{name} must be a non-negative integer or null")


def _control_scalar(
    client: DatabricksRestClient,
    warehouse_id: str,
    statement: str,
    *,
    timeout: float,
    catalog: str,
    schema: str,
) -> Any:
    response = client.execute_statement(
        statement,
        warehouse_id,
        timeout=timeout,
        catalog=catalog,
        schema=schema,
    )
    rows = statement_rows(response)
    if not rows:
        raise RuntimeError("control statement returned no rows")
    return next(iter(rows[0].values()))


def _server_hostname(host: str) -> str:
    parsed = urllib.parse.urlparse(host)
    if not parsed.hostname:
        raise ValueError("Databricks host has no hostname")
    return parsed.hostname


def _check_connector_version() -> str:
    try:
        installed = importlib.metadata.version("databricks-sql-connector")
    except importlib.metadata.PackageNotFoundError as exc:
        raise RuntimeError(
            "databricks-sql-connector[kernel]==4.5.0 is required"
        ) from exc
    if installed != CONNECTOR_VERSION:
        raise RuntimeError(
            f"databricks-sql-connector {installed} is installed; exactly "
            f"{CONNECTOR_VERSION} is required"
        )
    return installed


def _open_kernel_connection(args: argparse.Namespace) -> Any:
    # Optional dependencies stay behind this online-only boundary.
    from databricks import sql  # type: ignore[import-not-found]

    kwargs: dict[str, Any] = {
        "server_hostname": _server_hostname(args.host),
        "http_path": args.http_path,
        "catalog": args.catalog,
        "schema": args.schema,
        "session_configuration": dict(SESSION_CONFIGURATION),
        "use_kernel": True,
        "user_agent_entry": "CostBench-StatementExecution",
    }
    if args.token:
        kwargs["access_token"] = args.token
    else:
        # The Statement Execution kernel manages OAuth M2M token refresh
        # directly. A Thrift-style custom credentials_provider is explicitly
        # unsupported when use_kernel=True.
        kwargs["oauth_client_id"] = args.client_id
        kwargs["oauth_client_secret"] = args.client_secret
    return sql.connect(**kwargs)


def _make_rest_client(args: argparse.Namespace) -> DatabricksRestClient:
    return DatabricksRestClient(
        args.host,
        token=args.token,
        client_id=args.client_id,
        client_secret=args.client_secret,
        timeout=max(30.0, args.control_timeout),
    )


def _close_quietly(connection: Any) -> None:
    if connection is not None:
        try:
            connection.close()
        except Exception:
            pass


def _report_tags(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def _comment(base: str, workload: str, warehouse_mode: str) -> str:
    details = (
        f"{workload}, {warehouse_mode}, Statement Execution kernel, "
        "use_cached_result=false"
    )
    return f"{base.strip()} ({details})" if base.strip() else f"({details})"


def run(
    workload: str,
    default_queries: Path,
    default_interval: float,
    argv: Sequence[str] | None = None,
) -> int:
    parser = build_parser(workload, default_queries, default_interval)
    args = finalize_args(parser, parser.parse_args(argv))
    queries = load_queries(
        args.queries,
        args.catalog,
        args.schema,
        raw_table=args.raw_table,
        mv_table=args.mv_table,
    )
    expected = EXPECTED_QUERY_COUNTS[workload]
    if len(queries) != expected:
        parser.error(f"{args.queries} contains {len(queries)} statements; expected {expected}")
    if args.print_queries:
        for index, query in enumerate(queries, start=1):
            print(f"-- query {index}\n{query};\n")
        return 0

    connector_version = _check_connector_version()
    run_id = args.run_id or (
        f"dbx-{workload}-{utc_now().strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
    )
    if args.output is None:
        stamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
        args.output = args.output_dir / f"{workload}_{stamp}.jsonl"
    args.output = args.output.expanduser().resolve()
    args.producer_progress = args.producer_progress.expanduser().resolve()
    if args.freshness_monitor:
        args.freshness_monitor = args.freshness_monitor.expanduser().resolve()

    secrets = tuple(
        value for value in (args.token, args.client_secret) if isinstance(value, str)
    )
    rest_client = _make_rest_client(args)
    frozen_warehouse = redact_secrets(
        {
            "captured_at": iso_utc(),
            "warehouse_id": args.rt_warehouse_id,
            "warehouse_mode": args.warehouse_mode,
            "http_path": args.http_path,
            "api_metadata": rest_client.get_warehouse(args.rt_warehouse_id),
            "connector_version": connector_version,
            "use_kernel": True,
            "session_configuration": SESSION_CONFIGURATION,
        },
        secrets,
    )
    try:
        version = str(
            _control_scalar(
                rest_client,
                args.control_warehouse_id,
                "SELECT version() AS version",
                timeout=args.control_timeout,
                catalog=args.catalog,
                schema=args.schema,
            )
        )
    except Exception as exc:
        version = "unknown"
        print(
            f"WARN: control-warehouse version lookup failed: {type(exc).__name__}: {exc}",
            file=sys.stderr,
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

    print(f"Parsed {len(queries)} queries from {args.queries}", file=sys.stderr)
    print(f"Writing JSONL to {args.output}", file=sys.stderr)
    print(
        f"Run ID {run_id}; fixed-rate interval {args.interval:g}s; Ctrl-C to stop.",
        file=sys.stderr,
    )

    connection = None
    iteration = 0
    anchor_wall = utc_now()
    anchor_monotonic = time.monotonic()
    try:
        while not stop.is_set() and (args.iterations == 0 or iteration < args.iterations):
            delay = fixed_rate_delay(
                anchor_monotonic,
                args.interval,
                iteration,
                time.monotonic(),
            )
            if delay and stop.wait(delay):
                break

            iteration += 1
            scheduled_fire = fixed_rate_next_fire(
                anchor_monotonic,
                args.interval,
                iteration - 1,
            )
            scheduled_start = anchor_wall + timedelta(
                seconds=scheduled_fire - anchor_monotonic
            )
            actual_start_monotonic = time.monotonic()
            iteration_started = utc_now()
            producer = producer_progress_snapshot(args.producer_progress)
            freshness = freshness_progress_snapshot(args.freshness_monitor)
            mv_rows = freshness["mv_rows"]
            mv_rows_source = freshness["mv_rows_source"]

            print(
                f"[{iteration_started.strftime('%H:%M:%S')}] iteration {iteration}: "
                f"raw_rows={producer['raw_rows']}",
                file=sys.stderr,
            )
            observations: list[dict[str, Any]] = []
            if connection is None:
                try:
                    connection = _open_kernel_connection(args)
                except Exception as exc:
                    error = str(
                        redact_secrets(
                            f"connection failed: {type(exc).__name__}: {exc}",
                            secrets,
                        )
                    )
                    print(f"  {error}", file=sys.stderr)
                    observations = [
                        failed_observation(number, query, error)
                        for number, query in enumerate(queries, start=1)
                    ]
            if connection is not None:
                for query_number, query in enumerate(queries, start=1):
                    observation = execute_measured_query(
                        connection,
                        rest_client,
                        query,
                        query_number=query_number,
                        iteration=iteration,
                        workload=workload,
                        run_id=run_id,
                        history_timeout=args.history_timeout,
                        history_poll_interval=args.history_poll_interval,
                        secrets=secrets,
                    )
                    observations.append(observation)
                    print(
                        f"  q{query_number}/{len(queries)}: "
                        f"server={observation['canonical_duration_sec']}s "
                        f"wall={observation['client_wall_time_sec']:.3f}s "
                        f"id={observation['statement_id']} "
                        f"errors={len(observation['errors'])}",
                        file=sys.stderr,
                    )

            metric_arrays = aligned_metric_arrays(observations)
            iteration_finished = utc_now()
            elapsed = max(0.0, time.monotonic() - actual_start_monotonic)
            record = {
                "schema_version": 2,
                "run_id": run_id,
                "runner": workload,
                "workload": workload,
                "iteration": iteration,
                "scheduled_start_at": iso_utc(scheduled_start),
                "start_lag_sec": max(0.0, actual_start_monotonic - scheduled_fire),
                "scheduled_interval_sec": args.interval,
                "iteration_started_at": iso_utc(iteration_started),
                "iteration_finished_at": iso_utc(iteration_finished),
                "iteration_elapsed_sec": elapsed,
                "raw_rows": producer["raw_rows"],
                "raw_rows_source": producer["raw_rows_source"],
                "mv_rows": mv_rows,
                "mv_rows_source": mv_rows_source,
                "system": args.system,
                "version": version,
                "machine": args.machine,
                "cluster_size": args.cluster_size,
                "comment": _comment(args.comment, workload, args.warehouse_mode),
                "tags": _report_tags(args.tags),
                **metric_arrays,
                "query_errors": [item["errors"] for item in observations],
                "query_evidence": observations,
                "warehouse_mode": args.warehouse_mode,
                "query_warehouse": frozen_warehouse,
                "rt_warehouse": frozen_warehouse,
                "control_warehouse_id": args.control_warehouse_id,
                "producer_progress_path": producer["path"],
                "producer_progress_loaded_at": producer["loaded_at"],
                "producer_progress_file_modified_at": producer.get("file_modified_at"),
                "producer_progress_updated_at": producer["progress_updated_at"],
                "producer_progress_run_id": producer["progress_run_id"],
                "producer_progress_finished": producer["finished"],
                "producer_progress_error": producer["error"],
                "producer_progress_evidence": producer["progress_fields"],
                "freshness_monitor_path": freshness["path"],
                "freshness_monitor_loaded_at": freshness["loaded_at"],
                "freshness_monitor_file_modified_at": freshness.get("file_modified_at"),
                "freshness_monitor_updated_at": freshness["progress_updated_at"],
                "mv_refresh_finished_at": freshness["refresh_finished_at"],
                "mv_source_rows": freshness["mv_source_rows"],
                "mv_rows_behind": freshness["mv_rows_behind"],
                "freshness_monitor_error": freshness["error"],
            }
            validate_jsonl_record(record, expected)
            atomic_append_jsonl(args.output, redact_secrets(record, secrets))

            next_fire = fixed_rate_next_fire(
                anchor_monotonic,
                args.interval,
                iteration,
            )
            now = time.monotonic()
            if next_fire <= now:
                print(
                    f"  cadence overrun={now - next_fire:.3f}s; next starts immediately",
                    file=sys.stderr,
                )
            else:
                print(f"  next iteration in {next_fire - now:.3f}s", file=sys.stderr)
    finally:
        _close_quietly(connection)

    print(f"Stopped after {iteration} iteration(s).", file=sys.stderr)
    return 0


def runner_main(
    workload: str,
    default_queries: Path,
    default_interval: float,
    argv: Sequence[str] | None = None,
) -> int:
    try:
        return run(workload, default_queries, default_interval, argv)
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"FATAL: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
