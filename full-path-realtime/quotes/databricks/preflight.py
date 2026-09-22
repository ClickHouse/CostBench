#!/usr/bin/env python3
"""Fail-closed environment preflight for the Databricks Lakehouse//RT benchmark."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Any, Mapping

from dbx_common import (
    DatabricksRestClient,
    EnvironmentConfig,
    atomic_write_json,
    iso_utc,
    load_queries,
    probe_tls_endpoint,
    quote_identifier,
    redact_secrets,
    render_sql,
    split_sql_statements,
    sql_string,
    statement_rows,
    validate_config,
)

SCRIPT_DIR = Path(__file__).resolve().parent
EXPECTED_QUERY_COUNTS = {"dashboard": 4, "drilldown": 2}
PACKAGE_CONTRACT = {
    "databricks.sql": {
        "distribution": "databricks-sql-connector",
        "required_for": "Lakehouse//RT Statement Execution API runners",
        "online_required": True,
        "environment": "runner",
    },
    "databricks.sdk": {
        "distribution": "databricks-sdk",
        "required_for": "OAuth M2M authentication for SQL connector runners",
        "online_required": True,
        "environment": "runner",
    },
    "pyarrow": {
        "distribution": "pyarrow",
        "required_for": "Parquet ingestion",
        "online_required": True,
        "environment": "zerobus",
    },
    "zerobus.sdk": {
        "distribution": "databricks-zerobus-ingest-sdk",
        "required_for": "Zerobus ingestion",
        "online_required": True,
        "environment": "zerobus",
    },
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the local Databricks benchmark contract and optionally run "
            "read-only workspace checks"
        )
    )
    parser.add_argument("--online", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--host")
    parser.add_argument("--cloud", choices=("aws", "azure", "gcp"))
    parser.add_argument("--region")
    parser.add_argument("--workspace-id")
    parser.add_argument("--catalog")
    parser.add_argument("--schema")
    parser.add_argument("--raw-table")
    parser.add_argument("--mv-table")
    parser.add_argument("--rt-warehouse")
    parser.add_argument(
        "--query-warehouse-mode",
        choices=("lakehouse-rt", "serverless-baseline"),
        default="lakehouse-rt",
    )
    parser.add_argument("--control-warehouse")
    parser.add_argument("--zerobus-endpoint")
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("--network-timeout", type=float, default=30.0)
    parser.add_argument(
        "--runner-python",
        default=os.environ.get("DATABRICKS_RUNNER_PYTHON") or sys.executable,
        help="Python interpreter containing requirements-runner.txt",
    )
    parser.add_argument(
        "--zerobus-python",
        default=os.environ.get("DATABRICKS_ZEROBUS_PYTHON") or sys.executable,
        help="Python interpreter containing requirements-zerobus.txt",
    )
    args = parser.parse_args(argv)
    if args.timeout <= 0 or args.poll_interval <= 0 or args.network_timeout <= 0:
        parser.error("timeouts and poll interval must be greater than zero")
    return args


def config_from_args(
    args: argparse.Namespace,
    env: Mapping[str, str] | None = None,
) -> EnvironmentConfig:
    return EnvironmentConfig.from_env(
        env,
        host=args.host,
        cloud=args.cloud,
        region=args.region,
        workspace_id=args.workspace_id,
        catalog=args.catalog,
        schema=args.schema,
        raw_table=args.raw_table,
        mv_table=args.mv_table,
        rt_warehouse_id=args.rt_warehouse,
        control_warehouse_id=args.control_warehouse,
        zerobus_endpoint=args.zerobus_endpoint,
    )


def _external_package_probe(
    python: str,
    module: str,
    distribution: str,
) -> dict[str, Any]:
    probe = (
        "import importlib.metadata, importlib.util, json, sys\n"
        "try:\n"
        "    available = importlib.util.find_spec(sys.argv[1]) is not None\n"
        "except (ImportError, ModuleNotFoundError, AttributeError, ValueError):\n"
        "    available = False\n"
        "version = None\n"
        "if available:\n"
        "    try:\n"
        "        version = importlib.metadata.version(sys.argv[2])\n"
        "    except importlib.metadata.PackageNotFoundError:\n"
        "        available = False\n"
        "print(json.dumps({'available': available, 'version': version}))\n"
    )
    try:
        completed = subprocess.run(
            [python, "-c", probe, module, distribution],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {
            "available": False,
            "version": None,
            "probe_error": f"{type(exc).__name__}: {exc}",
        }
    if completed.returncode != 0:
        error = completed.stderr.strip() or f"exit status {completed.returncode}"
        return {
            "available": False,
            "version": None,
            "probe_error": error[:500],
        }
    try:
        result = json.loads(completed.stdout)
    except (json.JSONDecodeError, TypeError) as exc:
        return {
            "available": False,
            "version": None,
            "probe_error": f"invalid probe output: {type(exc).__name__}: {exc}",
        }
    return {
        "available": result.get("available") is True,
        "version": result.get("version"),
        "probe_error": None,
    }


def package_checks(
    *,
    runner_python: str | None = None,
    zerobus_python: str | None = None,
) -> dict[str, dict[str, Any]]:
    interpreters = {
        "runner": runner_python or sys.executable,
        "zerobus": zerobus_python or sys.executable,
    }
    checks: dict[str, dict[str, Any]] = {}
    for module, details in PACKAGE_CONTRACT.items():
        environment = str(details["environment"])
        python = interpreters[environment]
        probe = _external_package_probe(
            python,
            module,
            str(details["distribution"]),
        )
        checks[module] = {
            **details,
            "interpreter": python,
            **probe,
        }
    return checks


def load_create_materialized_view_statement(
    path: Path,
    catalog: str,
    schema: str,
) -> str:
    """Render DDL and return its one complete CREATE MATERIALIZED VIEW statement."""
    rendered = render_sql(path.read_text(encoding="utf-8"), catalog, schema)
    statements = split_sql_statements(rendered)
    matches = [
        statement
        for statement in statements
        if re.match(
            r"^CREATE\s+(?:OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW\b",
            statement,
            flags=re.IGNORECASE,
        )
    ]
    if len(matches) != 1:
        raise ValueError(
            "rendered create.sql must contain exactly one "
            f"CREATE MATERIALIZED VIEW statement; found {len(matches)}"
        )
    return matches[0]


def _credential_shape(env: Mapping[str, str]) -> dict[str, Any]:
    token = bool(env.get("DATABRICKS_TOKEN", "").strip())
    client_id = bool(env.get("DATABRICKS_CLIENT_ID", "").strip())
    client_secret = bool(env.get("DATABRICKS_CLIENT_SECRET", "").strip())
    return {
        "pat_configured": token,
        "oauth_client_id_configured": client_id,
        "oauth_client_secret_configured": client_secret,
        "oauth_client_credentials_complete": client_id and client_secret,
    }


def offline_checks(
    config: EnvironmentConfig,
    env: Mapping[str, str] | None = None,
    *,
    script_dir: Path = SCRIPT_DIR,
    runner_python: str | None = None,
    zerobus_python: str | None = None,
) -> dict[str, Any]:
    source = os.environ if env is None else env
    validation = validate_config(config, online=False)
    errors = list(validation["errors"])
    warnings = list(validation["warnings"])
    credentials = _credential_shape(source)
    if credentials["oauth_client_id_configured"] != credentials[
        "oauth_client_secret_configured"
    ]:
        errors.append(
            "DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET must be set together"
        )
    if not credentials["pat_configured"] and not credentials[
        "oauth_client_credentials_complete"
    ]:
        warnings.append(
            "no Databricks PAT or complete OAuth client credentials are configured"
        )
    if not credentials["oauth_client_credentials_complete"]:
        warnings.append(
            "Zerobus requires DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET"
        )

    actual_counts: dict[str, int | None] = {"dashboard": None, "drilldown": None}
    queries: dict[str, list[str]] = {}
    for workload, filename in (
        ("dashboard", "queries_mv.sql"),
        ("drilldown", "queries_raw.sql"),
    ):
        try:
            loaded = load_queries(
                script_dir / filename,
                config.catalog,
                config.schema,
                raw_table=config.raw_table,
                mv_table=config.mv_table,
            )
            queries[workload] = loaded
            actual_counts[workload] = len(loaded)
        except (OSError, ValueError) as exc:
            errors.append(f"could not load {filename}: {type(exc).__name__}: {exc}")

    for workload, expected in EXPECTED_QUERY_COUNTS.items():
        actual = actual_counts[workload]
        if actual is not None and actual != expected:
            errors.append(
                f"expected {expected} {workload} SQL statements, found {actual}"
            )

    create_sql_path = script_dir / "create.sql"
    create_materialized_view: str | None = None
    try:
        create_materialized_view = load_create_materialized_view_statement(
            create_sql_path,
            config.catalog,
            config.schema,
        )
    except (OSError, ValueError) as exc:
        errors.append(
            "could not extract CREATE MATERIALIZED VIEW from create.sql: "
            f"{type(exc).__name__}: {exc}"
        )

    packages = package_checks(
        runner_python=runner_python,
        zerobus_python=zerobus_python,
    )
    for module, details in packages.items():
        if not details["available"]:
            warnings.append(
                f"{details['distribution']} is unavailable; needed for "
                f"{details['required_for']}"
            )

    return {
        "status": "error" if errors else ("warning" if warnings else "ok"),
        "config_validation": validation,
        "credentials": credentials,
        "query_contract": {
            "expected": dict(EXPECTED_QUERY_COUNTS),
            "actual": actual_counts,
            "files": {
                "dashboard": str(script_dir / "queries_mv.sql"),
                "drilldown": str(script_dir / "queries_raw.sql"),
            },
        },
        "ddl_contract": {
            "file": str(create_sql_path),
            "create_materialized_view_found": create_materialized_view is not None,
        },
        "packages": packages,
        "errors": errors,
        "warnings": warnings,
        "_queries": queries,
        "_create_materialized_view": create_materialized_view,
    }


def _safe_warehouse(metadata: Mapping[str, Any]) -> dict[str, Any]:
    allowed = (
        "id",
        "name",
        "state",
        "cluster_size",
        "min_num_clusters",
        "max_num_clusters",
        "auto_stop_mins",
        "enable_serverless_compute",
        "warehouse_type",
        "creator_name",
    )
    return {key: metadata.get(key) for key in allowed if key in metadata}


def _uri_is_at_or_below(location: str, root: str) -> bool:
    child = urllib.parse.urlparse(location.rstrip("/"))
    parent = urllib.parse.urlparse(root.rstrip("/"))
    if child.scheme.lower() != parent.scheme.lower() or child.netloc.lower() != parent.netloc.lower():
        return False
    parent_path = parent.path.rstrip("/")
    child_path = child.path.rstrip("/")
    return child_path == parent_path or child_path.startswith(parent_path + "/")


def _table_summary(metadata: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: metadata.get(key)
        for key in (
            "full_name",
            "name",
            "catalog_name",
            "schema_name",
            "table_type",
            "data_source_format",
            "storage_location",
            "created_at",
            "updated_at",
        )
        if key in metadata
    }


def predictive_optimization_check(
    full_name: str,
    metadata: Mapping[str, Any] | None,
    errors: list[str],
) -> dict[str, Any]:
    """Require the Unity Catalog API's effective table flag to be ENABLE."""
    metadata = metadata if isinstance(metadata, Mapping) else {}
    effective = metadata.get("effective_predictive_optimization_flag")
    effective = effective if isinstance(effective, Mapping) else {}
    configured = str(metadata.get("enable_predictive_optimization") or "").upper()
    effective_value = str(effective.get("value") or "").upper()
    enabled = effective_value == "ENABLE"
    result = {
        "status": "ok" if enabled else "error",
        "source": "Unity Catalog Tables API",
        "configured_value": configured or None,
        "effective_value": effective_value or None,
        "effectively_enabled": enabled,
        "inherited_from_type": effective.get("inherited_from_type"),
        "inherited_from_name": effective.get("inherited_from_name"),
    }
    if not enabled:
        errors.append(
            f"{full_name} effective Predictive Optimization flag must be ENABLE; "
            f"found {effective_value or 'missing'}"
        )
    return result


def _decode_json(value: Any) -> Any:
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


def _row_value(row: Mapping[str, Any], name: str) -> Any:
    wanted = name.lower()
    for key, value in row.items():
        if str(key).lower() == wanted:
            return value
    return None


def _clustering_columns(value: Any) -> list[str] | None:
    decoded = _decode_json(value)
    if not isinstance(decoded, list):
        return None
    result: list[str] = []
    for item in decoded:
        if isinstance(item, list) and len(item) == 1:
            item = item[0]
        if not isinstance(item, str):
            return None
        result.append(item)
    return result


def target_contract_check(
    client: DatabricksRestClient,
    config: EnvironmentConfig,
    *,
    raw_metadata: Mapping[str, Any] | None,
    expected_mv_owner: str,
    timeout: float,
    poll_interval: float,
    errors: list[str],
) -> dict[str, Any]:
    """Prove the fresh raw/MV contract without scanning either data object."""
    statements: dict[str, Any] = {}
    observed: dict[str, Any] = {}
    checks: dict[str, bool] = {}
    local_errors: list[str] = []

    def execute(name: str, sql: str) -> list[dict[str, Any]]:
        response = _statement(
            client,
            config,
            config.control_warehouse_id or "",
            sql,
            timeout=timeout,
            poll_interval=poll_interval,
        )
        rows = statement_rows(response)
        statements[name] = {
            "statement_id": response.get("statement_id"),
            "row_count": len(rows),
        }
        return rows

    try:
        detail_rows = execute(
            "raw_detail", f"DESCRIBE DETAIL {quote_identifier(config.catalog)}."
            f"{quote_identifier(config.schema)}.{quote_identifier(config.raw_table)}"
        )
        history_rows = execute(
            "raw_history",
            f"DESCRIBE HISTORY {quote_identifier(config.catalog)}."
            f"{quote_identifier(config.schema)}.{quote_identifier(config.raw_table)} "
            "LIMIT 2",
        )
        mv_rows = execute(
            "mv_metadata",
            f"DESCRIBE TABLE EXTENDED {quote_identifier(config.catalog)}."
            f"{quote_identifier(config.schema)}.{quote_identifier(config.mv_table)} "
            "AS JSON",
        )
        if len(detail_rows) != 1 or len(mv_rows) != 1:
            raise RuntimeError("target metadata returned an unexpected result shape")

        raw_detail = detail_rows[0]
        raw_properties = (
            raw_metadata.get("properties", {})
            if isinstance(raw_metadata, Mapping)
            else {}
        )
        raw_properties = (
            raw_properties if isinstance(raw_properties, Mapping) else {}
        )
        mv_document = _decode_json(_row_value(mv_rows[0], "json_metadata"))
        if not isinstance(mv_document, Mapping):
            raise RuntimeError("MV JSON metadata is missing or invalid")
        mv_properties = mv_document.get("table_properties", {})
        mv_properties = (
            mv_properties if isinstance(mv_properties, Mapping) else {}
        )
        refresh = mv_document.get("refresh_information", {})
        refresh = refresh if isinstance(refresh, Mapping) else {}
        raw_clustering = _clustering_columns(
            raw_properties.get("clusteringColumns")
        )
        mv_clustering = _clustering_columns(
            mv_properties.get("clusteringColumns")
        )
        history_versions = [
            int(_row_value(row, "version")) for row in history_rows
        ]
        history_operations = [
            str(_row_value(row, "operation") or "").upper()
            for row in history_rows
        ]
        schedule = str(refresh.get("refresh_schedule") or "").upper()
        observed = {
            "raw": {
                "format": _row_value(raw_detail, "format"),
                "num_files": _row_value(raw_detail, "numFiles"),
                "size_in_bytes": _row_value(raw_detail, "sizeInBytes"),
                "clustering_columns": raw_clustering,
                "history_versions": history_versions,
                "history_operations": history_operations,
                "source_features": {
                    key: raw_properties.get(key)
                    for key in (
                        "delta.enableDeletionVectors",
                        "delta.enableRowTracking",
                        "delta.enableChangeDataFeed",
                    )
                },
            },
            "materialized_view": {
                "owner": mv_document.get("owner"),
                "clustering_columns": mv_clustering,
                "predictive_optimization": mv_document.get(
                    "predictive_optimization"
                ),
                "refresh_policy": refresh.get("refresh_policy"),
                "refresh_schedule": refresh.get("refresh_schedule"),
            },
        }
        checks = {
            "raw_format_delta": str(_row_value(raw_detail, "format")).lower()
            == "delta",
            "raw_zero_active_files": int(_row_value(raw_detail, "numFiles")) == 0,
            "raw_zero_active_bytes": int(_row_value(raw_detail, "sizeInBytes")) == 0,
            "raw_only_version_zero": history_versions == [0],
            "raw_created_once": history_operations == ["CREATE TABLE"],
            "raw_clustering": raw_clustering == ["sym", "t"],
            "raw_deletion_vectors": str(
                raw_properties.get("delta.enableDeletionVectors")
            ).lower()
            == "true",
            "raw_row_tracking": str(
                raw_properties.get("delta.enableRowTracking")
            ).lower()
            == "true",
            "raw_change_data_feed": str(
                raw_properties.get("delta.enableChangeDataFeed")
            ).lower()
            == "true",
            "mv_owner": mv_document.get("owner") == expected_mv_owner,
            "mv_clustering": mv_clustering == ["sym", "day"],
            "mv_predictive_optimization": str(
                mv_document.get("predictive_optimization")
            ).upper()
            == "ENABLE",
            "mv_incremental_strict": str(
                refresh.get("refresh_policy")
            ).upper()
            == "INCREMENTALSTRICT",
            "mv_one_minute_update_trigger": (
                "TRIGGER ON UPDATE" in schedule
                and (
                    "INTERVAL 60 SECOND" in schedule
                    or "INTERVAL 1 MINUTE" in schedule
                )
            ),
        }
        local_errors.extend(name for name, passed in checks.items() if not passed)
    except Exception as exc:
        local_errors.append(f"{type(exc).__name__}: {exc}")

    if local_errors:
        errors.extend(
            f"target contract check failed: {message}" for message in local_errors
        )
    return {
        "status": "error" if local_errors else "ok",
        "method": "Unity Catalog metadata plus DESCRIBE metadata statements",
        "statements": statements,
        "observed": observed,
        "checks": checks,
        "errors": local_errors,
    }


def _statement(
    client: DatabricksRestClient,
    config: EnvironmentConfig,
    warehouse_id: str,
    sql: str,
    *,
    timeout: float,
    poll_interval: float,
) -> dict[str, Any]:
    return client.execute_statement(
        sql,
        warehouse_id,
        timeout=timeout,
        poll_interval=poll_interval,
        catalog=config.catalog,
        schema=config.schema,
    )


def _check_warehouse(
    client: DatabricksRestClient,
    warehouse_id: str,
    label: str,
    errors: list[str],
) -> dict[str, Any]:
    try:
        metadata = client.get_warehouse(warehouse_id)
    except Exception as exc:
        errors.append(f"could not inspect {label} warehouse: {type(exc).__name__}: {exc}")
        return {"status": "error"}
    returned_id = metadata.get("id")
    if returned_id != warehouse_id:
        errors.append(
            f"{label} warehouse lookup returned ID {returned_id!r}, expected {warehouse_id!r}"
        )
    return {
        "status": "ok" if returned_id == warehouse_id else "error",
        "metadata": _safe_warehouse(metadata),
    }


def _check_table(
    client: DatabricksRestClient,
    full_name: str,
    expected_type: str,
    errors: list[str],
) -> tuple[dict[str, Any], Mapping[str, Any] | None]:
    encoded = urllib.parse.quote(full_name, safe="")
    try:
        metadata = client.request("GET", f"/api/2.1/unity-catalog/tables/{encoded}")
    except Exception as exc:
        errors.append(f"could not inspect {full_name}: {type(exc).__name__}: {exc}")
        return {"status": "error"}, None
    summary = _table_summary(metadata)
    table_type = str(metadata.get("table_type", "")).upper()
    data_format = str(metadata.get("data_source_format", "")).upper()
    table_errors: list[str] = []
    if table_type != expected_type:
        table_errors.append(
            f"{full_name} must have table_type={expected_type}, found {table_type or 'unknown'}"
        )
    if expected_type == "MATERIALIZED_VIEW":
        format_valid = data_format in {"", "DELTA"}
    else:
        format_valid = data_format == "DELTA"
    if not format_valid:
        table_errors.append(
            f"{full_name} must use Delta, found {data_format or 'unknown'}"
        )
    errors.extend(table_errors)
    return {
        "status": "error" if table_errors else "ok",
        "metadata": summary,
        "errors": table_errors,
    }, metadata


def online_checks(
    config: EnvironmentConfig,
    queries: Mapping[str, list[str]],
    env: Mapping[str, str] | None = None,
    *,
    create_materialized_view_statement: str | None = None,
    runner_python: str | None = None,
    zerobus_python: str | None = None,
    timeout: float = 120.0,
    poll_interval: float = 0.5,
    network_timeout: float = 5.0,
    query_warehouse_mode: str = "lakehouse-rt",
) -> dict[str, Any]:
    source = os.environ if env is None else env
    errors: list[str] = []
    warnings: list[str] = []
    validation = validate_config(config, online=True)
    errors.extend(validation["errors"])
    blocking_errors = list(validation["errors"])
    warnings.extend(validation["warnings"])
    credentials = _credential_shape(source)
    token = source.get("DATABRICKS_TOKEN", "").strip() or None
    client_id = source.get("DATABRICKS_CLIENT_ID", "").strip() or None
    client_secret = source.get("DATABRICKS_CLIENT_SECRET", "").strip() or None
    if bool(client_id) != bool(client_secret):
        message = "DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET must be set together"
        errors.append(message)
        blocking_errors.append(message)
    if not token and not (client_id and client_secret):
        message = "DATABRICKS_TOKEN or OAuth client credentials are required online"
        errors.append(message)
        blocking_errors.append(message)
    if not (client_id and client_secret):
        errors.append(
            "Zerobus requires service-principal DATABRICKS_CLIENT_ID and "
            "DATABRICKS_CLIENT_SECRET"
        )
    packages = package_checks(
        runner_python=runner_python,
        zerobus_python=zerobus_python,
    )
    for details in packages.values():
        if details["online_required"] and not details["available"]:
            errors.append(
                f"{details['distribution']} is required for benchmark execution but unavailable"
            )

    report: dict[str, Any] = {
        "status": "pending",
        "config_validation": validation,
        "credentials": credentials,
        "packages": packages,
        "errors": errors,
        "warnings": warnings,
    }
    if blocking_errors:
        report["status"] = "error"
        report["skipped"] = "online calls skipped because required configuration is invalid"
        return report

    assert config.host
    assert config.rt_warehouse_id
    assert config.control_warehouse_id
    assert config.zerobus_endpoint
    client = DatabricksRestClient(
        config.host,
        token=token,
        client_id=client_id,
        client_secret=client_secret,
        timeout=network_timeout,
    )
    report["authentication"] = {"status": "configured", "mode": client.auth_mode}

    serving_label = (
        "Lakehouse//RT"
        if query_warehouse_mode == "lakehouse-rt"
        else "Serverless SQL baseline"
    )
    warehouses = {
        "lakehouse_rt": _check_warehouse(
            client, config.rt_warehouse_id, serving_label, errors
        ),
        "control": _check_warehouse(
            client, config.control_warehouse_id, "control", errors
        ),
    }
    report["warehouses"] = warehouses
    try:
        workspace_warehouse_config = client.request(
            "GET", "/api/2.0/sql/config/warehouses"
        )
        warehouses["workspace_configuration"] = {
            "enabled_warehouse_types": workspace_warehouse_config.get(
                "enabled_warehouse_types"
            ),
            "enable_serverless_compute": workspace_warehouse_config.get(
                "enable_serverless_compute"
            ),
        }
    except Exception as exc:
        warnings.append(
            "workspace warehouse configuration was unavailable: "
            f"{type(exc).__name__}: {exc}"
        )

    rt_select: dict[str, Any]
    try:
        response = _statement(
            client,
            config,
            config.rt_warehouse_id,
            "SELECT 1 AS preflight_ok, current_catalog() AS current_catalog, "
            "current_schema() AS current_schema",
            timeout=timeout,
            poll_interval=poll_interval,
        )
        rt_select = {
            "status": "ok",
            "statement_id": response.get("statement_id"),
            "rows": statement_rows(response),
        }
    except Exception as exc:
        rt_select = {"status": "error"}
        errors.append(
            f"Statement Execution API SELECT failed on {serving_label} warehouse: "
            f"{type(exc).__name__}: {exc}"
        )
    report["statement_execution_on_rt"] = rt_select

    rt_metadata = warehouses.get("lakehouse_rt", {}).get("metadata", {})
    type_signal = rt_metadata.get("warehouse_type")
    if query_warehouse_mode == "lakehouse-rt":
        report["lakehouse_rt_type"] = {
            "status": "inconclusive",
            "warehouse_api_signal": type_signal,
            "statement_execution_succeeded": rt_select.get("status") == "ok",
            "reason": (
                "The public Warehouses API does not expose a reliable REAL_TIME enum. "
                "A successful Statement Execution API query and warehouse "
                "configuration are evidence, but do not conclusively identify the "
                "warehouse type."
            ),
        }
        warnings.append(
            "Lakehouse//RT warehouse type is inconclusive because the public "
            "Warehouses API does not expose a reliable REAL_TIME discriminator"
        )
    else:
        report["lakehouse_rt_type"] = {
            "status": "not_applicable",
            "warehouse_api_signal": type_signal,
            "statement_execution_succeeded": rt_select.get("status") == "ok",
            "reason": "This run intentionally uses the Serverless SQL baseline.",
        }
    report["query_warehouse_mode"] = query_warehouse_mode

    mv_create_compatibility: dict[str, Any] = {
        "method": "EXPLAIN CREATE MATERIALIZED VIEW",
        "warehouse_id": config.control_warehouse_id,
    }
    if create_materialized_view_statement is None:
        mv_create_compatibility["status"] = "error"
        errors.append(
            "CREATE MATERIALIZED VIEW statement was unavailable for EXPLAIN"
        )
    else:
        try:
            response = _statement(
                client,
                config,
                config.control_warehouse_id,
                "EXPLAIN " + create_materialized_view_statement,
                timeout=timeout,
                poll_interval=poll_interval,
            )
            explain_rows = statement_rows(response)
            explain_text = json.dumps(explain_rows, ensure_ascii=False).lower()
            incrementalizable = (
                "can be incrementally refreshed" in explain_text
                and "no issues detected" in explain_text
            )
            mv_create_compatibility.update(
                {
                    "status": "ok" if incrementalizable else "error",
                    "statement_id": response.get("statement_id"),
                    "rows": explain_rows,
                    "incrementalizable": incrementalizable,
                }
            )
            if not incrementalizable:
                errors.append(
                    "EXPLAIN CREATE MATERIALIZED VIEW did not prove incremental "
                    "refresh eligibility with no issues"
                )
        except Exception as exc:
            mv_create_compatibility["status"] = "error"
            mv_create_compatibility["error"] = f"{type(exc).__name__}: {exc}"
            errors.append(
                "CREATE MATERIALIZED VIEW is not control-warehouse SQL-compatible: "
                f"{type(exc).__name__}: {exc}"
            )
    report["materialized_view_create_compatibility"] = mv_create_compatibility

    compatibility: list[dict[str, Any]] = []
    for workload in ("dashboard", "drilldown"):
        for number, query in enumerate(queries.get(workload, []), start=1):
            item: dict[str, Any] = {"workload": workload, "query_number": number}
            try:
                response = _statement(
                    client,
                    config,
                    config.rt_warehouse_id,
                    f"EXPLAIN FORMATTED\n{query}",
                    timeout=timeout,
                    poll_interval=poll_interval,
                )
                item.update(
                    {"status": "ok", "statement_id": response.get("statement_id")}
                )
            except Exception as exc:
                item["status"] = "error"
                item["error"] = f"{type(exc).__name__}: {exc}"
                errors.append(
                    f"{workload} query {number} is not {serving_label} SQL-compatible: "
                    f"{type(exc).__name__}: {exc}"
                )
            compatibility.append(item)
    report["query_compatibility"] = {
        "method": "EXPLAIN FORMATTED (compiles without running benchmark scans)",
        "queries": compatibility,
    }

    statement_id = rt_select.get("statement_id")
    if statement_id:
        response = None
        system_permission_error: Exception | None = None
        for attempt in range(3):
            try:
                response = _statement(
                    client,
                    config,
                    config.control_warehouse_id,
                    "SELECT statement_id FROM system.query.history "
                    f"WHERE statement_id = {sql_string(str(statement_id))} LIMIT 1",
                    timeout=timeout,
                    poll_interval=poll_interval,
                )
                system_permission_error = None
                break
            except Exception as exc:
                system_permission_error = exc
                if attempt < 2:
                    time.sleep(2**attempt)
        if response is not None:
            report["system_table_permission"] = {
                "status": "ok",
                "table": "system.query.history",
                "statement_id": response.get("statement_id"),
            }
        else:
            report["system_table_permission"] = {"status": "error"}
            errors.append(
                "cannot read system.query.history on the control warehouse: "
                f"{type(system_permission_error).__name__}: "
                f"{system_permission_error}"
            )

        history = None
        history_error: Exception | None = None
        for _ in range(5):
            try:
                history = client.query_history_by_statement_id(str(statement_id))
                history_error = None
            except Exception as exc:
                history_error = exc
                break
            if history is not None:
                break
            time.sleep(0.5)
        if history is None:
            report["query_history_api"] = {"status": "error"}
            if history_error:
                errors.append(
                    "query-history lookup failed: "
                    f"{type(history_error).__name__}: {history_error}"
                )
            else:
                errors.append(
                    "query-history API did not return the Lakehouse//RT preflight statement"
                )
        else:
            report["query_history_api"] = {
                "status": "ok",
                "statement_id": history.get("statement_id"),
                "warehouse_id": history.get("warehouse_id")
                or history.get("endpoint_id"),
                "status_value": history.get("status"),
            }
    else:
        report["system_table_permission"] = {
            "status": "skipped",
            "reason": "Lakehouse//RT statement did not produce an ID",
        }
        report["query_history_api"] = {
            "status": "skipped",
            "reason": "Lakehouse//RT statement did not produce an ID",
        }

    raw_report, raw_metadata = _check_table(
        client, config.raw_full_name, "MANAGED", errors
    )
    mv_report, mv_metadata = _check_table(
        client, config.mv_full_name, "MATERIALIZED_VIEW", errors
    )
    report["tables"] = {"raw": raw_report, "materialized_view": mv_report}
    raw_predictive = predictive_optimization_check(
        config.raw_full_name,
        raw_metadata,
        errors,
    )
    mv_predictive = predictive_optimization_check(
        config.mv_full_name,
        mv_metadata,
        errors,
    )
    report["predictive_optimization"] = {
        "status": (
            "ok"
            if raw_predictive["effectively_enabled"]
            and mv_predictive["effectively_enabled"]
            else "error"
        ),
        "raw": raw_predictive,
        "materialized_view": mv_predictive,
    }
    report["target_contract"] = target_contract_check(
        client,
        config,
        raw_metadata=raw_metadata,
        expected_mv_owner=client_id or "",
        timeout=timeout,
        poll_interval=poll_interval,
        errors=errors,
    )

    metastore_summary: Mapping[str, Any] | None = None
    catalog_metadata: Mapping[str, Any] | None = None
    try:
        metastore_summary = client.request(
            "GET", "/api/2.1/unity-catalog/metastore_summary"
        )
        report["metastore"] = {
            "metastore_id": metastore_summary.get("metastore_id"),
            "cloud": metastore_summary.get("cloud"),
            "region": metastore_summary.get("region"),
            "storage_root_configured": bool(metastore_summary.get("storage_root")),
        }
    except Exception as exc:
        errors.append(
            "could not inspect metastore default storage: "
            f"{type(exc).__name__}: {exc}"
        )
        report["metastore"] = {"status": "error"}

    try:
        encoded_catalog = urllib.parse.quote(config.catalog, safe="")
        catalog_metadata = client.request(
            "GET", f"/api/2.1/unity-catalog/catalogs/{encoded_catalog}"
        )
        report["catalog_storage"] = {
            "name": catalog_metadata.get("name"),
            "storage_root": catalog_metadata.get("storage_root"),
        }
    except Exception as exc:
        errors.append(
            "could not inspect catalog managed storage: "
            f"{type(exc).__name__}: {exc}"
        )
        report["catalog_storage"] = {"status": "error"}

    raw_location = raw_metadata.get("storage_location") if raw_metadata else None
    metastore_root = (
        metastore_summary.get("storage_root") if metastore_summary else None
    )
    catalog_root = (
        catalog_metadata.get("storage_root") if catalog_metadata else None
    )
    storage_check: dict[str, Any] = {
        "status": "error",
        "raw_storage_location_configured": bool(raw_location),
        "catalog_managed_storage_configured": bool(catalog_root),
        "metastore_default_storage_configured": bool(metastore_root),
    }
    if not raw_location:
        errors.append("raw table metadata omitted storage_location")
    elif catalog_root:
        uses_catalog = _uri_is_at_or_below(
            str(raw_location), str(catalog_root)
        )
        uses_default = bool(metastore_root) and _uri_is_at_or_below(
            str(raw_location), str(metastore_root)
        )
        storage_check["uses_catalog_managed_storage"] = uses_catalog
        storage_check["uses_metastore_default_storage"] = uses_default
        storage_check["status"] = (
            "ok" if uses_catalog and not uses_default else "error"
        )
        if not uses_catalog:
            errors.append(
                "raw table storage is not under the catalog managed storage root"
            )
        if uses_default:
            errors.append(
                "raw table storage is under the metastore default storage root; "
                "Zerobus requires non-default managed storage"
            )
    elif not metastore_root:
        errors.append(
            "catalog and metastore metadata omitted storage roots; managed "
            "storage cannot be verified"
        )
    else:
        uses_default = _uri_is_at_or_below(
            str(raw_location), str(metastore_root)
        )
        storage_check["uses_metastore_default_storage"] = uses_default
        storage_check["status"] = "error" if uses_default else "ok"
        if uses_default:
            errors.append(
                "raw table storage is under the metastore default storage root; "
                "Zerobus requires non-default managed storage"
            )
    report["tables"]["raw_storage_assumption"] = storage_check

    endpoint_report: dict[str, Any] = {"url": config.zerobus_endpoint}
    try:
        endpoint_report.update(
            {
                "status": "ok",
                "tls": probe_tls_endpoint(
                    config.zerobus_endpoint, timeout=network_timeout
                ),
            }
        )
    except Exception as exc:
        endpoint_report["status"] = "error"
        errors.append(
            f"Zerobus endpoint is not reachable over TLS: {type(exc).__name__}: {exc}"
        )
    report["zerobus_endpoint"] = endpoint_report

    if client_id and client_secret and config.workspace_id:
        authorization_details = [
            {
                "type": "unity_catalog_privileges",
                "privileges": ["USE CATALOG"],
                "object_type": "CATALOG",
                "object_full_path": config.catalog,
            },
            {
                "type": "unity_catalog_privileges",
                "privileges": ["USE SCHEMA"],
                "object_type": "SCHEMA",
                "object_full_path": f"{config.catalog}.{config.schema}",
            },
            {
                "type": "unity_catalog_privileges",
                "privileges": ["SELECT", "MODIFY"],
                "object_type": "TABLE",
                "object_full_path": config.raw_full_name,
            },
        ]
        try:
            client.oauth_token(
                resource=(
                    f"api://databricks/workspaces/{config.workspace_id}"
                    "/zerobusDirectWriteApi"
                ),
                authorization_details=authorization_details,
            )
            report["zerobus_service_principal"] = {
                "status": "ok",
                "scoped_oauth_token_acquired": True,
                "required_privileges": authorization_details,
            }
        except Exception as exc:
            report["zerobus_service_principal"] = {"status": "error"}
            errors.append(
                "could not acquire a scoped Zerobus service-principal token: "
                f"{type(exc).__name__}: {exc}"
            )

    report["status"] = "error" if errors else ("warning" if warnings else "ok")
    return report


def _without_private_fields(report: dict[str, Any]) -> dict[str, Any]:
    offline = report.get("offline")
    if isinstance(offline, dict):
        offline.pop("_queries", None)
        offline.pop("_create_materialized_view", None)
    return report


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = config_from_args(args)
    offline = offline_checks(
        config,
        runner_python=args.runner_python,
        zerobus_python=args.zerobus_python,
    )
    queries = offline.get("_queries", {})
    create_materialized_view = offline.get("_create_materialized_view")
    report: dict[str, Any] = {
        "generated_at": iso_utc(),
        "mode": "online" if args.online else "offline",
        "config": config.as_report_dict(),
        "offline": offline,
    }
    if args.online:
        try:
            report["online"] = online_checks(
                config,
                queries,
                create_materialized_view_statement=create_materialized_view,
                runner_python=args.runner_python,
                zerobus_python=args.zerobus_python,
                timeout=args.timeout,
                poll_interval=args.poll_interval,
                network_timeout=args.network_timeout,
                query_warehouse_mode=args.query_warehouse_mode,
            )
        except Exception as exc:
            report["online"] = {
                "status": "error",
                "errors": [
                    f"unexpected online preflight failure: {type(exc).__name__}: {exc}"
                ],
                "warnings": [],
            }

    all_errors = list(offline["errors"])
    all_warnings = list(offline["warnings"])
    if args.online:
        all_errors.extend(report["online"]["errors"])
        all_warnings.extend(report["online"]["warnings"])
    report["summary"] = {
        "status": "error" if all_errors else ("warning" if all_warnings else "ok"),
        "error_count": len(all_errors),
        "warning_count": len(all_warnings),
    }
    safe_report = redact_secrets(
        _without_private_fields(report),
        (
            os.environ.get("DATABRICKS_TOKEN"),
            os.environ.get("DATABRICKS_CLIENT_SECRET"),
        ),
    )
    rendered = json.dumps(safe_report, indent=2, sort_keys=True)
    print(rendered)
    if args.output:
        atomic_write_json(args.output, safe_report)
    return 1 if all_errors else 0


if __name__ == "__main__":
    sys.exit(main())
