#!/usr/bin/env python3
"""Plan and evaluate the Databricks tune-endurance qualification matrix.

The planner is intentionally offline.  It records commands and evidence paths,
but it never reads credentials or claims that an online command ran.  The
evaluator is also offline and fails closed when evidence is absent or
incomplete.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shlex
import sys
import tempfile
import urllib.parse
from pathlib import Path
from typing import Any, Mapping, Sequence

SCHEMA_VERSION = 1
TASK = "tune-endurance"
CORRECTNESS_ROWS = 10_000_000
CAPACITY_TARGETS = (100_000, 500_000, 1_000_000)
CAPACITY_DURATION_SECONDS = 75
METRICS_INTERVAL_SECONDS = 5
MIN_CAPACITY_INTERVALS = 10
DEFAULT_TOLERANCE = 0.10
DEFAULT_MAX_PRODUCER_CPU_PERCENT = 90.0
DEFAULT_MAX_PRODUCER_NETWORK_PERCENT = 80.0
DEFAULT_ENDURANCE_SECONDS = 1_800
MIN_ENDURANCE_SECONDS = 1_800
MAX_ENDURANCE_SECONDS = 3_600
REQUIRED_CASE_IDS = (
    "correctness-10m",
    "capacity-100k",
    "capacity-500k",
    "capacity-1m",
    "endurance-1m",
)
PAT_VALUE = re.compile(r"\bdapi[A-Za-z0-9_-]{8,}\b")
AUTH_VALUE = re.compile(r"(?i)\b(?:Bearer|Basic)\s+[A-Za-z0-9._~+/=-]{8,}")


def _atomic_write(path: Path, data: bytes, *, mode: int | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        if mode is not None:
            os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    rendered = json.dumps(
        value,
        indent=2,
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
    )
    _atomic_write(path, (rendered + "\n").encode("utf-8"))


def _json_digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _reject_embedded_credentials(value: Any) -> None:
    rendered = json.dumps(value, ensure_ascii=False)
    if PAT_VALUE.search(rendered) or AUTH_VALUE.search(rendered):
        raise ValueError(
            "qualification plans must reference credential environment variables, "
            "not embed credential values"
        )


def _safe_identifier(value: str, label: str, *, allow_hyphen: bool = True) -> str:
    pattern = r"[A-Za-z0-9_-]+" if allow_hyphen else r"[A-Za-z0-9_]+"
    if not re.fullmatch(pattern, value):
        allowed = "letters, digits, '_' and '-'" if allow_hyphen else "letters, digits, and '_'"
        raise ValueError(f"{label} must contain only ASCII {allowed}")
    return value


def _https_origin(value: str, label: str) -> str:
    parsed = urllib.parse.urlparse(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path not in ("", "/")
    ):
        raise ValueError(f"{label} must be a credential-free HTTPS origin")
    return value.rstrip("/")


def _positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be a positive integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{label} must be a positive integer") from exc
    if parsed <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return parsed


def _finite_positive(value: Any, label: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{label} must be finite and positive") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError(f"{label} must be finite and positive")
    return parsed


def _slug(value: str) -> str:
    result = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_").lower()
    if not result:
        raise ValueError(f"query size has no usable identifier characters: {value!r}")
    return result


def _artifact_paths(
    case_dir: Path,
    *,
    full: bool,
    query_workloads: bool = False,
) -> dict[str, str]:
    ingest = case_dir / "ingest"
    result = {
        "preflight": str(case_dir / "preflight_report.json"),
        "source_manifest": str(ingest / "source_manifest.json"),
        "ingest_progress": str(ingest / "ingest_progress.json"),
        "ingest_metrics": str(ingest / "ingest_metrics.jsonl"),
        "ingest_summary": str(ingest / "ingest_summary.json"),
    }
    if full or query_workloads:
        result.update(
            {
                "dashboard": str(case_dir / "dashboard.jsonl"),
                "drilldown": str(case_dir / "drilldown.jsonl"),
                "freshness": str(case_dir / "freshness.jsonl"),
                "freshness_progress": str(case_dir / "freshness_progress.json"),
            }
        )
    if full:
        result.update(
            {
                "evidence_dir": str(case_dir / "evidence"),
                "evidence_summary": str(case_dir / "evidence" / "evidence_summary.json"),
                "cost_summary": str(case_dir / "cost_summary.json"),
                "validation_report": str(case_dir / "validation_report.json"),
            }
        )
    return result


def _command(step: str, argv: Sequence[str], **extra: Any) -> dict[str, Any]:
    return {"step": step, "argv": list(argv), **extra}


def _preflight_command(
    python: str,
    zerobus_python: str,
    script_dir: Path,
    artifacts: Mapping[str, str],
    *,
    host: str,
    endpoint: str,
    catalog: str,
    schema: str,
    raw_table: str,
    mv_table: str,
    rt_warehouse_id: str,
    control_warehouse_id: str,
) -> dict[str, Any]:
    return _command(
        "online-preflight",
        (
            python,
            str(script_dir / "preflight.py"),
            "--online",
            "--output",
            artifacts["preflight"],
            "--runner-python",
            python,
            "--zerobus-python",
            zerobus_python,
            "--host",
            host,
            "--catalog",
            catalog,
            "--schema",
            schema,
            "--raw-table",
            raw_table,
            "--mv-table",
            mv_table,
            "--rt-warehouse",
            rt_warehouse_id,
            "--control-warehouse",
            control_warehouse_id,
            "--zerobus-endpoint",
            endpoint,
        ),
        credential_environment=[
            "DATABRICKS_TOKEN",
            "DATABRICKS_CLIENT_ID",
            "DATABRICKS_CLIENT_SECRET",
        ],
    )


def _ingest_command(
    python: str,
    script_dir: Path,
    artifacts: Mapping[str, str],
    *,
    data_dir: Path,
    host: str,
    endpoint: str,
    catalog: str,
    schema: str,
    raw_table: str,
    control_warehouse_id: str,
    run_id: str,
    expected_rows: int,
    target_eps: int,
    producer: Mapping[str, Any],
) -> dict[str, Any]:
    return _command(
        "zerobus-ingest",
        (
            python,
            str(script_dir / "ingest_zerobus.py"),
            "--dir",
            str(data_dir),
            "--host",
            host,
            "--endpoint",
            endpoint,
            "--catalog",
            catalog,
            "--schema",
            schema,
            "--table",
            raw_table,
            "--count-warehouse",
            control_warehouse_id,
            "--workers",
            str(producer["stream_count"]),
            "--batch-size",
            str(producer["batch_size"]),
            "--queue-capacity",
            str(producer["queue_capacity"]),
            "--target-eps",
            str(target_eps),
            "--compression",
            str(producer["compression"]),
            "--metrics-interval",
            str(producer["metrics_interval_seconds"]),
            "--max-rows",
            str(expected_rows),
            "--expected-rows",
            str(expected_rows),
            "--allow-partial",
            "--run-id",
            run_id,
            "--output-dir",
            str(Path(artifacts["ingest_progress"]).parent),
        ),
        credential_environment=[
            "DATABRICKS_TOKEN",
            "DATABRICKS_CLIENT_ID",
            "DATABRICKS_CLIENT_SECRET",
        ],
        concurrency_group="measured-run",
    )


def _runner_command(
    python: str,
    script_dir: Path,
    workload: str,
    artifacts: Mapping[str, str],
    *,
    host: str,
    catalog: str,
    schema: str,
    mv_table: str,
    rt_warehouse_id: str,
    control_warehouse_id: str,
    run_id: str,
    interval: int,
    iterations: int,
    query_size: str,
) -> dict[str, Any]:
    argv = [
        python,
        str(script_dir / f"run_{workload}.py"),
        "--host",
        host,
        "--rt-warehouse-id",
        rt_warehouse_id,
        "--control-warehouse-id",
        control_warehouse_id,
        "--catalog",
        catalog,
        "--schema",
        schema,
        "--mv-table",
        mv_table,
        "--producer-progress",
        artifacts["ingest_progress"],
        "--run-id",
        run_id,
        "--interval",
        str(interval),
        "--iterations",
        str(iterations),
        "--machine",
        query_size,
        "--output",
        artifacts[workload],
    ]
    freshness_progress = artifacts.get("freshness_progress")
    if freshness_progress:
        argv.extend(("--freshness-monitor", freshness_progress))
    return _command(
        f"{workload}-runner",
        argv,
        credential_environment=[
            "DATABRICKS_TOKEN",
            "DATABRICKS_CLIENT_ID",
            "DATABRICKS_CLIENT_SECRET",
        ],
        concurrency_group="measured-run",
    )


def _freshness_command(
    python: str,
    script_dir: Path,
    artifacts: Mapping[str, str],
    *,
    host: str,
    catalog: str,
    schema: str,
    raw_table: str,
    mv_table: str,
    control_warehouse_id: str,
    run_id: str,
    iterations: int,
) -> dict[str, Any]:
    return _command(
        "freshness-monitor",
        (
            python,
            str(script_dir / "monitor_freshness.py"),
            "--host",
            host,
            "--control-warehouse-id",
            control_warehouse_id,
            "--catalog",
            catalog,
            "--schema",
            schema,
            "--raw-table",
            raw_table,
            "--mv-table",
            mv_table,
            "--producer-progress",
            artifacts["ingest_progress"],
            "--since",
            "${CASE_SINCE_UTC:?set CASE_SINCE_UTC}",
            "--interval",
            "30",
            "--iterations",
            str(iterations),
            "--run-id",
            run_id,
            "--output",
            artifacts["freshness"],
            "--progress-json",
            artifacts["freshness_progress"],
        ),
        credential_environment=[
            "DATABRICKS_TOKEN",
            "DATABRICKS_CLIENT_ID",
            "DATABRICKS_CLIENT_SECRET",
        ],
        concurrency_group="measured-run",
    )


def _post_run_commands(
    python: str,
    script_dir: Path,
    artifacts: Mapping[str, str],
    *,
    host: str,
    catalog: str,
    schema: str,
    raw_table: str,
    mv_table: str,
    rt_warehouse_id: str,
    control_warehouse_id: str,
    run_id: str,
    expected_rows: int,
    eps_min: float,
    eps_max: float,
) -> list[dict[str, Any]]:
    evidence = _command(
        "collect-evidence",
        (
            python,
            str(script_dir / "collect_evidence.py"),
            "--host",
            host,
            "--control-warehouse-id",
            control_warehouse_id,
            "--rt-warehouse-id",
            rt_warehouse_id,
            "--catalog",
            catalog,
            "--schema",
            schema,
            "--raw-table",
            raw_table,
            "--mv-table",
            mv_table,
            "--since",
            "${CASE_SINCE_UTC:?set CASE_SINCE_UTC}",
            "--until",
            "${CASE_UNTIL_UTC:?set CASE_UNTIL_UTC}",
            "--run-id",
            run_id,
            "--output-dir",
            artifacts["evidence_dir"],
        ),
        credential_environment=[
            "DATABRICKS_TOKEN",
            "DATABRICKS_CLIENT_ID",
            "DATABRICKS_CLIENT_SECRET",
        ],
    )
    cost = _command(
        "summarize-cost",
        (
            python,
            str(script_dir / "costs" / "summarize_run.py"),
            "--evidence-dir",
            artifacts["evidence_dir"],
            "--since",
            "${CASE_SINCE_UTC:?set CASE_SINCE_UTC}",
            "--until",
            "${CASE_UNTIL_UTC:?set CASE_UNTIL_UTC}",
            "--rt-warehouse-id",
            rt_warehouse_id,
            "--control-warehouse-id",
            control_warehouse_id,
            "--output",
            artifacts["cost_summary"],
        ),
    )
    validate = _command(
        "validate-run",
        (
            python,
            str(script_dir / "validate_run.py"),
            "--source-manifest",
            artifacts["source_manifest"],
            "--ingest-progress",
            artifacts["ingest_progress"],
            "--ingest-metrics",
            artifacts["ingest_metrics"],
            "--dashboard",
            artifacts["dashboard"],
            "--drilldown",
            artifacts["drilldown"],
            "--freshness",
            artifacts["freshness"],
            "--evidence-summary",
            artifacts["evidence_summary"],
            "--cost-summary",
            artifacts["cost_summary"],
            "--expected-rows",
            str(expected_rows),
            "--eps-min",
            format(eps_min, ".12g"),
            "--eps-max",
            format(eps_max, ".12g"),
            "--output",
            artifacts["validation_report"],
        ),
    )
    return [evidence, cost, validate]


def _case_common(
    case_id: str,
    *,
    run_id: str,
    schema: str,
    raw_table: str,
    mv_table: str,
    artifacts: Mapping[str, str],
    commands: Sequence[Mapping[str, Any]],
    acceptance: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "id": case_id,
        "state": "planned",
        "run_id": run_id,
        "target": {
            "schema": schema,
            "raw_table": raw_table,
            "materialized_view": mv_table,
        },
        "fresh_target_required": True,
        "destructive_ddl_warning": (
            "create.sql drops the target materialized view and raw table. Apply it "
            "before this case to reset the dedicated qualification schema. Never "
            "run qualification cases concurrently."
        ),
        "preconditions": [
            "Provision or select the declared warehouse configuration while no measured run is live.",
            "Apply create.sql to reset the dedicated qualification schema after reviewing its DROP statements.",
            "Set CASE_SINCE_UTC and CASE_UNTIL_UTC for commands that reference the bounded run window.",
            "Keep credentials only in DATABRICKS_TOKEN or DATABRICKS_CLIENT_ID/DATABRICKS_CLIENT_SECRET.",
        ],
        "artifacts": dict(artifacts),
        "commands": [dict(command) for command in commands],
        "acceptance": dict(acceptance),
    }


def build_plan(
    *,
    output_dir: Path,
    repo_dir: Path,
    data_dir: Path,
    catalog: str,
    schema: str,
    raw_table: str,
    mv_table: str,
    rt_warehouse_id: str,
    control_warehouse_id: str,
    producer_host_description: str,
    producer_network_capacity_gbps: float,
    host: str,
    endpoint: str,
    stream_count: int,
    batch_size: int,
    queue_capacity: int,
    compression: str,
    query_size_candidates: Sequence[str],
    capacity_tolerance: float = DEFAULT_TOLERANCE,
    capacity_duration_seconds: int = CAPACITY_DURATION_SECONDS,
    metrics_interval_seconds: int = METRICS_INTERVAL_SECONDS,
    max_producer_cpu_percent: float = DEFAULT_MAX_PRODUCER_CPU_PERCENT,
    max_producer_network_percent: float = DEFAULT_MAX_PRODUCER_NETWORK_PERCENT,
    endurance_duration_seconds: int = DEFAULT_ENDURANCE_SECONDS,
    run_prefix: str = "tune_endurance",
    python: str = "python3",
    producer_python: str | None = None,
) -> dict[str, Any]:
    """Return one deterministic, secret-free qualification plan."""
    output_dir = output_dir.expanduser().resolve()
    repo_dir = repo_dir.expanduser().resolve()
    data_dir = data_dir.expanduser().resolve()
    script_dir = repo_dir / "full-path-realtime" / "quotes" / "databricks"
    for path, label in ((repo_dir, "repo_dir"), (data_dir, "data_dir")):
        if not path.is_absolute():
            raise ValueError(f"{label} must resolve to an absolute path")
    catalog = _safe_identifier(catalog, "catalog")
    schema = _safe_identifier(schema, "schema")
    raw_table = _safe_identifier(raw_table, "raw table", allow_hyphen=False)
    mv_table = _safe_identifier(mv_table, "materialized-view table")
    run_prefix = _safe_identifier(run_prefix, "run prefix")
    if not rt_warehouse_id or not control_warehouse_id:
        raise ValueError("RT and control warehouse IDs are required")
    if rt_warehouse_id == control_warehouse_id:
        raise ValueError("RT and control warehouse IDs must differ")
    if not producer_host_description.strip():
        raise ValueError("producer host description is required")
    producer_network_capacity_gbps = _finite_positive(
        producer_network_capacity_gbps, "producer network capacity Gbps"
    )
    max_producer_cpu_percent = _finite_positive(
        max_producer_cpu_percent, "maximum producer CPU percent"
    )
    max_producer_network_percent = _finite_positive(
        max_producer_network_percent, "maximum producer network percent"
    )
    if max_producer_cpu_percent > 100 or max_producer_network_percent > 100:
        raise ValueError("producer CPU and network thresholds cannot exceed 100 percent")
    host = _https_origin(host, "host")
    endpoint = _https_origin(endpoint, "endpoint")
    stream_count = _positive_int(stream_count, "stream count")
    batch_size = _positive_int(batch_size, "batch size")
    queue_capacity = _positive_int(queue_capacity, "queue capacity")
    metrics_interval_seconds = _positive_int(
        metrics_interval_seconds, "metrics interval"
    )
    capacity_duration_seconds = _positive_int(
        capacity_duration_seconds, "capacity duration"
    )
    tolerance = _finite_positive(capacity_tolerance, "capacity tolerance")
    if tolerance >= 1:
        raise ValueError("capacity tolerance must be less than 1")
    duration = _positive_int(endurance_duration_seconds, "endurance duration")
    if not MIN_ENDURANCE_SECONDS <= duration <= MAX_ENDURANCE_SECONDS:
        raise ValueError("endurance duration must be between 1800 and 3600 seconds")
    compression = compression.upper().replace("-", "_")
    if compression not in {"NONE", "LZ4_FRAME", "ZSTD"}:
        raise ValueError("compression must be NONE, LZ4_FRAME, or ZSTD")
    candidates = [
        part.strip()
        for value in query_size_candidates
        for part in str(value).split(",")
        if part.strip()
    ]
    if not candidates or any(not value for value in candidates):
        raise ValueError("at least one non-empty query-size candidate is required")
    if len(set(candidates)) != len(candidates):
        raise ValueError("query-size candidates must be unique")
    if capacity_duration_seconds / metrics_interval_seconds < MIN_CAPACITY_INTERVALS + 1:
        raise ValueError(
            "capacity duration must allow one warm-up plus at least 10 metric intervals"
        )
    producer_python = producer_python or python

    producer = {
        "host_description": producer_host_description.strip(),
        "stream_count": stream_count,
        "batch_size": batch_size,
        "queue_capacity": queue_capacity,
        "compression": compression,
        "metrics_interval_seconds": metrics_interval_seconds,
        "network_capacity_gbps": producer_network_capacity_gbps,
        "maximum_host_cpu_percent": max_producer_cpu_percent,
        "maximum_network_utilization_percent": max_producer_network_percent,
    }
    matrix_dir = output_dir / "cases"
    cases: list[dict[str, Any]] = []

    correctness_id = "correctness-10m"
    correctness_schema = schema
    correctness_dir = matrix_dir / correctness_id
    correctness_artifacts = _artifact_paths(correctness_dir, full=True)
    correctness_run = f"{run_prefix}_correctness_10m"
    lower = CAPACITY_TARGETS[0] * (1 - tolerance)
    upper = CAPACITY_TARGETS[0] * (1 + tolerance)
    correctness_commands = [
        _preflight_command(
            python,
            producer_python,
            script_dir,
            correctness_artifacts,
            host=host,
            endpoint=endpoint,
            catalog=catalog,
            schema=correctness_schema,
            raw_table=raw_table,
            mv_table=mv_table,
            rt_warehouse_id=rt_warehouse_id,
            control_warehouse_id=control_warehouse_id,
        ),
        _ingest_command(
            producer_python,
            script_dir,
            correctness_artifacts,
            data_dir=data_dir,
            host=host,
            endpoint=endpoint,
            catalog=catalog,
            schema=correctness_schema,
            raw_table=raw_table,
            control_warehouse_id=control_warehouse_id,
            run_id=correctness_run,
            expected_rows=CORRECTNESS_ROWS,
            target_eps=CAPACITY_TARGETS[0],
            producer=producer,
        ),
        _freshness_command(
            python,
            script_dir,
            correctness_artifacts,
            host=host,
            catalog=catalog,
            schema=correctness_schema,
            raw_table=raw_table,
            mv_table=mv_table,
            control_warehouse_id=control_warehouse_id,
            run_id=correctness_run,
            iterations=4,
        ),
        _runner_command(
            python,
            script_dir,
            "dashboard",
            correctness_artifacts,
            host=host,
            catalog=catalog,
            schema=correctness_schema,
            mv_table=mv_table,
            rt_warehouse_id=rt_warehouse_id,
            control_warehouse_id=control_warehouse_id,
            run_id=correctness_run,
            interval=600,
            iterations=1,
            query_size=candidates[0],
        ),
        _runner_command(
            python,
            script_dir,
            "drilldown",
            correctness_artifacts,
            host=host,
            catalog=catalog,
            schema=correctness_schema,
            mv_table=mv_table,
            rt_warehouse_id=rt_warehouse_id,
            control_warehouse_id=control_warehouse_id,
            run_id=correctness_run,
            interval=3600,
            iterations=1,
            query_size=candidates[0],
        ),
        *_post_run_commands(
            python,
            script_dir,
            correctness_artifacts,
            host=host,
            catalog=catalog,
            schema=correctness_schema,
            raw_table=raw_table,
            mv_table=mv_table,
            rt_warehouse_id=rt_warehouse_id,
            control_warehouse_id=control_warehouse_id,
            run_id=correctness_run,
            expected_rows=CORRECTNESS_ROWS,
            eps_min=lower,
            eps_max=upper,
        ),
    ]
    cases.append(
        _case_common(
            correctness_id,
            run_id=correctness_run,
            schema=correctness_schema,
            raw_table=raw_table,
            mv_table=mv_table,
            artifacts=correctness_artifacts,
            commands=correctness_commands,
            acceptance={
                "validation_accepted": True,
                "expected_rows": CORRECTNESS_ROWS,
                "exact_source_cap_required": "--max-rows 10000000",
                "target_eps": CAPACITY_TARGETS[0],
            },
        )
    )

    for target, label in zip(CAPACITY_TARGETS, ("100k", "500k", "1m")):
        case_id = f"capacity-{label}"
        case_schema = schema
        case_dir = matrix_dir / case_id
        artifacts = _artifact_paths(case_dir, full=False)
        run_id = f"{run_prefix}_capacity_{label}"
        expected_rows = target * capacity_duration_seconds
        commands = [
            _preflight_command(
                python,
                producer_python,
                script_dir,
                artifacts,
                host=host,
                endpoint=endpoint,
                catalog=catalog,
                schema=case_schema,
                raw_table=raw_table,
                mv_table=mv_table,
                rt_warehouse_id=rt_warehouse_id,
                control_warehouse_id=control_warehouse_id,
            ),
            _ingest_command(
                producer_python,
                script_dir,
                artifacts,
                data_dir=data_dir,
                host=host,
                endpoint=endpoint,
                catalog=catalog,
                schema=case_schema,
                raw_table=raw_table,
                control_warehouse_id=control_warehouse_id,
                run_id=run_id,
                expected_rows=expected_rows,
                target_eps=target,
                producer=producer,
            ),
        ]
        cases.append(
            _case_common(
                case_id,
                run_id=run_id,
                schema=case_schema,
                raw_table=raw_table,
                mv_table=mv_table,
                artifacts=artifacts,
                commands=commands,
                acceptance={
                    "target_committed_eps": target,
                    "expected_rows": expected_rows,
                    "duration_seconds": capacity_duration_seconds,
                    "tolerance_fraction": tolerance,
                    "warmup_metric_intervals": 1,
                    "minimum_in_tolerance_non_warmup_intervals": MIN_CAPACITY_INTERVALS,
                    "maximum_host_cpu_percent": max_producer_cpu_percent,
                    "network_capacity_gbps": producer_network_capacity_gbps,
                    "maximum_network_utilization_percent": (
                        max_producer_network_percent
                    ),
                    "clean_finished_exact_progress": True,
                    "no_errors": True,
                },
            )
        )

    query_cases: list[dict[str, Any]] = []
    for ordinal, query_size in enumerate(candidates, start=1):
        case_id = f"query-size-{ordinal:02d}-{_slug(query_size)}"
        case_schema = schema
        case_dir = matrix_dir / case_id
        artifacts = _artifact_paths(case_dir, full=False, query_workloads=True)
        run_id = f"{run_prefix}_qsize_{ordinal:02d}"
        query_rows = CAPACITY_TARGETS[-1] * 120
        commands = [
            _command(
                "provision-query-size",
                (
                    "operator-provision-query-size",
                    "--warehouse-id",
                    rt_warehouse_id,
                    "--query-size",
                    query_size,
                ),
                operator_action=True,
                live_resize_prohibited=True,
                instruction=(
                    "Stop all measured work, explicitly provision this size, and "
                    "record the provider-returned size before preflight."
                ),
            ),
            _preflight_command(
                python,
                producer_python,
                script_dir,
                artifacts,
                host=host,
                endpoint=endpoint,
                catalog=catalog,
                schema=case_schema,
                raw_table=raw_table,
                mv_table=mv_table,
                rt_warehouse_id=rt_warehouse_id,
                control_warehouse_id=control_warehouse_id,
            ),
            _ingest_command(
                producer_python,
                script_dir,
                artifacts,
                data_dir=data_dir,
                host=host,
                endpoint=endpoint,
                catalog=catalog,
                schema=case_schema,
                raw_table=raw_table,
                control_warehouse_id=control_warehouse_id,
                run_id=run_id,
                expected_rows=query_rows,
                target_eps=CAPACITY_TARGETS[-1],
                producer=producer,
            ),
            _freshness_command(
                python,
                script_dir,
                artifacts,
                host=host,
                catalog=catalog,
                schema=case_schema,
                raw_table=raw_table,
                mv_table=mv_table,
                control_warehouse_id=control_warehouse_id,
                run_id=run_id,
                iterations=5,
            ),
            _runner_command(
                python,
                script_dir,
                "dashboard",
                artifacts,
                host=host,
                catalog=catalog,
                schema=case_schema,
                mv_table=mv_table,
                rt_warehouse_id=rt_warehouse_id,
                control_warehouse_id=control_warehouse_id,
                run_id=run_id,
                interval=600,
                iterations=1,
                query_size=query_size,
            ),
            _runner_command(
                python,
                script_dir,
                "drilldown",
                artifacts,
                host=host,
                catalog=catalog,
                schema=case_schema,
                mv_table=mv_table,
                rt_warehouse_id=rt_warehouse_id,
                control_warehouse_id=control_warehouse_id,
                run_id=run_id,
                interval=3600,
                iterations=1,
                query_size=query_size,
            ),
        ]
        query_cases.append(
            _case_common(
                case_id,
                run_id=run_id,
                schema=case_schema,
                raw_table=raw_table,
                mv_table=mv_table,
                artifacts=artifacts,
                commands=commands,
                acceptance={
                    "candidate_ordinal": ordinal,
                    "query_size": query_size,
                    "rt_warehouse_id": rt_warehouse_id,
                    "target_committed_eps": CAPACITY_TARGETS[-1],
                    "tolerance_fraction": tolerance,
                    "expected_rows": query_rows,
                    "exact_size_proven_by_preflight_and_runner": True,
                    "dashboard_query_count": 4,
                    "drilldown_query_count": 2,
                    "waiting_at_capacity_ms": 0,
                    "waiting_for_compute_ms": 0,
                    "queue_time_ms": 0,
                    "result_from_cache": False,
                    "no_errors": True,
                },
            )
        )

    endurance_id = "endurance-1m"
    endurance_schema = schema
    endurance_dir = matrix_dir / endurance_id
    endurance_artifacts = _artifact_paths(endurance_dir, full=True)
    endurance_run = f"{run_prefix}_endurance_1m"
    endurance_rows = CAPACITY_TARGETS[-1] * duration
    dashboard_iterations = duration // 600 + 1
    drilldown_iterations = duration // 3600 + 1
    freshness_iterations = duration // 30 + 1
    endurance_commands = [
        _command(
            "provision-selected-query-size",
            (
                "operator-provision-query-size",
                "--warehouse-id",
                rt_warehouse_id,
                "--query-size",
                "${SELECTED_QUERY_SIZE:?set SELECTED_QUERY_SIZE}",
            ),
            operator_action=True,
            live_resize_prohibited=True,
            instruction=(
                "After the sweep evaluator selects a size, stop all measured work "
                "and explicitly provision that size before endurance preflight."
            ),
        ),
        _preflight_command(
            python,
            producer_python,
            script_dir,
            endurance_artifacts,
            host=host,
            endpoint=endpoint,
            catalog=catalog,
            schema=endurance_schema,
            raw_table=raw_table,
            mv_table=mv_table,
            rt_warehouse_id=rt_warehouse_id,
            control_warehouse_id=control_warehouse_id,
        ),
        _ingest_command(
            producer_python,
            script_dir,
            endurance_artifacts,
            data_dir=data_dir,
            host=host,
            endpoint=endpoint,
            catalog=catalog,
            schema=endurance_schema,
            raw_table=raw_table,
            control_warehouse_id=control_warehouse_id,
            run_id=endurance_run,
            expected_rows=endurance_rows,
            target_eps=CAPACITY_TARGETS[-1],
            producer=producer,
        ),
        _freshness_command(
            python,
            script_dir,
            endurance_artifacts,
            host=host,
            catalog=catalog,
            schema=endurance_schema,
            raw_table=raw_table,
            mv_table=mv_table,
            control_warehouse_id=control_warehouse_id,
            run_id=endurance_run,
            iterations=freshness_iterations,
        ),
        _runner_command(
            python,
            script_dir,
            "dashboard",
            endurance_artifacts,
            host=host,
            catalog=catalog,
            schema=endurance_schema,
            mv_table=mv_table,
            rt_warehouse_id=rt_warehouse_id,
            control_warehouse_id=control_warehouse_id,
            run_id=endurance_run,
            interval=600,
            iterations=dashboard_iterations,
            query_size="${SELECTED_QUERY_SIZE:?set SELECTED_QUERY_SIZE}",
        ),
        _runner_command(
            python,
            script_dir,
            "drilldown",
            endurance_artifacts,
            host=host,
            catalog=catalog,
            schema=endurance_schema,
            mv_table=mv_table,
            rt_warehouse_id=rt_warehouse_id,
            control_warehouse_id=control_warehouse_id,
            run_id=endurance_run,
            interval=3600,
            iterations=drilldown_iterations,
            query_size="${SELECTED_QUERY_SIZE:?set SELECTED_QUERY_SIZE}",
        ),
        *_post_run_commands(
            python,
            script_dir,
            endurance_artifacts,
            host=host,
            catalog=catalog,
            schema=endurance_schema,
            raw_table=raw_table,
            mv_table=mv_table,
            rt_warehouse_id=rt_warehouse_id,
            control_warehouse_id=control_warehouse_id,
            run_id=endurance_run,
            expected_rows=endurance_rows,
            eps_min=CAPACITY_TARGETS[-1] * (1 - tolerance),
            eps_max=CAPACITY_TARGETS[-1] * (1 + tolerance),
        ),
    ]
    cases.append(
        _case_common(
            endurance_id,
            run_id=endurance_run,
            schema=endurance_schema,
            raw_table=raw_table,
            mv_table=mv_table,
            artifacts=endurance_artifacts,
            commands=endurance_commands,
            acceptance={
                "target_committed_eps": CAPACITY_TARGETS[-1],
                "expected_rows": endurance_rows,
                "minimum_duration_seconds": duration,
                "maximum_duration_seconds": MAX_ENDURANCE_SECONDS,
                "dashboard_interval_seconds": 600,
                "drilldown_interval_seconds": 3600,
                "minimum_accepted_dashboard_iterations": max(3, duration // 600),
                "minimum_accepted_drilldown_iterations": 1,
                "freshness_accepted": True,
                "resource_warmup_metric_intervals": 1,
                "maximum_host_cpu_percent": max_producer_cpu_percent,
                "network_capacity_gbps": producer_network_capacity_gbps,
                "maximum_network_utilization_percent": (
                    max_producer_network_percent
                ),
                "no_errors": True,
                "configuration_identical_to_accepted_capacity_1m": True,
            },
        )
    )

    matrix = {
        "correctness": [cases[0]],
        "capacity": cases[1:4],
        "query_size_sweep": query_cases,
        "endurance": [cases[4]],
    }
    plan = {
        "schema_version": SCHEMA_VERSION,
        "task": TASK,
        "state": "planned",
        "status_note": "Commands are planned and have not been run by this tool.",
        "inputs": {
            "repo_dir": str(repo_dir),
            "data_dir": str(data_dir),
            "catalog": catalog,
            "base_schema": schema,
            "raw_table": raw_table,
            "materialized_view": mv_table,
            "rt_warehouse_id": rt_warehouse_id,
            "control_warehouse_id": control_warehouse_id,
            "workspace_host": host,
            "zerobus_endpoint": endpoint,
            "interpreters": {
                "runner": python,
                "zerobus_producer": producer_python,
            },
            "producer": producer,
            "query_size_candidates_smallest_to_largest": candidates,
        },
        "credential_environment_references": [
            "DATABRICKS_TOKEN",
            "DATABRICKS_CLIENT_ID",
            "DATABRICKS_CLIENT_SECRET",
        ],
        "execution_rules": {
            "stage_order": [
                "correctness",
                "capacity",
                "query_size_sweep",
                "endurance",
            ],
            "stop_on_failure": True,
            "shared_dedicated_qualification_schema": True,
            "fresh_tables_per_case": True,
            "cases_must_run_sequentially": True,
            "resize_during_live_measurement_prohibited": True,
            "113b_run_requires_qualified_report": True,
        },
        "matrix": matrix,
        "command_file": str(output_dir / "qualification_commands.sh"),
    }
    plan["matrix_sha256"] = _json_digest(matrix)
    _reject_embedded_credentials(plan)
    return plan


def _shell_token(value: str) -> str:
    if value.startswith("${") and value.endswith("}"):
        return value
    return shlex.quote(value)


def render_command_file(plan: Mapping[str, Any]) -> str:
    lines = [
        "#!/bin/sh",
        "set -eu",
        "# Generated qualification commands. No credential values are embedded.",
        "# Export either DATABRICKS_TOKEN or OAuth client credential variables.",
        "# Run one case at a time and reset the qualification tables with create.sql.",
        ': "${QUALIFY_CASE:?Set QUALIFY_CASE to one case ID or evaluate}"',
        "QUALIFY_MATCHED=0",
        "",
    ]
    matrix = plan["matrix"]
    for stage in ("correctness", "capacity", "query_size_sweep", "endurance"):
        lines.append(f"# ===== {stage} =====")
        for case in matrix[stage]:
            confirmation = re.sub(r"[^A-Za-z0-9]", "_", case["id"]).upper()
            acceptance = case["acceptance"]
            duration = acceptance.get("duration_seconds")
            if duration is None:
                duration = acceptance.get("minimum_duration_seconds")
            if duration is None:
                expected = _number(acceptance.get("expected_rows"))
                target = _number(
                    acceptance.get("target_eps")
                    or acceptance.get("target_committed_eps")
                )
                duration = int(expected / target) if expected and target else 120
            lines.extend(
                (
                    "",
                    f"# Case {case['id']} (planned; fresh tables required)",
                    f"# {case['destructive_ddl_warning']}",
                    f"if [ \"$QUALIFY_CASE\" = {_shell_token(case['id'])} ]; then",
                    "QUALIFY_MATCHED=1",
                    "# Apply create.sql to reset the qualification schema before continuing.",
                    (
                        f': "${{QUALIFY_PREPARED_{confirmation}:?Set '
                        f'QUALIFY_PREPARED_{confirmation}=yes only after reset DDL}}"'
                    ),
                    (
                        "CASE_SINCE_UTC=$(python3 -c "
                        + shlex.quote(
                            "from datetime import datetime,timezone; "
                            "print(datetime.now(timezone.utc).isoformat("
                            "timespec='milliseconds').replace('+00:00','Z'))"
                        )
                        + ")"
                    ),
                    (
                        "CASE_UNTIL_UTC=$(python3 -c "
                        + shlex.quote(
                            "import sys; from datetime import datetime,timedelta; "
                            "start=datetime.fromisoformat(sys.argv[1].replace('Z','+00:00')); "
                            "print((start+timedelta(seconds=int(sys.argv[2])))."
                            "isoformat(timespec='milliseconds').replace('+00:00','Z'))"
                        )
                        + f' "$CASE_SINCE_UTC" {int(duration)})'
                    ),
                    "export CASE_SINCE_UTC CASE_UNTIL_UTC",
                )
            )
            if stage == "endurance":
                lines.append(
                    ': "${SELECTED_QUERY_SIZE:?Run the sweep evaluator and set '
                    'SELECTED_QUERY_SIZE}"'
                )
            concurrent: list[Mapping[str, Any]] = []

            def flush_concurrent() -> None:
                if not concurrent:
                    return
                lines.append("QUALIFY_PIDS=")
                for index, grouped in enumerate(concurrent):
                    command_line = " ".join(
                        _shell_token(value) for value in grouped["argv"]
                    )
                    lines.append(f"{command_line} &")
                    lines.append('QUALIFY_PID=$!')
                    lines.append('QUALIFY_PIDS="$QUALIFY_PIDS $QUALIFY_PID"')
                    if index == 0 and grouped["step"] == "zerobus-ingest":
                        progress = _shell_token(case["artifacts"]["ingest_progress"])
                        lines.extend(
                            (
                                "QUALIFY_WAITED=0",
                                f"while [ ! -s {progress} ]; do",
                                '  kill -0 "$QUALIFY_PID" 2>/dev/null || '
                                'wait "$QUALIFY_PID"',
                                '  [ "$QUALIFY_WAITED" -lt 300 ] || '
                                '{ echo "producer progress did not appear" >&2; exit 1; }',
                                "  sleep 1",
                                "  QUALIFY_WAITED=$((QUALIFY_WAITED + 1))",
                                "done",
                            )
                        )
                lines.extend(
                    (
                        "QUALIFY_STATUS=0",
                        "for QUALIFY_PID in $QUALIFY_PIDS; do",
                        '  wait "$QUALIFY_PID" || QUALIFY_STATUS=$?',
                        "done",
                        '[ "$QUALIFY_STATUS" -eq 0 ] || exit "$QUALIFY_STATUS"',
                    )
                )
                concurrent.clear()

            for command in case["commands"]:
                lines.append(f"# Step: {command['step']}")
                if command.get("operator_action"):
                    flush_concurrent()
                    lines.append("# OPERATOR ACTION: " + command["instruction"])
                    lines.append("# " + " ".join(_shell_token(v) for v in command["argv"]))
                    provision = f"QUALIFY_PROVISIONED_{confirmation}"
                    lines.append(
                        f': "${{{provision}:?Set {provision}=yes after explicit provisioning}}"'
                    )
                    continue
                if command.get("concurrency_group"):
                    concurrent.append(command)
                else:
                    flush_concurrent()
                    lines.append(" ".join(_shell_token(v) for v in command["argv"]))
            flush_concurrent()
            lines.append("fi")
        lines.append("")
    lines.extend(
        (
            "# Evaluate only after all referenced artifacts exist:",
            'if [ "$QUALIFY_CASE" = evaluate ]; then',
            "QUALIFY_MATCHED=1",
            " ".join(
                (
                    _shell_token(sys.executable or "python3"),
                    _shell_token(str(Path(__file__).resolve())),
                    "evaluate",
                    "--plan",
                    _shell_token("${QUALIFICATION_PLAN:?set QUALIFICATION_PLAN}"),
                    "--output",
                    _shell_token("${QUALIFICATION_REPORT:?set QUALIFICATION_REPORT}"),
                )
            ),
            "fi",
            '[ "$QUALIFY_MATCHED" -eq 1 ] || '
            '{ echo "unknown QUALIFY_CASE: $QUALIFY_CASE" >&2; exit 2; }',
            "",
        )
    )
    return "\n".join(lines)


def write_plan(plan: Mapping[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir = output_dir.expanduser().resolve()
    plan_path = output_dir / "qualification_plan.json"
    command_path = Path(str(plan["command_file"]))
    atomic_write_json(plan_path, plan)
    _atomic_write(command_path, render_command_file(plan).encode("utf-8"), mode=0o755)
    return plan_path, command_path


def _read_json(path: Path, label: str) -> tuple[dict[str, Any] | None, str | None]:
    if not path.is_file():
        return None, f"{label} is missing: {path}"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, Mapping):
            raise ValueError("top-level JSON value is not an object")
        return dict(value), None
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return None, f"{label} is invalid: {type(exc).__name__}: {exc}"


def _read_jsonl(
    path: Path, label: str
) -> tuple[list[dict[str, Any]] | None, str | None]:
    if not path.is_file():
        return None, f"{label} is missing: {path}"
    rows: list[dict[str, Any]] = []
    try:
        for number, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, Mapping):
                raise ValueError(f"line {number} is not an object")
            rows.append(dict(value))
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return None, f"{label} is invalid: {type(exc).__name__}: {exc}"
    return rows, None


def _nested(value: Any, *path: str) -> Any:
    current = value
    for component in path:
        if not isinstance(current, Mapping) or component not in current:
            return None
        current = current[component]
    return current


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _integer(value: Any) -> int | None:
    number = _number(value)
    if number is None or number < 0 or int(number) != number:
        return None
    return int(number)


def _case_result(case: Mapping[str, Any], state: str, reasons: Sequence[str], **evidence: Any) -> dict[str, Any]:
    return {
        "id": case.get("id"),
        "state": state,
        "reasons": list(reasons),
        "evidence": evidence,
    }


def _evaluate_correctness(case: Mapping[str, Any]) -> dict[str, Any]:
    path = Path(case["artifacts"]["validation_report"])
    report, error = _read_json(path, "correctness validation report")
    if error and not path.is_file():
        return _case_result(case, "not_run", [error])
    if error or report is None:
        return _case_result(case, "failed", [str(error)])
    expected = _integer(_nested(report, "configuration", "expected_rows"))
    reasons = []
    if report.get("accepted") is not True:
        reasons.append("validation report accepted is not exact true")
    if expected != CORRECTNESS_ROWS:
        reasons.append("validation report expected_rows is not exactly 10000000")
    return _case_result(
        case,
        "failed" if reasons else "passed",
        reasons,
        path=str(path),
        accepted=report.get("accepted"),
        expected_rows=expected,
    )


def _progress_reasons(progress: Mapping[str, Any], expected_rows: int) -> list[str]:
    workers = _integer(_nested(progress, "config", "workers"))
    task_count = _integer(_nested(progress, "source", "task_count"))
    completed_ranges = progress.get("completed_task_ranges")
    compact_complete = (
        task_count is not None
        and task_count > 0
        and completed_ranges == [[0, task_count - 1]]
    )
    stream_status = progress.get("stream_status")
    stream_status = stream_status if isinstance(stream_status, Mapping) else {}
    clean_streams = [
        isinstance(status, Mapping)
        and status.get("close_succeeded") is True
        and status.get("unacked_inspection_succeeded") is True
        and _integer(status.get("unacked_batches")) == 0
        for status in stream_status.values()
    ]
    checks = {
        "finished is not exact true": progress.get("finished") is True,
        "running is not exact false": progress.get("running") is False,
        "clean_checkpoint is not exact true": progress.get("clean_checkpoint") is True,
        "safe_to_resume is not exact true": progress.get("safe_to_resume") is True,
        "stopped_early is not exact false": progress.get("stopped_early") is False,
        "ambiguous is not exact false": progress.get("ambiguous") is False,
        "terminal_error is present": progress.get("terminal_error") in (None, ""),
        "errors is not an empty array": progress.get("errors") == [],
        "pending_batches is not zero": _integer(progress.get("pending_batches")) == 0,
        "pending_rows is not zero": _integer(progress.get("pending_rows")) == 0,
        "partial_task_rows is not empty": progress.get("partial_task_rows") == {},
        "baseline table was not proven empty": (
            _integer(progress.get("baseline_table_rows")) == 0
            and progress.get("baseline_table_rows_unknown") is False
        ),
        "provider committed row count is not exact": (
            _integer(progress.get("provider_committed_rows")) == expected_rows
        ),
        "logical raw row count is not exact": (
            _integer(progress.get("logical_raw_rows")) == expected_rows
        ),
        "submitted row count is not exact": (
            _integer(progress.get("submitted_rows")) == expected_rows
        ),
        "source row count is not exact": (
            _integer(_nested(progress, "source", "total_rows")) == expected_rows
        ),
        "source tasks are not all completed": (
            task_count is not None
            and task_count > 0
            and _integer(progress.get("completed_tasks")) == task_count
            and (
                progress.get("completed_task_ordinals") == list(range(task_count))
                or compact_complete
            )
        ),
        "producer streams are not all clean": (
            workers is not None
            and workers > 0
            and len(stream_status) == workers
            and all(clean_streams)
        ),
    }
    return [reason for reason, passed in checks.items() if not passed]


def _capacity_intervals(
    rows: Sequence[Mapping[str, Any]],
    *,
    minimum: float,
    maximum: float,
    warmup: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    evidence: list[dict[str, Any]] = []
    reasons: list[str] = []
    previous_rows: int | None = None
    previous_elapsed: float | None = None
    eligible_ordinal = 0
    for index, row in enumerate(rows):
        if row.get("errors") not in (None, []):
            reasons.append(f"metrics row {index} contains errors")
        if row.get("terminal_error") not in (None, ""):
            reasons.append(f"metrics row {index} contains a terminal error")
        committed = _integer(row.get("provider_committed_rows"))
        elapsed = _number(row.get("elapsed_sec"))
        explicit = _number(
            _nested(row, "eps", "interval_provider_committed_rows_per_sec")
        )
        if explicit is None:
            explicit = _number(row.get("interval_eps"))
        interval_eps = explicit
        source = "explicit"
        if interval_eps is None and committed is not None and elapsed is not None:
            if previous_rows is not None and previous_elapsed is not None:
                delta_rows = committed - previous_rows
                delta_time = elapsed - previous_elapsed
                if delta_rows < 0 or delta_time <= 0:
                    reasons.append(f"metrics row {index} counters are not monotonic")
                else:
                    interval_eps = delta_rows / delta_time
                    source = "derived"
        marked_warmup = any(
            row.get(name) is True for name in ("warmup", "warm_up", "is_warmup")
        )
        ignored = marked_warmup or eligible_ordinal < warmup
        eligible_ordinal += 1
        passed = (
            not ignored
            and interval_eps is not None
            and minimum <= interval_eps <= maximum
        )
        evidence.append(
            {
                "index": index,
                "ignored_as_warmup": ignored,
                "eps": interval_eps,
                "source": source,
                "within_tolerance": passed,
            }
        )
        if committed is not None and elapsed is not None:
            previous_rows, previous_elapsed = committed, elapsed
    return evidence, reasons


def _producer_resource_headroom(
    rows: Sequence[Mapping[str, Any]],
    *,
    warmup: int,
    maximum_host_cpu_percent: float,
    network_capacity_gbps: float,
    maximum_network_utilization_percent: float,
) -> tuple[dict[str, Any], list[str]]:
    """Evaluate producer host saturation from interval telemetry."""
    measured = rows[warmup:]
    cpu_values: list[float] = []
    network_values: list[float] = []
    link_bytes_per_second = network_capacity_gbps * 1_000_000_000 / 8
    reasons: list[str] = []
    if not measured:
        reasons.append("producer resource telemetry has no post-warmup intervals")
    for index, row in enumerate(measured, start=warmup):
        cpu = _number(_nested(row, "host", "host_cpu_utilization_percent"))
        receive = _number(_nested(row, "host", "network_receive_bytes_per_second"))
        transmit = _number(
            _nested(row, "host", "network_transmit_bytes_per_second")
        )
        if cpu is None or not 0 <= cpu <= 100:
            reasons.append(f"metrics row {index} has invalid host CPU utilization")
        else:
            cpu_values.append(cpu)
        if (
            receive is None
            or transmit is None
            or receive < 0
            or transmit < 0
        ):
            reasons.append(f"metrics row {index} has invalid host network throughput")
        else:
            network_values.append(
                100.0 * max(receive, transmit) / link_bytes_per_second
            )

    def percentile_95(values: Sequence[float]) -> float | None:
        if not values:
            return None
        ordered = sorted(values)
        return ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)]

    cpu_p95 = percentile_95(cpu_values)
    network_p95 = percentile_95(network_values)
    if cpu_p95 is not None and cpu_p95 > maximum_host_cpu_percent:
        reasons.append(
            f"producer host CPU p95 {cpu_p95:.2f}% exceeds "
            f"{maximum_host_cpu_percent:.2f}%"
        )
    if (
        network_p95 is not None
        and network_p95 > maximum_network_utilization_percent
    ):
        reasons.append(
            f"producer network p95 {network_p95:.2f}% exceeds "
            f"{maximum_network_utilization_percent:.2f}% of declared capacity"
        )
    return {
        "measured_intervals": len(measured),
        "host_cpu_p95_percent": cpu_p95,
        "maximum_host_cpu_percent": maximum_host_cpu_percent,
        "network_p95_utilization_percent": network_p95,
        "maximum_network_utilization_percent": maximum_network_utilization_percent,
        "declared_network_capacity_gbps": network_capacity_gbps,
    }, reasons


def _evaluate_capacity(
    case: Mapping[str, Any],
    expected_producer: Mapping[str, Any],
) -> dict[str, Any]:
    progress_path = Path(case["artifacts"]["ingest_progress"])
    metrics_path = Path(case["artifacts"]["ingest_metrics"])
    progress, progress_error = _read_json(progress_path, "capacity progress")
    metrics, metrics_error = _read_jsonl(metrics_path, "capacity metrics")
    missing = [
        error
        for path, error in (
            (progress_path, progress_error),
            (metrics_path, metrics_error),
        )
        if error and not path.is_file()
    ]
    if missing:
        return _case_result(case, "not_run", missing)
    if progress_error or metrics_error or progress is None or metrics is None:
        return _case_result(
            case,
            "failed",
            [value for value in (progress_error, metrics_error) if value],
        )
    acceptance = case["acceptance"]
    expected_rows = int(acceptance["expected_rows"])
    target = float(acceptance["target_committed_eps"])
    tolerance = float(acceptance["tolerance_fraction"])
    minimum, maximum = target * (1 - tolerance), target * (1 + tolerance)
    reasons = _progress_reasons(progress, expected_rows)
    if _number(_nested(progress, "config", "target_rows_per_sec")) != target:
        reasons.append("producer configured target EPS does not match the capacity case")
    producer, producer_reasons = _producer_signature(progress)
    reasons.extend(producer_reasons)
    if producer != dict(expected_producer):
        reasons.append("producer configuration differs from the frozen plan inputs")
    average = _number(_nested(progress, "eps", "average_provider_committed_rows_per_sec"))
    if average is None or not minimum <= average <= maximum:
        reasons.append("average provider-committed EPS is missing or outside tolerance")
    intervals, interval_reasons = _capacity_intervals(
        metrics,
        minimum=minimum,
        maximum=maximum,
        warmup=int(acceptance["warmup_metric_intervals"]),
    )
    reasons.extend(interval_reasons)
    headroom, headroom_reasons = _producer_resource_headroom(
        metrics,
        warmup=int(acceptance["warmup_metric_intervals"]),
        maximum_host_cpu_percent=float(acceptance["maximum_host_cpu_percent"]),
        network_capacity_gbps=float(acceptance["network_capacity_gbps"]),
        maximum_network_utilization_percent=float(
            acceptance["maximum_network_utilization_percent"]
        ),
    )
    reasons.extend(headroom_reasons)
    passing = sum(item["within_tolerance"] for item in intervals)
    minimum_count = int(acceptance["minimum_in_tolerance_non_warmup_intervals"])
    if passing < minimum_count:
        reasons.append(
            f"only {passing} in-tolerance non-warmup intervals; require {minimum_count}"
        )
    return _case_result(
        case,
        "failed" if reasons else "passed",
        reasons,
        progress_path=str(progress_path),
        metrics_path=str(metrics_path),
        target_eps=target,
        average_eps=average,
        bounds={"minimum": minimum, "maximum": maximum},
        passing_non_warmup_intervals=passing,
        intervals=intervals,
        producer_resource_headroom=headroom,
        producer_signature=producer,
    )


def _warehouse_metadata(preflight: Mapping[str, Any]) -> Mapping[str, Any]:
    value = _nested(preflight, "online", "warehouses", "lakehouse_rt", "metadata")
    return value if isinstance(value, Mapping) else {}


def _warehouse_size(value: Mapping[str, Any]) -> Any:
    for name in ("cluster_size", "warehouse_size", "size"):
        if value.get(name) not in (None, ""):
            return value.get(name)
    return None


def _preflight_reasons(
    preflight: Mapping[str, Any],
    expected_size: str | None = None,
    expected_warehouse_id: str | None = None,
) -> list[str]:
    reasons: list[str] = []
    if _nested(preflight, "summary", "status") not in ("ok", "warning"):
        reasons.append("preflight summary status is missing or error")
    if _nested(preflight, "online", "status") not in ("ok", "warning"):
        reasons.append("online preflight status is missing or error")
    if _nested(preflight, "online", "errors") != []:
        reasons.append("online preflight errors is missing or non-empty")
    for table in ("raw", "materialized_view"):
        predictive = _nested(
            preflight,
            "online",
            "predictive_optimization",
            table,
        )
        if (
            not isinstance(predictive, Mapping)
            or predictive.get("status") != "ok"
            or predictive.get("effective_value") != "ENABLE"
            or predictive.get("effectively_enabled") is not True
        ):
            reasons.append(
                f"preflight does not prove Predictive Optimization enabled for {table}"
            )
    metadata = _warehouse_metadata(preflight)
    if not metadata:
        reasons.append("preflight omits RT warehouse metadata")
    if expected_size is not None and _warehouse_size(metadata) != expected_size:
        reasons.append("preflight does not prove the exact query size")
    if expected_warehouse_id is not None:
        configured_id = _nested(preflight, "config", "rt_warehouse_id")
        if configured_id != expected_warehouse_id:
            reasons.append("preflight config does not prove the RT warehouse ID")
        if metadata.get("id") != expected_warehouse_id:
            reasons.append("preflight metadata does not prove the RT warehouse ID")
    return reasons


def _record_warehouse_size(record: Mapping[str, Any]) -> Any:
    warehouse = record.get("rt_warehouse")
    if not isinstance(warehouse, Mapping):
        return None
    metadata = warehouse.get("api_metadata")
    if isinstance(metadata, Mapping):
        found = _warehouse_size(metadata)
        if found is not None:
            return found
    return _warehouse_size(warehouse)


def _query_record_reasons(
    record: Mapping[str, Any],
    *,
    workload: str,
    query_count: int,
    query_size: str,
    rt_warehouse_id: str,
) -> list[str]:
    reasons: list[str] = []
    if record.get("workload", record.get("runner")) != workload:
        reasons.append(f"record is not labeled {workload}")
    if _record_warehouse_size(record) != query_size:
        reasons.append("runner record does not prove the exact query size")
    if _nested(record, "rt_warehouse", "warehouse_id") != rt_warehouse_id:
        reasons.append("runner record does not prove the RT warehouse ID")
    if record.get("producer_progress_error") not in (None, ""):
        reasons.append("runner could not read producer progress")
    observations = record.get("query_evidence")
    if not isinstance(observations, list) or len(observations) != query_count:
        reasons.append(f"query_evidence is not exactly {query_count} entries")
        return reasons
    aligned_results = record.get("result")
    if (
        not isinstance(aligned_results, list)
        or len(aligned_results) != query_count
        or any(not isinstance(value, list) or len(value) != 1 for value in aligned_results)
    ):
        reasons.append(f"aligned canonical result is not exactly {query_count} entries")
        aligned_results = []
    aligned_cache = record.get("cache_hit")
    if aligned_cache != [[False] for _ in range(query_count)]:
        reasons.append("aligned cache_hit is not exact false for every canonical query")
    query_errors = record.get("query_errors")
    if query_errors != [[] for _ in range(query_count)]:
        reasons.append("aligned query_errors is not empty for every canonical query")
    for offset, observation in enumerate(observations):
        prefix = f"query {offset + 1}"
        if not isinstance(observation, Mapping):
            reasons.append(f"{prefix} evidence is not an object")
            continue
        metrics = observation.get("metrics")
        if not isinstance(metrics, Mapping):
            reasons.append(f"{prefix} metrics are missing")
            continue
        if observation.get("execution_succeeded") is not True:
            reasons.append(f"{prefix} execution did not succeed")
        canonical = _number(observation.get("canonical_duration_sec"))
        if canonical is None:
            reasons.append(f"{prefix} canonical duration is missing")
        if (
            aligned_results
            and _number(aligned_results[offset][0]) != canonical
        ):
            reasons.append(f"{prefix} aligned canonical result disagrees")
        if observation.get("errors") != []:
            reasons.append(f"{prefix} contains errors")
        if metrics.get("result_from_cache") is not False:
            reasons.append(f"{prefix} result cache evidence is not exact false")
        if metrics.get("cache_origin_statement_id") not in (None, ""):
            reasons.append(f"{prefix} reports a cache origin")
        if _number(metrics.get("waiting_at_capacity_ms")) != 0:
            reasons.append(f"{prefix} waiting-at-capacity is nonzero or unproven")
        if _number(metrics.get("waiting_for_compute_ms")) != 0:
            reasons.append(f"{prefix} waiting-for-compute is nonzero or unproven")
        if _number(metrics.get("queue_time_ms")) != 0:
            reasons.append(f"{prefix} queue time is nonzero or unproven")
        status = str(metrics.get("status", "")).upper()
        if status and status != "FINISHED":
            reasons.append(f"{prefix} query-history status is not FINISHED")
    return reasons


def _evaluate_query_size(
    case: Mapping[str, Any],
    expected_producer: Mapping[str, Any],
) -> dict[str, Any]:
    artifacts = case["artifacts"]
    paths = {
        name: Path(artifacts[name])
        for name in ("preflight", "ingest_progress", "dashboard", "drilldown")
    }
    preflight, preflight_error = _read_json(paths["preflight"], "query-size preflight")
    progress, progress_error = _read_json(
        paths["ingest_progress"], "query-size producer progress"
    )
    dashboard, dashboard_error = _read_jsonl(paths["dashboard"], "dashboard report")
    drilldown, drilldown_error = _read_jsonl(paths["drilldown"], "drilldown report")
    errors = {
        "preflight": preflight_error,
        "ingest_progress": progress_error,
        "dashboard": dashboard_error,
        "drilldown": drilldown_error,
    }
    missing = [
        error
        for name, error in errors.items()
        if error and not paths[name].is_file()
    ]
    if missing:
        return _case_result(case, "not_run", missing)
    invalid = [error for error in errors.values() if error]
    if (
        invalid
        or preflight is None
        or progress is None
        or dashboard is None
        or drilldown is None
    ):
        return _case_result(case, "failed", invalid)
    acceptance = case["acceptance"]
    size = str(acceptance["query_size"])
    rt_warehouse_id = str(acceptance["rt_warehouse_id"])
    reasons = _preflight_reasons(preflight, size, rt_warehouse_id)
    reasons.extend(_progress_reasons(progress, int(acceptance["expected_rows"])))
    target = float(acceptance["target_committed_eps"])
    tolerance = float(acceptance["tolerance_fraction"])
    if _number(_nested(progress, "config", "target_rows_per_sec")) != target:
        reasons.append("query-size producer target EPS does not match the fixed plan")
    average = _number(_nested(progress, "eps", "average_provider_committed_rows_per_sec"))
    if (
        average is None
        or not target * (1 - tolerance) <= average <= target * (1 + tolerance)
    ):
        reasons.append("query-size producer average EPS is outside tolerance")
    producer, producer_reasons = _producer_signature(progress)
    reasons.extend(producer_reasons)
    if producer != dict(expected_producer):
        reasons.append("query-size producer configuration differs from the frozen plan")
    if not dashboard:
        reasons.append("dashboard report contains no iterations")
    if not drilldown:
        reasons.append("drilldown report contains no iterations")
    for workload, rows, count in (
        ("dashboard", dashboard, int(acceptance["dashboard_query_count"])),
        ("drilldown", drilldown, int(acceptance["drilldown_query_count"])),
    ):
        for index, record in enumerate(rows):
            reasons.extend(
                f"{workload} iteration {index + 1}: {reason}"
                for reason in _query_record_reasons(
                    record,
                    workload=workload,
                    query_count=count,
                    query_size=size,
                    rt_warehouse_id=rt_warehouse_id,
                )
            )
    return _case_result(
        case,
        "failed" if reasons else "passed",
        reasons,
        query_size=size,
        candidate_ordinal=acceptance["candidate_ordinal"],
        dashboard_iterations=len(dashboard),
        drilldown_iterations=len(drilldown),
        producer_signature=producer,
    )


def _producer_signature(progress: Mapping[str, Any]) -> tuple[dict[str, Any], list[str]]:
    config = progress.get("config")
    target = progress.get("target")
    config = config if isinstance(config, Mapping) else {}
    target = target if isinstance(target, Mapping) else {}
    signature = {
        "stream_count": config.get("workers"),
        "batch_size": config.get("batch_size"),
        "queue_capacity": config.get("queue_capacity"),
        "compression": config.get("ipc_compression", config.get("compression")),
        "host": target.get("host"),
        "endpoint": target.get("zerobus_endpoint"),
    }
    missing = [name for name, value in signature.items() if value in (None, "")]
    return signature, [f"producer signature omits {name}" for name in missing]


def _rt_signature(preflight: Mapping[str, Any]) -> tuple[dict[str, Any], list[str]]:
    metadata = _warehouse_metadata(preflight)
    signature = {
        "warehouse_id": metadata.get("id"),
        "query_size": _warehouse_size(metadata),
        "min_num_clusters": metadata.get("min_num_clusters"),
        "max_num_clusters": metadata.get("max_num_clusters"),
        "enable_serverless_compute": metadata.get("enable_serverless_compute"),
    }
    missing = [name for name, value in signature.items() if value is None]
    return signature, [f"RT warehouse signature omits {name}" for name in missing]


def _freshness_gate(validation: Mapping[str, Any]) -> Mapping[str, Any] | None:
    gates = validation.get("gates")
    if not isinstance(gates, list):
        return None
    return next(
        (
            gate
            for gate in gates
            if isinstance(gate, Mapping) and gate.get("id") == "freshness"
        ),
        None,
    )


def _evaluate_endurance(
    case: Mapping[str, Any],
    capacity_1m: Mapping[str, Any],
    selected_query_size: str | None,
    expected_producer: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    artifacts = case["artifacts"]
    paths = {
        "validation": Path(artifacts["validation_report"]),
        "progress": Path(artifacts["ingest_progress"]),
        "metrics": Path(artifacts["ingest_metrics"]),
        "preflight": Path(artifacts["preflight"]),
        "capacity_progress": Path(capacity_1m["artifacts"]["ingest_progress"]),
        "capacity_preflight": Path(capacity_1m["artifacts"]["preflight"]),
    }
    loaded: dict[str, dict[str, Any] | None] = {}
    errors: dict[str, str | None] = {}
    for name, path in paths.items():
        if name == "metrics":
            value, errors[name] = _read_jsonl(path, name)
            loaded[name] = value  # type: ignore[assignment]
        else:
            loaded[name], errors[name] = _read_json(path, name)
    missing = [
        error
        for name, error in errors.items()
        if error and not paths[name].is_file()
    ]
    if missing:
        return _case_result(case, "not_run", missing), None
    invalid = [error for error in errors.values() if error]
    if invalid or any(value is None for value in loaded.values()):
        return _case_result(case, "failed", invalid), None
    validation = loaded["validation"] or {}
    progress = loaded["progress"] or {}
    metrics = loaded["metrics"] or []
    preflight = loaded["preflight"] or {}
    capacity_progress = loaded["capacity_progress"] or {}
    capacity_preflight = loaded["capacity_preflight"] or {}
    acceptance = case["acceptance"]
    reasons: list[str] = []
    if validation.get("accepted") is not True:
        reasons.append("endurance validation report accepted is not exact true")
    duration = _number(_nested(validation, "window", "duration_seconds"))
    minimum_duration = float(acceptance["minimum_duration_seconds"])
    maximum_duration = float(acceptance["maximum_duration_seconds"])
    if duration is None or not minimum_duration <= duration <= maximum_duration:
        reasons.append("endurance runtime is outside the required 30-60 minute window")
    dashboard_count = _integer(
        _nested(validation, "query_samples", "dashboard", "accepted_count")
    )
    drilldown_count = _integer(
        _nested(validation, "query_samples", "drilldown", "accepted_count")
    )
    if dashboard_count is None or dashboard_count < int(
        acceptance["minimum_accepted_dashboard_iterations"]
    ):
        reasons.append(
            "fewer than the planned dashboard iterations were accepted"
        )
    if drilldown_count is None or drilldown_count < int(
        acceptance["minimum_accepted_drilldown_iterations"]
    ):
        reasons.append(
            "fewer than the planned drilldown iterations were accepted"
        )
    freshness = _freshness_gate(validation)
    freshness_count = _integer(
        _nested(validation, "freshness_samples", "accepted_count")
    )
    if (
        freshness is None
        or freshness.get("passed") is not True
        or freshness_count is None
        or freshness_count < 1
    ):
        reasons.append("freshness evidence was not accepted")
    expected_rows = int(acceptance["expected_rows"])
    reasons.extend(_progress_reasons(progress, expected_rows))
    headroom, headroom_reasons = _producer_resource_headroom(
        metrics,  # type: ignore[arg-type]
        warmup=int(acceptance["resource_warmup_metric_intervals"]),
        maximum_host_cpu_percent=float(acceptance["maximum_host_cpu_percent"]),
        network_capacity_gbps=float(acceptance["network_capacity_gbps"]),
        maximum_network_utilization_percent=float(
            acceptance["maximum_network_utilization_percent"]
        ),
    )
    reasons.extend(headroom_reasons)
    if _number(_nested(progress, "config", "target_rows_per_sec")) != float(
        acceptance["target_committed_eps"]
    ):
        reasons.append("endurance producer target EPS does not match the plan")
    reasons.extend(_preflight_reasons(preflight, selected_query_size))
    reasons.extend(_preflight_reasons(capacity_preflight, selected_query_size))
    producer_1m, producer_1m_reasons = _producer_signature(capacity_progress)
    producer_endurance, producer_endurance_reasons = _producer_signature(progress)
    reasons.extend(producer_1m_reasons)
    reasons.extend(producer_endurance_reasons)
    if producer_1m != producer_endurance:
        reasons.append("producer stream/batch/compression/host configuration drifted")
    if producer_endurance != dict(expected_producer):
        reasons.append("endurance producer configuration differs from the frozen plan")
    rt_1m, rt_1m_reasons = _rt_signature(capacity_preflight)
    rt_endurance, rt_endurance_reasons = _rt_signature(preflight)
    reasons.extend(rt_1m_reasons)
    reasons.extend(rt_endurance_reasons)
    if rt_1m != rt_endurance:
        reasons.append("RT warehouse query-size/autoscaling configuration drifted")
    if selected_query_size is None:
        reasons.append("no query size was selected by a passing sweep case")
    elif rt_endurance.get("query_size") != selected_query_size:
        reasons.append("endurance RT query size is not the selected query size")
    frozen = {
        "producer": producer_endurance,
        "rt_warehouse": rt_endurance,
        "target_eps": acceptance["target_committed_eps"],
        "dashboard_interval_seconds": acceptance["dashboard_interval_seconds"],
        "drilldown_interval_seconds": acceptance["drilldown_interval_seconds"],
    }
    return (
        _case_result(
            case,
            "failed" if reasons else "passed",
            reasons,
            runtime_seconds=duration,
            accepted_dashboard_iterations=dashboard_count,
            accepted_drilldown_iterations=drilldown_count,
            accepted_freshness_samples=freshness_count,
            producer_resource_headroom=headroom,
            producer_signature=producer_endurance,
            rt_warehouse_signature=rt_endurance,
        ),
        frozen if not reasons else None,
    )


def _validate_plan(plan: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    if plan.get("schema_version") != SCHEMA_VERSION:
        reasons.append("unsupported qualification plan schema_version")
    if plan.get("task") != TASK:
        reasons.append("plan task is not tune-endurance")
    matrix = plan.get("matrix")
    if not isinstance(matrix, Mapping):
        reasons.append("plan matrix is missing")
        return reasons
    stage_cases = {
        stage: value if isinstance(value, list) else []
        for stage in ("correctness", "capacity", "query_size_sweep", "endurance")
        for value in (matrix.get(stage),)
    }
    for stage, values in stage_cases.items():
        if not isinstance(matrix.get(stage), list):
            reasons.append(f"plan matrix stage {stage} is not an array")
    if plan.get("matrix_sha256") != _json_digest(matrix):
        reasons.append("frozen matrix digest does not match")
    actual_ids = {
        case.get("id")
        for stage in ("correctness", "capacity", "endurance")
        for case in stage_cases[stage]
        if isinstance(case, Mapping)
    }
    missing_ids = set(REQUIRED_CASE_IDS) - actual_ids
    if missing_ids:
        reasons.append("required cases are missing: " + ", ".join(sorted(missing_ids)))
    correctness = stage_cases["correctness"]
    if (
        not isinstance(correctness, list)
        or len(correctness) != 1
        or _integer(_nested(correctness[0], "acceptance", "expected_rows"))
        != CORRECTNESS_ROWS
        or _integer(_nested(correctness[0], "acceptance", "target_eps"))
        != CAPACITY_TARGETS[0]
    ):
        reasons.append("correctness matrix is not exact 10M at 100k EPS")
    capacity = stage_cases["capacity"]
    capacity_targets = (
        [
            _integer(_nested(case, "acceptance", "target_committed_eps"))
            for case in capacity
        ]
        if isinstance(capacity, list)
        else []
    )
    if capacity_targets != list(CAPACITY_TARGETS):
        reasons.append("capacity matrix is not ordered 100k, 500k, 1M")
    elif any(
        _integer(_nested(case, "acceptance", "minimum_in_tolerance_non_warmup_intervals"))
        is None
        or int(case["acceptance"]["minimum_in_tolerance_non_warmup_intervals"])
        < MIN_CAPACITY_INTERVALS
        for case in capacity
    ):
        reasons.append("capacity matrix permits fewer than 10 measured intervals")
    query_cases = stage_cases["query_size_sweep"]
    if not query_cases:
        reasons.append("query-size sweep is empty")
    else:
        ordinals = [
            _integer(_nested(case, "acceptance", "candidate_ordinal"))
            for case in query_cases
        ]
        if ordinals != list(range(1, len(query_cases) + 1)):
            reasons.append("query-size candidates are not frozen smallest-to-largest")
        actual_sizes = [
            _nested(case, "acceptance", "query_size") for case in query_cases
        ]
        expected_sizes = _nested(
            plan, "inputs", "query_size_candidates_smallest_to_largest"
        )
        if actual_sizes != expected_sizes:
            reasons.append("query-size cases do not match the ordered input candidates")
        if any(
            _integer(_nested(case, "acceptance", "target_committed_eps"))
            != CAPACITY_TARGETS[-1]
            for case in query_cases
        ):
            reasons.append("query-size sweep does not use one fixed 1M EPS producer")
    endurance = stage_cases["endurance"]
    minimum_endurance = (
        _number(_nested(endurance[0], "acceptance", "minimum_duration_seconds"))
        if isinstance(endurance, list) and len(endurance) == 1
        else None
    )
    maximum_endurance = (
        _number(_nested(endurance[0], "acceptance", "maximum_duration_seconds"))
        if isinstance(endurance, list) and len(endurance) == 1
        else None
    )
    if (
        not isinstance(endurance, list)
        or len(endurance) != 1
        or _integer(_nested(endurance[0], "acceptance", "target_committed_eps"))
        != CAPACITY_TARGETS[-1]
        or minimum_endurance is None
        or minimum_endurance < DEFAULT_ENDURANCE_SECONDS
        or maximum_endurance is None
        or maximum_endurance > MAX_ENDURANCE_SECONDS
        or _integer(_nested(endurance[0], "acceptance", "dashboard_interval_seconds"))
        != 600
        or _integer(_nested(endurance[0], "acceptance", "drilldown_interval_seconds"))
        != 3600
    ):
        reasons.append("endurance matrix is not exact 1M EPS with 600s/3600s schedules")
    cases = [
        case
        for stage in ("correctness", "capacity", "query_size_sweep", "endurance")
        for case in stage_cases[stage]
        if isinstance(case, Mapping)
    ]
    schemas = [_nested(case, "target", "schema") for case in cases]
    run_ids = [case.get("run_id") for case in cases]
    qualification_schema = _nested(plan, "inputs", "base_schema")
    identities_valid = all(
        isinstance(value, str) and bool(value) for value in schemas + run_ids
    )
    if (
        any(case.get("fresh_target_required") is not True for case in cases)
        or not identities_valid
        or any(value != qualification_schema for value in schemas)
        or len(set(str(value) for value in run_ids)) != len(run_ids)
        or _nested(
            plan,
            "execution_rules",
            "shared_dedicated_qualification_schema",
        )
        is not True
        or _nested(plan, "execution_rules", "fresh_tables_per_case") is not True
        or _nested(plan, "execution_rules", "cases_must_run_sequentially")
        is not True
    ):
        reasons.append(
            "cases do not prove one shared qualification schema, fresh sequential "
            "table resets, and distinct run IDs"
        )
    return reasons


def evaluate_plan(plan: Mapping[str, Any]) -> dict[str, Any]:
    """Evaluate all evidence named by *plan* without using the network."""
    plan_reasons = _validate_plan(plan)
    if plan_reasons:
        return {
            "schema_version": SCHEMA_VERSION,
            "task": TASK,
            "state": "failed",
            "qualified": False,
            "reasons": plan_reasons,
            "stages": {},
        }
    matrix = plan["matrix"]
    producer_inputs = _nested(plan, "inputs", "producer")
    producer_inputs = producer_inputs if isinstance(producer_inputs, Mapping) else {}
    expected_producer = {
        "stream_count": producer_inputs.get("stream_count"),
        "batch_size": producer_inputs.get("batch_size"),
        "queue_capacity": producer_inputs.get("queue_capacity"),
        "compression": producer_inputs.get("compression"),
        "host": _nested(plan, "inputs", "workspace_host"),
        "endpoint": _nested(plan, "inputs", "zerobus_endpoint"),
    }
    correctness_case = matrix["correctness"][0]
    correctness = _evaluate_correctness(correctness_case)

    capacity_results = [
        _evaluate_capacity(case, expected_producer) for case in matrix["capacity"]
    ]
    capacity_reasons: list[str] = []
    if all(item["state"] == "passed" for item in capacity_results):
        averages = [item["evidence"]["average_eps"] for item in capacity_results]
        if any(current < previous for previous, current in zip(averages, averages[1:])):
            capacity_reasons.append(
                "capacity average provider-committed EPS is not monotonic"
            )
    capacity_state = (
        "not_run"
        if any(item["state"] == "not_run" for item in capacity_results)
        else "failed"
        if capacity_reasons or any(item["state"] == "failed" for item in capacity_results)
        else "passed"
    )
    capacity = {
        "state": capacity_state,
        "reasons": capacity_reasons,
        "cases": capacity_results,
    }

    query_results = [
        _evaluate_query_size(case, expected_producer)
        for case in matrix["query_size_sweep"]
    ]
    passing_query_results = [
        item for item in query_results if item["state"] == "passed"
    ]
    selected_query_size = (
        min(
            passing_query_results,
            key=lambda item: item["evidence"]["candidate_ordinal"],
        )["evidence"]["query_size"]
        if passing_query_results
        else None
    )
    query_state = (
        "passed"
        if passing_query_results
        else "failed"
        if any(item["state"] == "failed" for item in query_results)
        else "not_run"
    )
    query_stage = {
        "state": query_state,
        "reasons": [] if passing_query_results else ["no query-size candidate passed"],
        "cases": query_results,
    }

    capacity_1m = next(
        case for case in matrix["capacity"] if case["id"] == "capacity-1m"
    )
    endurance, frozen = _evaluate_endurance(
        matrix["endurance"][0],
        capacity_1m,
        selected_query_size,
        expected_producer,
    )
    stages = {
        "correctness": correctness,
        "capacity": capacity,
        "query_size_sweep": query_stage,
        "endurance": endurance,
    }
    qualified = all(
        stage["state"] == "passed"
        for stage in stages.values()
    )
    state = (
        "passed"
        if qualified
        else "failed"
        if any(stage["state"] == "failed" for stage in stages.values())
        else "not_run"
    )
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "task": TASK,
        "plan_matrix_sha256": plan["matrix_sha256"],
        "state": state,
        "qualified": qualified,
        "stages": stages,
        "reasons": (
            []
            if qualified
            else [
                f"{name} is {stage['state']}"
                for name, stage in stages.items()
                if stage["state"] != "passed"
            ]
        ),
    }
    if selected_query_size is not None:
        report["selected_query_size"] = selected_query_size
    if qualified and frozen is not None:
        frozen["producer"]["host_description"] = producer_inputs.get(
            "host_description"
        )
        for name in (
            "network_capacity_gbps",
            "maximum_host_cpu_percent",
            "maximum_network_utilization_percent",
        ):
            frozen["producer"][name] = producer_inputs.get(name)
        report["frozen_settings"] = frozen
    return report


def _env_default(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    return value.strip() if value and value.strip() else default


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Plan or evaluate tune-endurance qualification"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan", help="write a frozen offline qualification plan")
    plan.add_argument("--output-dir", type=Path, required=True)
    plan.add_argument(
        "--repo-dir",
        type=Path,
        default=Path(__file__).resolve().parents[3],
    )
    plan.add_argument(
        "--data-dir",
        type=Path,
        default=Path(value) if (value := _env_default("QUOTES_DATA_DIR")) else None,
        required=_env_default("QUOTES_DATA_DIR") is None,
    )
    plan.add_argument("--catalog", default=_env_default("DATABRICKS_CATALOG", "workspace"))
    plan.add_argument("--schema", default=_env_default("DATABRICKS_SCHEMA", "benchmarking"))
    plan.add_argument("--raw-table", default=_env_default("DATABRICKS_RAW_TABLE", "quotes"))
    plan.add_argument("--mv-table", default=_env_default("DATABRICKS_MV_TABLE", "quotes_daily"))
    plan.add_argument(
        "--rt-warehouse-id",
        default=_env_default("DATABRICKS_RT_WAREHOUSE_ID"),
        required=_env_default("DATABRICKS_RT_WAREHOUSE_ID") is None,
    )
    plan.add_argument(
        "--control-warehouse-id",
        default=_env_default("DATABRICKS_CONTROL_WAREHOUSE_ID"),
        required=_env_default("DATABRICKS_CONTROL_WAREHOUSE_ID") is None,
    )
    plan.add_argument(
        "--producer-host-description",
        "--producer-host",
        dest="producer_host_description",
        default=_env_default("QUALIFICATION_PRODUCER_HOST"),
        required=_env_default("QUALIFICATION_PRODUCER_HOST") is None,
    )
    plan.add_argument(
        "--producer-network-capacity-gbps",
        type=float,
        default=(
            float(value)
            if (value := _env_default("QUALIFICATION_PRODUCER_NETWORK_GBPS"))
            else None
        ),
        required=_env_default("QUALIFICATION_PRODUCER_NETWORK_GBPS") is None,
        help="declared usable producer link capacity for saturation checks",
    )
    plan.add_argument(
        "--host",
        default=_env_default("DATABRICKS_HOST"),
        required=_env_default("DATABRICKS_HOST") is None,
    )
    plan.add_argument(
        "--endpoint",
        "--zerobus-endpoint",
        dest="endpoint",
        default=_env_default("ZEROBUS_ENDPOINT"),
        required=_env_default("ZEROBUS_ENDPOINT") is None,
    )
    plan.add_argument("--stream-count", "--streams", type=int, default=16)
    plan.add_argument("--batch-size", type=int, default=50_000)
    plan.add_argument("--queue-capacity", type=int, default=64)
    plan.add_argument(
        "--compression",
        choices=("NONE", "LZ4_FRAME", "ZSTD"),
        default="NONE",
    )
    plan.add_argument(
        "--query-size-candidates",
        "--query-sizes",
        dest="query_size_candidates",
        nargs="+",
        default=(
            _env_default("DATABRICKS_QUERY_SIZE_CANDIDATES", "").split(",")
            if _env_default("DATABRICKS_QUERY_SIZE_CANDIDATES")
            else None
        ),
        required=_env_default("DATABRICKS_QUERY_SIZE_CANDIDATES") is None,
        metavar="SIZE",
        help="ordered smallest to largest",
    )
    plan.add_argument("--capacity-tolerance", type=float, default=DEFAULT_TOLERANCE)
    plan.add_argument(
        "--max-producer-cpu-percent",
        type=float,
        default=DEFAULT_MAX_PRODUCER_CPU_PERCENT,
    )
    plan.add_argument(
        "--max-producer-network-percent",
        type=float,
        default=DEFAULT_MAX_PRODUCER_NETWORK_PERCENT,
    )
    plan.add_argument(
        "--capacity-duration-seconds",
        type=int,
        default=CAPACITY_DURATION_SECONDS,
    )
    plan.add_argument(
        "--metrics-interval-seconds",
        type=int,
        default=METRICS_INTERVAL_SECONDS,
    )
    plan.add_argument(
        "--endurance-duration-seconds",
        type=int,
        default=DEFAULT_ENDURANCE_SECONDS,
    )
    plan.add_argument("--run-prefix", default="tune_endurance")
    plan.add_argument(
        "--python",
        default=_env_default("DATABRICKS_RUNNER_PYTHON", "python3"),
        help="Python interpreter containing requirements-runner.txt",
    )
    plan.add_argument(
        "--producer-python",
        default=_env_default("DATABRICKS_ZEROBUS_PYTHON"),
        help="Python interpreter containing requirements-zerobus.txt",
    )

    evaluate = commands.add_parser(
        "evaluate", help="evaluate only artifacts referenced by a plan"
    )
    evaluate.add_argument("--plan", type=Path, required=True)
    evaluate.add_argument("--output", type=Path, required=True)
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "plan":
        plan = build_plan(
            output_dir=args.output_dir,
            repo_dir=args.repo_dir,
            data_dir=args.data_dir,
            catalog=args.catalog,
            schema=args.schema,
            raw_table=args.raw_table,
            mv_table=args.mv_table,
            rt_warehouse_id=args.rt_warehouse_id,
            control_warehouse_id=args.control_warehouse_id,
            producer_host_description=args.producer_host_description,
            producer_network_capacity_gbps=args.producer_network_capacity_gbps,
            host=args.host,
            endpoint=args.endpoint,
            stream_count=args.stream_count,
            batch_size=args.batch_size,
            queue_capacity=args.queue_capacity,
            compression=args.compression,
            query_size_candidates=args.query_size_candidates,
            capacity_tolerance=args.capacity_tolerance,
            capacity_duration_seconds=args.capacity_duration_seconds,
            metrics_interval_seconds=args.metrics_interval_seconds,
            max_producer_cpu_percent=args.max_producer_cpu_percent,
            max_producer_network_percent=args.max_producer_network_percent,
            endurance_duration_seconds=args.endurance_duration_seconds,
            run_prefix=args.run_prefix,
            python=args.python,
            producer_python=args.producer_python,
        )
        plan_path, command_path = write_plan(plan, args.output_dir)
        print(f"Qualification plan written to {plan_path}")
        print(f"Generated command file written to {command_path}")
        print("State: planned (no online stages were run)")
        return 0

    plan_path = args.plan.expanduser().resolve()
    plan, error = _read_json(plan_path, "qualification plan")
    if error or plan is None:
        report = {
            "schema_version": SCHEMA_VERSION,
            "task": TASK,
            "state": "failed",
            "qualified": False,
            "reasons": [error or "qualification plan is unavailable"],
            "stages": {},
        }
    else:
        report = evaluate_plan(plan)
    output = args.output.expanduser().resolve()
    if output.exists() and output.is_dir():
        output = output / "qualification_report.json"
    atomic_write_json(output, report)
    print(f"Qualification report written to {output}")
    print(f"State: {report['state']}; qualified={str(report['qualified']).lower()}")
    return 0 if report["qualified"] else 1


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return run(argv)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"FATAL: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
