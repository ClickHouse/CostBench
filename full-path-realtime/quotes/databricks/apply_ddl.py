#!/usr/bin/env python3
"""Render and apply the checked-in Databricks benchmark DDL, fail closed."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
import tempfile
import urllib.parse
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from dbx_common import (
    DatabricksRestClient,
    atomic_write_json,
    iso_utc,
    normalize_host,
    redact_secrets,
    render_sql,
    split_sql_statements,
)

SCRIPT_DIR = Path(__file__).resolve().parent
DDL_PATH = SCRIPT_DIR / "create.sql"
RENDERED_SQL_NAME = "create.rendered.sql"
REPORT_NAME = "apply_ddl_report.json"
SAFE_COMPONENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
FORBIDDEN_DDL_RE = (
    re.compile(
        r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:CATALOG|SCHEMA|DATABASE)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?"
        r"(?:EXTERNAL\s+LOCATION|STORAGE\s+CREDENTIAL)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\bMANAGED\s+LOCATION\b", re.IGNORECASE),
)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _atomic_write_new_text(path: Path, value: str) -> None:
    """Publish one new UTF-8 file atomically without permitting replacement."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(tmp_name, path)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass


def _validate_component(label: str, value: str) -> None:
    if not SAFE_COMPONENT_RE.fullmatch(value):
        raise ValueError(
            f"{label} must contain only ASCII letters, digits, '_' or '-', "
            "and must start with a letter or digit"
        )


def prepare_ddl(
    catalog: str,
    schema: str,
    *,
    ddl_path: Path = DDL_PATH,
) -> tuple[bytes, str, list[str]]:
    """Load, render, split, and safety-check the complete checked-in DDL."""
    _validate_component("catalog", catalog)
    _validate_component("schema", schema)
    source = ddl_path.read_bytes()
    rendered = render_sql(source.decode("utf-8"), catalog, schema)
    statements = split_sql_statements(rendered)
    if not statements:
        raise ValueError("rendered create.sql contains no SQL statements")
    for index, statement in enumerate(statements, start=1):
        if any(pattern.search(statement) for pattern in FORBIDDEN_DDL_RE):
            raise ValueError(
                f"statement {index} attempts catalog, schema, or managed-storage "
                "administration; those are prerequisites and are not applied here"
            )
    return source, rendered, statements


def _statement_record(index: int, statement: str) -> dict[str, Any]:
    return {
        "index": index,
        "sha256": sha256_bytes(statement.encode("utf-8")),
        "status": "not_run",
        "statement_id": None,
        "error": None,
        "started_at": None,
        "finished_at": None,
    }


def apply_ddl(
    *,
    host: str,
    catalog: str,
    schema: str,
    control_warehouse_id: str,
    confirm_destructive_target: str,
    output_dir: Path,
    token: str | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
    statement_timeout: float = 300.0,
    poll_interval: float = 0.5,
    network_timeout: float = 30.0,
    ddl_path: Path = DDL_PATH,
    client_factory: Callable[..., Any] = DatabricksRestClient,
) -> dict[str, Any]:
    """Apply every rendered statement and persist an auditable report."""
    target = f"{catalog}.{schema}"
    if confirm_destructive_target != target:
        raise ValueError(
            "--confirm-destructive-target must exactly equal "
            f"{target!r}; received {confirm_destructive_target!r}"
        )
    normalized_host = normalize_host(host)
    parsed_host = urllib.parse.urlparse(normalized_host or "")
    if (
        not normalized_host
        or parsed_host.scheme != "https"
        or not parsed_host.hostname
        or parsed_host.username
        or parsed_host.password
        or parsed_host.query
        or parsed_host.fragment
        or parsed_host.path not in ("", "/")
    ):
        raise ValueError("host must be an HTTPS workspace origin without credentials or a path")
    if not control_warehouse_id or not SAFE_COMPONENT_RE.fullmatch(control_warehouse_id):
        raise ValueError("control warehouse ID is missing or contains unsupported characters")
    if bool(client_id) != bool(client_secret):
        raise ValueError("workspace OAuth client ID and secret must be supplied together")
    if bool(token) == bool(client_id and client_secret):
        raise ValueError("configure exactly one workspace PAT or OAuth M2M credential")
    if statement_timeout <= 0 or poll_interval <= 0 or network_timeout <= 0:
        raise ValueError("timeouts and poll interval must be greater than zero")

    source, rendered, statements = prepare_ddl(catalog, schema, ddl_path=ddl_path)
    output_dir = output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=False)
    rendered_path = output_dir / RENDERED_SQL_NAME
    report_path = output_dir / REPORT_NAME
    _atomic_write_new_text(rendered_path, rendered)

    secrets = tuple(value for value in (token, client_secret) if value)
    records = [
        _statement_record(index, statement)
        for index, statement in enumerate(statements, start=1)
    ]
    report: dict[str, Any] = {
        "schema_version": 1,
        "tool": "apply_ddl",
        "status": "running",
        "started_at": iso_utc(),
        "finished_at": None,
        "target": target,
        "control_warehouse_id": control_warehouse_id,
        "authentication_mode": "pat" if token else "oauth-client-credentials",
        "input": {
            "path": str(ddl_path.resolve()),
            "sha256": sha256_bytes(source),
        },
        "rendered_sql": {
            "path": str(rendered_path),
            "sha256": sha256_bytes(rendered.encode("utf-8")),
        },
        "statement_count": len(statements),
        "statements": records,
        "errors": [],
    }
    atomic_write_json(report_path, redact_secrets(report, secrets))

    try:
        client = client_factory(
            normalized_host,
            token=token,
            client_id=client_id,
            client_secret=client_secret,
            timeout=network_timeout,
        )
    except Exception as exc:
        error = redact_secrets(f"{type(exc).__name__}: {exc}", secrets)
        report["status"] = "failed"
        report["finished_at"] = iso_utc()
        report["errors"].append(error)
        atomic_write_json(report_path, redact_secrets(report, secrets))
        return redact_secrets(report, secrets)

    for record, statement in zip(records, statements):
        record["status"] = "running"
        record["started_at"] = iso_utc()
        atomic_write_json(report_path, redact_secrets(report, secrets))
        statement_tags: Mapping[str, str] = {
            "benchmark": "quotes-full-path",
            "operation": "apply-ddl",
            "target": target,
            "statement_index": str(record["index"]),
            "statement_sha256": str(record["sha256"]),
        }
        try:
            response = client.execute_statement(
                statement,
                control_warehouse_id,
                timeout=statement_timeout,
                poll_interval=poll_interval,
                catalog=catalog,
                schema=schema,
                query_tags=statement_tags,
            )
            statement_id = response.get("statement_id")
            if not isinstance(statement_id, str) or not statement_id:
                raise RuntimeError("Statement Execution response omitted statement_id")
            record["statement_id"] = statement_id
            record["status"] = "succeeded"
        except Exception as exc:
            error = redact_secrets(f"{type(exc).__name__}: {exc}", secrets)
            record["status"] = "failed"
            record["error"] = error
            report["errors"].append(
                {
                    "statement_index": record["index"],
                    "error": error,
                }
            )
            report["status"] = "failed"
            record["finished_at"] = iso_utc()
            report["finished_at"] = iso_utc()
            atomic_write_json(report_path, redact_secrets(report, secrets))
            return redact_secrets(report, secrets)
        record["finished_at"] = iso_utc()
        atomic_write_json(report_path, redact_secrets(report, secrets))

    report["status"] = "succeeded"
    report["finished_at"] = iso_utc()
    atomic_write_json(report_path, redact_secrets(report, secrets))
    return redact_secrets(report, secrets)


def build_parser(env: Mapping[str, str] | None = None) -> argparse.ArgumentParser:
    source = os.environ if env is None else env
    parser = argparse.ArgumentParser(
        description="Render and destructively apply the checked-in benchmark create.sql"
    )
    parser.add_argument("--host", default=source.get("DATABRICKS_HOST"))
    parser.add_argument(
        "--control-warehouse-id",
        default=source.get("DATABRICKS_CONTROL_WAREHOUSE_ID"),
    )
    parser.add_argument("--catalog", default=source.get("DATABRICKS_CATALOG"))
    parser.add_argument("--schema", default=source.get("DATABRICKS_SCHEMA"))
    parser.add_argument("--confirm-destructive-target", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--statement-timeout", type=float, default=300.0)
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("--network-timeout", type=float, default=30.0)
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    for name in ("host", "control_warehouse_id", "catalog", "schema"):
        if not getattr(args, name):
            parser.error(
                f"--{name.replace('_', '-')} or its corresponding environment "
                "variable is required"
            )
    token = os.environ.get("DATABRICKS_TOKEN", "").strip() or None
    client_id = os.environ.get("DATABRICKS_CLIENT_ID", "").strip() or None
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "").strip() or None
    try:
        report = apply_ddl(
            host=args.host,
            catalog=args.catalog,
            schema=args.schema,
            control_warehouse_id=args.control_warehouse_id,
            confirm_destructive_target=args.confirm_destructive_target,
            output_dir=args.output_dir,
            token=token,
            client_id=client_id,
            client_secret=client_secret,
            statement_timeout=args.statement_timeout,
            poll_interval=args.poll_interval,
            network_timeout=args.network_timeout,
        )
    except (OSError, UnicodeError, ValueError) as exc:
        safe = redact_secrets(str(exc), (token, client_secret))
        parser.error(safe)
    print(f"DDL status: {report['status']}")
    print(f"Artifacts: {args.output_dir.expanduser().resolve()}")
    return 0 if report["status"] == "succeeded" else 1


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return run(argv)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        secrets = (
            os.environ.get("DATABRICKS_TOKEN"),
            os.environ.get("DATABRICKS_CLIENT_SECRET"),
        )
        safe = redact_secrets(f"{type(exc).__name__}: {exc}", secrets)
        print(f"FATAL: {safe}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
