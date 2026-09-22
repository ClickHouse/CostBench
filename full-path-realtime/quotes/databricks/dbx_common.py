#!/usr/bin/env python3
"""Standard-library helpers shared by the Databricks benchmark tools."""

from __future__ import annotations

import base64
import json
import os
import re
import socket
import ssl
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

SQL_PLACEHOLDERS = (
    "__CATALOG__",
    "__SCHEMA__",
    "__RAW_TABLE__",
    "__MV_TABLE__",
)
TERMINAL_STATEMENT_STATES = {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}
SECRET_KEY_RE = re.compile(
    r"(?:authorization|client[_-]?secret|password|private[_-]?key|access[_-]?token|^token$)",
    re.IGNORECASE,
)
BEARER_RE = re.compile(r"(?i)\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+")
PAT_RE = re.compile(r"\bdapi[A-Za-z0-9_-]{8,}\b")
SAFE_COMPONENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
WORKSPACE_ID_RE = re.compile(r"^[0-9]+$")
WAREHOUSE_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")

ENV_ALIASES: dict[str, tuple[str, ...]] = {
    "host": (
        "DATABRICKS_HOST",
        "DATABRICKS_SERVER_HOSTNAME",
        "DATABRICKS_WORKSPACE_URL",
        "WORKSPACE_URL",
    ),
    "cloud": ("DATABRICKS_CLOUD", "DBX_CLOUD", "CLOUD"),
    "region": ("DATABRICKS_REGION", "DBX_REGION", "REGION"),
    "workspace_id": ("DATABRICKS_WORKSPACE_ID", "WORKSPACE_ID"),
    "catalog": ("DATABRICKS_CATALOG", "DBX_CATALOG", "CATALOG"),
    "schema": ("DATABRICKS_SCHEMA", "DBX_SCHEMA", "SCHEMA"),
    "raw_table": ("DATABRICKS_RAW_TABLE", "DBX_RAW_TABLE", "RAW_TABLE"),
    "mv_table": ("DATABRICKS_MV_TABLE", "DBX_MV_TABLE", "MV_TABLE"),
    "rt_warehouse_id": (
        "DATABRICKS_RT_WAREHOUSE_ID",
        "DBX_RT_WAREHOUSE_ID",
        "RT_WAREHOUSE_ID",
    ),
    "control_warehouse_id": (
        "DATABRICKS_CONTROL_WAREHOUSE_ID",
        "DBX_CONTROL_WAREHOUSE_ID",
        "CONTROL_WAREHOUSE_ID",
    ),
    "zerobus_endpoint": (
        "ZEROBUS_ENDPOINT",
        "ZEROBUS_SERVER_ENDPOINT",
        "DATABRICKS_ZEROBUS_ENDPOINT",
    ),
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime | None = None) -> str:
    value = utc_now() if value is None else value
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
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "items"):
        return dict(value)
    return str(value)


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    finally:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass


def atomic_write_json(path: Path, payload: Any) -> None:
    """Replace *path* atomically with one stable, newline-terminated JSON value."""
    rendered = json.dumps(payload, default=json_default, indent=2, sort_keys=True) + "\n"
    _atomic_write(path, rendered.encode("utf-8"))


def atomic_append_jsonl(path: Path, payload: Any) -> None:
    """Append one JSONL record with one O_APPEND write and an fsync."""
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = (
        json.dumps(payload, default=json_default, separators=(",", ":"), sort_keys=False) + "\n"
    ).encode("utf-8")
    fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o644)
    try:
        written = os.write(fd, rendered)
        if written != len(rendered):
            raise OSError(f"short JSONL write: wrote {written} of {len(rendered)} bytes")
        os.fsync(fd)
    finally:
        os.close(fd)


append_jsonl = atomic_append_jsonl


def redact_secrets(value: Any, secrets: Iterable[str | None] = ()) -> Any:
    """Return a report-safe copy of nested data or a redacted string."""
    known = tuple(secret for secret in secrets if secret)
    if isinstance(value, Mapping):
        return {
            str(key): (
                "***REDACTED***"
                if SECRET_KEY_RE.search(str(key))
                else redact_secrets(item, known)
            )
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [redact_secrets(item, known) for item in value]
    if not isinstance(value, str):
        return value
    redacted = value
    for secret in sorted(known, key=len, reverse=True):
        redacted = redacted.replace(secret, "***REDACTED***")
    redacted = BEARER_RE.sub(lambda match: f"{match.group(1)} ***REDACTED***", redacted)
    return PAT_RE.sub("***REDACTED***", redacted)


def quote_identifier(value: str) -> str:
    if not value or "\x00" in value:
        raise ValueError("SQL identifier must be non-empty and contain no NUL")
    return f"`{value.replace('`', '``')}`"


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def render_sql(
    sql: str,
    catalog: str,
    schema: str,
    *,
    raw_table: str = "quotes",
    mv_table: str = "quotes_daily",
) -> str:
    rendered = sql.replace("__CATALOG__", quote_identifier(catalog))
    rendered = rendered.replace("__SCHEMA__", quote_identifier(schema))
    rendered = rendered.replace("__RAW_TABLE__", quote_identifier(raw_table))
    rendered = rendered.replace("__MV_TABLE__", quote_identifier(mv_table))
    unresolved = [token for token in SQL_PLACEHOLDERS if token in rendered]
    if unresolved:
        raise ValueError(f"unresolved SQL placeholders: {', '.join(unresolved)}")
    return rendered


def split_sql_statements(sql: str) -> list[str]:
    """Split SQL outside strings, identifiers, and line/block comments.

    Ordinary comments are removed. Databricks optimizer hints (``/*+ ... */``)
    are retained because removing them can change query behavior.
    """

    statements: list[str] = []
    current: list[str] = []
    quote: str | None = None
    block_depth = 0
    keep_block = False
    i = 0

    while i < len(sql):
        char = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""

        if block_depth:
            if char == "/" and nxt == "*":
                block_depth += 1
                if keep_block:
                    current.extend((char, nxt))
                i += 2
                continue
            if char == "*" and nxt == "/":
                block_depth -= 1
                if keep_block:
                    current.extend((char, nxt))
                i += 2
                if block_depth == 0:
                    was_kept = keep_block
                    keep_block = False
                    if not was_kept:
                        current.append(" ")
                continue
            if keep_block:
                current.append(char)
            elif char in "\r\n":
                current.append(char)
            i += 1
            continue

        if quote is None and char == "-" and nxt == "-":
            i += 2
            while i < len(sql) and sql[i] not in "\r\n":
                i += 1
            current.append("\n")
            continue

        if quote is None and char == "/" and nxt == "*":
            keep_block = i + 2 < len(sql) and sql[i + 2] == "+"
            block_depth = 1
            if keep_block:
                current.extend((char, nxt))
            else:
                current.append(" ")
            i += 2
            continue

        if quote is None and char in ("'", '"', "`"):
            quote = char
            current.append(char)
            i += 1
            continue

        if quote is not None:
            current.append(char)
            if char == quote:
                if nxt == quote:
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
    if block_depth:
        raise ValueError("unterminated SQL block comment")
    statement = "".join(current).strip()
    if statement:
        statements.append(statement)
    return statements


def load_queries(
    path: Path,
    catalog: str,
    schema: str,
    *,
    raw_table: str = "quotes",
    mv_table: str = "quotes_daily",
) -> list[str]:
    return split_sql_statements(
        render_sql(
            path.read_text(encoding="utf-8"),
            catalog,
            schema,
            raw_table=raw_table,
            mv_table=mv_table,
        )
    )


def _first_env(env: Mapping[str, str], names: Sequence[str]) -> str | None:
    for name in names:
        value = env.get(name)
        if value is not None and value.strip():
            return value.strip()
    return None


def _warehouse_from_http_path(env: Mapping[str, str]) -> str | None:
    path = env.get("DATABRICKS_HTTP_PATH", "").strip().rstrip("/")
    marker = "/sql/1.0/warehouses/"
    if marker not in path:
        return None
    return path.rsplit("/", 1)[-1] or None


def normalize_host(value: str | None) -> str | None:
    if value is None or not value.strip():
        return None
    host = value.strip()
    if "://" not in host:
        host = f"https://{host}"
    return host.rstrip("/")


def infer_cloud(host: str | None) -> str | None:
    hostname = (urllib.parse.urlparse(host).hostname or "").lower() if host else ""
    if hostname.endswith(".azuredatabricks.net"):
        return "azure"
    if hostname.endswith(".gcp.databricks.com"):
        return "gcp"
    if hostname.endswith(".cloud.databricks.com"):
        return "aws"
    return None


def normalize_cloud(value: str | None) -> str | None:
    if value is None or not value.strip():
        return None
    aliases = {
        "aws": "aws",
        "amazon": "aws",
        "azure": "azure",
        "az": "azure",
        "gcp": "gcp",
        "google": "gcp",
        "google-cloud": "gcp",
    }
    return aliases.get(value.strip().lower(), value.strip().lower())


@dataclass(frozen=True)
class EnvironmentConfig:
    host: str | None
    cloud: str | None
    region: str | None
    workspace_id: str | None
    catalog: str
    schema: str
    raw_table: str
    mv_table: str
    rt_warehouse_id: str | None
    control_warehouse_id: str | None
    zerobus_endpoint: str | None

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        **overrides: str | None,
    ) -> "EnvironmentConfig":
        source = os.environ if env is None else env
        values = {name: _first_env(source, aliases) for name, aliases in ENV_ALIASES.items()}
        for name, value in overrides.items():
            if name not in values:
                raise TypeError(f"unknown configuration override: {name}")
            if value is not None:
                values[name] = value.strip() if isinstance(value, str) else value

        values["host"] = normalize_host(values["host"])
        values["cloud"] = normalize_cloud(values["cloud"]) or infer_cloud(values["host"])
        values["catalog"] = values["catalog"] or "workspace"
        values["schema"] = values["schema"] or "benchmarking"
        values["raw_table"] = values["raw_table"] or "quotes"
        values["mv_table"] = values["mv_table"] or "quotes_daily"
        values["zerobus_endpoint"] = normalize_host(values["zerobus_endpoint"])
        if not values["rt_warehouse_id"]:
            values["rt_warehouse_id"] = _warehouse_from_http_path(source)
        if (
            not values["zerobus_endpoint"]
            and values["workspace_id"]
            and values["region"]
            and values["cloud"]
        ):
            try:
                values["zerobus_endpoint"] = construct_zerobus_endpoint(
                    values["workspace_id"], values["region"], values["cloud"]
                )
            except ValueError:
                pass
        return cls(**values)  # type: ignore[arg-type]

    def as_report_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def raw_full_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.raw_table}"

    @property
    def mv_full_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.mv_table}"


def construct_zerobus_endpoint(workspace_id: str, region: str, cloud: str) -> str:
    cloud = normalize_cloud(cloud)
    if not WORKSPACE_ID_RE.fullmatch(workspace_id):
        raise ValueError("workspace ID must contain decimal digits only")
    if not SAFE_COMPONENT_RE.fullmatch(region):
        raise ValueError("region contains unsupported characters")
    suffixes = {
        "aws": "cloud.databricks.com",
        "azure": "azuredatabricks.net",
        "gcp": "gcp.databricks.com",
    }
    try:
        suffix = suffixes[cloud]
    except KeyError as exc:
        raise ValueError(f"unsupported cloud for Zerobus endpoint: {cloud!r}") from exc
    return f"https://{workspace_id}.zerobus.{region}.{suffix}"


def zerobus_endpoint_errors(
    endpoint: str,
    *,
    workspace_id: str | None = None,
    region: str | None = None,
    cloud: str | None = None,
) -> list[str]:
    errors: list[str] = []
    parsed = urllib.parse.urlparse(endpoint)
    if parsed.scheme != "https":
        errors.append("Zerobus endpoint must use https")
    if not parsed.hostname:
        errors.append("Zerobus endpoint must include a hostname")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        errors.append("Zerobus endpoint must not contain credentials, query, or fragment")
    if parsed.path not in ("", "/"):
        errors.append("Zerobus endpoint must not contain a path")
    try:
        port = parsed.port
    except ValueError:
        errors.append("Zerobus endpoint contains an invalid port")
    else:
        if port not in (None, 443):
            errors.append("Zerobus endpoint port must be 443 when specified")
    if workspace_id and region and cloud:
        try:
            expected = urllib.parse.urlparse(
                construct_zerobus_endpoint(workspace_id, region, cloud)
            ).hostname
            if parsed.hostname and parsed.hostname.lower() != expected:
                errors.append(f"Zerobus endpoint hostname does not match {cloud} configuration")
        except ValueError as exc:
            errors.append(str(exc))
    return errors


def validate_config(config: EnvironmentConfig, *, online: bool = False) -> dict[str, list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    parsed = urllib.parse.urlparse(config.host) if config.host else None

    if config.host:
        if parsed is None or parsed.scheme != "https" or not parsed.hostname:
            errors.append("Databricks host must be an https origin")
        elif (
            parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in ("", "/")
        ):
            errors.append("Databricks host must not contain credentials, query, fragment, or path")
    elif online:
        errors.append("Databricks host is required online")

    if config.cloud and config.cloud not in {"aws", "azure", "gcp"}:
        errors.append("cloud must be one of: aws, azure, gcp")
    elif online and not config.cloud:
        errors.append("cloud is required online")
    if config.cloud and config.host:
        inferred = infer_cloud(config.host)
        if inferred and inferred != config.cloud:
            errors.append("cloud does not match the Databricks host suffix")

    if config.workspace_id and not WORKSPACE_ID_RE.fullmatch(config.workspace_id):
        errors.append("workspace ID must contain decimal digits only")
    elif online and not config.workspace_id:
        errors.append("workspace ID is required online")
    if config.region and not SAFE_COMPONENT_RE.fullmatch(config.region):
        errors.append("region contains unsupported characters")
    elif online and not config.region:
        errors.append("region is required online")

    for label, value in (
        ("catalog", config.catalog),
        ("schema", config.schema),
        ("raw table", config.raw_table),
        ("MV table", config.mv_table),
    ):
        if not SAFE_COMPONENT_RE.fullmatch(value):
            errors.append(f"{label} must contain only ASCII letters, digits, '_' or '-'")
    if not re.fullmatch(r"[A-Za-z0-9_]+", config.raw_table):
        errors.append("raw table must contain only ASCII letters, digits, and '_' for Zerobus")

    for label, value in (
        ("Lakehouse//RT warehouse ID", config.rt_warehouse_id),
        ("control warehouse ID", config.control_warehouse_id),
    ):
        if value and not WAREHOUSE_ID_RE.fullmatch(value):
            errors.append(f"{label} contains unsupported characters")
        elif online and not value:
            errors.append(f"{label} is required online")

    if config.zerobus_endpoint:
        errors.extend(
            zerobus_endpoint_errors(
                config.zerobus_endpoint,
                workspace_id=config.workspace_id,
                region=config.region,
                cloud=config.cloud,
            )
        )
    elif online:
        errors.append(
            "Zerobus endpoint is required online (or supply workspace ID, region, and cloud)"
        )
    elif not (config.workspace_id and config.region and config.cloud):
        warnings.append(
            "Zerobus endpoint cannot be constructed until workspace ID, region, and cloud are set"
        )
    return {"errors": errors, "warnings": warnings}


class DatabricksApiError(RuntimeError):
    def __init__(
        self,
        status: int | None,
        method: str,
        path: str,
        detail: Any,
        *,
        secrets: Iterable[str | None] = (),
    ):
        self.status = status
        self.method = method
        self.path = path
        self.detail = redact_secrets(detail, secrets)
        prefix = f"Databricks API {method} {path}"
        if status is not None:
            prefix += f" returned HTTP {status}"
        super().__init__(f"{prefix}: {self.detail}")


class DatabricksRestClient:
    """Small Databricks REST client supporting PAT and OAuth M2M."""

    def __init__(
        self,
        host: str,
        *,
        token: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        timeout: float = 30.0,
        opener: Any = None,
    ):
        normalized = normalize_host(host)
        if normalized is None:
            raise ValueError("Databricks host is required")
        if bool(client_id) != bool(client_secret):
            raise ValueError("OAuth client ID and client secret must be supplied together")
        if not token and not (client_id and client_secret):
            raise ValueError("DATABRICKS_TOKEN or OAuth client credentials are required")
        self.host = normalized
        self._pat = token
        self._client_id = client_id
        self._client_secret = client_secret
        self.timeout = timeout
        self._opener = opener or urllib.request.urlopen
        self._oauth_access_token: str | None = None
        self._oauth_expires_at = 0.0

    @property
    def auth_mode(self) -> str:
        return "pat" if self._pat else "oauth-client-credentials"

    def _known_secrets(self) -> tuple[str | None, ...]:
        return (self._pat, self._client_secret, self._oauth_access_token)

    def _open_json(self, request: urllib.request.Request) -> dict[str, Any]:
        try:
            with self._opener(request, timeout=self.timeout) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                detail: Any = json.loads(raw.decode("utf-8", errors="replace"))
            except json.JSONDecodeError:
                detail = raw.decode("utf-8", errors="replace")[:4096]
            path = urllib.parse.urlsplit(request.full_url).path
            raise DatabricksApiError(
                exc.code,
                request.method,
                path,
                detail,
                secrets=self._known_secrets(),
            ) from None
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            path = urllib.parse.urlsplit(request.full_url).path
            raise DatabricksApiError(
                None,
                request.method,
                path,
                str(exc),
                secrets=self._known_secrets(),
            ) from None
        if not raw:
            return {}
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError as exc:
            path = urllib.parse.urlsplit(request.full_url).path
            raise DatabricksApiError(
                None,
                request.method,
                path,
                "non-JSON response",
                secrets=self._known_secrets(),
            ) from exc
        if not isinstance(decoded, dict):
            raise DatabricksApiError(
                None,
                request.method,
                urllib.parse.urlsplit(request.full_url).path,
                "non-object JSON response",
                secrets=self._known_secrets(),
            )
        return decoded

    def oauth_token(
        self,
        *,
        resource: str | None = None,
        authorization_details: Sequence[Mapping[str, Any]] | None = None,
        cache: bool = False,
    ) -> str:
        if not self._client_id or not self._client_secret:
            raise ValueError("OAuth client credentials are required")
        if cache and self._oauth_access_token and time.monotonic() < self._oauth_expires_at:
            return self._oauth_access_token
        form: dict[str, str] = {"grant_type": "client_credentials", "scope": "all-apis"}
        if resource:
            form["resource"] = resource
        if authorization_details:
            form["authorization_details"] = json.dumps(
                authorization_details, separators=(",", ":")
            )
        basic = base64.b64encode(
            f"{self._client_id}:{self._client_secret}".encode("utf-8")
        ).decode("ascii")
        request = urllib.request.Request(
            f"{self.host}/oidc/v1/token",
            data=urllib.parse.urlencode(form).encode("ascii"),
            headers={
                "Accept": "application/json",
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            method="POST",
        )
        response = self._open_json(request)
        access_token = response.get("access_token")
        if not isinstance(access_token, str) or not access_token:
            raise DatabricksApiError(
                None,
                "POST",
                "/oidc/v1/token",
                "response omitted access_token",
                secrets=self._known_secrets(),
            )
        if cache and not resource and not authorization_details:
            self._oauth_access_token = access_token
            expires_in = max(0, int(response.get("expires_in", 3600)) - 60)
            self._oauth_expires_at = time.monotonic() + expires_in
        return access_token

    def _authorization(self) -> str:
        token = self._pat or self.oauth_token(cache=True)
        return f"Bearer {token}"

    def request(
        self,
        method: str,
        path: str,
        *,
        payload: Mapping[str, Any] | None = None,
        query: Mapping[str, Any] | Sequence[tuple[str, Any]] | None = None,
    ) -> dict[str, Any]:
        if not path.startswith("/"):
            raise ValueError("REST path must start with '/'")
        url = f"{self.host}{path}"
        if query:
            url += "?" + urllib.parse.urlencode(query, doseq=True)
        data = None
        headers = {"Accept": "application/json", "Authorization": self._authorization()}
        if payload is not None:
            data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
        return self._open_json(request)

    def get_warehouse(self, warehouse_id: str) -> dict[str, Any]:
        safe_id = urllib.parse.quote(warehouse_id, safe="")
        return self.request("GET", f"/api/2.0/sql/warehouses/{safe_id}")

    def execute_statement(
        self,
        statement: str,
        warehouse_id: str,
        *,
        timeout: float = 120.0,
        poll_interval: float = 0.5,
        catalog: str | None = None,
        schema: str | None = None,
        query_tags: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "statement": statement,
            "warehouse_id": warehouse_id,
            "wait_timeout": "0s",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        }
        if catalog:
            payload["catalog"] = catalog
        if schema:
            payload["schema"] = schema
        if query_tags:
            payload["query_tags"] = [
                {"key": str(key), "value": str(value)}
                for key, value in query_tags.items()
            ]
        response = self.request("POST", "/api/2.0/sql/statements", payload=payload)
        statement_id = response.get("statement_id")
        if not isinstance(statement_id, str) or not statement_id:
            raise DatabricksApiError(
                None,
                "POST",
                "/api/2.0/sql/statements",
                "response omitted statement_id",
                secrets=self._known_secrets(),
            )
        deadline = time.monotonic() + timeout
        while True:
            state = str(response.get("status", {}).get("state", "")).upper()
            if state in TERMINAL_STATEMENT_STATES:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"statement {statement_id} did not finish within {timeout:g} seconds"
                )
            time.sleep(max(0.05, poll_interval))
            response = self.request(
                "GET",
                f"/api/2.0/sql/statements/{urllib.parse.quote(statement_id, safe='')}",
            )
        if state != "SUCCEEDED":
            detail = response.get("status", {}).get("error") or f"terminal state {state}"
            raise DatabricksApiError(
                None,
                "GET",
                f"/api/2.0/sql/statements/{statement_id}",
                detail,
                secrets=self._known_secrets(),
            )
        return response

    def query_history_by_statement_id(self, statement_id: str) -> dict[str, Any] | None:
        response = self.request(
            "GET",
            "/api/2.0/sql/history/queries",
            query=[
                ("filter_by.statement_ids", statement_id),
                ("include_metrics", "true"),
                ("max_results", "1"),
            ],
        )
        records = response.get("res")
        if records is None:
            records = response.get("queries")
        if not isinstance(records, list):
            return None
        return next(
            (
                item
                for item in records
                if isinstance(item, dict)
                and (
                    item.get("statement_id") == statement_id
                    or item.get("query_id") == statement_id
                )
            ),
            None,
        )


def _statement_columns(response: Mapping[str, Any]) -> tuple[list[str], list[dict[str, Any]]]:
    raw_columns = response.get("manifest", {}).get("schema", {}).get("columns", [])
    columns = (
        [dict(column) for column in raw_columns if isinstance(column, Mapping)]
        if isinstance(raw_columns, list)
        else []
    )
    names = [
        str(column.get("name", f"column_{index}"))
        for index, column in enumerate(columns)
    ]
    return names, columns


def _rows_from_data(
    data: Any,
    names: Sequence[str],
    *,
    strict_schema: bool = False,
) -> list[dict[str, Any]]:
    if not isinstance(data, list):
        raise ValueError("Statement result data_array is not an array")
    rows: list[dict[str, Any]] = []
    for raw_row in data:
        if not isinstance(raw_row, list):
            raise ValueError("Statement result contains a non-array row")
        if strict_schema and len(raw_row) != len(names):
            raise RuntimeError(
                "Statement result row does not match the manifest schema "
                f"({len(raw_row)} values for {len(names)} columns)"
            )
        rows.append(
            {
                names[index] if index < len(names) else f"column_{index}": value
                for index, value in enumerate(raw_row)
            }
        )
    return rows


def statement_rows(response: Mapping[str, Any]) -> list[dict[str, Any]]:
    names, _columns = _statement_columns(response)
    result = response.get("result")
    data = result.get("data_array", []) if isinstance(result, Mapping) else []
    if not isinstance(data, list):
        return []
    rows: list[dict[str, Any]] = []
    for raw_row in data:
        if isinstance(raw_row, list):
            rows.extend(_rows_from_data([raw_row], names))
    return rows


def _bounded_positive_int(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _workspace_chunk_path(
    host: str,
    link: Any,
    *,
    statement_id: str | None = None,
) -> str:
    if not isinstance(link, str) or not link:
        raise ValueError("Statement result next_chunk_internal_link is not a URL")
    parsed = urllib.parse.urlsplit(link)
    workspace = urllib.parse.urlsplit(host)
    if parsed.fragment or parsed.username or parsed.password:
        raise ValueError("Statement result chunk link contains unsupported URL components")
    if parsed.scheme or parsed.netloc:
        if (
            parsed.scheme.lower() != workspace.scheme.lower()
            or parsed.netloc.lower() != workspace.netloc.lower()
        ):
            raise ValueError("Statement result chunk link is outside the workspace origin")
    elif not parsed.path.startswith("/"):
        raise ValueError("Statement result chunk link must be a workspace-origin path")
    prefix = "/api/2.0/sql/statements/"
    if not parsed.path.startswith(prefix):
        raise ValueError("Statement result chunk link is not a Statement API path")
    remainder = parsed.path[len(prefix) :]
    linked_statement_id, separator, linked_resource = remainder.partition("/")
    resource_parts = linked_resource.split("/")
    if (
        separator != "/"
        or len(resource_parts) != 3
        or resource_parts[:2] != ["result", "chunks"]
        or not resource_parts[2]
        or resource_parts[2] in {".", ".."}
    ):
        raise ValueError("Statement result chunk link is not a result-chunk path")
    if (
        statement_id is not None
        and urllib.parse.unquote(linked_statement_id) != statement_id
    ):
        raise ValueError("Statement result chunk link refers to another statement")
    return parsed.path + (f"?{parsed.query}" if parsed.query else "")


def bounded_statement_rows(
    client: DatabricksRestClient,
    response: Mapping[str, Any],
    *,
    max_rows: int,
    max_bytes: int,
    max_chunks: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Retrieve every inline Statement result chunk within strict local limits.

    Databricks can return a successful INLINE result over multiple workspace-local
    chunks. This helper follows only same-workspace Statement API links and fails
    instead of returning a partial or provider-truncated result.
    """

    row_limit = _bounded_positive_int("max_rows", max_rows)
    byte_limit = _bounded_positive_int("max_bytes", max_bytes)
    chunk_limit = _bounded_positive_int("max_chunks", max_chunks)
    names, columns = _statement_columns(response)
    manifest = response.get("manifest", {})
    if not isinstance(manifest, Mapping):
        raise ValueError("Statement result manifest is not an object")
    expected_rows = manifest.get("total_row_count")
    if expected_rows is not None:
        try:
            expected_rows = int(expected_rows)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("Statement manifest total_row_count is invalid") from exc
        if expected_rows < 0:
            raise ValueError("Statement manifest total_row_count is negative")
        if expected_rows > row_limit:
            raise RuntimeError(
                f"Statement result exceeds max_rows ({expected_rows} > {row_limit})"
            )

    expected_chunks = manifest.get("total_chunk_count")
    if expected_chunks is not None:
        try:
            expected_chunks = int(expected_chunks)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("Statement manifest total_chunk_count is invalid") from exc
        if expected_chunks < 0:
            raise ValueError("Statement manifest total_chunk_count is negative")
        if expected_chunks > chunk_limit:
            raise RuntimeError(
                f"Statement result exceeds max_chunks ({expected_chunks} > {chunk_limit})"
            )

    raw_result = response.get("result")
    if raw_result is None:
        result: Mapping[str, Any] | None = None
    elif isinstance(raw_result, Mapping):
        result = raw_result
    else:
        raise ValueError("Statement result is not an object")
    if manifest.get("truncated") is True or (
        result is not None and result.get("truncated") is True
    ):
        raise RuntimeError("Statement result was truncated by the provider")
    if (
        expected_chunks == 0
        and result is not None
        and result.get("data_array", []) == []
        and not result.get("next_chunk_internal_link")
    ):
        result = None

    all_rows: list[dict[str, Any]] = []
    encoded_bytes = 0
    chunk_count = 0
    seen_links: set[str] = set()
    chunk = result
    expected_offset = 0
    expected_chunk_index = 0

    while chunk is not None:
        chunk_count += 1
        if chunk_count > chunk_limit:
            raise RuntimeError(f"Statement result exceeds max_chunks ({chunk_limit})")
        if chunk.get("truncated") is True:
            raise RuntimeError("Statement result chunk was truncated by the provider")
        data = chunk.get("data_array", [])
        if not isinstance(data, list):
            raise ValueError("Statement result data_array is not an array")
        declared_row_count = chunk.get("row_count")
        if declared_row_count is not None:
            try:
                parsed_row_count = int(declared_row_count)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError("Statement result chunk row_count is invalid") from exc
            if parsed_row_count != len(data):
                raise RuntimeError(
                    "Statement result chunk is incomplete "
                    f"({len(data)} rows returned, chunk declared {parsed_row_count})"
                )
        chunk_index = chunk.get("chunk_index")
        if chunk_index is not None:
            try:
                parsed_chunk_index = int(chunk_index)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError("Statement result chunk_index is invalid") from exc
            if parsed_chunk_index != expected_chunk_index:
                raise RuntimeError(
                    "Statement result chunks are incomplete or out of order "
                    f"(chunk {parsed_chunk_index}, expected {expected_chunk_index})"
                )
        try:
            encoded_bytes += len(
                json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            )
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("Statement result rows are not JSON serializable") from exc
        if encoded_bytes > byte_limit:
            raise RuntimeError(
                f"Statement result exceeds max_bytes ({encoded_bytes} > {byte_limit})"
            )

        row_offset = chunk.get("row_offset")
        if row_offset is not None:
            try:
                parsed_offset = int(row_offset)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError("Statement result chunk row_offset is invalid") from exc
            if parsed_offset != expected_offset:
                raise RuntimeError(
                    "Statement result chunks are incomplete or out of order "
                    f"(offset {parsed_offset}, expected {expected_offset})"
                )
        all_rows.extend(_rows_from_data(data, names, strict_schema=True))
        expected_offset = len(all_rows)
        if len(all_rows) > row_limit:
            raise RuntimeError(
                f"Statement result exceeds max_rows ({len(all_rows)} > {row_limit})"
            )

        link = chunk.get("next_chunk_internal_link")
        if not link:
            break
        response_statement_id = response.get("statement_id")
        path = _workspace_chunk_path(
            client.host,
            link,
            statement_id=(
                str(response_statement_id)
                if response_statement_id not in (None, "")
                else None
            ),
        )
        if path in seen_links:
            raise RuntimeError("Statement result chunk link cycle detected")
        seen_links.add(path)
        if chunk_count >= chunk_limit:
            raise RuntimeError(f"Statement result exceeds max_chunks ({chunk_limit})")
        fetched = client.request("GET", path)
        nested = fetched.get("result")
        chunk = nested if isinstance(nested, Mapping) else fetched
        expected_chunk_index += 1

    if expected_rows is not None and len(all_rows) != expected_rows:
        raise RuntimeError(
            "Statement result is incomplete "
            f"({len(all_rows)} rows retrieved, manifest declared {expected_rows})"
        )
    if expected_chunks is not None and chunk_count != expected_chunks:
        raise RuntimeError(
            "Statement result is incomplete "
            f"({chunk_count} chunks retrieved, manifest declared {expected_chunks})"
        )
    return all_rows, {
        "statement_id": response.get("statement_id"),
        "row_count": len(all_rows),
        "chunk_count": chunk_count,
        "encoded_data_bytes": encoded_bytes,
        "schema_columns": columns,
    }


def probe_tls_endpoint(endpoint: str, timeout: float = 5.0) -> dict[str, Any]:
    parsed = urllib.parse.urlparse(endpoint)
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("endpoint must be an https URL with a hostname")
    port = parsed.port or 443
    started = time.monotonic()
    context = ssl.create_default_context()
    with socket.create_connection((parsed.hostname, port), timeout=timeout) as raw:
        with context.wrap_socket(raw, server_hostname=parsed.hostname) as tls:
            version = tls.version()
    return {
        "hostname": parsed.hostname,
        "port": port,
        "tls_version": version,
        "latency_ms": round((time.monotonic() - started) * 1000, 1),
    }
