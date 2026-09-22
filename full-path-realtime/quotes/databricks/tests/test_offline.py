from __future__ import annotations

import io
import json
import os
import re
import sys
import tempfile
import unittest
import urllib.error
import urllib.parse
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dbx_common import (  # noqa: E402
    DatabricksRestClient,
    EnvironmentConfig,
    atomic_append_jsonl,
    atomic_write_json,
    bounded_statement_rows,
    construct_zerobus_endpoint,
    load_queries,
    redact_secrets,
    render_sql,
    split_sql_statements,
    validate_config,
    zerobus_endpoint_errors,
)
import apply_ddl  # noqa: E402
import compact_evidence  # noqa: E402
import compact_query_results  # noqa: E402
import collect_evidence  # noqa: E402
import finalize_results  # noqa: E402
import preflight  # noqa: E402
import publish_results  # noqa: E402
import qualify  # noqa: E402
import run_dashboard  # noqa: E402
import run_drilldown  # noqa: E402
import validate_run  # noqa: E402
from costs import summarize_run  # noqa: E402
from monitor_freshness import (  # noqa: E402
    AGE_SEMANTICS,
    build_control_queries,
    classify_maintenance_type,
    latest_successful_refresh,
    progress_payload,
    timestamp_age_sec,
)
from runner_common import (  # noqa: E402
    SESSION_CONFIGURATION,
    _open_kernel_connection,
    aligned_metric_arrays,
    build_parser,
    execute_measured_query,
    fixed_rate_delay,
    fixed_rate_next_fire,
    freshness_progress_snapshot,
    normalize_query_history,
    poll_query_history,
    producer_progress_snapshot,
    stable_result_hash,
    validate_jsonl_record,
)
from preflight import (  # noqa: E402
    EXPECTED_QUERY_COUNTS,
    PACKAGE_CONTRACT,
    load_create_materialized_view_statement,
    offline_checks,
    package_checks,
    predictive_optimization_check,
)
from ingest_zerobus import (  # noqa: E402
    BatchCoordinate,
    PendingBatch,
    ProgressJournal,
    RateLimiter,
    Task,
    add_ordinal_range,
    checkpoint_pending,
    compact_metrics_payload,
    discover_source_files,
    enumerate_tasks_and_manifest,
    map_ipc_compression,
    normalize_batch,
    protect_target_table,
    target_schema,
    validate_expected_rows,
    validate_resume_journal,
)


class _FakeType:
    def __init__(self, name, *, bit_width=None, value_type=None):
        self.name = name
        self.bit_width = bit_width
        self.value_type = value_type

    def __repr__(self):
        return self.name


class _FakeField:
    def __init__(self, name, type_, nullable):
        self.name = name
        self.type = type_
        self.nullable = nullable


class _FakeSchema(list):
    @property
    def names(self):
        return [field.name for field in self]

    def get_field_index(self, name):
        return self.names.index(name)


class _FakeArray:
    def __init__(self, type_, marker):
        self.type = type_
        self.marker = marker


class _FakeRecordBatch:
    def __init__(self, arrays, schema):
        self.arrays = arrays
        self.schema = schema

    def column(self, index):
        return self.arrays[index]

    @classmethod
    def from_arrays(cls, arrays, schema):
        return cls(arrays, schema)


class _FakeArrow:
    RecordBatch = _FakeRecordBatch

    class types:
        @staticmethod
        def is_list(value):
            return value.name == "list"

        @staticmethod
        def is_large_list(value):
            return False

        @staticmethod
        def is_fixed_size_list(value):
            return False

        @staticmethod
        def is_unsigned_integer(value):
            return value.name.startswith("uint")

        @staticmethod
        def is_signed_integer(value):
            return value.name.startswith("int")

    @staticmethod
    def large_utf8():
        return _FakeType("large_utf8")

    @staticmethod
    def int16():
        return _FakeType("int16", bit_width=16)

    @staticmethod
    def int64():
        return _FakeType("int64", bit_width=64)

    @staticmethod
    def float64():
        return _FakeType("float64")

    @staticmethod
    def list_(value_type):
        return _FakeType("list", value_type=value_type)

    @staticmethod
    def field(name, type_, nullable=True):
        return _FakeField(name, type_, nullable)

    @staticmethod
    def schema(fields):
        return _FakeSchema(fields)


class _FakeCompute:
    def __init__(self, maxima=None):
        self.casts = []
        self.maxima = maxima or {}

    def cast(self, source, target, safe):
        self.casts.append((source.marker, target.name, safe))
        return (source.marker, target.name)

    @staticmethod
    def list_flatten(source):
        return source

    def max(self, source, skip_nulls=True):
        value = self.maxima.get(source.marker)
        if value is None:
            return None

        class Scalar:
            def as_py(self):
                return value

        return Scalar()


class SqlTests(unittest.TestCase):
    def test_splitter_ignores_line_and_nested_block_comment_semicolons(self):
        sql = """
        -- leading ; comment
        SELECT 'a;b' AS single, "c;d" AS double, `e;f` AS identifier;
        /* outer ;
           /* nested ; */
        */
        SELECT 2 /* ordinary ; comment */;
        """
        self.assertEqual(
            split_sql_statements(sql),
            [
                """SELECT 'a;b' AS single, "c;d" AS double, `e;f` AS identifier""",
                "SELECT 2",
            ],
        )

    def test_splitter_preserves_optimizer_hints(self):
        self.assertEqual(
            split_sql_statements("SELECT /*+ REPARTITION(1) ; */ 1;"),
            ["SELECT /*+ REPARTITION(1) ; */ 1"],
        )

    def test_splitter_handles_doubled_quotes_and_rejects_unterminated_input(self):
        self.assertEqual(
            split_sql_statements("SELECT 'it''s;a'; SELECT `a``;b`;"),
            ["SELECT 'it''s;a'", "SELECT `a``;b`"],
        )
        with self.assertRaisesRegex(ValueError, "unterminated SQL quote"):
            split_sql_statements("SELECT 'broken;")
        with self.assertRaisesRegex(ValueError, "unterminated SQL block comment"):
            split_sql_statements("SELECT 1 /* broken;")

    def test_render_quotes_catalog_and_schema(self):
        rendered = render_sql(
            "SELECT * FROM __CATALOG__.__SCHEMA__.quotes",
            "benchmark-catalog",
            "schema name",
        )
        self.assertEqual(
            rendered,
            "SELECT * FROM `benchmark-catalog`.`schema name`.quotes",
        )
        custom = render_sql(
            "SELECT * FROM __CATALOG__.__SCHEMA__.__RAW_TABLE__ "
            "JOIN __CATALOG__.__SCHEMA__.__MV_TABLE__",
            "benchmark-catalog",
            "schema name",
            raw_table="raw table",
            mv_table="daily`mv",
        )
        self.assertEqual(
            custom,
            "SELECT * FROM `benchmark-catalog`.`schema name`.`raw table` "
            "JOIN `benchmark-catalog`.`schema name`.`daily``mv`",
        )


class ApplyDdlTests(unittest.TestCase):
    @staticmethod
    def invoke(output_dir, client_factory, **overrides):
        values = {
            "host": "https://dbc-example.cloud.databricks.com",
            "catalog": "fresh_catalog",
            "schema": "fresh_schema",
            "control_warehouse_id": "control-warehouse",
            "confirm_destructive_target": "fresh_catalog.fresh_schema",
            "output_dir": output_dir,
            "token": "dapiSecretValue123456",
            "client_factory": client_factory,
        }
        values.update(overrides)
        return apply_ddl.apply_ddl(**values)

    def test_confirmation_mismatch_blocks_before_client_or_statement_calls(self):
        calls = []

        def client_factory(*args, **kwargs):
            calls.append((args, kwargs))
            raise AssertionError("client must not be created")

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "blocked"
            with self.assertRaisesRegex(ValueError, "must exactly equal"):
                self.invoke(
                    output_dir,
                    client_factory,
                    confirm_destructive_target="fresh_catalog.wrong_schema",
                )
            self.assertFalse(output_dir.exists())
        self.assertEqual(calls, [])

    def test_complete_rendered_sql_and_all_four_statements_are_recorded(self):
        class FakeClient:
            def __init__(self, *args, **kwargs):
                self.calls = []

            def execute_statement(self, statement, warehouse_id, **kwargs):
                self.calls.append((statement, warehouse_id, kwargs))
                return {"statement_id": f"statement-{len(self.calls)}"}

        client = FakeClient()
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "ddl"
            report = self.invoke(
                output_dir,
                lambda *args, **kwargs: client,
            )
            persisted = json.loads(
                (output_dir / apply_ddl.REPORT_NAME).read_text(encoding="utf-8")
            )
            rendered = (output_dir / apply_ddl.RENDERED_SQL_NAME).read_text(
                encoding="utf-8"
            )

        expected = render_sql(
            (ROOT / "create.sql").read_text(encoding="utf-8"),
            "fresh_catalog",
            "fresh_schema",
        )
        self.assertEqual(rendered, expected)
        self.assertNotIn("__CATALOG__", rendered)
        self.assertNotIn("__SCHEMA__", rendered)
        self.assertEqual(len(split_sql_statements(rendered)), 4)
        self.assertEqual(len(client.calls), 4)
        self.assertTrue(
            all(
                call[2]["query_tags"]["operation"] == "apply-ddl"
                and call[1] == "control-warehouse"
                for call in client.calls
            )
        )
        self.assertEqual(report["status"], "succeeded")
        self.assertEqual(persisted["statement_count"], 4)
        self.assertEqual(
            [item["statement_id"] for item in persisted["statements"]],
            [f"statement-{index}" for index in range(1, 5)],
        )
        self.assertTrue(
            all(item["status"] == "succeeded" for item in persisted["statements"])
        )

    def test_statement_failure_is_recorded_redacted_and_stops_execution(self):
        secret = "dapiSecretValue123456"

        class FakeClient:
            def __init__(self):
                self.calls = []

            def execute_statement(self, statement, warehouse_id, **kwargs):
                self.calls.append(statement)
                if len(self.calls) == 2:
                    raise RuntimeError(f"Authorization: Bearer {secret}")
                return {"statement_id": f"statement-{len(self.calls)}"}

        client = FakeClient()
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "ddl"
            report = self.invoke(
                output_dir,
                lambda *args, **kwargs: client,
                token=secret,
            )
            persisted_text = (
                output_dir / apply_ddl.REPORT_NAME
            ).read_text(encoding="utf-8")
            persisted = json.loads(persisted_text)

        self.assertEqual(len(client.calls), 2)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(
            [item["status"] for item in persisted["statements"]],
            ["succeeded", "failed", "not_run", "not_run"],
        )
        self.assertEqual(len(persisted["errors"]), 1)
        self.assertNotIn(secret, persisted_text)
        self.assertIn("***REDACTED***", persisted_text)


class FreshnessMonitorTests(unittest.TestCase):
    def test_control_sql_is_bounded_targeted_and_never_counts_raw(self):
        queries = build_control_queries(
            "catalog",
            "schema",
            "quotes'raw",
            "quotes daily",
            "2026-09-10T00:00:00Z",
            "2026-09-10T00:05:00Z",
        )
        ingest = queries["zerobus_ingest"]
        events = queries["mv_events"]
        self.assertIn("table_name = 'catalog.schema.quotes''raw'", ingest)
        self.assertIn("commit_time >=", ingest)
        self.assertIn("commit_time <=", ingest)
        self.assertIn("timestamp >=", events)
        self.assertIn("timestamp <=", events)
        self.assertIn("LIMIT 100", events)
        self.assertIn("event_type IN ('update_progress', 'planning_information')", events)
        self.assertIn(
            "event_log(TABLE(`catalog`.`schema`.`quotes daily`))",
            events,
        )
        self.assertEqual(
            queries["raw_history"],
            "DESCRIBE HISTORY `catalog`.`schema`.`quotes'raw` LIMIT 1",
        )
        self.assertNotIn("count(*)", "\n".join(queries.values()).lower())

    def test_maintenance_classification_and_event_detail_correlation(self):
        cases = {
            "MAINTENANCE_TYPE_COMPLETE_RECOMPUTE": "full",
            "COMPLETE_RECOMPUTE": "full",
            "FULL_REFRESH": "full",
            "MAINTENANCE_TYPE_NO_OP": "neutral",
            "MAINTENANCE_TYPE_GROUP_AGGREGATE": "incremental",
            None: "unknown",
        }
        for maintenance_type, expected in cases.items():
            with self.subTest(maintenance_type=maintenance_type):
                self.assertEqual(
                    classify_maintenance_type(maintenance_type),
                    expected,
                )

        rows = [
            {
                "timestamp": "2026-09-10T00:04:00Z",
                "event_type": "update_progress",
                "origin": json.dumps({"update_id": "update-2"}),
                "details": json.dumps(
                    {
                        "update_progress": {"state": "COMPLETED"},
                        "metrics": {"num_output_rows": "321"},
                    }
                ),
            },
            {
                "timestamp": "2026-09-10T00:03:00Z",
                "event_type": "planning_information",
                "origin": {
                    "update_id": "update-2",
                    "ingestion_source_table_version": "17",
                },
                "details": json.dumps(
                    {
                        "planning_information": {
                            "technique_information": [
                                {
                                    "maintenance_type": (
                                        "MAINTENANCE_TYPE_COMPLETE_RECOMPUTE"
                                    ),
                                    "is_chosen": True,
                                },
                                {
                                    "maintenance_type": (
                                        "MAINTENANCE_TYPE_GROUP_AGGREGATE"
                                    ),
                                    "is_chosen": False,
                                },
                            ],
                            "source_table_information": [
                                {
                                    "table_name": "`catalog`.`schema`.`quotes`",
                                    "num_rows": "300",
                                    "num_files": "4",
                                    "full_size": "5000",
                                    "num_changed_rows": "100",
                                    "num_changed_files": "2",
                                    "change_size": "1200",
                                }
                            ],
                        }
                    }
                ),
            },
        ]
        refresh = latest_successful_refresh(rows)
        self.assertEqual(
            refresh,
            {
                "finished_at": "2026-09-10T00:04:00.000Z",
                "output_rows": 321,
                "maintenance_type": "MAINTENANCE_TYPE_COMPLETE_RECOMPUTE",
                "maintenance_class": "full",
                "planned_at": "2026-09-10T00:03:00.000Z",
                "source_snapshot": {
                    "table_name": "`catalog`.`schema`.`quotes`",
                    "num_rows": 300,
                    "num_files": 4,
                    "full_size_bytes": 5000,
                    "changed_rows": 100,
                    "changed_files": 2,
                    "change_size_bytes": 1200,
                    "delta_version": 17,
                },
                "update_id": "update-2",
            },
        )

    def test_observational_age_and_runner_progress_shape(self):
        self.assertEqual(
            timestamp_age_sec(
                "2026-09-10T00:02:30Z",
                "2026-09-10T00:01:00Z",
            ),
            90.0,
        )
        self.assertIsNone(
            timestamp_age_sec("2026-09-10T00:02:30Z", "not-a-timestamp")
        )
        snapshot = progress_payload(
            {
                "schema_version": 1,
                "run_id": "run-1",
                "iteration": 7,
                "observed_at": "2026-09-10T00:02:30Z",
                "iteration_finished_at": "2026-09-10T00:02:31Z",
                "producer_progress": {
                    "provider_committed_rows": 120,
                },
                "zerobus_ingest": {
                    "cumulative_committed_records": 119,
                    "cumulative_committed_bytes": 1_000,
                    "cumulative_error_count": 0,
                    "latest_commit_version": 17,
                    "latest_commit_time": "2026-09-10T00:02:29Z",
                },
                "raw_history": {
                    "version": 18,
                    "timestamp": "2026-09-10T00:02:30Z",
                },
                "mv_rows": 123,
                "latest_successful_refresh": {
                    "finished_at": "2026-09-10T00:01:00Z",
                    "output_rows": 123,
                    "maintenance_type": "MAINTENANCE_TYPE_GROUP_AGGREGATE",
                },
                "mv_source_rows": 100,
                "mv_rows_behind": 20,
                "raw_commit_age_sec": 1.0,
                "producer_progress_age_sec": 2.0,
                "mv_refresh_age_sec": 90.0,
                "query_evidence": {
                    "mv_events": {"statement_id": "statement-1"}
                },
                "errors": [],
            }
        )
        self.assertIsNone(snapshot["mv_rows"])
        self.assertEqual(
            snapshot["latest_successful_refresh"]["finished_at"],
            "2026-09-10T00:01:00Z",
        )
        self.assertEqual(snapshot["mv_source_rows"], 100)
        self.assertEqual(snapshot["mv_rows_behind"], 20)
        self.assertEqual(snapshot["client_durable_rows"], 120)
        self.assertEqual(snapshot["provider_committed_rows"], 119)
        self.assertEqual(snapshot["raw_delta_version"], 18)
        self.assertEqual(snapshot["statement_ids"]["mv_events"], "statement-1")
        self.assertEqual(snapshot["updated_at"], "2026-09-10T00:02:31Z")
        self.assertEqual(snapshot["age_semantics"], AGE_SEMANTICS)


class EndpointTests(unittest.TestCase):
    def test_constructs_documented_cloud_endpoints(self):
        cases = {
            "aws": "https://123456789.zerobus.us-west-2.cloud.databricks.com",
            "azure": "https://123456789.zerobus.eastus.azuredatabricks.net",
            "gcp": "https://123456789.zerobus.us-east1.gcp.databricks.com",
        }
        regions = {"aws": "us-west-2", "azure": "eastus", "gcp": "us-east1"}
        for cloud, expected in cases.items():
            with self.subTest(cloud=cloud):
                self.assertEqual(
                    construct_zerobus_endpoint("123456789", regions[cloud], cloud),
                    expected,
                )

    def test_endpoint_validation_detects_cloud_mismatch_and_paths(self):
        errors = zerobus_endpoint_errors(
            "https://123.zerobus.us-west-2.gcp.databricks.com/a",
            workspace_id="123",
            region="us-west-2",
            cloud="aws",
        )
        self.assertTrue(any("path" in error for error in errors))
        self.assertTrue(any("does not match" in error for error in errors))


class RedactionAndWriteTests(unittest.TestCase):
    def test_redacts_sensitive_keys_known_values_pat_and_authorization(self):
        secret = "not-patterned-value"
        source = {
            "client_secret": secret,
            "message": (
                f"request used {secret}, Bearer eyJ.abc.def, "
                "and dapi1234567890abcdef"
            ),
            "nested": [{"Authorization": "Basic abc123=="}],
        }
        redacted = redact_secrets(source, [secret])
        rendered = json.dumps(redacted)
        self.assertNotIn(secret, rendered)
        self.assertNotIn("eyJ.abc.def", rendered)
        self.assertNotIn("dapi1234567890abcdef", rendered)
        self.assertNotIn("abc123==", rendered)
        self.assertIn("***REDACTED***", rendered)

    def test_rest_errors_do_not_echo_configured_secrets(self):
        secret = "arbitrary-secret-without-a-known-prefix"

        def fail(request, timeout):
            raise urllib.error.HTTPError(
                request.full_url,
                401,
                "Unauthorized",
                {},
                io.BytesIO(
                    json.dumps({"message": f"credential {secret} rejected"}).encode()
                ),
            )

        client = DatabricksRestClient(
            "https://dbc-example.cloud.databricks.com",
            token=secret,
            opener=fail,
        )
        with self.assertRaises(Exception) as raised:
            client.get_warehouse("abc123")
        self.assertNotIn(secret, str(raised.exception))
        self.assertIn("***REDACTED***", str(raised.exception))

    def test_atomic_json_and_jsonl_helpers(self):
        with tempfile.TemporaryDirectory() as tmp:
            json_path = Path(tmp) / "nested" / "report.json"
            jsonl_path = Path(tmp) / "events.jsonl"
            atomic_write_json(json_path, {"when": "now", "value": 1})
            atomic_append_jsonl(jsonl_path, {"value": 1})
            atomic_append_jsonl(jsonl_path, {"value": 2})
            self.assertEqual(json.loads(json_path.read_text())["value"], 1)
            self.assertEqual(
                [json.loads(line)["value"] for line in jsonl_path.read_text().splitlines()],
                [1, 2],
            )


class _FakeJsonResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class RestStatementTests(unittest.TestCase):
    def test_statement_execution_forwards_query_tags(self):
        seen = {}

        def opener(request, timeout):
            seen["payload"] = json.loads(request.data.decode("utf-8"))
            seen["timeout"] = timeout
            return _FakeJsonResponse(
                {
                    "statement_id": "statement-1",
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {"schema": {"columns": []}},
                    "result": {"data_array": []},
                }
            )

        client = DatabricksRestClient(
            "https://dbc-example.cloud.databricks.com",
            token="secret-token",
            opener=opener,
        )
        client.execute_statement(
            "SELECT 1",
            "control-warehouse",
            query_tags={"collector": "freshness", "iteration": "1"},
        )
        self.assertEqual(
            seen["payload"]["query_tags"],
            [
                {"key": "collector", "value": "freshness"},
                {"key": "iteration", "value": "1"},
            ],
        )

    def test_bounded_statement_rows_follows_all_workspace_chunks(self):
        seen = []

        def opener(request, timeout):
            seen.append(request.full_url)
            return _FakeJsonResponse(
                {
                    "chunk_index": 1,
                    "row_offset": 2,
                    "row_count": 1,
                    "data_array": [["three", 3]],
                }
            )

        client = DatabricksRestClient(
            "https://dbc-example.cloud.databricks.com",
            token="secret-token",
            opener=opener,
        )
        response = {
            "statement_id": "statement-1",
            "manifest": {
                "total_row_count": 3,
                "total_chunk_count": 2,
                "schema": {
                    "columns": [
                        {"name": "word", "type_text": "STRING"},
                        {"name": "number", "type_text": "INT"},
                    ]
                },
            },
            "result": {
                "chunk_index": 0,
                "row_offset": 0,
                "row_count": 2,
                "data_array": [["one", 1], ["two", 2]],
                "next_chunk_internal_link": (
                    "/api/2.0/sql/statements/statement-1/result/chunks/1"
                    "?row_offset=2"
                ),
            },
        }
        rows, metadata = bounded_statement_rows(
            client,
            response,
            max_rows=3,
            max_bytes=1_000,
            max_chunks=2,
        )
        self.assertEqual(
            rows,
            [
                {"word": "one", "number": 1},
                {"word": "two", "number": 2},
                {"word": "three", "number": 3},
            ],
        )
        self.assertEqual(metadata["row_count"], 3)
        self.assertEqual(metadata["chunk_count"], 2)
        self.assertEqual(
            [column["name"] for column in metadata["schema_columns"]],
            ["word", "number"],
        )
        self.assertEqual(len(seen), 1)
        self.assertTrue(seen[0].startswith(client.host + "/api/2.0/sql/statements/"))

    def test_bounded_statement_rows_rejects_foreign_links_and_limits(self):
        client = DatabricksRestClient(
            "https://dbc-example.cloud.databricks.com",
            token="secret-token",
            opener=lambda *_args, **_kwargs: self.fail("foreign link was requested"),
        )
        base = {
            "statement_id": "statement-1",
            "manifest": {
                "schema": {"columns": [{"name": "value"}]},
            },
            "result": {
                "data_array": [[1]],
                "next_chunk_internal_link": (
                    "https://attacker.invalid/api/2.0/sql/statements/x/result/chunks/1"
                ),
            },
        }
        with self.assertRaisesRegex(ValueError, "outside the workspace origin"):
            bounded_statement_rows(
                client,
                base,
                max_rows=10,
                max_bytes=1_000,
                max_chunks=2,
            )
        wrong_statement = {
            **base,
            "result": {
                "data_array": [[1]],
                "next_chunk_internal_link": (
                    "/api/2.0/sql/statements/other/result/chunks/1"
                ),
            },
        }
        with self.assertRaisesRegex(ValueError, "another statement"):
            bounded_statement_rows(
                client,
                wrong_statement,
                max_rows=10,
                max_bytes=1_000,
                max_chunks=2,
            )
        with self.assertRaisesRegex(RuntimeError, "max_chunks"):
            bounded_statement_rows(
                client,
                {
                    **base,
                    "result": {
                        "data_array": [[1]],
                        "next_chunk_internal_link": (
                            "/api/2.0/sql/statements/statement-1/result/chunks/1"
                        ),
                    },
                },
                max_rows=10,
                max_bytes=1_000,
                max_chunks=1,
            )

        too_many_rows = {
            **base,
            "manifest": {
                "total_row_count": 2,
                "schema": {"columns": [{"name": "value"}]},
            },
            "result": {"data_array": [[1], [2]]},
        }
        with self.assertRaisesRegex(RuntimeError, "max_rows"):
            bounded_statement_rows(
                client,
                too_many_rows,
                max_rows=1,
                max_bytes=1_000,
                max_chunks=1,
            )
        with self.assertRaisesRegex(RuntimeError, "max_bytes"):
            bounded_statement_rows(
                client,
                {
                    **too_many_rows,
                    "manifest": {
                        "total_row_count": 2,
                        "schema": {"columns": [{"name": "value"}]},
                    },
                },
                max_rows=2,
                max_bytes=2,
                max_chunks=1,
            )

    def test_bounded_statement_rows_preserves_empty_result_schema(self):
        client = DatabricksRestClient(
            "https://dbc-example.cloud.databricks.com",
            token="secret-token",
            opener=lambda *_args, **_kwargs: self.fail("no chunk should be requested"),
        )
        rows, metadata = bounded_statement_rows(
            client,
            {
                "statement_id": "statement-empty",
                "manifest": {
                    "total_row_count": 0,
                    "total_chunk_count": 0,
                    "schema": {
                        "columns": [{"name": "empty_value", "type_text": "STRING"}]
                    },
                },
                "result": None,
            },
            max_rows=1,
            max_bytes=10,
            max_chunks=1,
        )
        self.assertEqual(rows, [])
        self.assertEqual(metadata["chunk_count"], 0)
        self.assertEqual(metadata["schema_columns"][0]["name"], "empty_value")
        with self.assertRaisesRegex(RuntimeError, "truncated"):
            bounded_statement_rows(
                client,
                {
                    "manifest": {
                        "truncated": True,
                        "schema": {"columns": [{"name": "value"}]},
                    },
                    "result": {"data_array": []},
                },
                max_rows=1,
                max_bytes=10,
                max_chunks=1,
            )


class EvidenceCollectorTests(unittest.TestCase):
    def make_queries(self):
        return collect_evidence.build_evidence_queries(
            catalog="catalog'name",
            schema="schema name",
            raw_table="raw`quotes",
            mv_table="daily quotes",
            rt_warehouse_id="rt'warehouse",
            control_warehouse_id="control-warehouse",
            run_id="run'id",
            since="2026-09-10T00:00:00Z",
            until="2026-09-10T01:00:00Z",
            row_limit=123,
            history_limit=45,
        )

    def test_evidence_window_is_required_utc_ordered_and_bounded(self):
        start, end = collect_evidence.validate_window(
            "2026-09-10T00:00:00Z",
            "2026-09-13T00:00:00Z",
        )
        self.assertEqual((end - start).total_seconds(), 72 * 3600)
        for since, until, message in (
            (
                "2026-09-10T00:00:00",
                "2026-09-10T01:00:00Z",
                "UTC timezone",
            ),
            (
                "2026-09-10T00:00:00+01:00",
                "2026-09-10T01:00:00Z",
                "must be UTC",
            ),
            (
                "2026-09-10T01:00:00Z",
                "2026-09-10T01:00:00Z",
                "later",
            ),
            (
                "2026-09-10T00:00:00Z",
                "2026-09-13T00:00:00.001Z",
                "maximum",
            ),
        ):
            with self.subTest(since=since, until=until):
                with self.assertRaisesRegex(ValueError, message):
                    collect_evidence.validate_window(since, until)

    def test_evidence_sql_is_targeted_time_bounded_and_never_counts_raw(self):
        queries = self.make_queries()
        ingest = queries["zerobus_ingest_summary"]
        self.assertIn("table_name = 'catalog''name.schema name.raw`quotes'", ingest)
        self.assertIn("commit_time >=", ingest)
        self.assertIn("commit_time <", ingest)

        history = queries["query_history"]
        self.assertIn("compute.warehouse_id = 'rt''warehouse'", history)
        self.assertIn("query_tags['run_id'] = 'run''id'", history)
        self.assertIn("start_time >=", history)
        self.assertIn("start_time <", history)
        self.assertIn("LIMIT 123", history)

        billing = queries["billing_usage"]
        self.assertIn("usage_start_time <", billing)
        self.assertIn("usage_end_time >", billing)
        self.assertIn("usage_metadata.warehouse_id IN", billing)
        self.assertIn("PREDICTIVE_OPTIMIZATION", billing)
        self.assertIn("usage_metadata.dlt_pipeline_id", billing)
        self.assertIn(
            "event_log(TABLE(`catalog'name`.`schema name`.`daily quotes`))",
            billing,
        )
        self.assertIn("to_json(usage_metadata)", billing)
        self.assertIn("LIMIT 123", billing)

        rendered = "\n".join(queries.values())
        self.assertIsNone(
            re.search(
                r"(?is)count\s*\(\s*\*\s*\).*from\s+"
                r"`catalog'name`\.`schema name`\.`raw``quotes`",
                rendered,
            )
        )
        self.assertNotIn("SELECT * FROM `catalog'name`.`schema name`.`raw``quotes`", rendered)

    def test_dynamic_zerobus_stream_query_requires_documented_filters(self):
        schema_rows = [
            {"col_name": "stream_id", "data_type": "string"},
            {"col_name": "event_time", "data_type": "timestamp"},
            {"col_name": "table_name", "data_type": "string"},
            {"col_name": "errors", "data_type": "array"},
        ]
        sql = collect_evidence.build_zerobus_stream_query(
            schema_rows,
            target="catalog.schema.raw",
            since="2026-09-10T00:00:00Z",
            until="2026-09-10T01:00:00Z",
            row_limit=99,
        )
        self.assertIn("`table_name` = 'catalog.schema.raw'", sql)
        self.assertIn("`event_time` >=", sql)
        self.assertIn("`event_time` <", sql)
        self.assertIn("ORDER BY `event_time`, `stream_id`", sql)
        self.assertTrue(sql.endswith("LIMIT 99"))
        with self.assertRaisesRegex(ValueError, "missing documented"):
            collect_evidence.build_zerobus_stream_query(
                schema_rows[:1],
                target="catalog.schema.raw",
                since="2026-09-10T00:00:00Z",
                until="2026-09-10T01:00:00Z",
                row_limit=99,
            )

    def test_summary_aggregates_provider_mv_stream_and_predictive_evidence(self):
        datasets = {
            name: {
                "required": name in collect_evidence.REQUIRED_DATASETS,
                "row_count": 0,
                "path": f"/tmp/{name}.jsonl",
                "statement_id": f"statement-{name}",
                "error": None,
            }
            for name in (
                *collect_evidence.REQUIRED_DATASETS,
                *collect_evidence.OPTIONAL_DATASETS,
            )
        }
        rows = {
            "zerobus_ingest_summary": [
                {
                    "provider_committed_records": "123",
                    "provider_committed_bytes": "456",
                    "provider_errors": "2",
                    "min_commit_version": 7,
                    "max_commit_version": 9,
                }
            ],
            "zerobus_stream_errors": [
                {"errors": [{"error_code": 1}, {"error_code": 2}]}
            ],
            "mv_event_log": [
                {
                    "event_type": "planning_information",
                    "details_json": json.dumps(
                        {
                            "planning_information": {
                                "technique_information": [
                                    {
                                        "maintenance_type": (
                                            "MAINTENANCE_TYPE_COMPLETE_RECOMPUTE"
                                        ),
                                        "is_chosen": True,
                                    }
                                ]
                            }
                        }
                    ),
                }
            ],
            "warehouse_config_snapshots": [{"warehouse_id": "rt"}],
            "raw_table_detail": [{"numRows": "1000"}],
            "mv_table_detail": [{"numRows": 100}],
            "predictive_optimization_operations": [
                {
                    "operation_type": "CLUSTERING",
                    "usage_unit": "ESTIMATED_DBU",
                    "usage_quantity": "1.25",
                },
                {
                    "operation_type": "CLUSTERING",
                    "usage_unit": "ESTIMATED_DBU",
                    "usage_quantity": "0.75",
                },
            ],
            "billing_usage": [
                {
                    "sku_name": "SERVERLESS",
                    "usage_unit": "DBU",
                    "usage_quantity": "3.5",
                }
            ],
        }
        summary = collect_evidence.build_summary(
            since="2026-09-10T00:00:00Z",
            until="2026-09-10T01:00:00Z",
            run_id="run-1",
            targets={"raw": "catalog.schema.raw"},
            datasets=datasets,
            rows=rows,
            collected_at="2026-09-10T02:00:00Z",
        )
        self.assertEqual(summary["provider_committed_records"], 123)
        self.assertEqual(summary["provider_committed_bytes"], 456)
        self.assertEqual(summary["provider_errors"], 2)
        self.assertEqual(summary["zerobus_stream_error_count"], 2)
        self.assertEqual(summary["mv_maintenance"]["full_refresh_count"], 1)
        self.assertEqual(
            summary["predictive_optimization"]["operation_counts_by_type"],
            {"CLUSTERING": 2},
        )
        self.assertEqual(
            summary["predictive_optimization"]["usage_by_unit"]["ESTIMATED_DBU"],
            "2.00",
        )
        self.assertEqual(summary["table_num_rows"]["raw"], 1000)
        self.assertTrue(summary["required_dataset_completeness"]["complete"])
        self.assertTrue(summary["complete"])
        datasets["zerobus_stream_schema"]["error"] = "unsupported schema"
        optional_failure = collect_evidence.build_summary(
            since="2026-09-10T00:00:00Z",
            until="2026-09-10T01:00:00Z",
            run_id="run-1",
            targets={},
            datasets=datasets,
            rows=rows,
        )
        self.assertTrue(
            optional_failure["required_dataset_completeness"]["complete"]
        )
        self.assertFalse(optional_failure["complete"])

    def test_summary_marks_failed_required_source_incomplete(self):
        datasets = {
            name: {
                "required": True,
                "row_count": 0,
                "path": f"/tmp/{name}.jsonl",
                "statement_id": None,
                "error": "permission denied" if name == "query_history" else None,
            }
            for name in collect_evidence.REQUIRED_DATASETS
        }
        datasets.update(
            {
                name: {
                    "required": False,
                    "row_count": 0,
                    "path": f"/tmp/{name}.jsonl",
                    "statement_id": None,
                    "error": None,
                }
                for name in collect_evidence.OPTIONAL_DATASETS
            }
        )
        summary = collect_evidence.build_summary(
            since="2026-09-10T00:00:00Z",
            until="2026-09-10T01:00:00Z",
            run_id="run-1",
            targets={},
            datasets=datasets,
            rows={},
        )
        self.assertEqual(
            summary["required_dataset_completeness"]["failed"],
            ["query_history"],
        )
        self.assertFalse(summary["required_dataset_completeness"]["complete"])
        self.assertFalse(summary["complete"])

    def test_collection_failure_still_writes_summary_and_returns_incomplete(self):
        class FailingClient:
            def execute_statement(self, *_args, **_kwargs):
                raise RuntimeError("permission denied")

            def request(self, *_args, **_kwargs):
                raise RuntimeError("permission denied")

        with tempfile.TemporaryDirectory() as tmp:
            summary, required_complete = collect_evidence.collect(
                FailingClient(),
                control_warehouse_id="control",
                rt_warehouse_id="rt",
                catalog="catalog",
                schema="schema",
                raw_table="raw",
                mv_table="mv",
                since=collect_evidence.parse_utc_timestamp(
                    "2026-09-10T00:00:00Z"
                ),
                until=collect_evidence.parse_utc_timestamp(
                    "2026-09-10T01:00:00Z"
                ),
                run_id="run-1",
                output_dir=Path(tmp),
                row_limit=100,
                history_limit=10,
                statement_timeout=1,
                poll_interval=0.1,
                max_result_bytes=1_000,
                max_result_chunks=10,
            )
            written = json.loads(
                (Path(tmp) / "evidence_summary.json").read_text(encoding="utf-8")
            )
            self.assertTrue((Path(tmp) / "collector_errors.jsonl").exists())
        self.assertFalse(required_complete)
        self.assertFalse(summary["required_dataset_completeness"]["complete"])
        self.assertFalse(written["required_dataset_completeness"]["complete"])
        self.assertIn(
            "query_history",
            written["required_dataset_completeness"]["failed"],
        )

    def test_compact_evidence_keeps_clustering_and_reconciliation_only(self):
        outputs = compact_evidence.build_compact_evidence(
            {
                "run_id": "run-1",
                "provider_committed_records": 100,
                "provider_committed_bytes": 200,
                "provider_errors": 0,
                "zerobus_stream_error_count": 0,
                "table_details": {
                    "raw": {
                        "name": "catalog.schema.quotes",
                        "clusteringColumns": '["sym","t"]',
                        "numFiles": "3",
                    },
                    "materialized_view": {
                        "json_metadata": json.dumps(
                            {
                                "table_name": "quotes_daily",
                                "clustering_columns": ["sym", "day"],
                            }
                        )
                    },
                },
                "predictive_optimization": {
                    "operation_count": 1,
                    "operation_counts_by_type": {"CLUSTERING": 1},
                },
                "mv_maintenance": {
                    "maintenance_type_counts": {
                        "MAINTENANCE_TYPE_GROUP_AGGREGATE": 2
                    },
                    "maintenance_class_counts": {"incremental": 2},
                    "full_refresh_count": 0,
                },
            },
            [
                {
                    "table_name": "quotes",
                    "operation_id": "operation-1",
                    "operation_type": "CLUSTERING",
                    "operation_status": "SUCCESSFUL",
                    "operation_metrics_json": '{"number_of_clustered_files":"2"}',
                }
            ],
            {
                "online": {
                    "predictive_optimization": {
                        "status": "ok",
                        "raw": {
                            "configured_value": "INHERIT",
                            "effective_value": "ENABLE",
                            "effectively_enabled": True,
                        },
                        "materialized_view": {
                            "configured_value": "INHERIT",
                            "effective_value": "ENABLE",
                            "effectively_enabled": True,
                        },
                    }
                }
            },
        )
        clustering = outputs["clustering_status"]
        self.assertEqual(
            clustering["tables"]["raw"]["clustering_columns"], ["sym", "t"]
        )
        self.assertEqual(
            clustering["tables"]["materialized_view"]["clustering_columns"],
            ["sym", "day"],
        )
        self.assertEqual(
            clustering["predictive_optimization"]["clustering_operation_count"],
            1,
        )
        self.assertTrue(
            clustering["tables"]["raw"][
                "predictive_optimization_effectively_enabled"
            ]
        )
        self.assertEqual(
            outputs["provider_reconciliation"]["provider_committed_records"],
            100,
        )
        self.assertEqual(outputs["mv_refresh_summary"]["full_refresh_count"], 0)

    def test_result_manifest_excludes_runtime_source_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_root = root / "run"
            (run_root / "ingest").mkdir(parents=True)
            (run_root / "ingest" / "ingest_summary.json").write_text("{}\n")
            source_manifest = root / "source_manifest.json"
            source_manifest.write_text(
                json.dumps(
                    {
                        "total_rows": 100,
                        "selected_file_count": 2,
                        "selected_row_group_count": 3,
                        "manifest_sha256": "a" * 64,
                        "quotes_0_parquet_included": False,
                        "excluded_files": ["quotes_0.parquet"],
                    }
                )
            )
            source_hashes = root / "source_hashes.json"
            source_hashes.write_text(
                json.dumps({"status": "complete", "file_count": 2})
            )
            with patch("sys.stdout", new=io.StringIO()):
                status = finalize_results.run(
                    [
                        "--run-root",
                        str(run_root),
                        "--run-id",
                        "run-1",
                        "--source-manifest",
                        str(source_manifest),
                        "--source-hashes",
                        str(source_hashes),
                    ]
                )
            manifest = json.loads(
                (run_root / "validation" / "manifest.json").read_text()
            )
        self.assertEqual(status, 0)
        self.assertEqual(manifest["run_id"], "run-1")
        self.assertEqual(manifest["artifact_count"], 1)
        self.assertEqual(
            manifest["artifacts"][0]["path"], "ingest/ingest_summary.json"
        )
        self.assertFalse(manifest["runtime_state"]["included_in_results"])


class _FakeMeasuredCursor:
    def __init__(self, query_id="query-1", rows=None):
        self.query_id = query_id
        self.rows = list(rows or [(1, "one"), (2, "two")])
        self.execute_calls = []
        self.closed = False

    def execute(self, sql, query_tags=None):
        self.execute_calls.append((sql, query_tags))

    def fetchmany(self, size):
        self.fetch_sizes = getattr(self, "fetch_sizes", [])
        self.fetch_sizes.append(size)
        if not self.rows:
            return []
        rows, self.rows = self.rows[:size], self.rows[size:]
        return rows

    def close(self):
        self.closed = True


class _FakeMeasuredConnection:
    def __init__(self, cursor):
        self.measured_cursor = cursor

    def cursor(self):
        return self.measured_cursor


class _FakeHistoryClient:
    def __init__(self, history):
        self.history = history
        self.ids = []

    def query_history_by_statement_id(self, statement_id):
        self.ids.append(statement_id)
        if isinstance(self.history, list):
            return self.history.pop(0)
        return self.history


class RunnerHistoryTests(unittest.TestCase):
    def test_kernel_connection_uses_native_oauth_arguments(self):
        captured = {}

        class FakeSql:
            @staticmethod
            def connect(**kwargs):
                captured.update(kwargs)
                return "connection"

        fake_databricks = type("FakeDatabricks", (), {"sql": FakeSql})
        args = SimpleNamespace(
            host="https://dbc-example.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/query",
            catalog="costbench",
            schema="rt_qualification",
            token=None,
            client_id="client-id",
            client_secret="client-secret",
        )
        with patch.dict(sys.modules, {"databricks": fake_databricks}):
            self.assertEqual(_open_kernel_connection(args), "connection")
        self.assertTrue(captured["use_kernel"])
        self.assertEqual(captured["oauth_client_id"], "client-id")
        self.assertEqual(captured["oauth_client_secret"], "client-secret")
        self.assertNotIn("credentials_provider", captured)

    def test_current_rest_timing_and_metric_aliases_are_normalized(self):
        normalized = normalize_query_history(
            {
                "query_id": "query-1",
                "status": "FINISHED",
                "is_final": True,
                "duration": 1500,
                "rows_produced": 7,
                "cache_query_id": None,
                "metrics": {
                    "compilation_time_ms": 11,
                    "execution_time_ms": 1200,
                    "result_fetch_time_ms": 23,
                    "waiting_at_capacity_duration_ms": 17,
                    "read_bytes": 1000,
                    "read_cache_bytes": 250,
                    "read_files_count": 8,
                    "read_partitions_count": 4,
                    "rows_read_count": 99,
                    "result_from_cache": False,
                },
            }
        )
        self.assertEqual(normalized["statement_id"], "query-1")
        self.assertEqual(normalized["duration_sec"], 1.5)
        self.assertEqual(normalized["duration_source"], "history.duration")
        self.assertEqual(normalized["compilation_time_ms"], 11)
        self.assertEqual(normalized["execution_time_ms"], 1200)
        self.assertEqual(normalized["queue_time_ms"], 17)
        self.assertEqual(normalized["result_fetch_time_ms"], 23)
        self.assertEqual(normalized["bytes_read"], 1000)
        self.assertEqual(normalized["files_read"], 8)
        self.assertEqual(normalized["partitions_read"], 4)
        self.assertEqual(normalized["rows_read"], 99)
        self.assertEqual(normalized["rows_produced"], 7)
        self.assertEqual(normalized["read_io_cache_percent"], 25.0)
        self.assertIs(normalized["result_from_cache"], False)

    def test_system_history_duration_aliases_are_normalized(self):
        normalized = normalize_query_history(
            {
                "statement_id": "statement-2",
                "execution_status": "FINISHED",
                "total_duration_ms": 2500,
                "compilation_duration_ms": 100,
                "execution_duration_ms": 2200,
                "result_fetch_duration_ms": 300,
                "waiting_for_compute_duration_ms": 45,
                "read_rows": 10,
                "produced_rows": 2,
                "from_result_cache": False,
                "cache_origin_statement_id": None,
                "read_io_cache_percent": 12.5,
            }
        )
        self.assertEqual(normalized["duration_sec"], 2.5)
        self.assertEqual(normalized["compilation_time_ms"], 100)
        self.assertEqual(normalized["execution_time_ms"], 2200)
        self.assertEqual(normalized["result_fetch_time_ms"], 300)
        self.assertEqual(normalized["queue_time_ms"], 45)
        self.assertEqual(normalized["rows_read"], 10)
        self.assertEqual(normalized["rows_produced"], 2)
        self.assertEqual(normalized["read_io_cache_percent"], 12.5)

    def test_result_cache_true_or_missing_rejects_canonical_result(self):
        for marker, metrics in (
            ("true", {"result_from_cache": True}),
            ("missing", {}),
        ):
            with self.subTest(marker=marker):
                cursor = _FakeMeasuredCursor()
                connection = _FakeMeasuredConnection(cursor)
                history = {
                    "query_id": "query-1",
                    "status": "FINISHED",
                    "is_final": True,
                    "duration": 1000,
                    "metrics": metrics,
                }
                ticks = iter((10.0, 11.5))
                observation = execute_measured_query(
                    connection,
                    _FakeHistoryClient(history),
                    "SELECT 1",
                    query_number=1,
                    iteration=3,
                    workload="dashboard",
                    run_id="run-1",
                    history_timeout=0,
                    history_poll_interval=1,
                    clock=lambda: next(ticks),
                )
                self.assertIsNone(observation["canonical_duration_sec"])
                self.assertEqual(observation["result_row_count"], 2)
                self.assertEqual(cursor.fetch_sizes, [1000, 1000])
                self.assertIsNotNone(observation["result_hash_sha256"])
                self.assertTrue(any("cache" in error for error in observation["errors"]))
                self.assertEqual(
                    cursor.execute_calls[0][1],
                    {
                        "benchmark": "full-path-realtime",
                        "run_id": "run-1",
                        "workload": "dashboard",
                        "query_number": "1",
                        "iteration": "3",
                    },
                )

    def test_exact_false_cache_evidence_accepts_provider_duration(self):
        cursor = _FakeMeasuredCursor()
        ticks = iter((2.0, 2.25))
        observation = execute_measured_query(
            _FakeMeasuredConnection(cursor),
            _FakeHistoryClient(
                {
                    "query_id": "query-1",
                    "status": "FINISHED",
                    "is_final": True,
                    "duration": 750,
                    "metrics": {"result_from_cache": False},
                }
            ),
            "SELECT 1",
            query_number=1,
            iteration=1,
            workload="drilldown",
            run_id="run-2",
            history_timeout=0,
            history_poll_interval=1,
            clock=lambda: next(ticks),
        )
        self.assertEqual(observation["canonical_duration_sec"], 0.75)
        self.assertEqual(observation["client_wall_time_sec"], 0.25)
        self.assertEqual(observation["errors"], [])

    def test_query_history_lookup_accepts_current_query_id_response(self):
        seen = {}

        def opener(request, timeout):
            seen["url"] = request.full_url
            seen["timeout"] = timeout
            return _FakeJsonResponse(
                {
                    "res": [
                        {
                            "query_id": "query-current",
                            "is_final": True,
                            "status": "FINISHED",
                        }
                    ]
                }
            )

        client = DatabricksRestClient(
            "https://dbc-example.cloud.databricks.com",
            token="secret-token",
            opener=opener,
        )
        record = client.query_history_by_statement_id("query-current")
        self.assertEqual(record["query_id"], "query-current")
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(seen["url"]).query)
        self.assertEqual(query["filter_by.statement_ids"], ["query-current"])
        self.assertEqual(query["include_metrics"], ["true"])

    def test_query_history_poll_waits_for_explicit_final_record(self):
        client = _FakeHistoryClient(
            [
                {
                    "query_id": "query-1",
                    "status": "FINISHED",
                    "is_final": False,
                },
                {
                    "query_id": "query-1",
                    "status": "FINISHED",
                    "is_final": True,
                    "duration": 10,
                },
            ]
        )
        now = [0.0]

        def sleep(seconds):
            now[0] += seconds

        record, error = poll_query_history(
            client,
            "query-1",
            timeout=1.0,
            poll_interval=0.1,
            clock=lambda: now[0],
            sleeper=sleep,
        )
        self.assertIsNone(error)
        self.assertIs(record["is_final"], True)
        self.assertEqual(client.ids, ["query-1", "query-1"])


class RunnerSchedulingAndProgressTests(unittest.TestCase):
    def test_fixed_rate_schedule_stays_anchored_and_overruns_start_now(self):
        self.assertEqual(fixed_rate_next_fire(100.0, 10.0, 0), 100.0)
        self.assertEqual(fixed_rate_next_fire(100.0, 10.0, 3), 130.0)
        self.assertEqual(fixed_rate_delay(100.0, 10.0, 2, 115.0), 5.0)
        self.assertEqual(fixed_rate_delay(100.0, 10.0, 2, 125.0), 0.0)

    def test_progress_and_freshness_row_parsing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logical = root / "logical.json"
            fallback = root / "fallback.json"
            freshness = root / "freshness.json"
            logical.write_text(
                json.dumps(
                    {
                        "logical_raw_rows": 123,
                        "provider_committed_rows": 23,
                        "baseline_table_rows": 100,
                        "updated_at": "2026-09-10T00:00:00Z",
                        "run_id": "producer-1",
                    }
                ),
                encoding="utf-8",
            )
            fallback.write_text(
                json.dumps(
                    {
                        "provider_committed_rows": 23,
                        "baseline_table_rows": 100,
                    }
                ),
                encoding="utf-8",
            )
            freshness.write_text(
                json.dumps(
                    {
                        "latest_successful_refresh": {
                            "output_rows": 45,
                            "finished_at": "2026-09-10T00:01:00Z",
                        },
                        "updated_at": "2026-09-10T00:01:01Z",
                    }
                ),
                encoding="utf-8",
            )
            logical_snapshot = producer_progress_snapshot(logical)
            fallback_snapshot = producer_progress_snapshot(fallback)
            freshness_snapshot = freshness_progress_snapshot(freshness)
        self.assertEqual(logical_snapshot["raw_rows"], 123)
        self.assertEqual(
            logical_snapshot["raw_rows_source"],
            "producer_progress.logical_raw_rows",
        )
        self.assertEqual(fallback_snapshot["raw_rows"], 123)
        self.assertIn("provider_committed_rows", fallback_snapshot["raw_rows_source"])
        self.assertIsNone(freshness_snapshot["mv_rows"])
        self.assertEqual(
            freshness_snapshot["mv_rows_source"],
            "not_collected_auxiliary_data_queries_prohibited",
        )
        self.assertEqual(
            freshness_snapshot["refresh_finished_at"],
            "2026-09-10T00:01:00Z",
        )

    def test_result_hash_is_stable_and_order_sensitive(self):
        rows_a = [(1, {"b": 2, "a": 1}), (Decimal("1.20"), b"x")]
        rows_b = [(1, {"a": 1, "b": 2}), (Decimal("1.20"), b"x")]
        rows_reversed = list(reversed(rows_a))
        count_a, hash_a = stable_result_hash(rows_a)
        count_b, hash_b = stable_result_hash(rows_b)
        _, hash_reversed = stable_result_hash(rows_reversed)
        self.assertEqual(count_a, 2)
        self.assertEqual(count_b, 2)
        self.assertEqual(hash_a, hash_b)
        self.assertNotEqual(hash_a, hash_reversed)

    def test_aligned_arrays_preserve_failed_query_position_and_validate(self):
        observations = [
            {
                "canonical_duration_sec": 1.0,
                "client_wall_time_sec": 1.2,
                "statement_id": "q1",
                "result_row_count": 2,
                "result_hash_sha256": "a" * 64,
                "metrics": {
                    "compilation_time_ms": 10,
                    "execution_time_ms": 900,
                    "queue_time_ms": 20,
                    "result_fetch_time_ms": 30,
                    "result_from_cache": False,
                    "read_io_cache_percent": 50,
                },
                "errors": [],
            },
            {
                "canonical_duration_sec": None,
                "client_wall_time_sec": 0.1,
                "statement_id": "q2",
                "result_row_count": None,
                "result_hash_sha256": None,
                "metrics": {},
                "errors": ["failed"],
            },
        ]
        arrays = aligned_metric_arrays(observations)
        self.assertEqual(arrays["result"], [[1.0], [None]])
        self.assertEqual(arrays["statement_ids"], [["q1"], ["q2"]])
        self.assertEqual(arrays["execution_time"], [[0.9], [None]])
        record = {
            "iteration": 1,
            "iteration_started_at": "2026-09-10T00:00:00Z",
            "iteration_finished_at": "2026-09-10T00:00:01Z",
            "raw_rows": 100,
            "mv_rows": 10,
            "system": "Databricks",
            "version": "test",
            "machine": "Small",
            "cluster_size": "1",
            "comment": "(dashboard)",
            "tags": ["managed"],
            **arrays,
            "query_evidence": observations,
            "query_errors": [item["errors"] for item in observations],
        }
        validate_jsonl_record(record, 2)
        malformed = dict(record)
        malformed["result"] = [[1.0]]
        with self.assertRaisesRegex(ValueError, "result"):
            validate_jsonl_record(malformed, 2)

    def test_wrapper_defaults_and_four_plus_two_contract(self):
        dashboard_parser = build_parser(
            run_dashboard.WORKLOAD,
            run_dashboard.DEFAULT_QUERY_FILE,
            run_dashboard.DEFAULT_INTERVAL_SECONDS,
            env={},
        )
        drilldown_parser = build_parser(
            run_drilldown.WORKLOAD,
            run_drilldown.DEFAULT_QUERY_FILE,
            run_drilldown.DEFAULT_INTERVAL_SECONDS,
            env={},
        )
        self.assertEqual(dashboard_parser.parse_args([]).interval, 600.0)
        self.assertEqual(drilldown_parser.parse_args([]).interval, 3600.0)
        self.assertEqual(
            len(load_queries(run_dashboard.DEFAULT_QUERY_FILE, "cat", "schema")),
            4,
        )
        self.assertEqual(
            len(load_queries(run_drilldown.DEFAULT_QUERY_FILE, "cat", "schema")),
            2,
        )
        custom_dashboard = load_queries(
            run_dashboard.DEFAULT_QUERY_FILE,
            "cat",
            "schema",
            mv_table="quotes_daily_custom",
        )
        custom_drilldown = load_queries(
            run_drilldown.DEFAULT_QUERY_FILE,
            "cat",
            "schema",
            raw_table="quotes_raw_custom",
        )
        self.assertTrue(
            all("`quotes_daily_custom`" in query for query in custom_dashboard)
        )
        self.assertTrue(
            all("`quotes_raw_custom`" in query for query in custom_drilldown)
        )
        self.assertEqual(
            SESSION_CONFIGURATION,
            {
                "use_cached_result": "false",
                "ansi_mode": "true",
                "timezone": "UTC",
            },
        )
        self.assertEqual(
            (ROOT / "requirements-runner.txt").read_text(encoding="utf-8").strip(),
            (
                "databricks-sql-connector[kernel]==4.5.0\n"
                "databricks-sdk==0.133.0"
            ),
        )
        self.assertNotIn("databricks", sys.modules)

        wrapper = (ROOT / "run_query_workloads.sh").read_text(encoding="utf-8")
        self.assertIn(".schema // \"rt_qualification\"", wrapper)
        self.assertIn('--schema "$SCHEMA"', wrapper)
        self.assertNotIn("--schema rt_qualification", wrapper)

        producer = (ROOT / "ingest_zerobus.py").read_text(encoding="utf-8")
        signal_handler = producer.split("def request_stop(", 1)[1].split(
            "signal.signal(", 1
        )[0]
        self.assertNotIn("journal.terminal_error", signal_handler)
        self.assertNotIn(
            "get_unacked_batches was not called because close did not succeed",
            producer,
        )

    def test_compact_query_result_removes_only_review_fields(self):
        source = {
            "schema_version": 2,
            "run_id": "run-1",
            "result": [[1.25]],
            "mv_rows": None,
            "mv_rows_source": "not collected",
            "result_row_count": [[1]],
            "result_hash": [["a" * 64]],
            "query_evidence": [{"statement_id": "statement-1"}],
            "rt_warehouse": {"warehouse_id": "warehouse-1"},
            "query_warehouse": {
                "warehouse_id": "warehouse-1",
                "api_metadata": {
                    "id": "warehouse-1",
                    "creator_name": "user@example.com",
                    "creator_id": 1,
                    "channel": {"name": "CURRENT"},
                    "jdbc_url": "jdbc:test",
                    "odbc_params": {"hostname": "example"},
                    "health": {"status": "HEALTHY"},
                },
            },
        }
        compact = compact_query_results.compact_record(source)
        self.assertEqual(compact["schema_version"], 3)
        self.assertEqual(compact["result"], [[1.25]])
        for name in compact_query_results.REMOVED_TOP_LEVEL_FIELDS:
            self.assertNotIn(name, compact)
        self.assertEqual(
            compact["query_warehouse"]["api_metadata"],
            {
                "id": "warehouse-1",
                "health": {"status": "HEALTHY"},
            },
        )
        self.assertEqual(source["schema_version"], 2)
        self.assertIn("query_evidence", source)

    def test_compact_query_file_preserves_full_audit_copy(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "source.jsonl"
            output = root / "output.jsonl"
            audit = root / "audit.jsonl"
            record = {
                "schema_version": 2,
                "result": [[1.0]],
                "query_evidence": [{"statement_id": "statement-1"}],
            }
            source.write_text(json.dumps(record) + "\n")
            report = compact_query_results.compact_file(
                source,
                output,
                audit_output=audit,
            )
            self.assertEqual(audit.read_bytes(), source.read_bytes())
            compact = json.loads(output.read_text())
            self.assertEqual(compact, {"schema_version": 3, "result": [[1.0]]})
            self.assertLess(report["output_size_bytes"], report["source_size_bytes"])


class ConfigAndContractTests(unittest.TestCase):
    def complete_config(self) -> EnvironmentConfig:
        return EnvironmentConfig.from_env(
            {
                "DATABRICKS_HOST": "https://dbc-example.cloud.databricks.com",
                "DATABRICKS_CLOUD": "aws",
                "DATABRICKS_REGION": "us-west-2",
                "DATABRICKS_WORKSPACE_ID": "123456789",
                "DATABRICKS_CATALOG": "bench",
                "DATABRICKS_SCHEMA": "quotes",
                "DATABRICKS_RAW_TABLE": "quotes_raw",
                "DATABRICKS_MV_TABLE": "quotes_daily",
                "DATABRICKS_RT_WAREHOUSE_ID": "abc123",
                "DATABRICKS_CONTROL_WAREHOUSE_ID": "def456",
            }
        )

    def test_complete_config_constructs_endpoint_and_validates_online(self):
        config = self.complete_config()
        self.assertEqual(
            config.zerobus_endpoint,
            "https://123456789.zerobus.us-west-2.cloud.databricks.com",
        )
        self.assertEqual(validate_config(config, online=True)["errors"], [])

    def test_online_config_fails_closed_when_required_values_are_missing(self):
        config = EnvironmentConfig.from_env({})
        errors = validate_config(config, online=True)["errors"]
        self.assertTrue(any("host is required" in error for error in errors))
        self.assertTrue(any("workspace ID is required" in error for error in errors))
        self.assertTrue(any("warehouse ID is required" in error for error in errors))

    def test_partial_service_principal_environment_is_an_offline_error(self):
        report = offline_checks(
            self.complete_config(),
            {"DATABRICKS_CLIENT_ID": "client-without-secret"},
        )
        self.assertTrue(
            any("must be set together" in error for error in report["errors"])
        )

    def test_offline_contract_requires_four_dashboard_and_two_drilldown_queries(self):
        self.assertEqual(
            EXPECTED_QUERY_COUNTS,
            {"dashboard": 4, "drilldown": 2},
        )
        with tempfile.TemporaryDirectory() as tmp:
            fixture_dir = Path(tmp)
            (fixture_dir / "queries_mv.sql").write_text(
                "\n".join(f"SELECT {number};" for number in range(4)),
                encoding="utf-8",
            )
            (fixture_dir / "queries_raw.sql").write_text(
                "SELECT 1; SELECT 2;",
                encoding="utf-8",
            )
            (fixture_dir / "create.sql").write_text(
                "CREATE TABLE __CATALOG__.__SCHEMA__.quotes (id BIGINT);"
                "CREATE MATERIALIZED VIEW __CATALOG__.__SCHEMA__.quotes_daily "
                "AS SELECT count(*) AS n FROM __CATALOG__.__SCHEMA__.quotes;",
                encoding="utf-8",
            )
            report = offline_checks(
                self.complete_config(),
                {
                    "DATABRICKS_TOKEN": "test-token",
                    "DATABRICKS_CLIENT_ID": "test-client",
                    "DATABRICKS_CLIENT_SECRET": "test-secret",
                },
                script_dir=fixture_dir,
            )
        self.assertEqual(
            report["query_contract"]["actual"],
            {"dashboard": 4, "drilldown": 2},
        )
        self.assertFalse(
            any("SQL statements" in error for error in report["errors"])
        )
        self.assertTrue(report["ddl_contract"]["create_materialized_view_found"])

    def test_preflight_extracts_one_complete_rendered_mv_create_statement(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "create.sql"
            path.write_text(
                "-- semicolon in comment ;\n"
                "CREATE TABLE __CATALOG__.__SCHEMA__.raw (id BIGINT);\n"
                "CREATE MATERIALIZED VIEW __CATALOG__.__SCHEMA__.mv\n"
                "AS SELECT ';' AS marker, count(*) AS n\n"
                "FROM __CATALOG__.__SCHEMA__.raw;\n",
                encoding="utf-8",
            )
            statement = load_create_materialized_view_statement(
                path,
                "catalog-name",
                "schema name",
            )
            self.assertTrue(statement.startswith("CREATE MATERIALIZED VIEW"))
            self.assertIn("`catalog-name`.`schema name`.mv", statement)
            self.assertIn("FROM `catalog-name`.`schema name`.raw", statement)
            self.assertIn("';'", statement)

            path.write_text(
                "CREATE MATERIALIZED VIEW a AS SELECT 1;"
                "CREATE MATERIALIZED VIEW b AS SELECT 2;",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "exactly one"):
                load_create_materialized_view_statement(path, "catalog", "schema")

    def test_checked_in_queries_match_current_four_plus_two_contract(self):
        report = offline_checks(
            self.complete_config(),
            {
                "DATABRICKS_TOKEN": "test-token",
                "DATABRICKS_CLIENT_ID": "test-client",
                "DATABRICKS_CLIENT_SECRET": "test-secret",
            },
        )
        self.assertEqual(
            report["query_contract"]["actual"],
            {"dashboard": 4, "drilldown": 2},
        )
        self.assertFalse(
            any("SQL statements" in error for error in report["errors"])
        )
        self.assertTrue(report["ddl_contract"]["create_materialized_view_found"])

    def test_preflight_requires_effective_predictive_optimization_enablement(self):
        errors = []
        enabled = predictive_optimization_check(
            "catalog.schema.quotes",
            {
                "enable_predictive_optimization": "INHERIT",
                "effective_predictive_optimization_flag": {
                    "value": "ENABLE",
                    "inherited_from_type": "SCHEMA",
                    "inherited_from_name": "catalog.schema",
                },
            },
            errors,
        )
        self.assertEqual(enabled["status"], "ok")
        self.assertTrue(enabled["effectively_enabled"])
        self.assertEqual(enabled["configured_value"], "INHERIT")
        self.assertEqual(enabled["inherited_from_type"], "SCHEMA")
        self.assertEqual(errors, [])

        for metadata, observed in (
            (
                {
                    "enable_predictive_optimization": "INHERIT",
                    "effective_predictive_optimization_flag": {
                        "value": "DISABLE"
                    },
                },
                "DISABLE",
            ),
            ({}, "missing"),
        ):
            with self.subTest(observed=observed):
                errors = []
                disabled = predictive_optimization_check(
                    "catalog.schema.quotes",
                    metadata,
                    errors,
                )
                self.assertEqual(disabled["status"], "error")
                self.assertFalse(disabled["effectively_enabled"])
                self.assertTrue(any(observed in error for error in errors))

    def test_preflight_target_contract_is_metadata_only_and_fail_closed(self):
        def response(columns, rows, statement_id):
            return {
                "statement_id": statement_id,
                "manifest": {
                    "schema": {
                        "columns": [{"name": name} for name in columns]
                    }
                },
                "result": {"data_array": rows},
            }

        mv_metadata = {
            "owner": "application-id",
            "predictive_optimization": "ENABLE",
            "table_properties": {
                "clusteringColumns": '[["sym"],["day"]]'
            },
            "refresh_information": {
                "refresh_policy": "IncrementalStrict",
                "refresh_schedule": (
                    "TRIGGER ON UPDATE AT MOST EVERY INTERVAL 60 SECOND"
                ),
            },
        }
        responses = [
            response(
                ["format", "numFiles", "sizeInBytes"],
                [["delta", "0", "0"]],
                "detail",
            ),
            response(["version", "operation"], [["0", "CREATE TABLE"]], "history"),
            response(
                ["json_metadata"],
                [[json.dumps(mv_metadata)]],
                "mv-metadata",
            ),
        ]
        errors = []
        with patch("preflight._statement", side_effect=responses):
            report = preflight.target_contract_check(
                object(),
                SimpleNamespace(
                    control_warehouse_id="control",
                    catalog="costbench",
                    schema="schema",
                    raw_table="quotes",
                    mv_table="quotes_daily",
                ),
                raw_metadata={
                    "properties": {
                        "clusteringColumns": '[["sym"],["t"]]',
                        "delta.enableDeletionVectors": "true",
                        "delta.enableRowTracking": "true",
                        "delta.enableChangeDataFeed": "true",
                    }
                },
                expected_mv_owner="application-id",
                timeout=10,
                poll_interval=0.1,
                errors=errors,
            )
        self.assertEqual(report["status"], "ok")
        self.assertTrue(all(report["checks"].values()))
        self.assertEqual(errors, [])

    def test_checked_in_ddl_matches_managed_delta_contract(self):
        ddl = (ROOT / "create.sql").read_text(encoding="utf-8")
        statements = split_sql_statements(ddl)

        self.assertEqual(len(statements), 4)
        self.assertTrue(statements[0].startswith("DROP MATERIALIZED VIEW"))
        self.assertTrue(statements[1].startswith("DROP TABLE"))
        self.assertEqual(ddl.count("__CATALOG__.__SCHEMA__"), 5)
        self.assertNotIn("workspace.benchmarking", ddl)
        self.assertNotIn("PARTITIONED BY", ddl.upper())
        self.assertEqual(ddl.upper().count("USING DELTA"), 2)
        self.assertIn("CLUSTER BY (sym, t)", ddl)
        self.assertIn("CLUSTER BY (sym, day)", ddl)
        self.assertIn("REFRESH POLICY INCREMENTAL STRICT", ddl)
        self.assertIn(
            "TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE",
            ddl,
        )
        for feature in (
            "delta.enableDeletionVectors",
            "delta.enableRowTracking",
            "delta.enableChangeDataFeed",
        ):
            with self.subTest(feature=feature):
                self.assertIn(f"'{feature}' = 'true'", ddl)

        table_columns = re.search(
            r"CREATE TABLE\s+\S+\s*\((.*?)\)\s*USING DELTA",
            statements[2],
            flags=re.DOTALL,
        )
        self.assertIsNotNone(table_columns)
        self.assertEqual(
            [
                " ".join(line.strip().rstrip(",").split())
                for line in table_columns.group(1).splitlines()
                if line.strip()
            ],
            [
                "sym STRING",
                "bx SMALLINT",
                "bp DOUBLE",
                "bs BIGINT",
                "ax SMALLINT",
                "ap DOUBLE",
                "`as` BIGINT",
                "c SMALLINT",
                "i ARRAY<SMALLINT>",
                "t BIGINT",
                "q BIGINT",
                "z SMALLINT",
            ],
        )

    def test_canonical_setup_sql_is_fresh_explicit_and_metadata_only(self):
        sql = (
            ROOT / "create_full_serverless_baseline_r3_20260918.sql"
        ).read_text(encoding="utf-8")
        statements = split_sql_statements(sql)
        rendered = "\n".join(statements).upper()
        self.assertEqual(len(statements), 18)
        self.assertNotIn("DROP ", rendered)
        self.assertNotIn("IF NOT EXISTS", rendered)
        self.assertIn("rt_full_serverless_baseline_r3_20260918", sql)
        self.assertNotIn("CREATE TABLE", statements[0].upper())
        self.assertIn("CREATE TABLE", statements[3].upper())
        self.assertNotIn(" LIKE ", statements[3].upper())
        self.assertTrue(statements[5].upper().startswith("EXPLAIN CREATE"))
        self.assertIn("SET OWNER TO", statements[8].upper())
        self.assertIn("DESCRIBE DETAIL", rendered)
        self.assertIn("DESCRIBE HISTORY", rendered)
        self.assertNotRegex(
            rendered,
            r"SELECT\s+COUNT\s*\(\s*\*\s*\)\s+FROM",
        )

    def test_s3_source_staging_fails_closed_on_canonical_row_count(self):
        source = (ROOT / "stage_source_from_s3.sh").read_text(encoding="utf-8")
        self.assertIn("aws s3 sync", source)
        self.assertIn("113_219_565_734", source)
        self.assertIn("quotes_0.parquet is outside", source)
        self.assertIn("schema_variant_count", source)
        self.assertNotIn("--delete", source)

    def test_offline_imports_do_not_require_optional_dependencies(self):
        with patch.dict(
            os.environ,
            {
                "DATABRICKS_TOKEN": "",
                "DATABRICKS_CLIENT_ID": "",
                "DATABRICKS_CLIENT_SECRET": "",
            },
            clear=False,
        ):
            report = offline_checks(EnvironmentConfig.from_env({}), {})
        self.assertIn("packages", report)
        self.assertIn("databricks.sql", report["packages"])
        self.assertIn("databricks.sdk", report["packages"])
        self.assertIn("pyarrow", report["packages"])
        self.assertIn("zerobus.sdk", report["packages"])
        self.assertEqual(
            PACKAGE_CONTRACT["zerobus.sdk"]["distribution"],
            "databricks-zerobus-ingest-sdk",
        )

    def test_package_checks_use_role_specific_interpreters(self):
        checks = package_checks(
            runner_python="/missing/runner-python",
            zerobus_python="/missing/zerobus-python",
        )
        self.assertEqual(
            checks["databricks.sql"]["interpreter"],
            "/missing/runner-python",
        )
        self.assertEqual(
            checks["databricks.sdk"]["interpreter"],
            "/missing/runner-python",
        )
        self.assertEqual(
            checks["pyarrow"]["interpreter"],
            "/missing/zerobus-python",
        )
        self.assertEqual(
            checks["zerobus.sdk"]["interpreter"],
            "/missing/zerobus-python",
        )


class ZerobusProducerModelTests(unittest.TestCase):
    def test_producer_does_not_serialize_arrow_for_byte_accounting(self):
        source = (ROOT / "ingest_zerobus.py").read_text(encoding="utf-8")
        self.assertNotIn("arrow_ipc_nbytes", source)
        self.assertIn("buffer_bytes = int(normalized.nbytes)", source)

    def test_completed_ordinal_ranges_update_incrementally(self):
        ranges = []
        for ordinal in (2, 0, 1, 5, 4, 3, 3):
            ranges = add_ordinal_range(ranges, ordinal)
        self.assertEqual(ranges, [[0, 5]])

    @staticmethod
    def _statement_response(statement_id, columns, row):
        return {
            "statement_id": statement_id,
            "manifest": {
                "schema": {"columns": [{"name": name} for name in columns]}
            },
            "result": {"data_array": [row]},
        }

    @staticmethod
    def _target_check_args():
        return SimpleNamespace(
            host="https://example.cloud.databricks.com",
            token="",
            client_id="client-id",
            client_secret="client-secret",
            statement_timeout=30.0,
            count_warehouse="warehouse-id",
            catalog="costbench",
            schema="rt_qualification",
            table="quotes",
            allow_nonempty_table=False,
        )

    def test_target_protection_uses_delta_metadata_not_count(self):
        calls = []

        class Client:
            def execute_statement(_self, statement, *_args, **_kwargs):
                calls.append(statement)
                if statement.startswith("DESCRIBE DETAIL"):
                    return self._statement_response(
                        "detail-id", ["numFiles", "sizeInBytes"], ["0", "0"]
                    )
                return self._statement_response(
                    "history-id", ["version"], ["0"]
                )

        with patch("ingest_zerobus.DatabricksRestClient", return_value=Client()):
            report = protect_target_table(
                self._target_check_args(), resume_journal=None
            )

        self.assertTrue(report["fresh"])
        self.assertEqual(report["row_count"], 0)
        self.assertEqual(report["method"], "delta_metadata")
        self.assertEqual(report["history_versions"], [0])
        self.assertEqual(len(calls), 2)
        self.assertFalse(any("COUNT(" in statement.upper() for statement in calls))

    def test_target_protection_rejects_non_pristine_history(self):
        class Client:
            def execute_statement(_self, statement, *_args, **_kwargs):
                if statement.startswith("DESCRIBE DETAIL"):
                    return self._statement_response(
                        "detail-id", ["numFiles", "sizeInBytes"], ["0", "0"]
                    )
                return {
                    "statement_id": "history-id",
                    "manifest": {
                        "schema": {"columns": [{"name": "version"}]}
                    },
                    "result": {"data_array": [["1"], ["0"]]},
                }

        with patch("ingest_zerobus.DatabricksRestClient", return_value=Client()):
            with self.assertRaisesRegex(RuntimeError, "not a pristine"):
                protect_target_table(
                    self._target_check_args(), resume_journal=None
                )

    def make_tasks(self, root):
        return [
            Task(0, 0, root / "a.parquet", "a.parquet", 0, 3),
            Task(1, 0, root / "a.parquet", "a.parquet", 1, 2),
        ]

    def make_manifest(self):
        return {
            "manifest_sha256": "a" * 64,
            "total_rows": 5,
            "selected_file_count": 1,
            "selected_row_group_count": 2,
        }

    def make_config(self):
        return {"workers": 1, "target_rows_per_sec": 1_000_000.0}

    def make_target(self):
        return {
            "catalog": "workspace",
            "schema": "benchmarking",
            "table": "quotes",
            "full_name": "workspace.benchmarking.quotes",
            "host": "https://example.cloud.databricks.com",
            "zerobus_endpoint": "https://123.zerobus.us-west-2.cloud.databricks.com",
        }

    def test_importing_producer_does_not_import_optional_packages(self):
        self.assertNotIn("pyarrow", sys.modules)
        self.assertNotIn("zerobus", sys.modules)
        self.assertNotIn("zerobus.sdk", sys.modules)

    def test_target_schema_and_normalization_are_exact_safe_and_columnar(self):
        schema = target_schema(_FakeArrow)
        self.assertEqual(
            schema.names,
            ["sym", "bx", "bp", "bs", "ax", "ap", "as", "c", "i", "t", "q", "z"],
        )
        self.assertTrue(all(field.nullable for field in schema))
        self.assertEqual(schema[0].type.name, "large_utf8")
        self.assertEqual(schema[8].type.name, "list")
        self.assertEqual(schema[8].type.value_type.name, "int16")

        source_names = list(reversed(schema.names)) + ["ignored"]
        source_fields = [
            _FakeField(name, _FakeType("source"), True) for name in source_names
        ]
        source_schema = _FakeSchema(source_fields)
        source_arrays = [
            _FakeArray(_FakeType("source"), name) for name in source_names
        ]
        source_batch = _FakeRecordBatch(source_arrays, source_schema)
        compute = _FakeCompute()
        normalized = normalize_batch(
            source_batch,
            pa_module=_FakeArrow,
            pc_module=compute,
        )
        self.assertEqual(normalized.schema.names, schema.names)
        self.assertEqual(
            [marker for marker, _type in normalized.arrays],
            schema.names,
        )
        self.assertTrue(all(safe for _marker, _type, safe in compute.casts))
        self.assertEqual(len(compute.casts), 12)

    def test_normalization_explicitly_rejects_unsigned_overflow(self):
        schema = target_schema(_FakeArrow)
        arrays = [
            _FakeArray(field.type, field.name)
            for field in schema
        ]
        arrays[schema.get_field_index("bx")] = _FakeArray(
            _FakeType("uint16", bit_width=16),
            "bx",
        )
        batch = _FakeRecordBatch(arrays, schema)
        with self.assertRaisesRegex(OverflowError, "do not fit"):
            normalize_batch(
                batch,
                pa_module=_FakeArrow,
                pc_module=_FakeCompute(maxima={"bx": 65_535}),
            )

    def test_source_file_order_and_exact_sample_exclusion(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in (
                "quotes_2.parquet",
                "quotes_0.parquet",
                "quotes_00.parquet",
                "quotes_1.parquet",
            ):
                (root / name).touch()
            selected, excluded = discover_source_files(root, "quotes_*.parquet")
            self.assertEqual(
                [path.name for path in selected],
                ["quotes_00.parquet", "quotes_1.parquet", "quotes_2.parquet"],
            )
            self.assertEqual(excluded, ["quotes_0.parquet"])
            included, excluded = discover_source_files(
                root,
                "quotes_*.parquet",
                include_quotes_0=True,
                max_files=2,
            )
            self.assertEqual(
                [path.name for path in included],
                ["quotes_0.parquet", "quotes_00.parquet"],
            )
            self.assertEqual(excluded, [])

    def test_manifest_can_cap_qualification_source_at_exact_row_count(self):
        class Metadata:
            num_row_groups = 2

            @staticmethod
            def row_group(index):
                return type("RowGroup", (), {"num_rows": (6, 7)[index]})()

        class FakeParquetFile:
            metadata = Metadata()

        class FakeParquet:
            @staticmethod
            def ParquetFile(_path):
                return FakeParquetFile()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "quotes_1.parquet").write_bytes(b"test")
            tasks, manifest = enumerate_tasks_and_manifest(
                root,
                "*.parquet",
                FakeParquet,
                include_quotes_0=False,
                max_files=None,
                max_row_groups=None,
                max_rows=10,
            )
            _, deferred = enumerate_tasks_and_manifest(
                root,
                "*.parquet",
                FakeParquet,
                include_quotes_0=False,
                max_files=None,
                max_row_groups=None,
                max_rows=10,
                hash_files=False,
            )
        self.assertEqual([task.rows for task in tasks], [6, 4])
        self.assertEqual(manifest["total_rows"], 10)
        self.assertEqual(manifest["max_rows"], 10)
        self.assertEqual(manifest["files"][0]["selected_row_group_rows"], [6, 4])
        self.assertIsNone(deferred["files"][0]["sha256"])
        self.assertEqual(deferred["files"][0]["sha256_status"], "deferred")

    def test_batch_coordinates_are_canonical_and_bounded(self):
        task = Task(7, 2, Path("/tmp/q.parquet"), "q.parquet", 3, 100)
        coordinate = BatchCoordinate.from_task(
            task,
            batch_ordinal=4,
            row_offset=75,
            row_count=25,
        )
        self.assertEqual(
            coordinate.as_dict(),
            {
                "task_ordinal": 7,
                "file_ordinal": 2,
                "file": "q.parquet",
                "row_group": 3,
                "batch_ordinal": 4,
                "row_offset": 75,
                "row_count": 25,
            },
        )
        with self.assertRaisesRegex(ValueError, "beyond"):
            BatchCoordinate.from_task(
                task,
                batch_ordinal=5,
                row_offset=99,
                row_count=2,
            )

    def test_rate_limiter_never_catches_up_after_idle_time(self):
        now = [0.0]
        limiter = RateLimiter(100.0, clock=lambda: now[0])
        self.assertEqual(limiter.reserve(100), 0.0)
        now[0] = 10.0
        self.assertEqual(limiter.reserve(100), 0.0)
        self.assertEqual(limiter.reserve(100), 1.0)
        self.assertEqual(limiter.snapshot()["scheduled_rows"], 300)
        self.assertTrue(limiter.snapshot()["no_catch_up"])

    def test_compact_ingest_metrics_omit_repeated_runtime_state(self):
        compact = compact_metrics_payload(
            {
                "run_id": "run-1",
                "updated_at": "2026-09-10T00:01:00Z",
                "elapsed_sec": 60,
                "source": {"total_rows": 1000},
                "provider_committed_rows": 900,
                "submitted_rows": 950,
                "pending_rows": 50,
                "completed_tasks": 9,
                "errors": [],
                "stream_status": {
                    "one": {"state": "open", "rotation_count": 2},
                    "two": {"state": "closed", "rotation_count": 3},
                },
                "eps": {
                    "target_rows_per_sec": 100,
                    "average_provider_committed_rows_per_sec": 15,
                    "session_provider_committed_rows_per_sec": 15,
                },
                "memory": {"process_rss_bytes": 123},
                "host": {"producer_process_cpu_cores": 1.5},
                "rate_limiter": {"scheduled_rows": 1000},
                "pending_batch_evidence": [{"large": "value"}],
                "config": {"repeated": "value"},
            }
        )
        self.assertEqual(compact["provider_committed_rows"], 900)
        self.assertEqual(compact["streams"]["rotation_count_total"], 5)
        self.assertEqual(compact["streams"]["states"], {"open": 1, "closed": 1})
        self.assertEqual(compact["producer"]["process_rss_bytes"], 123)
        self.assertNotIn("pending_batch_evidence", compact)
        self.assertNotIn("config", compact)

    def test_compact_evidence_ledger_keeps_only_lifecycle_events(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            tasks = self.make_tasks(root)
            journal = ProgressJournal(
                progress_path=root / "progress.json",
                ledger_path=root / "evidence.jsonl",
                metrics_path=root / "metrics.jsonl",
                run_id="test-run",
                manifest=self.make_manifest(),
                target=self.make_target(),
                config={**self.make_config(), "compact_evidence": True},
                tasks=tasks,
                baseline_table_rows=0,
                baseline_unknown=False,
                secrets=(),
            )
            journal.record_assignment(tasks[0], 1, "worker-01")
            journal.record_transport_wait(
                "worker-01",
                generation=2,
                operation="wait",
                duration_sec=3.5,
                status="completed",
                pending_batches=1,
                pending_rows=3,
            )
            events = [
                json.loads(line)["event"]
                for line in (root / "evidence.jsonl").read_text().splitlines()
            ]
            self.assertEqual(events, ["run_started", "transport_wait"])
            recovery = journal.snapshot()["transport_recovery"]
            self.assertEqual(recovery["completed_count"], 1)
            self.assertEqual(recovery["failed_count"], 0)
            self.assertEqual(recovery["max_duration_sec"], 3.5)

    def test_compression_names_map_to_exact_sdk_enum_members(self):
        class FakeCompression:
            NONE = object()
            LZ4_FRAME = object()
            ZSTD = object()

        self.assertIs(map_ipc_compression("none", FakeCompression), FakeCompression.NONE)
        self.assertIs(
            map_ipc_compression("lz4-frame", FakeCompression),
            FakeCompression.LZ4_FRAME,
        )
        self.assertIs(map_ipc_compression("ZSTD", FakeCompression), FakeCompression.ZSTD)
        with self.assertRaisesRegex(ValueError, "one of"):
            map_ipc_compression("gzip", FakeCompression)

    def test_expected_row_gate_requires_explicit_partial_mode(self):
        validate_expected_rows(113_219_565_734, allow_partial=False)
        with self.assertRaisesRegex(RuntimeError, "--allow-partial"):
            validate_expected_rows(10, allow_partial=False)
        validate_expected_rows(10, allow_partial=True)

    def test_submitted_batches_are_not_counted_until_durable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            tasks = self.make_tasks(root)
            journal = ProgressJournal(
                progress_path=root / "progress.json",
                ledger_path=root / "evidence.jsonl",
                metrics_path=root / "metrics.jsonl",
                run_id="test-run",
                manifest=self.make_manifest(),
                target=self.make_target(),
                config=self.make_config(),
                tasks=tasks,
                baseline_table_rows=0,
                baseline_unknown=False,
                secrets=(),
            )
            coordinate = BatchCoordinate.from_task(
                tasks[0],
                batch_ordinal=0,
                row_offset=0,
                row_count=3,
            )
            pending = PendingBatch(
                coordinate=coordinate,
                rows=3,
                uncompressed_arrow_buffer_bytes=123,
                logical_offset=42,
                report_offset=42,
                submitted_at="2026-09-10T00:00:00.000Z",
                worker=1,
                stream="worker-01-arrow",
            )
            journal.record_submitted(pending)
            submitted = journal.snapshot()
            self.assertEqual(submitted["submitted_rows"], 3)
            self.assertEqual(submitted["provider_committed_rows"], 0)
            self.assertEqual(submitted["completed_tasks"], 0)
            self.assertEqual(submitted["pending_batches"], 1)

            journal.record_durable([pending])
            durable = journal.snapshot()
            self.assertEqual(durable["provider_committed_rows"], 3)
            self.assertEqual(
                durable["durable_uncompressed_arrow_buffer_bytes"],
                123,
            )
            self.assertEqual(durable["completed_task_ordinals"], [0])
            self.assertEqual(durable["completed_task_ranges"], [[0, 0]])
            self.assertEqual(durable["pending_batches"], 0)
            events = [
                json.loads(line)["event"]
                for line in (root / "evidence.jsonl").read_text().splitlines()
            ]
            self.assertLess(
                events.index("batch_submitted"),
                events.index("batch_durable"),
            )

    def test_failed_checkpoint_records_transport_duration_and_keeps_pending(self):
        class FailedStream:
            def wait_for_offset(self, _offset):
                raise RuntimeError("remote reset")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            tasks = self.make_tasks(root)
            journal = ProgressJournal(
                progress_path=root / "progress.json",
                ledger_path=root / "evidence.jsonl",
                metrics_path=root / "metrics.jsonl",
                run_id="test-run",
                manifest=self.make_manifest(),
                target=self.make_target(),
                config={**self.make_config(), "compact_evidence": True},
                tasks=tasks,
                baseline_table_rows=0,
                baseline_unknown=False,
                secrets=(),
            )
            pending = PendingBatch(
                coordinate=BatchCoordinate.from_task(
                    tasks[0],
                    batch_ordinal=0,
                    row_offset=0,
                    row_count=3,
                ),
                rows=3,
                uncompressed_arrow_buffer_bytes=123,
                logical_offset=1,
                report_offset=1,
                submitted_at="2026-09-10T00:00:00.000Z",
                worker=1,
                stream="worker-01-arrow",
            )
            journal.record_submitted(pending)
            batches = [pending]
            with patch("sys.stderr", new=io.StringIO()):
                with self.assertRaisesRegex(RuntimeError, "remote reset"):
                    checkpoint_pending(
                        FailedStream(),
                        batches,
                        journal,
                        "wait",
                        stream_label="worker-01-arrow",
                        generation=7,
                        log_threshold_seconds=2,
                    )
            self.assertEqual(batches, [pending])
            recovery = journal.snapshot()["transport_recovery"]
            self.assertEqual(recovery["failed_count"], 1)
            self.assertEqual(recovery["last_event"]["generation"], 7)
            self.assertEqual(recovery["last_event"]["operation"], "wait")
            self.assertIn(
                "remote reset", recovery["last_event"]["error"]
            )

    def test_only_clean_whole_task_checkpoint_can_resume(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            tasks = self.make_tasks(root)
            manifest = self.make_manifest()
            target = self.make_target()
            config = self.make_config()
            journal = ProgressJournal(
                progress_path=root / "progress.json",
                ledger_path=root / "evidence.jsonl",
                metrics_path=root / "metrics.jsonl",
                run_id="test-run",
                manifest=manifest,
                target=target,
                config=config,
                tasks=tasks,
                baseline_table_rows=0,
                baseline_unknown=False,
                secrets=(),
            )
            coordinate = BatchCoordinate.from_task(
                tasks[0],
                batch_ordinal=0,
                row_offset=0,
                row_count=3,
            )
            pending = PendingBatch(
                coordinate=coordinate,
                rows=3,
                uncompressed_arrow_buffer_bytes=123,
                logical_offset=42,
                report_offset=42,
                submitted_at="2026-09-10T00:00:00.000Z",
                worker=1,
                stream="worker-01-arrow",
            )
            journal.record_submitted(pending)
            journal.record_durable([pending])
            journal.stream_update(
                "worker-01-arrow",
                worker=1,
                stream="worker-01-arrow",
                close_succeeded=True,
                unacked_inspection_succeeded=True,
                unacked_batches=0,
            )
            clean = journal.finish(stopped_early=True)
            self.assertFalse(clean["finished"])
            self.assertTrue(clean["clean_checkpoint"])
            self.assertEqual(
                validate_resume_journal(
                    clean,
                    manifest=manifest,
                    target=target,
                    config=config,
                    tasks=tasks,
                ),
                {0},
            )
            compact = dict(clean)
            compact.pop("completed_task_ordinals")
            self.assertEqual(
                validate_resume_journal(
                    compact,
                    manifest=manifest,
                    target=target,
                    config=config,
                    tasks=tasks,
                ),
                {0},
            )

            unclean = dict(clean)
            unclean["safe_to_resume"] = False
            unclean["terminal_error"] = "ambiguous close"
            with self.assertRaisesRegex(RuntimeError, "fresh target table"):
                validate_resume_journal(
                    unclean,
                    manifest=manifest,
                    target=target,
                    config=config,
                    tasks=tasks,
                )


class CostLedgerTests(unittest.TestCase):
    SINCE = "2026-09-10T00:00:00Z"
    UNTIL = "2026-09-10T00:10:00Z"

    def price(self, sku, value, **pricing):
        return {
            "sku_name": sku,
            "cloud": "AWS",
            "usage_unit": "DBU",
            "price_start_time": "2026-09-01T00:00:00Z",
            "price_end_time": None,
            "currency_code": "USD",
            "pricing_json": json.dumps({"default": value, **pricing}),
        }

    def usage(self, sku, quantity, product, metadata=None):
        return {
            "record_id": f"record-{sku}",
            "sku_name": sku,
            "cloud": "AWS",
            "usage_unit": "DBU",
            "usage_quantity": str(quantity),
            "record_type": "ORIGINAL",
            "usage_start_time": self.SINCE,
            "usage_end_time": self.UNTIL,
            "billing_origin_product": product,
            "usage_metadata_json": json.dumps(metadata or {}),
            "product_features_json": "{}",
        }

    def test_effective_list_default_extraction_and_price_match_fail_closed(self):
        extracted = summarize_run.extract_canonical_list_price(
            json.dumps(
                {
                    "default": "2.00",
                    "effective_list": {"default": "3.00"},
                    "promotional": {"default": "0.25"},
                    "contract": "0.10",
                }
            )
        )
        self.assertEqual(extracted["price"], Decimal("3.00"))
        usage = self.usage("RT", 1, "SQL", {"warehouse_id": "rt"})
        prices = [self.price("RT", "2"), self.price("RT", "2")]
        self.assertEqual(
            summarize_run.match_list_price(usage, prices)["status"],
            "ambiguous",
        )
        promo_only = dict(self.price("RT", "2"))
        promo_only["pricing_json"] = json.dumps(
            {"promotional": {"default": "0.25"}, "contract": "0.10"}
        )
        unpriced = summarize_run.match_list_price(usage, [promo_only])
        self.assertEqual(unpriced["status"], "unpriced")
        self.assertIsNone(unpriced["price"])
        earlier = self.price("RT", "1")
        earlier["price_start_time"] = "2026-08-01T00:00:00Z"
        earlier["price_end_time"] = "2026-09-01T00:00:00Z"
        effective = summarize_run.match_list_price(usage, [earlier, self.price("RT", "2")])
        self.assertEqual(effective["status"], "matched")
        self.assertEqual(effective["price"], Decimal("2"))

    def test_union_overlap_does_not_double_count_concurrent_queries(self):
        overlap = summarize_run.union_overlap_seconds(
            self.SINCE,
            self.UNTIL,
            [
                ("2026-09-10T00:01:00Z", "2026-09-10T00:05:00Z"),
                ("2026-09-10T00:03:00Z", "2026-09-10T00:07:00Z"),
                ("2026-09-10T00:06:00Z", "2026-09-10T00:08:00Z"),
            ],
        )
        self.assertEqual(overlap, 7 * 60)

    def test_post_ingest_query_cost_is_reported_separately(self):
        summary = summarize_run.build_cost_summary(
            billing_usage=[
                self.usage("RT", 10, "SQL", {"warehouse_id": "rt"})
            ],
            billing_list_prices=[self.price("RT", "1")],
            query_history=[
                {
                    "execution_status": "FINISHED",
                    "warehouse_id": "rt",
                    "start_time": "2026-09-10T00:01:00Z",
                    "end_time": "2026-09-10T00:02:00Z",
                    "query_tags_json": json.dumps({"run_id": "run"}),
                    "error_message": None,
                },
                {
                    "execution_status": "FINISHED",
                    "warehouse_id": "rt",
                    "start_time": "2026-09-10T00:06:00Z",
                    "end_time": "2026-09-10T00:07:00Z",
                    "query_tags_json": json.dumps({"run_id": "run"}),
                    "error_message": None,
                },
            ],
            mv_event_log=[],
            predictive_operations=[],
            measured_since=self.SINCE,
            measured_until=self.UNTIL,
            producer_finished_at="2026-09-10T00:05:00Z",
            rt_warehouse_id="rt",
            control_warehouse_id="control",
        )
        self.assertEqual(
            summary["primary_category_totals"]["primary_rt_serving_compute"][
                "amount"
            ],
            "1",
        )
        self.assertEqual(
            summary["secondary_category_totals"][
                "secondary_post_ingest_query_serving"
            ]["amount"],
            "1",
        )
        self.assertEqual(
            summary["excluded_category_totals"]["excluded_idle_or_minimum"][
                "amount"
            ],
            "8",
        )

    def test_category_classification_uses_target_identifiers(self):
        cases = (
            (
                self.usage("ZB", 1, "ZEROBUS_INGEST"),
                "primary_zerobus_ingest",
            ),
            (
                {
                    **self.usage("JOBS_SERVERLESS", 1, "LAKEFLOW_CONNECT"),
                    "product_features_json": json.dumps(
                        {
                            "lakeflow_connect": {
                                "zerobus_request_type": "GRPC"
                            }
                        }
                    ),
                },
                "primary_zerobus_ingest",
            ),
            (
                self.usage("MV", 1, "PIPELINES", {"pipeline_id": "mv-pipe"}),
                "primary_mv_refresh",
            ),
            (
                self.usage("PO", 1, "PREDICTIVE_OPTIMIZATION"),
                "primary_predictive_optimization",
            ),
            (
                self.usage("RT", 1, "SQL", {"warehouse_id": "rt"}),
                "primary_rt_serving_compute",
            ),
            (
                self.usage("STORAGE", 1, "STORAGE"),
                "excluded_storage",
            ),
            (
                self.usage("CONTROL", 1, "SQL", {"warehouse_id": "control"}),
                "excluded_control",
            ),
        )
        for row, expected in cases:
            with self.subTest(expected=expected):
                category, reason = summarize_run.classify_usage(
                    row,
                    rt_warehouse_id="rt",
                    control_warehouse_id="control",
                    mv_pipeline_ids={"mv-pipe"},
                )
                self.assertEqual(category, expected)
                self.assertTrue(reason)

    def test_beta_discount_applies_only_to_rt_and_leaves_canonical_unchanged(self):
        usage = [
            self.usage("RT", 10, "SQL", {"warehouse_id": "rt"}),
            self.usage("ZEROBUS", 3, "ZEROBUS_INGEST"),
        ]
        prices = [self.price("RT", "2"), self.price("ZEROBUS", "1")]
        summary = summarize_run.build_cost_summary(
            billing_usage=usage,
            billing_list_prices=prices,
            query_history=[
                {
                    "execution_status": "FINISHED",
                    "warehouse_id": "rt",
                    "query_tags_json": '{"run_id":"run-1"}',
                    "start_time": "2026-09-10T00:00:00Z",
                    "end_time": "2026-09-10T00:04:00Z",
                },
                {
                    "execution_status": "FINISHED",
                    "warehouse_id": "rt",
                    "query_tags_json": '{"run_id":"run-1"}',
                    "start_time": "2026-09-10T00:02:00Z",
                    "end_time": "2026-09-10T00:06:00Z",
                },
            ],
            mv_event_log=[],
            predictive_operations=[],
            measured_since=self.SINCE,
            measured_until=self.UNTIL,
            rt_warehouse_id="rt",
            control_warehouse_id="control",
        )
        self.assertTrue(summary["complete"])
        self.assertEqual(
            summary["primary_category_totals"][
                "primary_rt_serving_compute"
            ]["amount"],
            "12",
        )
        canonical = summary["canonical_undiscounted_primary_total"]
        self.assertEqual(canonical["amount"], "15")
        self.assertEqual(
            summary["canonical_undiscounted_fresh_path_total"]["amount"], "3"
        )
        self.assertEqual(
            summary["canonical_undiscounted_query_serving_total"]["amount"], "12"
        )
        scenario = summary["lakehouse_rt_beta_30_percent_off_scenario"]
        self.assertEqual(scenario["scenario_primary_total"]["amount"], "11.4")
        self.assertEqual(
            scenario["canonical_undiscounted_primary_total"],
            canonical,
        )
        self.assertTrue(scenario["canonical_total_unchanged"])

    def test_record_types_retain_exported_signed_quantities(self):
        rows = []
        for record_type, quantity in (
            ("ORIGINAL", "5"),
            ("RETRACTION", "-2"),
            ("RESTATEMENT", "2"),
        ):
            row = self.usage("ZEROBUS", quantity, "ZEROBUS_INGEST")
            row["record_id"] = record_type.lower()
            row["record_type"] = record_type
            rows.append(row)
        summary = summarize_run.build_cost_summary(
            billing_usage=rows,
            billing_list_prices=[self.price("ZEROBUS", "1")],
            query_history=[],
            mv_event_log=[],
            predictive_operations=[],
            measured_since=self.SINCE,
            measured_until=self.UNTIL,
            rt_warehouse_id="rt",
            control_warehouse_id="control",
        )
        self.assertEqual(
            summary["record_type_signed_quantities"],
            {"ORIGINAL": "5", "RESTATEMENT": "2", "RETRACTION": "-2"},
        )
        self.assertEqual(
            summary["canonical_undiscounted_primary_total"]["amount"],
            "5",
        )


class RunValidationTests(unittest.TestCase):
    EXPECTED = 100
    SINCE = "2026-09-10T00:00:00Z"
    UNTIL = "2026-09-10T01:00:00Z"

    def manifest(self):
        return {
            "total_rows": self.EXPECTED,
            "quotes_0_parquet_included": False,
            "excluded_files": ["quotes_0.parquet"],
            "files": [
                {
                    "path": "quotes_1.parquet",
                    "file_ordinal": 0,
                    "selected_rows": self.EXPECTED,
                }
            ],
            "tasks": [{"ordinal": 0, "rows": self.EXPECTED}],
            "selected_file_count": 1,
            "selected_row_group_count": 1,
            "manifest_sha256": "a" * 64,
        }

    def progress(self):
        return {
            "source": {
                "total_rows": self.EXPECTED,
                "task_count": 1,
                "manifest_sha256": "a" * 64,
            },
            "config": {"workers": 1},
            "provider_committed_rows": self.EXPECTED,
            "logical_raw_rows": self.EXPECTED,
            "submitted_rows": self.EXPECTED,
            "finished": True,
            "running": False,
            "clean_checkpoint": True,
            "ambiguous": False,
            "terminal_error": None,
            "errors": [],
            "pending_batches": 0,
            "pending_rows": 0,
            "partial_task_rows": {},
            "baseline_table_rows": 0,
            "baseline_table_rows_unknown": False,
            "completed_tasks": 1,
            "completed_task_ordinals": [0],
            "stream_status": {
                "worker-1": {
                    "close_succeeded": True,
                    "unacked_inspection_succeeded": True,
                    "unacked_batches": 0,
                }
            },
            "eps": {
                "average_provider_committed_rows_per_sec": 100,
            },
        }

    def query_record(self, workload, count, at):
        observations = [
            {
                "canonical_duration_sec": 1.0,
                "statement_id": f"{workload}-{number}",
                "execution_succeeded": True,
                "result_row_count": 1,
                "result_hash_sha256": f"{number:x}" * 64,
                "metrics": {
                    "result_from_cache": False,
                    "cache_origin_statement_id": None,
                },
                "errors": [],
            }
            for number in range(1, count + 1)
        ]
        interval = 600.0 if workload == "dashboard" else 3600.0
        return {
            "runner": workload,
            "workload": workload,
            "iteration": 1,
            "scheduled_start_at": at,
            "scheduled_interval_sec": interval,
            "iteration_started_at": at,
            "iteration_finished_at": (
                "2026-09-10T00:03:21Z"
                if workload == "dashboard"
                else "2026-09-10T00:04:01Z"
            ),
            "raw_rows": 50,
            "producer_progress_finished": False,
            "producer_progress_error": None,
            "rt_warehouse": {"id": "rt", "size": "Medium"},
            "result": [[1.0] for _ in observations],
            "compilation_time": [[0.1] for _ in observations],
            "execution_time": [[0.8] for _ in observations],
            "queue_time": [[0.0] for _ in observations],
            "result_fetch_time": [[0.1] for _ in observations],
            "client_wall_time": [[1.1] for _ in observations],
            "statement_ids": [
                [observation["statement_id"]] for observation in observations
            ],
            "cache_hit": [[False] for _ in observations],
            "read_io_cache_percent": [[0.0] for _ in observations],
            "result_row_count": [[1] for _ in observations],
            "result_hash": [
                [observation["result_hash_sha256"]]
                for observation in observations
            ],
            "query_evidence": observations,
            "query_errors": [[] for _ in observations],
        }

    def evidence(self):
        return {
            "complete": True,
            "collection_window": {
                "since": self.SINCE,
                "until": self.UNTIL,
            },
            "required_dataset_completeness": {"complete": True},
            "provider_committed_records": self.EXPECTED,
            "provider_errors": 0,
            "zerobus_stream_error_count": 0,
            "table_num_rows": {"raw": self.EXPECTED},
            "mv_maintenance": {
                "maintenance_types": [
                    "MAINTENANCE_TYPE_GROUP_AGGREGATE"
                ],
                "full_refresh_count": 0,
            },
            "targets": {
                "rt_warehouse_id": "rt",
                "control_warehouse_id": "control",
            },
            "frozen_warehouse_configs": [
                {
                    "warehouse_id": "rt",
                    "change_time": "2026-09-09T00:00:00Z",
                    "delete_time": None,
                },
                {
                    "warehouse_id": "control",
                    "change_time": "2026-09-09T00:00:00Z",
                    "delete_time": None,
                },
            ],
        }

    def cost(self):
        canonical = {
            "currency": "USD",
            "amount": "10",
            "amount_by_currency": {"USD": "10"},
        }
        return {
            "complete": True,
            "completeness": {"complete": True},
            "canonical_undiscounted_primary_total": canonical,
            "canonical_undiscounted_fresh_path_total": {
                "currency": "USD",
                "amount": "6",
                "amount_by_currency": {"USD": "6"},
            },
            "canonical_undiscounted_query_serving_total": {
                "currency": "USD",
                "amount": "4",
                "amount_by_currency": {"USD": "4"},
            },
            "unknown_total": {
                "currency": "USD",
                "amount": "0",
                "amount_by_currency": {"USD": "0"},
            },
            "unpriced_total": {
                "usage_row_indices": [],
                "usage_quantity_by_unit": {},
            },
            "lakehouse_rt_beta_30_percent_off_scenario": {
                "discount_rate": "0.30",
                "applies_only_to": "primary_rt_serving_compute",
                "canonical_total_unchanged": True,
                "canonical_undiscounted_primary_total": canonical,
            },
            "predictive_optimization": {
                "operation_count": 1,
                "failed_operation_count": 0,
                "failed_operation_row_indices": [],
            },
        }

    def valid_inputs(self):
        return {
            "source_manifest": self.manifest(),
            "ingest_progress": self.progress(),
            "ingest_metrics": [
                {
                    "provider_committed_rows": self.EXPECTED,
                    "elapsed_sec": 1,
                    "interval_eps": 100,
                }
            ],
            "dashboard": [
                self.query_record("dashboard", 4, "2026-09-10T00:03:20Z")
            ],
            "drilldown": [
                self.query_record("drilldown", 2, "2026-09-10T00:04:00Z")
            ],
            "freshness": [
                {
                    "observed_at": "2026-09-10T00:03:00Z",
                    "raw_commit_age_sec": 5,
                    "mv_refresh_age_sec": 30,
                    "errors": [],
                    "age_semantics": (
                        "Observational ages are not exact per-record lag."
                    ),
                }
            ],
            "evidence_summary": self.evidence(),
            "cost_summary": self.cost(),
            "expected_rows": self.EXPECTED,
            "eps_min": 90,
            "eps_max": 110,
        }

    def test_valid_synthetic_complete_run_passes(self):
        report = validate_run.build_validation_report(**self.valid_inputs())
        self.assertTrue(report["accepted"])
        self.assertTrue(all(gate["passed"] for gate in report["gates"]))
        self.assertEqual(report["query_samples"]["dashboard"]["accepted_count"], 1)
        self.assertEqual(report["query_samples"]["drilldown"]["accepted_count"], 1)

    def test_warehouse_comparison_ignores_runtime_state_but_detects_config_change(self):
        inputs = self.valid_inputs()
        stable = {
            "warehouse_id": "rt",
            "use_kernel": True,
            "api_metadata": {
                "id": "rt",
                "cluster_size": "Medium",
                "min_num_clusters": 1,
                "max_num_clusters": 1,
            },
        }
        inputs["dashboard"][0]["rt_warehouse"] = {
            **stable,
            "captured_at": "2026-09-10T00:00:00Z",
            "api_metadata": {**stable["api_metadata"], "state": "STARTING"},
        }
        inputs["drilldown"][0]["rt_warehouse"] = {
            **stable,
            "captured_at": "2026-09-10T00:01:00Z",
            "api_metadata": {**stable["api_metadata"], "state": "RUNNING"},
        }
        report = validate_run.build_validation_report(**inputs)
        warehouse_gate = next(
            gate
            for gate in report["gates"]
            if gate["id"] == "warehouse_configuration"
        )
        self.assertTrue(warehouse_gate["passed"])

        inputs["drilldown"][0]["rt_warehouse"]["api_metadata"][
            "cluster_size"
        ] = "Large"
        report = validate_run.build_validation_report(**inputs)
        warehouse_gate = next(
            gate
            for gate in report["gates"]
            if gate["id"] == "warehouse_configuration"
        )
        self.assertFalse(warehouse_gate["passed"])

    def test_missing_evidence_and_reconciliation_fail(self):
        inputs = self.valid_inputs()
        inputs["input_errors"] = {"cost_summary": ["cost_summary: missing"]}
        inputs["evidence_summary"]["provider_committed_records"] = 99
        report = validate_run.build_validation_report(**inputs)
        failed = {
            gate["id"] for gate in report["gates"] if not gate["passed"]
        }
        self.assertIn("input_availability", failed)
        self.assertIn("row_reconciliation", failed)
        self.assertIn("cost_completeness", failed)

    def test_cache_hit_and_post_ingest_samples_fail(self):
        for failure in ("cache", "post_ingest"):
            with self.subTest(failure=failure):
                inputs = self.valid_inputs()
                sample = inputs["dashboard"][0]
                if failure == "cache":
                    sample["query_evidence"][0]["metrics"][
                        "result_from_cache"
                    ] = True
                    sample["cache_hit"][0] = [True]
                else:
                    sample["producer_progress_finished"] = True
                    sample["raw_rows"] = self.EXPECTED
                report = validate_run.build_validation_report(**inputs)
                dashboard_gate = next(
                    gate
                    for gate in report["gates"]
                    if gate["id"] == "dashboard_queries"
                )
                self.assertFalse(dashboard_gate["passed"])

    def test_full_refresh_cadence_and_freshness_failures(self):
        cases = ("full_refresh", "cadence", "freshness")
        for failure in cases:
            with self.subTest(failure=failure):
                inputs = self.valid_inputs()
                if failure == "full_refresh":
                    inputs["evidence_summary"]["mv_maintenance"] = {
                        "maintenance_types": [
                            "MAINTENANCE_TYPE_COMPLETE_RECOMPUTE"
                        ],
                        "full_refresh_count": 1,
                    }
                elif failure == "cadence":
                    second = self.query_record(
                        "dashboard",
                        4,
                        "2026-09-10T00:15:00Z",
                    )
                    second["iteration"] = 2
                    second["iteration_finished_at"] = (
                        "2026-09-10T00:15:01Z"
                    )
                    inputs["dashboard"].append(second)
                else:
                    inputs["freshness"][0]["mv_refresh_age_sec"] = 301
                report = validate_run.build_validation_report(**inputs)
                self.assertFalse(report["accepted"])

    def test_post_ingest_idle_age_is_excluded_from_freshness_sla(self):
        inputs = self.valid_inputs()
        inputs["ingest_progress"]["finished_at"] = "2026-09-10T00:30:00Z"
        inputs["freshness"].append(
            {
                "observed_at": "2026-09-10T00:45:00Z",
                "raw_commit_age_sec": 900,
                "mv_refresh_age_sec": 900,
                "mv_rows_behind": 0,
                "errors": [],
                "age_semantics": (
                    "Observational ages are not exact per-record lag."
                ),
            }
        )
        report = validate_run.build_validation_report(**inputs)
        freshness_gate = next(
            gate for gate in report["gates"] if gate["id"] == "freshness"
        )
        self.assertTrue(freshness_gate["passed"])
        self.assertEqual(
            freshness_gate["evidence"]["active_measurement_until"],
            "2026-09-10T00:30:00.000Z",
        )
        self.assertEqual(freshness_gate["evidence"]["accepted_indices"], [0])


class QualificationTests(unittest.TestCase):
    def make_plan(self, root):
        return qualify.build_plan(
            output_dir=root / "qualification",
            repo_dir=ROOT.parents[2],
            data_dir=root / "parquet",
            catalog="workspace",
            schema="rt_qualification",
            raw_table="quotes",
            mv_table="quotes_daily",
            rt_warehouse_id="rt-warehouse",
            control_warehouse_id="control-warehouse",
            producer_host_description="dedicated host in benchmark region",
            producer_network_capacity_gbps=12.5,
            host="https://dbc-example.cloud.databricks.com",
            endpoint="https://123.zerobus.us-west-2.cloud.databricks.com",
            stream_count=16,
            batch_size=50_000,
            queue_capacity=64,
            compression="NONE",
            query_size_candidates=("Small", "Medium"),
            python="/runner/python",
            producer_python="/zerobus/python",
        )

    def test_plan_uses_separate_runner_and_zerobus_interpreters(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan = self.make_plan(Path(tmp))
        self.assertEqual(
            plan["inputs"]["interpreters"],
            {
                "runner": "/runner/python",
                "zerobus_producer": "/zerobus/python",
            },
        )
        for stage in ("correctness", "capacity", "query_size_sweep", "endurance"):
            for case in plan["matrix"][stage]:
                for command in case["commands"]:
                    if command["step"] == "zerobus-ingest":
                        self.assertEqual(command["argv"][0], "/zerobus/python")
                    elif command["step"] == "online-preflight":
                        self.assertEqual(command["argv"][0], "/runner/python")
                        self.assertIn("/zerobus/python", command["argv"])

    @staticmethod
    def write_json(path, value):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf-8")

    @staticmethod
    def write_jsonl(path, rows):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "".join(json.dumps(row) + "\n" for row in rows),
            encoding="utf-8",
        )

    @staticmethod
    def stage_cases(plan, stage):
        return plan["matrix"][stage]

    def progress(self, case, target_eps):
        expected = case["acceptance"]["expected_rows"]
        return {
            "source": {"total_rows": expected, "task_count": 1},
            "target": {
                "host": "https://dbc-example.cloud.databricks.com",
                "zerobus_endpoint": (
                    "https://123.zerobus.us-west-2.cloud.databricks.com"
                ),
            },
            "config": {
                "workers": 16,
                "batch_size": 50_000,
                "queue_capacity": 64,
                "ipc_compression": "NONE",
                "target_rows_per_sec": target_eps,
            },
            "provider_committed_rows": expected,
            "logical_raw_rows": expected,
            "submitted_rows": expected,
            "finished": True,
            "running": False,
            "clean_checkpoint": True,
            "safe_to_resume": True,
            "stopped_early": False,
            "ambiguous": False,
            "terminal_error": None,
            "errors": [],
            "pending_batches": 0,
            "pending_rows": 0,
            "partial_task_rows": {},
            "baseline_table_rows": 0,
            "baseline_table_rows_unknown": False,
            "completed_tasks": 1,
            "completed_task_ordinals": [0],
            "stream_status": {
                f"worker-{number:02d}-arrow": {
                    "close_succeeded": True,
                    "unacked_inspection_succeeded": True,
                    "unacked_batches": 0,
                }
                for number in range(1, 17)
            },
            "eps": {
                "average_provider_committed_rows_per_sec": target_eps,
            },
        }

    @staticmethod
    def preflight(size):
        return {
            "config": {"rt_warehouse_id": "rt-warehouse"},
            "summary": {"status": "ok"},
            "online": {
                "status": "ok",
                "errors": [],
                "predictive_optimization": {
                    table: {
                        "status": "ok",
                        "effective_value": "ENABLE",
                        "effectively_enabled": True,
                    }
                    for table in ("raw", "materialized_view")
                },
                "warehouses": {
                    "lakehouse_rt": {
                        "metadata": {
                            "id": "rt-warehouse",
                            "cluster_size": size,
                            "min_num_clusters": 1,
                            "max_num_clusters": 1,
                            "enable_serverless_compute": True,
                        }
                    }
                },
            },
        }

    @staticmethod
    def query_record(workload, size):
        count = 4 if workload == "dashboard" else 2
        evidence = [
            {
                "execution_succeeded": True,
                "canonical_duration_sec": 1.0,
                "errors": [],
                "metrics": {
                    "status": "FINISHED",
                    "result_from_cache": False,
                    "cache_origin_statement_id": None,
                    "waiting_at_capacity_ms": 0,
                    "waiting_for_compute_ms": 0,
                    "queue_time_ms": 0,
                },
            }
            for _ in range(count)
        ]
        return {
            "workload": workload,
            "rt_warehouse": {
                "warehouse_id": "rt-warehouse",
                "api_metadata": {
                    "cluster_size": size,
                }
            },
            "producer_progress_error": None,
            "result": [[1.0] for _ in range(count)],
            "cache_hit": [[False] for _ in range(count)],
            "query_evidence": evidence,
            "query_errors": [[] for _ in range(count)],
        }

    def write_valid_evidence(self, plan):
        correctness = self.stage_cases(plan, "correctness")[0]
        self.write_json(
            correctness["artifacts"]["validation_report"],
            {
                "accepted": True,
                "configuration": {"expected_rows": 10_000_000},
            },
        )

        for case in self.stage_cases(plan, "capacity"):
            target = case["acceptance"]["target_committed_eps"]
            self.write_json(
                case["artifacts"]["ingest_progress"],
                self.progress(case, target),
            )
            self.write_jsonl(
                case["artifacts"]["ingest_metrics"],
                [
                    {
                        "provider_committed_rows": target * (index + 1) * 5,
                        "elapsed_sec": (index + 1) * 5,
                        "interval_eps": target,
                        "host": {
                            "host_cpu_utilization_percent": 40,
                            "network_receive_bytes_per_second": 10_000_000,
                            "network_transmit_bytes_per_second": 20_000_000,
                        },
                        "errors": [],
                    }
                    for index in range(12)
                ],
            )
            self.write_json(case["artifacts"]["preflight"], self.preflight("Small"))

        for case in self.stage_cases(plan, "query_size_sweep"):
            size = case["acceptance"]["query_size"]
            self.write_json(case["artifacts"]["preflight"], self.preflight(size))
            self.write_json(
                case["artifacts"]["ingest_progress"],
                self.progress(
                    case,
                    case["acceptance"]["target_committed_eps"],
                ),
            )
            self.write_jsonl(
                case["artifacts"]["dashboard"],
                [self.query_record("dashboard", size)],
            )
            self.write_jsonl(
                case["artifacts"]["drilldown"],
                [self.query_record("drilldown", size)],
            )

        endurance = self.stage_cases(plan, "endurance")[0]
        self.write_json(
            endurance["artifacts"]["ingest_progress"],
            self.progress(endurance, 1_000_000),
        )
        self.write_jsonl(
            endurance["artifacts"]["ingest_metrics"],
            [
                {
                    "provider_committed_rows": 1_000_000 * (index + 1) * 5,
                    "elapsed_sec": (index + 1) * 5,
                    "interval_eps": 1_000_000,
                    "host": {
                        "host_cpu_utilization_percent": 40,
                        "network_receive_bytes_per_second": 10_000_000,
                        "network_transmit_bytes_per_second": 20_000_000,
                    },
                    "errors": [],
                }
                for index in range(12)
            ],
        )
        self.write_json(endurance["artifacts"]["preflight"], self.preflight("Small"))
        self.write_json(
            endurance["artifacts"]["validation_report"],
            {
                "accepted": True,
                "window": {
                    "duration_seconds": endurance["acceptance"][
                        "minimum_duration_seconds"
                    ]
                },
                "query_samples": {
                    "dashboard": {
                        "accepted_count": endurance["acceptance"][
                            "minimum_accepted_dashboard_iterations"
                        ]
                    },
                    "drilldown": {
                        "accepted_count": endurance["acceptance"][
                            "minimum_accepted_drilldown_iterations"
                        ]
                    },
                },
                "freshness_samples": {"accepted_count": 1},
                "gates": [
                    {
                        "id": "freshness",
                        "passed": True,
                    }
                ],
            },
        )

    def test_plan_is_deterministic_complete_and_contains_no_secret_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.dict(
                os.environ,
                {
                    "DATABRICKS_TOKEN": "dapi1234567890abcdef",
                    "DATABRICKS_CLIENT_SECRET": "super-secret-value",
                },
                clear=False,
            ):
                first = self.make_plan(root)
                second = self.make_plan(root)
                plan_path, command_path = qualify.write_plan(
                    first, root / "qualification"
                )
            rendered = json.dumps(first) + command_path.read_text(encoding="utf-8")
        self.assertEqual(first, second)
        self.assertEqual(plan_path.name, "qualification_plan.json")
        self.assertNotIn("dapi1234567890abcdef", rendered)
        self.assertNotIn("super-secret-value", rendered)
        self.assertIn("DATABRICKS_CLIENT_SECRET", rendered)
        self.assertEqual(first["state"], "planned")
        self.assertEqual(
            [case["id"] for case in first["matrix"]["capacity"]],
            ["capacity-100k", "capacity-500k", "capacity-1m"],
        )
        planned_cases = [
            case
            for stage in ("correctness", "capacity", "query_size_sweep", "endurance")
            for case in first["matrix"][stage]
        ]
        self.assertEqual(
            {case["target"]["schema"] for case in planned_cases},
            {"rt_qualification"},
        )
        self.assertTrue(
            first["execution_rules"]["shared_dedicated_qualification_schema"]
        )
        self.assertTrue(first["execution_rules"]["cases_must_run_sequentially"])
        self.assertEqual(
            first["matrix"]["correctness"][0]["acceptance"]["expected_rows"],
            10_000_000,
        )
        self.assertEqual(
            first["matrix"]["endurance"][0]["acceptance"][
                "drilldown_interval_seconds"
            ],
            3600,
        )
        self.assertEqual(
            first["matrix"]["endurance"][0]["acceptance"][
                "minimum_duration_seconds"
            ],
            1800,
        )
        self.assertEqual(
            first["matrix"]["endurance"][0]["acceptance"][
                "minimum_accepted_dashboard_iterations"
            ],
            3,
        )
        self.assertEqual(
            first["matrix"]["endurance"][0]["acceptance"][
                "minimum_accepted_drilldown_iterations"
            ],
            1,
        )

    def test_missing_artifacts_are_not_run_and_never_qualified(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = qualify.evaluate_plan(self.make_plan(Path(tmp)))
        self.assertEqual(report["state"], "not_run")
        self.assertFalse(report["qualified"])
        self.assertEqual(report["stages"]["correctness"]["state"], "not_run")
        self.assertEqual(report["stages"]["capacity"]["state"], "not_run")
        self.assertEqual(report["stages"]["query_size_sweep"]["state"], "not_run")
        self.assertEqual(report["stages"]["endurance"]["state"], "not_run")

    def test_valid_matrix_qualifies_and_selects_smallest_size(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan = self.make_plan(Path(tmp))
            self.write_valid_evidence(plan)
            report = qualify.evaluate_plan(plan)
        self.assertTrue(report["qualified"])
        self.assertEqual(report["state"], "passed")
        self.assertEqual(report["selected_query_size"], "Small")
        self.assertEqual(
            report["frozen_settings"]["producer"]["stream_count"],
            16,
        )

    def test_capacity_short_or_out_of_tolerance_fails(self):
        for failure in ("short", "out_of_tolerance"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                plan = self.make_plan(Path(tmp))
                self.write_valid_evidence(plan)
                case = self.stage_cases(plan, "capacity")[0]
                target = case["acceptance"]["target_committed_eps"]
                count = 5 if failure == "short" else 12
                interval_eps = target if failure == "short" else target * 2
                self.write_jsonl(
                    case["artifacts"]["ingest_metrics"],
                    [
                        {
                            "provider_committed_rows": target * (index + 1) * 5,
                            "elapsed_sec": (index + 1) * 5,
                            "interval_eps": interval_eps,
                            "errors": [],
                        }
                        for index in range(count)
                    ],
                )
                report = qualify.evaluate_plan(plan)
            self.assertEqual(report["stages"]["capacity"]["state"], "failed")
            self.assertFalse(report["qualified"])

    def test_capacity_rejects_saturated_producer_host(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan = self.make_plan(Path(tmp))
            self.write_valid_evidence(plan)
            case = self.stage_cases(plan, "capacity")[-1]
            metrics_path = Path(case["artifacts"]["ingest_metrics"])
            metrics = [
                json.loads(line)
                for line in metrics_path.read_text(encoding="utf-8").splitlines()
            ]
            for row in metrics[1:]:
                row["host"]["host_cpu_utilization_percent"] = 99
            self.write_jsonl(metrics_path, metrics)
            report = qualify.evaluate_plan(plan)
        self.assertEqual(report["stages"]["capacity"]["state"], "failed")
        self.assertTrue(
            any(
                "producer host CPU p95" in reason
                for reason in report["stages"]["capacity"]["cases"][-1]["reasons"]
            )
        )

    def test_qualification_rejects_preflight_without_predictive_optimization(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan = self.make_plan(Path(tmp))
            self.write_valid_evidence(plan)
            case = self.stage_cases(plan, "query_size_sweep")[0]
            preflight_path = Path(case["artifacts"]["preflight"])
            preflight = json.loads(preflight_path.read_text(encoding="utf-8"))
            preflight["online"]["predictive_optimization"]["raw"][
                "effective_value"
            ] = "DISABLE"
            preflight["online"]["predictive_optimization"]["raw"][
                "effectively_enabled"
            ] = False
            preflight["online"]["predictive_optimization"]["raw"]["status"] = "error"
            self.write_json(preflight_path, preflight)
            report = qualify.evaluate_plan(plan)
        rejected = report["stages"]["query_size_sweep"]["cases"][0]
        self.assertEqual(rejected["state"], "failed")
        self.assertTrue(
            any("Predictive Optimization enabled for raw" in reason for reason in rejected["reasons"])
        )

    def test_queue_or_cache_failure_rejects_size(self):
        for failure in ("queue", "cache"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                plan = self.make_plan(Path(tmp))
                self.write_valid_evidence(plan)
                case = self.stage_cases(plan, "query_size_sweep")[0]
                record = self.query_record("dashboard", "Small")
                metrics = record["query_evidence"][0]["metrics"]
                if failure == "queue":
                    metrics["waiting_at_capacity_ms"] = 1
                    metrics["queue_time_ms"] = 1
                else:
                    metrics["result_from_cache"] = True
                self.write_jsonl(case["artifacts"]["dashboard"], [record])
                report = qualify.evaluate_plan(plan)
            first = report["stages"]["query_size_sweep"]["cases"][0]
            self.assertEqual(first["state"], "failed")
            self.assertEqual(report["selected_query_size"], "Medium")

    def test_endurance_duration_counts_and_config_drift_fail(self):
        for failure in ("duration", "dashboard_count", "drilldown_count", "drift"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp:
                plan = self.make_plan(Path(tmp))
                self.write_valid_evidence(plan)
                endurance = self.stage_cases(plan, "endurance")[0]
                validation_path = Path(
                    endurance["artifacts"]["validation_report"]
                )
                validation = json.loads(validation_path.read_text(encoding="utf-8"))
                if failure == "duration":
                    validation["window"]["duration_seconds"] = 1799
                elif failure == "dashboard_count":
                    validation["query_samples"]["dashboard"]["accepted_count"] = 2
                elif failure == "drilldown_count":
                    validation["query_samples"]["drilldown"]["accepted_count"] = 0
                else:
                    progress_path = Path(endurance["artifacts"]["ingest_progress"])
                    progress = json.loads(progress_path.read_text(encoding="utf-8"))
                    progress["config"]["batch_size"] = 25_000
                    self.write_json(progress_path, progress)
                if failure != "drift":
                    self.write_json(validation_path, validation)
                report = qualify.evaluate_plan(plan)
            self.assertEqual(report["stages"]["endurance"]["state"], "failed")
            self.assertFalse(report["qualified"])


class PublicationTests(unittest.TestCase):
    def write_json(self, path, value):
        path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")

    def write_jsonl(self, path, values):
        path.write_text(
            "".join(json.dumps(value) + "\n" for value in values),
            encoding="utf-8",
        )

    def query_record(self, workload, rows, start, finish):
        query_count = 4 if workload == "dashboard" else 2
        evidence = [
            {
                "canonical_duration_sec": float(index + 1),
                "statement_id": f"{workload}-{rows}-{index}",
                "metrics": {
                    "result_from_cache": False,
                    "cache_origin_statement_id": None,
                },
                "execution_succeeded": True,
                "result_row_count": index,
                "result_hash_sha256": f"{index + 1:064x}",
                "errors": [],
            }
            for index in range(query_count)
        ]
        return {
            "schema_version": 1,
            "workload": workload,
            "iteration": rows,
            "iteration_started_at": start,
            "iteration_finished_at": finish,
            "raw_rows": rows,
            "producer_progress_finished": False,
            "result": [
                [observation["canonical_duration_sec"]]
                for observation in evidence
            ],
            "cache_hit": [[False] for _ in evidence],
            "query_evidence": evidence,
            "query_errors": [[] for _ in evidence],
        }

    def clickhouse_record(self, workload, rows, start, finish):
        count = 4 if workload == "dashboard" else 2
        return {
            "iteration": rows,
            "iteration_started_at": start,
            "iteration_finished_at": finish,
            "raw_rows": rows,
            "system": "ClickHouse Cloud",
            "result": [[0.5 + index / 10] for index in range(count)],
        }

    def make_fixture(self, root, *, include_above_cap=True):
        dashboard_path = root / "dashboard.jsonl"
        drilldown_path = root / "drilldown.jsonl"
        freshness_path = root / "freshness.jsonl"
        cost_path = root / "cost_summary.json"
        validation_path = root / "validation_report.json"
        ch_dashboard_path = root / "clickhouse_dashboard.jsonl"
        ch_drilldown_path = root / "clickhouse_drilldown.jsonl"
        ch_ingest_path = root / "clickhouse_ingest.json"

        dashboard = [
            self.query_record(
                "dashboard",
                90_000_000_000,
                "2026-01-01T00:01:00Z",
                "2026-01-01T00:03:00Z",
            )
        ]
        freshness = [
            {
                "observed_at": "2026-01-01T00:02:00Z",
                "mv_refresh_age_sec": 30,
                "producer_progress": {"logical_raw_rows": 90_000_000_000},
                "latest_successful_refresh": {
                    "finished_at": "2026-01-01T00:01:30Z"
                },
                "errors": [],
            }
        ]
        if include_above_cap:
            dashboard.append(
                self.query_record(
                    "dashboard",
                    101_000_000_000,
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:06:00Z",
                )
            )
            freshness.append(
                {
                    "observed_at": "2026-01-01T00:05:30Z",
                    "mv_refresh_age_sec": 40,
                    "producer_progress": {
                        "logical_raw_rows": 101_000_000_000
                    },
                    "latest_successful_refresh": {
                        "finished_at": "2026-01-01T00:04:50Z"
                    },
                    "errors": [],
                }
            )
        drilldown = [
            self.query_record(
                "drilldown",
                91_000_000_000,
                "2026-01-01T00:02:00Z",
                "2026-01-01T00:04:00Z",
            )
        ]
        self.write_jsonl(dashboard_path, dashboard)
        self.write_jsonl(drilldown_path, drilldown)
        self.write_jsonl(freshness_path, freshness)
        self.write_jsonl(
            ch_dashboard_path,
            [
                self.clickhouse_record(
                    "dashboard",
                    89_000_000_000,
                    "2026-02-01T00:00:00Z",
                    "2026-02-01T00:01:00Z",
                ),
                self.clickhouse_record(
                    "dashboard",
                    92_000_000_000,
                    "2026-02-01T00:10:00Z",
                    "2026-02-01T00:11:00Z",
                ),
            ],
        )
        self.write_jsonl(
            ch_drilldown_path,
            [
                self.clickhouse_record(
                    "drilldown",
                    90_500_000_000,
                    "2026-02-01T00:02:00Z",
                    "2026-02-01T00:03:00Z",
                )
            ],
        )
        cost = {
            "complete": True,
            "completeness": {"complete": True},
            "canonical_undiscounted_primary_total": {
                "currency": "USD",
                "amount": "12",
            },
            "canonical_undiscounted_fresh_path_total": {
                "currency": "USD",
                "amount": "10",
            },
            "canonical_undiscounted_query_serving_total": {
                "currency": "USD",
                "amount": "2",
            },
            "primary_category_totals": {},
            "matched_lines": [
                {
                    "usage_row_index": 7,
                    "category": "primary_rt_serving_compute",
                    "currency": "USD",
                    "list_cost": "2",
                    "measured_interval": {
                        "start": "2026-01-01T00:00:00Z",
                        "end": "2026-01-01T00:10:00Z",
                    },
                    "allocation": {"numerator_microseconds": 600_000_000},
                }
            ],
            "query_serving_allocation": {
                "method": "union_overlap",
                "successful_query_union_intervals": [
                    {
                        "start": "2026-01-01T00:00:00Z",
                        "end": "2026-01-01T00:10:00Z",
                    }
                ],
            },
            "lakehouse_rt_beta_30_percent_off_scenario": {
                "discount_rate": "0.30",
                "applies_only_to": "primary_rt_serving_compute",
                "canonical_total_unchanged": True,
                "canonical_undiscounted_primary_total": {
                    "currency": "USD",
                    "amount": "12",
                },
            },
        }
        self.write_json(cost_path, cost)
        validation = {
            "accepted": True,
            "configuration": {"expected_rows": publish_results.EXPECTED_ROWS},
            "gates": [
                {
                    "id": "input_availability",
                    "evidence": {
                        "paths": {
                            "dashboard": str(dashboard_path),
                            "drilldown": str(drilldown_path),
                            "freshness": str(freshness_path),
                            "cost_summary": str(cost_path),
                        }
                    },
                }
            ],
            "query_samples": {
                "dashboard": {
                    "accepted_indices": list(range(len(dashboard)))
                },
                "drilldown": {"accepted_indices": [0]},
            },
            "freshness_samples": {
                "accepted_indices": list(range(len(freshness)))
            },
        }
        self.write_json(validation_path, validation)
        self.write_json(
            ch_ingest_path,
            {
                "system": "ClickHouse Cloud",
                "rows_ingested": publish_results.EXPECTED_ROWS,
                "costs": [
                    {
                        "tier": "Enterprise",
                        "compute_price_per_8gib_hour": 0.4,
                        "total_compute_cost_usd": 20,
                    }
                ],
            },
        )
        return {
            "validation_report_path": validation_path,
            "cost_summary_path": cost_path,
            "dashboard_path": dashboard_path,
            "drilldown_path": drilldown_path,
            "freshness_path": freshness_path,
            "clickhouse_dashboard_path": ch_dashboard_path,
            "clickhouse_drilldown_path": ch_drilldown_path,
            "clickhouse_ingest_cost_path": ch_ingest_path,
        }

    def test_rejected_validation_writes_no_outputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture = self.make_fixture(root)
            validation = json.loads(
                fixture["validation_report_path"].read_text(encoding="utf-8")
            )
            validation["accepted"] = False
            self.write_json(fixture["validation_report_path"], validation)
            output = root / "publication"
            with self.assertRaisesRegex(ValueError, "accepted"):
                publish_results.publish(**fixture, output_dir=output)
            self.assertFalse(output.exists())

    def test_incomplete_cost_writes_no_outputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture = self.make_fixture(root)
            cost = json.loads(
                fixture["cost_summary_path"].read_text(encoding="utf-8")
            )
            cost["complete"] = False
            self.write_json(fixture["cost_summary_path"], cost)
            output = root / "publication"
            with self.assertRaisesRegex(ValueError, "complete"):
                publish_results.publish(**fixture, output_dir=output)
            self.assertFalse(output.exists())

    def test_publisher_uses_only_validation_accepted_indices(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture = self.make_fixture(root, include_above_cap=False)
            dashboard_path = fixture["dashboard_path"]
            dashboard = [
                json.loads(line)
                for line in dashboard_path.read_text(encoding="utf-8").splitlines()
            ]
            dashboard.insert(
                0,
                self.query_record(
                    "dashboard",
                    80_000_000_000,
                    "2026-01-01T00:00:10Z",
                    "2026-01-01T00:00:50Z",
                ),
            )
            self.write_jsonl(dashboard_path, dashboard)
            validation = json.loads(
                fixture["validation_report_path"].read_text(encoding="utf-8")
            )
            validation["query_samples"]["dashboard"]["accepted_indices"] = [1]
            self.write_json(fixture["validation_report_path"], validation)

            output = root / "publication"
            publish_results.publish(**fixture, output_dir=output)
            accepted = [
                json.loads(line)
                for line in (output / "accepted_dashboard.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual([row["raw_rows"] for row in accepted], [90_000_000_000])
            self.assertEqual(accepted[0]["publication"]["source_index"], 1)

    def test_monotonic_matching_is_deterministic_and_equal_count(self):
        left = [{"raw_rows": value} for value in (10, 21, 35)]
        right = [{"raw_rows": value} for value in (9, 20, 22, 36)]
        first = publish_results.monotonic_nearest_matches(left, right)
        second = publish_results.monotonic_nearest_matches(left, right)
        self.assertEqual(first, second)
        self.assertEqual(
            [item["right_index"] for item in first],
            [0, 1, 3],
        )
        self.assertEqual(len(first), len(left))

    def test_overlap_allocation_uses_union_without_concurrency_double_count(self):
        lines = [
            {
                "category": "primary_rt_serving_compute",
                "list_cost": "10",
                "measured_interval": {
                    "start": "2026-01-01T00:00:00Z",
                    "end": "2026-01-01T00:10:00Z",
                },
            }
        ]
        intervals = [
            (
                validate_run.parse_utc_timestamp("2026-01-01T00:01:00Z"),
                validate_run.parse_utc_timestamp("2026-01-01T00:04:00Z"),
            ),
            (
                validate_run.parse_utc_timestamp("2026-01-01T00:02:00Z"),
                validate_run.parse_utc_timestamp("2026-01-01T00:05:00Z"),
            ),
        ]
        total, allocation = publish_results.allocate_matched_query_cost(
            lines, intervals
        )
        self.assertEqual(total, Decimal("4"))
        self.assertEqual(
            allocation[0]["matched_union_overlap_microseconds"],
            240_000_000,
        )

    def test_publish_caps_presentations_but_discloses_full_cost_and_hashes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fixture = self.make_fixture(root)
            output = root / "publication"
            publication = publish_results.publish(**fixture, output_dir=output)
            dashboard = [
                json.loads(line)
                for line in (output / "accepted_dashboard.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            freshness = [
                json.loads(line)
                for line in (output / "accepted_freshness.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual(len(dashboard), 1)
            self.assertEqual(len(freshness), 1)
            self.assertLessEqual(dashboard[0]["raw_rows"], 100_000_000_000)
            self.assertEqual(
                freshness[0]["raw_rows_source"],
                "producer_progress.logical_raw_rows",
            )
            fresh = json.loads(
                (
                    output
                    / "ingest_fresh_path_cost_clickhouse_vs_databricks_summary.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(fresh["costs"]["databricks"]["total_usd"], 10.0)
            self.assertEqual(
                fresh["expected_full_rows"], publish_results.EXPECTED_ROWS
            )
            self.assertIn("100B", fresh["disclosure"])
            artifact = publication["artifacts"]["accepted_dashboard.jsonl"]
            self.assertEqual(
                artifact["sha256"],
                publish_results.sha256(output / "accepted_dashboard.jsonl"),
            )
            source_name = publish_results.SOURCE_OUTPUT_NAMES["validation_report"]
            source_path = output / source_name
            self.assertEqual(
                source_path.read_bytes(),
                fixture["validation_report_path"].read_bytes(),
            )
            self.assertEqual(
                publication["sources"]["validation_report"]["sha256"],
                publish_results.sha256(source_path),
            )
            self.assertIn(source_name, publication["artifacts"])
            performance = json.loads(
                (
                    output
                    / "full_path_cost_performance_clickhouse_vs_databricks_summary.json"
                ).read_text(encoding="utf-8")
            )
            self.assertAlmostEqual(
                performance["databricks_query_cost_allocation"][
                    "matched_query_serving_usd"
                ],
                0.6,
            )

    def test_manifest_is_untouched_by_default_and_requires_explicit_activation(self):
        with tempfile.TemporaryDirectory(dir=ROOT) as tmp:
            root = Path(tmp)
            fixture = self.make_fixture(root)
            labels = {
                "aggregate_query_latency": ["ClickHouse Cloud"],
                "drilldown_query_latency": ["ClickHouse Cloud"],
                "mv_lag": ["ClickHouse Cloud"],
                "fresh_path_cost": ["ClickHouse"],
                "full_path_cost_performance": ["ClickHouse"],
                "full_path_cost_vs_query_runtime": ["ClickHouse"],
            }
            manifest_path = root / "manifest.json"
            self.write_json(
                manifest_path,
                {
                    "schema_version": 1,
                    "required_labels": labels,
                    "providers": {"clickhouse": {}},
                    "fresh_path_cost": {},
                    "cost_performance": {},
                },
            )
            original = manifest_path.read_bytes()
            publish_results.publish(
                **fixture,
                output_dir=root / "not-applied",
                global_manifest_path=manifest_path,
            )
            self.assertEqual(manifest_path.read_bytes(), original)

            publish_results.publish(
                **fixture,
                output_dir=root / "applied",
                global_manifest_path=manifest_path,
                apply_global_manifest=True,
            )
            activated = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertTrue(activated["providers"]["databricks"]["enabled"])
            self.assertNotIn(
                str(root),
                activated["providers"]["databricks"]["publication"],
            )
            self.assertEqual(
                activated["providers"]["databricks"]["validation"],
                (
                    root
                    / "applied"
                    / publish_results.SOURCE_OUTPUT_NAMES["validation_report"]
                )
                .resolve()
                .relative_to(publish_results.BENCHMARK_ROOT)
                .as_posix(),
            )
            for values in activated["required_labels"].values():
                self.assertEqual(values[-1], "Databricks")
            self.assertIn(
                "databricks_pairwise_summary",
                activated["fresh_path_cost"],
            )
            with self.assertRaisesRegex(ValueError, "already contains"):
                publish_results.publish(
                    **fixture,
                    output_dir=root / "conflicting",
                    global_manifest_path=manifest_path,
                    apply_global_manifest=True,
                )
            self.assertFalse((root / "conflicting").exists())


if __name__ == "__main__":
    unittest.main()
