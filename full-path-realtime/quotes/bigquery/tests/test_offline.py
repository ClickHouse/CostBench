from __future__ import annotations

import math
import sys
import threading
import time
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

from google.api_core.exceptions import AlreadyExists, DeadlineExceeded

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import ingest_parquet_rows as ingest  # noqa: E402
from bq_common import (  # noqa: E402
    aligned_metric_arrays,
    close_google_client,
    load_queries,
    offset_already_written_matches,
    query_job_stats,
    render_sql,
    split_sql_statements,
)


class FakeDirectCloseClient:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class FakeTransport:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class FakeTransportCloseClient:
    def __init__(self):
        self.transport = FakeTransport()


class FakeJob:
    job_id = "job_test"
    location = "US"
    state = "DONE"
    statement_type = "SELECT"
    created = None
    started = None
    ended = None
    slot_millis = 2500
    cache_hit = False

    def to_api_repr(self):
        return {
            "statistics": {
                "finalExecutionDurationMs": "1234",
                "totalSlotMs": "2500",
                "query": {
                    "totalBytesBilled": "10485760",
                    "totalBytesProcessed": "777",
                    "cacheHit": False,
                },
            }
        }


class FakeReloadedQueryJob(FakeJob):
    _properties = {
        "statistics": {
            "finalExecutionDurationMs": "400",
            "totalSlotMs": "300",
            "query": {
                "totalBytesBilled": "228589568",
                "totalBytesProcessed": "227636303",
                "cacheHit": False,
            },
        }
    }

    def to_api_repr(self):
        return {
            "jobReference": {"jobId": self.job_id},
            "configuration": {"query": {}},
        }


class FakeAppendFuture:
    def result(self, timeout=None):
        raise AlreadyExists(
            "The offset is within stream, expected offset 96936138, received 96805320"
        )


class FakeAppendStream:
    def __init__(self):
        self.closed = False

    def send(self, request):
        return FakeAppendFuture()

    def close(self):
        self.closed = True


class FakeBatch:
    num_rows = 130818


class FakeTimeoutFuture:
    def result(self, timeout=None):
        raise DeadlineExceeded("append acknowledgement timed out")


class FakeTimeoutStream(FakeAppendStream):
    def send(self, request):
        return FakeTimeoutFuture()


class OfflineTests(unittest.TestCase):
    def test_rate_limiter_can_be_disabled(self):
        limiter = ingest.RateLimiter(0)
        self.assertTrue(limiter.wait(130818, threading.Event()))
        self.assertIsNone(limiter.snapshot()["target_eps"])

    def test_counters_capture_last_ack_time(self):
        counters = ingest.Counters()
        started = time.monotonic()
        counters.update(acknowledged_rows=10)
        self.assertIsNotNone(counters.acknowledged_elapsed(started))

    def test_already_written_offset_must_match_exact_replayed_batch(self):
        message = "409 The offset is within stream, expected offset 96936138, received 96805320"
        self.assertTrue(offset_already_written_matches(message, 96805320, 130818))
        self.assertFalse(offset_already_written_matches(message, 96805321, 130818))
        self.assertFalse(offset_already_written_matches(message, 96805320, 130817))
        self.assertFalse(offset_already_written_matches("409 already exists", 96805320, 130818))

    def test_already_written_append_is_counted_once_and_reopened(self):
        stream = FakeAppendStream()
        reopened = object()
        counters = ingest.Counters()
        args = Namespace(append_timeout=120, max_retries=8, retry_base_seconds=0.5)
        with patch.object(ingest, "make_append_stream", return_value=reopened):
            result = ingest.send_with_offset(
                object(),
                stream,
                "projects/p/datasets/d/tables/t/streams/s",
                96805320,
                FakeBatch(),
                b"payload",
                counters,
                args,
            )
        snapshot = counters.snapshot()
        self.assertIs(result, reopened)
        self.assertTrue(stream.closed)
        self.assertEqual(snapshot["acknowledged_rows"], 130818)
        self.assertEqual(snapshot["append_requests"], 1)
        self.assertEqual(snapshot["retries"], 0)
        self.assertEqual(snapshot["recovered_already_exists_appends"], 1)
        self.assertEqual(snapshot["recovered_already_exists_rows"], 130818)
        self.assertEqual(snapshot["errors"], [])

    def test_server_row_count_recovery_is_counted_once_and_reopened(self):
        stream = FakeTimeoutStream()
        reopened = object()
        counters = ingest.Counters()
        args = Namespace(append_timeout=120, max_retries=8, retry_base_seconds=0.5)
        with (
            patch.object(ingest, "server_row_count", return_value=96936138),
            patch.object(ingest, "make_append_stream", return_value=reopened),
        ):
            result = ingest.send_with_offset(
                object(),
                stream,
                "projects/p/datasets/d/tables/t/streams/s",
                96805320,
                FakeBatch(),
                b"payload",
                counters,
                args,
            )
        snapshot = counters.snapshot()
        self.assertIs(result, reopened)
        self.assertTrue(stream.closed)
        self.assertEqual(snapshot["acknowledged_rows"], 130818)
        self.assertEqual(snapshot["append_requests"], 1)
        self.assertEqual(snapshot["retries"], 0)
        self.assertEqual(snapshot["recovered_server_row_count_appends"], 1)
        self.assertEqual(snapshot["recovered_server_row_count_rows"], 130818)

    def test_google_client_close_compatibility(self):
        direct = FakeDirectCloseClient()
        close_google_client(direct)
        self.assertTrue(direct.closed)

        via_transport = FakeTransportCloseClient()
        close_google_client(via_transport)
        self.assertTrue(via_transport.transport.closed)

        close_google_client(object())

    def test_query_counts(self):
        self.assertEqual(len(load_queries(ROOT / "queries_mv.sql", "p", "d")), 4)
        self.assertEqual(len(load_queries(ROOT / "queries_raw.sql", "p", "d")), 2)

    def test_ddl_count_and_placeholders(self):
        sql = render_sql((ROOT / "create.sql").read_text(), "p", "d")
        self.assertEqual(len(split_sql_statements(sql)), 2)
        self.assertNotIn("__PROJECT_ID__", sql)
        self.assertNotIn("__DATASET_ID__", sql)

    def test_splitter_ignores_comments_and_quoted_semicolon(self):
        sql = "-- one;\nSELECT 'a;b'; -- two;\nSELECT `x`;"
        self.assertEqual(split_sql_statements(sql), ["SELECT 'a;b'", "SELECT `x`"])

    def test_job_accounting_shape(self):
        stats = query_job_stats(FakeJob())
        self.assertEqual(stats["runtime_sec"], 1.234)
        self.assertEqual(stats["billed_slot_sec"], 2.5)
        self.assertEqual(stats["total_bytes_billed"], 10485760)
        self.assertEqual(stats["total_bytes_processed"], 777)
        self.assertFalse(stats["cache_hit"])

    def test_job_accounting_uses_reloaded_server_resource(self):
        stats = query_job_stats(FakeReloadedQueryJob())
        self.assertEqual(stats["runtime_sec"], 0.4)
        self.assertEqual(stats["billed_slot_sec"], 0.3)
        self.assertEqual(stats["total_bytes_billed"], 228589568)
        self.assertEqual(stats["total_bytes_processed"], 227636303)

    def test_metric_arrays_are_aligned_and_keep_failed_job_cost(self):
        arrays = aligned_metric_arrays(
            [
                {
                    "runtime_sec": 1.0,
                    "billed_slot_sec": 2.0,
                    "total_bytes_billed": 3,
                    "total_bytes_processed": 4,
                    "error": None,
                },
                {
                    "runtime_sec": 5.0,
                    "billed_slot_sec": 6.0,
                    "total_bytes_billed": 7,
                    "total_bytes_processed": 8,
                    "error": "failed",
                },
            ]
        )
        self.assertEqual(arrays["result"], [[1.0], [None]])
        self.assertEqual(arrays["billed_slot_sec"], [[2.0], [6.0]])
        self.assertEqual(arrays["billed_bytes"], [[3], [7]])
        self.assertEqual({len(value) for value in arrays.values()}, {2})

    def test_population_moment_formula_matches_raw_kurtosis(self):
        values = list(range(1, 11))
        n = len(values)
        s1 = sum(values)
        s2 = sum(x**2 for x in values)
        s3 = sum(x**3 for x in values)
        s4 = sum(x**4 for x in values)
        mean = s1 / n
        mu2 = s2 / n - mean**2
        mu3 = s3 / n - 3 * mean * s2 / n + 2 * mean**3
        mu4 = s4 / n - 4 * mean * s3 / n + 6 * mean**2 * s2 / n - 3 * mean**4
        self.assertAlmostEqual(mu3 / math.pow(mu2, 1.5), 0.0)
        self.assertAlmostEqual(mu4 / math.pow(mu2, 2), 1.7757575757575756)


if __name__ == "__main__":
    unittest.main()
