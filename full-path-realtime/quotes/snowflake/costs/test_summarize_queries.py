#!/usr/bin/env python3
"""Regression tests for Snowflake normalized query-cost attribution."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("summarize_queries.py")


def pricing(name: str, credits: float) -> dict:
    return {
        "pricing": [
            {
                "cloud": "aws",
                "region": "us-east-1",
                "plan": "enterprise",
                "credit_price_per_hour": 3.0,
                "warehouses": [{"name": name, "credits_per_hour": credits}],
            }
        ]
    }


class SummarizeQueriesTest(unittest.TestCase):
    def run_summary(self, *, fallback: bool) -> dict:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            input_path = root / "input.jsonl"
            primary_path = root / "interactive.json"
            fallback_path = root / "gen2.json"
            output_path = root / "summary.json"
            records = [
                {
                    "system": "Snowflake IT (AWS)",
                    "machine": "Interactive Small",
                    "cluster_size": "1.2",
                    "result": [[4.0], [5.0]],
                },
                {
                    "system": "Snowflake IT (AWS)",
                    "machine": "Interactive Small",
                    "cluster_size": "1.2",
                    "result": [[5.001], [12.0]],
                },
            ]
            input_path.write_text(
                "".join(json.dumps(record) + "\n" for record in records),
                encoding="utf-8",
            )
            primary_path.write_text(json.dumps(pricing("Interactive Small", 1.2)), encoding="utf-8")
            fallback_path.write_text(json.dumps(pricing("Gen2 Small", 2.7)), encoding="utf-8")
            command = [
                sys.executable,
                str(SCRIPT),
                str(input_path),
                str(primary_path),
                str(output_path),
            ]
            if fallback:
                command.extend(
                    [
                        "--fallback-pricing",
                        str(fallback_path),
                        "--fallback-warehouse",
                        "Gen2 Small",
                        "--fallback-threshold-seconds",
                        "5",
                    ]
                )
            subprocess.run(command, check=True, capture_output=True, text=True)
            return json.loads(output_path.read_text(encoding="utf-8"))

    def test_strict_threshold_prices_full_fallback_elapsed_without_double_count(self) -> None:
        summary = self.run_summary(fallback=True)
        attribution = summary["runtime_attribution"]
        self.assertEqual(attribution["primary_priced_query_jobs"], 2)
        self.assertEqual(attribution["fallback_priced_query_jobs"], 2)
        self.assertAlmostEqual(attribution["primary_priced_runtime_seconds"], 9.0)
        self.assertAlmostEqual(attribution["fallback_priced_runtime_seconds"], 17.001)
        self.assertAlmostEqual(
            attribution["primary_priced_runtime_seconds"]
            + attribution["fallback_priced_runtime_seconds"],
            summary["total_runtime_seconds"],
        )
        enterprise = summary["costs"][0]
        self.assertAlmostEqual(enterprise["total_compute_cost_usd"], 0.04725)

    def test_legacy_single_rate_prices_every_job_at_recorded_warehouse(self) -> None:
        summary = self.run_summary(fallback=False)
        attribution = summary["runtime_attribution"]
        self.assertEqual(attribution["primary_priced_query_jobs"], 4)
        self.assertEqual(attribution["fallback_priced_query_jobs"], 0)
        self.assertEqual(summary["query_cost_model"]["attribution_method"], "single_recorded_warehouse_rate")
        self.assertAlmostEqual(summary["costs"][0]["total_compute_cost_usd"], 0.026)


if __name__ == "__main__":
    unittest.main()
