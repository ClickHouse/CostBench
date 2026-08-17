#!/usr/bin/env python3
"""Regression tests for fail-closed global chart label inventories."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from _common import validate_required_labels  # noqa: E402


class ManifestTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = json.loads((HERE / "manifest.json").read_text(encoding="utf-8"))

    def test_all_canonical_families_declare_labels(self) -> None:
        self.assertEqual(
            set(self.manifest["required_labels"]),
            {
                "aggregate_query_latency",
                "drilldown_query_latency",
                "mv_lag",
                "fresh_path_cost",
                "full_path_cost_performance",
                "full_path_cost_vs_query_runtime",
            },
        )

    def test_label_validation_is_case_insensitive_and_exact(self) -> None:
        required = self.manifest["required_labels"]["fresh_path_cost"]
        validated = validate_required_labels(
            self.manifest,
            "fresh_path_cost",
            (label.swapcase() for label in reversed(required)),
        )
        self.assertEqual(validated, required)

        with self.assertRaisesRegex(ValueError, "incomplete"):
            validate_required_labels(
                self.manifest,
                "fresh_path_cost",
                required[:-1],
            )


if __name__ == "__main__":
    unittest.main()
