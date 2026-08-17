#!/usr/bin/env python3
"""Regression tests for portable, automatically derived progress matching."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


FULL_PATH_ROOT = Path(__file__).resolve().parents[1]
MATCHER = Path(__file__).with_name("match_progress.py")


class MatchProgressTest(unittest.TestCase):
    def test_automatic_window_and_relative_provenance(self) -> None:
        with tempfile.TemporaryDirectory(dir=FULL_PATH_ROOT) as temporary:
            directory = Path(temporary)
            reference = directory / "reference.jsonl"
            candidate = directory / "candidate.jsonl"
            output = directory / "matched.jsonl"

            reference.write_text(
                "\n".join(
                    json.dumps({"iteration": iteration, "raw_rows": rows})
                    for iteration, rows in ((1, 10), (2, 20))
                )
                + "\n",
                encoding="utf-8",
            )
            candidate.write_text(
                "\n".join(
                    json.dumps({"iteration": iteration, "raw_rows": rows})
                    for iteration, rows in ((1, 9), (2, 20), (3, 20))
                )
                + "\n",
                encoding="utf-8",
            )

            subprocess.run(
                [
                    sys.executable,
                    str(MATCHER),
                    "--reference",
                    str(reference),
                    "--candidate",
                    str(candidate),
                    "--output",
                    str(output),
                ],
                cwd=FULL_PATH_ROOT,
                check=True,
                stdout=subprocess.DEVNULL,
            )

            report = json.loads(
                output.with_suffix(".jsonl.match.json").read_text(encoding="utf-8")
            )
            self.assertEqual(report["count_mode"], "all_reference_active")
            self.assertIsNone(report["requested_count"])
            self.assertEqual(report["matched_observations"], 2)
            for key in ("reference_file", "candidate_file", "output_file"):
                self.assertFalse(Path(report[key]).is_absolute(), report[key])


if __name__ == "__main__":
    unittest.main()
