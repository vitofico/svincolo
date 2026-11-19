"""Run with python3 -m unittest discover -s tests -p test_tier_prices.py."""

import csv
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from test_cost_report import SCRIPT, TOKEN_FIELDS, jsonl, usage


class TierPricesTest(unittest.TestCase):
    def run_report(self, *events, threshold=100_000):
        price = dict(zip(TOKEN_FIELDS, ["0.10", "0.01", "0.125", "0.50"]))
        price["long_context"] = {
            "input_tokens_over": threshold,
            **dict(zip(TOKEN_FIELDS, ["0.50", "0.05", "0.625", "2.50"])),
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "prices.json"
            path.write_text(json.dumps({"cloud": {"model": {"2026-11-01": price}}}),
                            encoding="utf-8")
            return subprocess.run(
                [sys.executable, SCRIPT, "--month", "2026-11", "--prices", path],
                input=jsonl(*events), text=True, capture_output=True, check=False,
            )

    def test_prices_known_tiers_and_keeps_ambiguous_usage_unpriced(self):
        tokens = dict(cache_read_input_tokens=20_000, cache_creation_input_tokens=10_000,
                      output_tokens=1000)
        result = self.run_report(
            usage("short", caller="at threshold", input_tokens=70_000, **tokens),
            usage("long", caller="over threshold", input_tokens=70_001, attempts=2, **tokens),
            usage("short-retry", caller="at threshold", input_tokens=70_000, attempts=2,
                  known_attempts=2, **tokens),
            usage("ambiguous", caller="over threshold", input_tokens=70_001, attempts=3,
                  known_attempts=2, **tokens),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        rows = {row["caller"]: row for row in
                csv.DictReader(io.StringIO(result.stdout), delimiter="\t")}
        self.assertEqual(rows["at threshold"]["known_cost_usd"], "0.0179")
        self.assertEqual(rows["at threshold"]["unpriced_attempts"], "0")
        self.assertEqual(rows["over threshold"]["known_cost_usd"], "0.0447505")
        self.assertEqual(rows["over threshold"]["unknown_usage_attempts"], "2")
        self.assertEqual(rows["over threshold"]["unpriced_attempts"], "2")
        self.assertEqual(rows["over threshold"]["input_tokens"], "140002")
        self.assertEqual(rows[""]["known_cost_usd"], "0.0626505")

    def test_rejects_invalid_long_context_threshold(self):
        for threshold in (0, -1, True, "100000"):
            with self.subTest(threshold=threshold):
                result = self.run_report(threshold=threshold)
                self.assertEqual(result.returncode, 2)
                self.assertIn("invalid rates for cloud/model on 2026-11-01", result.stderr)
