import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location(
    "spend", Path(__file__).resolve().parents[1] / "deploy/cost-ui/spend.py"
)
spend = importlib.util.module_from_spec(spec)
spec.loader.exec_module(spend)


class SpendingTest(unittest.TestCase):
    def test_calendar_boundaries(self):
        now = datetime(2027, 1, 1, 12, tzinfo=timezone.utc)
        periods = spend.windows(now)
        self.assertEqual(periods["week"][0].date().isoformat(), "2026-12-28")
        self.assertEqual(periods["week"][1].date().isoformat(), "2027-01-04")
        self.assertEqual(periods["month"][1].date().isoformat(), "2027-02-01")

    def test_zero_cap_removal_and_concurrent_edits(self):
        with TemporaryDirectory() as tmp, patch.object(spend, "DIR", Path(tmp)):
            spend.initialize()
            rev = spend.revision(spend.read("limits.json"))
            spend.save(rev, "provider", "model-provider", "day", "0", "2.01")
            saved = spend.read("limits.json")
            rule = saved["provider"]["model-provider"]
            self.assertEqual(rule, {"soft": 0, "hard": 2010000, "period": "day"})
            state = {"periods": {"day": {"provider": {"model-provider": 2010000}}}}
            self.assertIn("hard cap", next(spend.alerts(saved, state)))
            with self.assertRaises(ValueError):
                spend.save(rev, "provider", "model-provider", "day", "", "")
            spend.save(spend.revision(saved), "provider", "model-provider", "day", "", "")
            self.assertEqual(spend.read("limits.json")["provider"], {})
