"""Offered-load pacing and rejection accounting must stay auditable."""

import json
import queue
import tempfile
import unittest
from pathlib import Path

from audit import estimate_budget
from common import Failure
from datasets import expected_rows
from schedule import PATTERNS
from workloads import Mixed

CONFIG = {
    "read_queue": 2,
    "write_queue_seconds": 1,
    "warmup_seconds": 10,
    "measurement_seconds": 20,
    "query_timeout_seconds": 30,
    "seed": 1,
    "study_budget_days": 1,
}


def trial(directory, **cell):
    spec = {"pattern": "dashboard", "rate": 10, "writes": 500, "repeat": 0, **cell}
    return Mixed("postgres", spec, 0, 1, 0, 0, CONFIG, Path(directory))


class PacingTests(unittest.TestCase):
    def test_burst_preserves_the_mean_offered_rate(self):
        with tempfile.TemporaryDirectory() as directory:
            mixed = trial(directory, pattern="burst")
            try:
                offset = 0.0
                offered = 0
                while offset < 60:
                    offset += mixed.read_interval(offset)
                    offered += 1
            finally:
                mixed.journal.close()
                mixed.writes.close()
                mixed.fresh.close()
        self.assertAlmostEqual(offered, 60 * 10, delta=2)

    def test_every_pattern_has_a_full_twenty_slot_mix(self):
        for pattern, mix in PATTERNS.items():
            self.assertEqual(len(mix), 20, pattern)


class RejectionTests(unittest.TestCase):
    def records(self, path):
        return [json.loads(line) for line in path.read_text().splitlines()]

    def test_reads_over_queue_bound_are_recorded_not_dropped(self):
        with tempfile.TemporaryDirectory() as directory:
            mixed = trial(directory)
            mixed.start = 0.0
            try:
                for _ in range(CONFIG["read_queue"] + 2):
                    mixed.offer_read("D1", 12.0)
            finally:
                mixed.journal.close()
                mixed.writes.close()
                mixed.fresh.close()
            offered = self.records(Path(directory) / "requests.jsonl")
        self.assertEqual(len(offered), 2)
        self.assertTrue(all(r["status"] == "rejected" for r in offered))
        self.assertTrue(all(r["phase"] == "measured" for r in offered))

    def test_rejected_writes_stay_in_the_offered_total(self):
        with tempfile.TemporaryDirectory() as directory:
            mixed = trial(directory)
            mixed.start = 0.0
            try:
                for offset in range(CONFIG["write_queue_seconds"] + 2):
                    mixed.offer_write(offset)
            finally:
                mixed.journal.close()
                mixed.writes.close()
                mixed.fresh.close()
            written = self.records(Path(directory) / "writes.jsonl")
        self.assertEqual(mixed.accepted, 500)
        self.assertEqual(mixed.rejected, 2 * 500)
        self.assertEqual(mixed.accepted + mixed.rejected, 3 * 500)
        self.assertEqual([r["status"] for r in written], ["rejected", "rejected"])

    def test_freshness_sampler_keeps_one_pending_sample(self):
        with tempfile.TemporaryDirectory() as directory:
            mixed = trial(directory)
            try:
                mixed.samples.put_nowait(("first", 0, 0, "measured"))
                with self.assertRaises(queue.Full):
                    mixed.samples.put_nowait(("second", 0, 0, "measured"))
            finally:
                mixed.journal.close()
                mixed.writes.close()
                mixed.fresh.close()


class PopulationTests(unittest.TestCase):
    MANIFEST = {"count": 100, "small_count": 9}

    def test_levels_scale_the_source_population(self):
        self.assertEqual(expected_rows(self.MANIFEST, "S", 0), 9)
        self.assertEqual(expected_rows(self.MANIFEST, "B", 0), 100)
        self.assertEqual(expected_rows(self.MANIFEST, "L", 0), 200)

    def test_a_limit_never_raises_the_expected_count(self):
        self.assertEqual(expected_rows(self.MANIFEST, "B", 40), 40)
        self.assertEqual(expected_rows(self.MANIFEST, "S", 40), 9)


class BudgetTests(unittest.TestCase):
    CELLS = [
        {
            "id": "0000-calibration-postgres",
            "block": "calibration",
            "engine": "postgres",
        },
        {
            "id": "0001-static-postgres",
            "block": "static",
            "engine": "postgres",
            "level": "L",
        },
    ]

    def budget(self, root, calibration_seconds):
        complete = root / "runs" / self.CELLS[0]["id"] / "complete.json"
        complete.parent.mkdir(parents=True)
        complete.write_text(json.dumps({"elapsed_seconds": calibration_seconds}))
        return estimate_budget(root, self.CELLS, CONFIG)

    def test_pending_calibration_defers_the_estimate(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(
                estimate_budget(Path(directory), self.CELLS, CONFIG), {"ready": False}
            )

    def test_estimate_over_budget_stops_the_study_and_saves_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(Failure, "above the 1-day budget"):
                self.budget(root, 86400)
            saved = json.loads((root / "runtime-budget.json").read_text())
            self.assertGreater(saved["remaining_estimate_days"], 1)

    def test_estimate_within_budget_records_the_reserve(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = self.budget(root, 600)
            self.assertTrue(result["ready"])
            self.assertEqual(result["reserve_fraction"], 0.3)
            self.assertAlmostEqual(
                result["remaining_estimate_days"], 600 * 2.5 * 1.3 / 86400
            )


if __name__ == "__main__":
    unittest.main()
