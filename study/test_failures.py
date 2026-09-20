"""Failure records must remain visible without aborting later experiments."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import experiments
import report
import run
from common import Failure, atomic


class FailureReportingTests(unittest.TestCase):
    def test_source_update_keeps_history_but_rejects_design_changes(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "protocol.json"
            old = {"config": {"size": 1}, "schedule": [], "source_hash": "old"}
            new = {**old, "source_hash": "new"}
            atomic(path, old)
            with self.assertRaises(Failure):
                run.freeze_protocol(path, new)
            run.freeze_protocol(path, new, True)
            self.assertEqual(json.loads(path.read_text()), new)
            history = list((path.parent / "protocol-history").glob("*.json"))
            self.assertEqual(json.loads(history[0].read_text())["previous"], old)
            with self.assertRaises(Failure):
                run.freeze_protocol(path, {**new, "config": {"size": 2}}, True)

    def test_failed_rerun_is_not_hidden_by_old_completion(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cell = {"id": "test", "engine": "postgres", "block": "correctness"}
            atomic(root / "protocol.json", {"schedule": [cell]})
            folder = root / "runs" / "test"
            atomic(folder / "complete.json", {"attempt": "runs/test/attempt-1"})
            atomic(
                folder / "attempt-2" / "failure.json",
                {
                    "cell": cell,
                    "error": "failed rerun",
                    "elapsed_seconds": 1,
                },
            )
            report.generate_report(root)
            summary = json.loads((root / "summary.json").read_text())
            self.assertEqual(summary["complete"], 0)
            self.assertEqual(
                summary["failed_cells"][0]["details"]["error"], "failed rerun"
            )

    def test_diagnostic_errors_fail_cell_and_preserve_all_records(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            with patch(
                "experiments.call",
                return_value={"status": "error", "error": "bad plan"},
            ):
                with self.assertRaisesRegex(Failure, "4 diagnostic queries failed"):
                    experiments.diagnostics(
                        "postgres", 0, 1, {"query_timeout_seconds": 1}, folder
                    )
            self.assertEqual(len(json.loads((folder / "plans.json").read_text())), 4)

    def test_failed_worker_does_not_raise_to_controller(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            cell = {"id": "test", "engine": "postgres", "block": "correctness"}
            folder = root / "runs" / "test"
            atomic(root / "protocol.json", {"schedule": [cell]})
            atomic(folder / "complete.json", {})
            with patch("run.multiprocessing.get_context") as context:
                context.return_value.Process.return_value.exitcode = 1
                self.assertFalse(
                    run.run_isolated_cell(
                        cell, root, root, {}, {}, "test", folder / "complete.json"
                    )
                )
            self.assertFalse((folder / "complete.json").exists())
            self.assertEqual(len(list(folder.glob("attempt-*/failure.json"))), 1)
