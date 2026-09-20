import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import infrastructure
import queries
import report
import run
from infrastructure import compose


class VisibilityTests(unittest.TestCase):
    def test_full_count_gets_configured_budget(self):
        cfg = {"visibility_timeout_seconds": 300, "query_timeout_seconds": 300}
        with patch(
            "infrastructure.query", return_value={"status": "ok", "rows": [[5]]}
        ) as query:
            infrastructure.visible("influxdb", 5, 0, 10, cfg)
        timeout = query.call_args.kwargs["timeout"]
        self.assertGreater(timeout, 60)
        self.assertLessEqual(timeout, 300)

    def test_timeout_is_fatal_and_does_not_claim_missing_rows(self):
        cfg = {"visibility_timeout_seconds": 300, "query_timeout_seconds": 300}
        with patch(
            "infrastructure.query", return_value={"status": "timeout", "rows": None}
        ) as query:
            with self.assertRaisesRegex(infrastructure.Failure, "actual count unknown"):
                infrastructure.visible("influxdb", 5, 0, 10, cfg)
        self.assertEqual(query.call_count, 1)


class QueryTests(unittest.TestCase):
    def setUp(self):
        self.events = queries.fixture()
        self.lo = min(e["ts"] for e in self.events)
        self.hi = max(e["ts"] for e in self.events) + 1

    def test_all_queries_have_output_contract(self):
        for engine in (*queries.SQL_ENGINES, "influxdb"):
            for q in queries.IDS + ["D1", "D2", "A1"]:
                spec = queries.definition(engine, q, self.lo, self.hi)
                self.assertTrue(spec["query"])
                self.assertGreater(spec["columns"], 0)
                expected = queries.reference(self.events, q, self.lo, self.hi)
                self.assertTrue(
                    all(len(r) == spec["columns"] for r in expected), (engine, q)
                )

    def test_half_open_split(self):
        a = queries.reference(self.events, "5", self.lo, self.hi)[0][0]
        b = queries.reference(self.events, "6", self.lo, self.hi)[0][0]
        self.assertEqual(a + b, len(self.events))

    def test_single_second_input_has_empty_first_half(self):
        events = [{"user": "one", "ssid": "ap", "ts": 10, "rssi": -50.0}]
        self.assertEqual(queries.reference(events, "5", 10, 11), [[0]])
        flux = queries.definition("influxdb", "5", 10, 11)["query"]
        self.assertIn("filter(fn:(r)=>false)", flux)
        self.assertIn("stop:time(v:11000000000)", flux)

    def test_quantile_modes_not_conflated(self):
        self.assertEqual(
            queries.definition("postgres", "14", self.lo, self.hi)["mode"], "exact_r7"
        )
        self.assertEqual(
            queries.definition("questdb", "14", self.lo, self.hi)["mode"], "approximate"
        )

    def test_tied_rank_is_deterministic(self):
        rows = queries.reference(self.events, "9", self.lo, self.hi)
        self.assertEqual([r[0] for r in rows], sorted(r[0] for r in rows))

    def test_empty_probe(self):
        event = {**self.events[0], "user": "missing"}
        self.assertEqual(
            queries.reference(self.events, "probe", self.lo, self.hi, event=event),
            [[0]],
        )

    def test_approximate_only_relaxes_value_column(self):
        self.assertTrue(queries.matches([[0.25, -45]], [[0.25, -47]], True))
        self.assertFalse(queries.matches([[0.50, -47]], [[0.25, -47]], True))


class ScheduleTests(unittest.TestCase):
    def setUp(self):
        self.cfg = json.loads((run.HERE / "config.json").read_text())

    def test_deterministic_and_unique(self):
        cells = run.schedule(self.cfg)
        self.assertEqual(cells, run.schedule(self.cfg))
        self.assertEqual(len(cells), len({c["id"] for c in cells}))
        self.assertEqual(len(cells), 432)

    def test_distribution_and_all_patterns(self):
        cells = run.schedule(self.cfg)
        self.assertEqual(
            {c["distribution"] for c in cells if c["block"] == "distribution"},
            {"hotspot", "fragmented"},
        )
        self.assertEqual(
            {c["pattern"] for c in cells if c["block"] == "mixed"},
            set(self.cfg["patterns"]),
        )

    def test_initial_validation_before_timing(self):
        c = run.schedule(self.cfg)
        self.assertTrue(all(x["block"] == "correctness" for x in c[:6]))

    def test_compose_one_service_and_limits(self):
        for engine, image in self.cfg["images"].items():
            c = compose(engine, image, self.cfg)
            self.assertEqual(list(c["services"]), ["db"])
            self.assertEqual(
                c["services"]["db"]["memswap_limit"], c["services"]["db"]["mem_limit"]
            )
            self.assertTrue(
                all(p.startswith("127.0.0.1:") for p in c["services"]["db"]["ports"])
            )


class ReportingTests(unittest.TestCase):
    def test_failures_not_removed_from_objective_denominator(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "requests.jsonl"
            p.write_text(
                "\n".join(
                    json.dumps(r)
                    for r in [
                        {
                            "phase": "measured",
                            "service": "dashboard",
                            "status": "ok",
                            "latency_us": 100,
                        },
                        {
                            "phase": "measured",
                            "service": "dashboard",
                            "status": "timeout",
                            "latency_us": 300000000,
                        },
                        {
                            "phase": "measured",
                            "service": "dashboard",
                            "status": "rejected",
                        },
                        {
                            "phase": "warmup",
                            "service": "dashboard",
                            "status": "ok",
                            "latency_us": 1,
                        },
                    ]
                )
            )
            r = report.aggregate_requests(p)["dashboard"]
            self.assertEqual(r["offered"], 3)
            self.assertEqual(r["completed"], 1)
            self.assertEqual(r["objective_violation_fraction"], 2 / 3)
            self.assertIsNone(r["p95_all_requests_ms"])

    def test_partial_journal_not_complete_result(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "raw"
            p.write_text('{"status":"ok"}\n{"status":')
            self.assertEqual(list(report.records(p)), [{"status": "ok"}])

    def test_empty_report_is_valid(self):
        with tempfile.TemporaryDirectory() as td:
            report.generate_report(Path(td))
            self.assertEqual(
                json.loads((Path(td) / "summary.json").read_text())["complete"], 0
            )
            self.assertIn("<!doctype html>", (Path(td) / "report.html").read_text())


if __name__ == "__main__":
    unittest.main()
