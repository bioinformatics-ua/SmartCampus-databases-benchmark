"""Correctness gates, static query trials, and plan diagnostics."""

import random

from common import (
    REQUEST_GRACE_SECONDS,
    STOP,
    Failure,
    Journal,
    atomic,
    call,
    query,
    require,
)
from infrastructure import visible
from queries import IDS, definition, fixture, matches, reference

DIAGNOSTIC_QUERIES = ("3", "7", "14", "19")


def validate(engine, cfg, directory, events=None):
    """Validate complete result sets against a separately calculated oracle."""
    events = fixture() if events is None else events
    lo = min(e["ts"] for e in events)
    hi = max(e["ts"] for e in events) + 1
    timeout = cfg["query_timeout_seconds"]

    require(call("/write", {"events": events}), "fixture ingestion")
    visible(engine, len(events), lo, hi, cfg)

    records = []
    for qid in IDS + ["D1", "D2", "A1", "probe"]:
        event = events[0] if qid == "probe" else None
        result = query(engine, qid, lo, hi, timeout, event=event)
        expected = reference(events, qid, lo, hi, event=event)
        result["expected"] = expected
        result["correct"] = result["status"] == "ok" and matches(
            result["rows"], expected, approx=result["mode"] == "approximate"
        )
        records.append(result)
        atomic(directory / "correctness.json", records)
        if not result["correct"]:
            raise Failure(f"{engine} {qid} correctness failure: {result}")

    invalid = call("/query", {"query": "not a query", "columns": 1, "timeout": 5})
    if invalid["status"] == "ok":
        raise Failure("invalid query returned success")
    atomic(directory / "invalid-query-test.json", invalid)

    # An already-expired context must be classified as a deadline, not a SQL error.
    expired = call(
        "/query",
        {
            "query": definition(engine, "3", lo, hi)["query"],
            "columns": 1,
            "timeout": 1e-9,
        },
    )
    atomic(directory / "deadline-test.json", expired)
    if expired["status"] != "timeout":
        raise Failure(f"deadline classification failed: {expired}")
    require(query(engine, "2", lo, hi, 10), "connection recovery after deadline")

    return {"validated_queries": len(records), "fixture_rows": len(events)}


def static(engine, lo, hi, cfg, directory, seed):
    """Run every query once to warm up, then once measured, in a seeded order."""
    journal = Journal(directory / "queries.jsonl")
    order = IDS.copy()
    random.Random(seed).shuffle(order)
    try:
        for phase in ("warmup", "measured"):
            for qid in order:
                if STOP.is_set():
                    raise Failure("interrupted")
                result = query(engine, qid, lo, hi, cfg["query_timeout_seconds"])
                result.update(phase=phase)
                journal.add(result)
                if result["status"] == "infrastructure_error":
                    raise Failure(result["error"])
                if result["status"] == "error":
                    raise Failure(f"validated query now failed: {result}")
    finally:
        journal.close()
    return {"query_order": order}


def influx_profiled(flux_query):
    """Enable the Flux query and operator profilers; imports must stay first."""
    lines = flux_query.splitlines()
    imports = [line for line in lines if line.startswith("import ")]
    body = [line for line in lines if not line.startswith("import ")]
    return "\n".join(
        [
            'import "profiler"',
            *imports,
            'option profiler.enabledProfilers=["query","operator"]',
            *body,
        ]
    )


def diagnostics(engine, lo, hi, cfg, directory):
    timeout = cfg["query_timeout_seconds"]
    records = []
    for qid in DIAGNOSTIC_QUERIES:
        spec = definition(engine, qid, lo, hi)
        if engine == "influxdb":
            request = {"query": influx_profiled(spec["query"]), "columns": 0}
        else:
            request = {"query": "EXPLAIN " + spec["query"], "columns": 1}
        result = call(
            "/query", {**request, "timeout": timeout}, timeout + REQUEST_GRACE_SECONDS
        )
        records.append({"query_id": qid, "sql": spec["query"], **result})
        atomic(directory / "plans.json", records)
    failed = [record for record in records if record["status"] != "ok"]
    if failed:
        raise Failure(f"{len(failed)} diagnostic queries failed; see plans.json")
    return {"plans": len(records)}
