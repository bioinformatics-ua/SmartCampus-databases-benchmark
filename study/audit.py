"""Historical provenance checks and automatic runtime budgeting."""

import json
import statistics
from collections import defaultdict
from pathlib import Path

from common import ROOT, Failure, atomic
from schedule import MIXED_BLOCKS

# Relative cost of each population level against the baseline (B) calibration.
LEVEL_COST = {"S": 0.2, "B": 1, "L": 2.5}


def audit_historical(destination: Path) -> None:
    """Keep downloaded measurements separate from the published rounded table."""
    grouped = defaultdict(list)
    source = ROOT / "src" / "benchmarks"

    for path in sorted(source.glob("*.json")):
        record = json.loads(path.read_text())
        grouped[record["dbType"]].append(record)

    result = {}
    for engine, records in grouped.items():
        queries = defaultdict(list)
        for record in records:
            for query in record["queries"]:
                queries[query["queryId"]].append(query["durationMs"])

        result[engine] = {
            "runs": len(records),
            "query_means_ms": {
                str(query): statistics.mean(values)
                for query, values in queries.items()
                if all(value >= 0 for value in values)
            },
            "negative_entries_are_not_proven_timeouts": {
                str(query): sum(value < 0 for value in values)
                for query, values in queries.items()
                if any(value < 0 for value in values)
            },
        }

    atomic(
        destination,
        {
            "source": str(source),
            "observations": result,
            "limitations": [
                "This revision is not established as the source of the conference tables.",
                "Six PostgreSQL queries were skipped in the downloaded implementation.",
                "Negative durations do not establish a timeout or unsupported operation.",
                "Historical results are excluded from new-machine speedup calculations.",
            ],
        },
    )


def estimate_budget(root: Path, cells: list, config: dict) -> dict:
    """Estimate calendar cost after all full-data calibration cells complete."""
    calibration = {}
    for cell in cells:
        if cell["block"] != "calibration":
            continue
        completion = root / "runs" / cell["id"] / "complete.json"
        if not completion.exists():
            return {"ready": False}
        result = json.loads(completion.read_text())
        calibration[cell["engine"]] = result["elapsed_seconds"]

    estimate = 0.0
    for cell in cells:
        if (root / "runs" / cell["id"] / "complete.json").exists():
            continue
        base = calibration.get(cell["engine"], 0)
        if cell["block"] in ("static", "distribution", "diagnostic"):
            estimate += base * LEVEL_COST[cell.get("level", "B")]
        elif cell["block"] in MIXED_BLOCKS:
            estimate += base + config["warmup_seconds"] + config["measurement_seconds"]
            estimate += config["query_timeout_seconds"]  # worst-case drain

    estimate *= 1.3  # 30% reserve
    result = {
        "ready": True,
        "remaining_estimate_days": estimate / 86400,
        "budget_days": config["study_budget_days"],
        "reserve_fraction": 0.3,
        "method": (
            "Per-engine full-data calibration, scale factors, workload windows, "
            "maximum drain, and 30% reserve."
        ),
        "limitations": (
            "An estimate, not a runtime guarantee. "
            "Hotspot and contention costs may be nonlinear."
        ),
    }
    atomic(root / "runtime-budget.json", result)
    if result["remaining_estimate_days"] > config["study_budget_days"]:
        raise Failure(
            f"Estimated remaining study is {result['remaining_estimate_days']:.1f} days, "
            f"above the {config['study_budget_days']}-day budget. "
            "Results are saved. Reduce the matrix in a new configuration/output directory "
            "rather than silently changing the frozen protocol."
        )
    return result
