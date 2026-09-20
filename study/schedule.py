"""Deterministic experimental matrix and workload proportions."""

import random

# Twenty-slot query mixes, shuffled once per repetition.
PATTERNS = {
    "dashboard": ["D1"] * 10 + ["D2"] * 10,
    "balanced": ["D1"] * 9 + ["D2"] * 9 + ["3", "19"],
    "report_heavy": ["D1"] * 5 + ["D2"] * 5 + ["3"] * 5 + ["19"] * 5,
    "alert_heavy": ["A1"] * 10 + ["D1"] * 4 + ["D2"] * 4 + ["3", "19"],
    "burst": ["D1"] * 9 + ["D2"] * 9 + ["3", "19"],
}

MIXED_BLOCKS = ("mixed", "control", "screen", "observer_control")

# Patterns that receive the full read/write rate sweep; others use one middle point.
SWEPT_PATTERNS = ("dashboard", "balanced")


def workload_cell(block, engine, pattern, rate, writes, repeat, **extra):
    return {
        "block": block,
        "engine": engine,
        "pattern": pattern,
        "rate": rate,
        "writes": writes,
        "repeat": repeat,
        "level": "B",
        "distribution": "base",
        **extra,
    }


def static_block(cfg, repeat):
    block = [
        {
            "block": "static",
            "engine": engine,
            "level": level,
            "distribution": "base",
            "repeat": repeat,
        }
        for level in cfg["levels"]
        for engine in cfg["engines"]
    ]
    block += [
        {
            "block": "distribution",
            "engine": engine,
            "level": "B",
            "distribution": distribution,
            "repeat": repeat,
        }
        for distribution in cfg["distributions"]
        if distribution != "base"
        for engine in cfg["engines"]
    ]
    return block


def mixed_block(cfg, repeat):
    block = []
    for engine in cfg["primary_engines"]:
        for pattern in cfg["patterns"]:
            swept = pattern in SWEPT_PATTERNS
            for rate in cfg["read_rates"] if swept else [10]:
                for writes in cfg["write_rates"] if swept else [500]:
                    block.append(
                        workload_cell("mixed", engine, pattern, rate, writes, repeat)
                    )
        # Read-only controls.
        for pattern in SWEPT_PATTERNS:
            block.append(workload_cell("control", engine, pattern, 10, 0, repeat))
        # Write-only controls.
        for writes in cfg["write_rates"]:
            block.append(
                workload_cell("control", engine, "dashboard", 0, writes, repeat)
            )
        block.append(
            workload_cell(
                "observer_control", engine, "balanced", 10, 500, repeat, freshness=False
            )
        )
    for engine in cfg["engines"]:
        if engine not in cfg["primary_engines"]:
            for pattern in SWEPT_PATTERNS:
                block.append(workload_cell("screen", engine, pattern, 10, 500, repeat))
    return block


def schedule(cfg, smoke=False):
    engines = cfg["engines"]
    cells = [{"block": "correctness", "engine": e} for e in engines]
    cells += [{"block": "correctness_real", "engine": e} for e in engines]
    cells += [
        {
            "block": "pilot",
            "engine": e,
            "level": "S",
            "distribution": "base",
            "limit": 2000 if smoke else 100000,
        }
        for e in engines
    ]
    cells += [
        {
            "block": "calibration",
            "engine": e,
            "level": "B",
            "distribution": "base",
            "limit": 0,
        }
        for e in engines
    ]

    if smoke:
        cells += [
            workload_cell("mixed", e, "balanced", rate=2, writes=10, repeat=0)
            for e in engines
        ]
    else:
        # Randomize execution order within each repetition block.
        rng = random.Random(cfg["seed"])
        for repeat in range(cfg["static_repetitions"]):
            block = static_block(cfg, repeat)
            rng.shuffle(block)
            cells += block
        for repeat in range(cfg["mixed_repetitions"]):
            block = mixed_block(cfg, repeat)
            rng.shuffle(block)
            cells += block
        cells += [
            {"block": "diagnostic", "engine": e, "level": "B", "distribution": "base"}
            for e in engines
        ]

    return [
        {**cell, "id": f"{i:04d}-{cell['block']}-{cell['engine']}"}
        for i, cell in enumerate(cells)
    ]
