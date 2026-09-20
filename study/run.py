#!/usr/bin/env python3
"""Launch, resume, and report a frozen Docker database study."""

import argparse
import fcntl
import hashlib
import json
import multiprocessing
import signal
import subprocess
import sys
import time
from pathlib import Path

from audit import audit_historical, estimate_budget
from common import HERE, ROOT, STOP, Failure, atomic, cmd, digest, log
from datasets import load_dataset, prepare_data, validation_sample
from experiments import diagnostics, static, validate
from infrastructure import Engine, check_disk, preflight, setup
from report import generate_report
from schedule import MIXED_BLOCKS, schedule
from workloads import Mixed

# Small limits for the fixture-based end-to-end test; timings are not results.
SMOKE_OVERRIDES = {
    "db_cpus": 2,
    "db_memory_gib": 4,
    "min_free_gib": 1,
    "static_repetitions": 1,
    "mixed_repetitions": 1,
    "warmup_seconds": 2,
    "measurement_seconds": 5,
    "query_timeout_seconds": 15,
    "visibility_timeout_seconds": 60,
    "read_workers": 4,
    "monitor_interval_seconds": 2,
    "batch_rows": 1000,
}


def source_hash():
    """Hash every file that affects measurements, so edits invalidate a frozen run."""
    files = (
        [ROOT / "run-study.sh"]
        + sorted(HERE.glob("*.py"))
        + sorted((HERE / "bridge").glob("*.go"))
        + [HERE / "bridge/go.mod", HERE / "bridge/go.sum"]
    )
    sha = hashlib.sha256()
    for path in files:
        sha.update(str(path.relative_to(ROOT)).encode())
        sha.update(path.read_bytes())
    return sha.hexdigest()


def resolve_images(cfg, root):
    """Freeze image digests once and verify their availability when resuming."""
    images_file = root / "images.json"
    if images_file.exists():
        images = json.loads(images_file.read_text())
    else:
        images = {}
        for engine in cfg["engines"]:
            image = cfg["images"][engine]
            log("pulling " + image)
            cmd(["docker", "pull", image])
            inspect = cmd(
                ["docker", "image", "inspect", image], capture_output=True, text=True
            )
            digests = json.loads(inspect.stdout)[0].get("RepoDigests")
            if not digests:
                raise Failure("image has no digest")
            images[engine] = digests[0]
        atomic(images_file, images)
    for image in images.values():
        cmd(["docker", "image", "inspect", image], stdout=subprocess.DEVNULL)
    return images


def execute_cell(cell, data, cfg, attempt):
    """Dispatch one experiment after its isolated database becomes ready."""
    engine = cell["engine"]
    block = cell["block"]
    setup(engine)

    if block == "correctness":
        return validate(engine, cfg, attempt)
    if block == "correctness_real":
        return validate(engine, cfg, attempt, validation_sample(data / "base.bin"))

    mixed = block in MIXED_BLOCKS
    lo, hi, count, origin = load_dataset(engine, cell, data, cfg, attempt, mixed)
    if mixed:
        return Mixed(engine, cell, lo, hi, count, origin, cfg, attempt).run()
    if block == "diagnostic":
        return diagnostics(engine, lo, hi, cfg, attempt)
    # Static, distribution, pilot and calibration cells.
    return static(engine, lo, hi, cfg, attempt, cfg["seed"] + cell.get("repeat", 0))


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=HERE / "config.json")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--plan", action="store_true")
    parser.add_argument("--report-only", action="store_true")
    parser.add_argument(
        "--accept-source-update",
        action="store_true",
        help="Record a source revision and resume; configuration and schedule must be unchanged",
    )
    parser.add_argument("--only", help="comma-separated engines, e.g. postgres,questdb")
    parser.add_argument(
        "--rerun",
        help="comma-separated existing cell IDs to rerun with new attempt directories",
    )
    parser.add_argument(
        "--max-cells",
        type=int,
        help="finish this many pending cells then stop cleanly",
    )
    return parser.parse_args()


def load_config(args):
    cfg = json.loads(args.config.read_text())
    if args.only:
        selected = args.only.split(",")
        cfg["engines"] = [e for e in cfg["engines"] if e in selected]
        cfg["primary_engines"] = [
            e for e in cfg["primary_engines"] if e in cfg["engines"]
        ]
        if not cfg["engines"]:
            raise Failure("no selected engines")
    if args.smoke:
        cfg.update(SMOKE_OVERRIDES)
    return cfg


def verify_completed(root, complete):
    """Fail if any artifact of a completed cell changed since it was recorded."""
    previous = json.loads(complete.read_text())
    for name, expected in previous.get("artifact_hashes", {}).items():
        artifact = root / previous["attempt"] / name
        if not artifact.is_file() or digest(artifact) != expected:
            raise Failure(f"Completed artifact changed or missing: {artifact}")


def run_cell(cell, root, data, cfg, images, project, complete):
    attempt = root / "runs" / cell["id"] / f"attempt-{time.time_ns()}"
    attempt.mkdir()
    atomic(attempt / "cell.json", cell)
    atomic(attempt / "protocol.json", json.loads((root / "protocol.json").read_text()))
    log(f"{cell['id']} {cell}")
    start = time.monotonic()
    try:
        engine = cell["engine"]
        with Engine(engine, images[engine], cfg, attempt, project):
            result = execute_cell(cell, data, cfg, attempt)
        result.update(
            elapsed_seconds=time.monotonic() - start,
            cell=cell,
            attempt=str(attempt.relative_to(root)),
        )
        atomic(attempt / "result.json", result)
        result["artifact_hashes"] = {
            str(path.relative_to(attempt)): digest(path)
            for path in sorted(attempt.rglob("*"))
            if path.is_file()
        }
        atomic(complete, result)
    except Exception as e:
        failure = {
            "error": str(e),
            "elapsed_seconds": time.monotonic() - start,
            "cell": cell,
        }
        atomic(attempt / "failure.json", failure)
        complete.unlink(missing_ok=True)
        raise
    finally:
        generate_report(root)


def cell_worker(*args):
    """Exit the worker after each cell so failed workload threads cannot leak."""
    try:
        run_cell(*args)
    except Exception as error:
        log(f"FAILED: {error}")
        sys.exit(1)


def run_isolated_cell(cell, root, data, cfg, images, project, complete):
    process = multiprocessing.get_context("spawn").Process(
        target=cell_worker,
        args=(cell, root, data, cfg, images, project, complete),
    )
    process.start()
    process.join()
    if process.exitcode == 0:
        return True

    # A killed worker might not have had a chance to write its failure record.
    folder = root / "runs" / cell["id"]
    attempts = sorted(folder.glob("attempt-*"))
    if not attempts or not (attempts[-1] / "failure.json").exists():
        attempt = folder / f"attempt-{time.time_ns()}"
        attempt.mkdir()
        atomic(
            attempt / "failure.json",
            {
                "cell": cell,
                "error": f"Cell worker exited with code {process.exitcode}",
                "elapsed_seconds": 0,
            },
        )
    complete.unlink(missing_ok=True)
    log(f"FAILED {cell['id']}; continuing with the next cell")
    generate_report(root)
    return False


def freeze_protocol(path, frozen, accept_source_update=False):
    if path.exists():
        previous = json.loads(path.read_text())
        if previous != frozen:
            unchanged_design = all(
                previous[key] == frozen[key] for key in ("config", "schedule")
            )
            if not accept_source_update or not unchanged_design:
                raise Failure(
                    "protocol/source changed: use a new --output, or --accept-source-update "
                    "for a recorded code-only update with unchanged config and schedule"
                )
            history = (
                path.parent / "protocol-history" / f"revision-{time.time_ns()}.json"
            )
            atomic(history, {"previous": previous, "replacement": frozen})
            log(f"Recorded source update: {history}; completed artifacts retained")
    atomic(path, frozen)


def run_study(args, cfg, cells, root):
    (root / "environment").mkdir(exist_ok=True)
    (root / "runs").mkdir(exist_ok=True)

    frozen = {"config": cfg, "source_hash": source_hash(), "schedule": cells}
    protocol = root / "protocol.json"
    freeze_protocol(protocol, frozen, args.accept_source_update)

    preflight(cfg, root, args.smoke)
    audit_historical(root / "historical-audit.json")
    images = resolve_images(cfg, root)
    log("preparing and verifying data")
    data = prepare_data(cfg, args.smoke)
    atomic(
        root / "data.json",
        {
            dist: json.loads((data / (dist + ".json")).read_text())
            for dist in cfg["distributions"]
        },
    )

    project = "campus-" + hashlib.sha256(str(root).encode()).hexdigest()[:12]
    rerun = set(args.rerun.split(",")) if args.rerun else set()
    if rerun - {c["id"] for c in cells}:
        raise Failure("unknown rerun cell ID")

    executed = 0
    failed = 0
    try:
        for cell in cells:
            if STOP.is_set():
                raise Failure("interrupted; rerun the same command to resume")
            cell_root = root / "runs" / cell["id"]
            cell_root.mkdir(exist_ok=True)
            complete = cell_root / "complete.json"

            if complete.exists() and cell["id"] not in rerun:
                verify_completed(root, complete)
                continue
            if args.rerun and cell["id"] not in rerun:
                continue  # --rerun runs only the named cells.

            check_disk(cfg, root)
            if not args.smoke:
                estimate_budget(root, cells, cfg)
            prerequisites = [
                check
                for check in cells
                if check["engine"] == cell["engine"]
                and check["block"] in ("correctness", "correctness_real")
                and cell["block"] not in ("correctness", "correctness_real")
                and not (root / "runs" / check["id"] / "complete.json").exists()
            ]
            if prerequisites:
                attempt = cell_root / f"attempt-{time.time_ns()}"
                attempt.mkdir()
                atomic(
                    attempt / "failure.json",
                    {
                        "cell": cell,
                        "status": "blocked",
                        "error": "Not measured: missing successful correctness checks: "
                        + ", ".join(check["id"] for check in prerequisites),
                        "elapsed_seconds": 0,
                    },
                )
                failed += 1
                log(f"BLOCKED {cell['id']}; continuing with the next cell")
            elif not run_isolated_cell(
                cell, root, data, cfg, images, project, complete
            ):
                failed += 1
            rerun.discard(cell["id"])

            executed += 1
            if args.max_cells and executed >= args.max_cells:
                break
        atomic(
            root / "study-status.json",
            {
                "status": "completed_with_failures" if failed else "completed",
                "attempted_this_launch": executed,
                "failed_this_launch": failed,
            },
        )
        log(
            f"Finished requested study cells; {failed} failed. Report: {root / 'report.html'}"
        )
        return 1 if failed else 0
    finally:
        generate_report(root)


def main():
    args = parse_args()
    cfg = load_config(args)
    cells = schedule(cfg, args.smoke)

    if args.plan:
        plan = {"config": cfg, "cells": cells, "total_cells": len(cells)}
        print(json.dumps(plan, indent=2))
        return

    default_output = ROOT / "study-results" / ("smoke" if args.smoke else "full")
    root = (args.output or default_output).resolve()
    root.mkdir(parents=True, exist_ok=True)

    if args.report_only:
        generate_report(root)
        return

    with open(ROOT / "study-results.lock", "a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise Failure("another study controller holds the lock")
        return run_study(args, cfg, cells, root)


if __name__ == "__main__":

    def interrupted(signum, frame):
        STOP.set()

    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    try:
        sys.exit(main() or 0)
    except (Failure, subprocess.CalledProcessError) as e:
        log("STOPPED: " + str(e))
        sys.exit(1)
