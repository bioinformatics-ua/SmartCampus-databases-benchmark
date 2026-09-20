"""Archive validation and deterministic population/distribution datasets."""

import json
import os
import shutil
import struct
import tarfile
from pathlib import Path

from common import (
    HERE,
    LOAD_GRACE_SECONDS,
    ROOT,
    Failure,
    atomic,
    call,
    cmd,
    digest,
    query,
    require,
)
from infrastructure import visible
from queries import fixture

# All mixed trials share a fixed virtual timeline, independent of execution date.
VIRTUAL_ORIGIN = 1767225600  # 2026-01-01T00:00:00Z


def safe_extract(archive, destination):
    """Extract only regular files and directories that stay inside `destination`."""
    destination.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive, "r:xz") as tar:
        for member in tar:
            name = Path(member.name)
            if (
                not (member.isfile() or member.isdir())
                or name.is_absolute()
                or ".." in name.parts
            ):
                raise Failure(f"Unsafe archive entry {member.name}")
            target = destination / member.name
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with tar.extractfile(member) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst, 8 << 20)


def duplicate_check(path):
    """Count repeated lines with an external sort, keeping the keys if any repeat."""
    sorted_path = path.with_suffix(".sorted")
    cmd(
        ["sort", "-S", "512M", "-T", path.parent, "-o", sorted_path, path],
        env={**os.environ, "LC_ALL": "C"},
    )
    previous = None
    duplicates = 0
    with open(sorted_path) as lines:
        for line in lines:
            if line == previous:
                duplicates += 1
            previous = line
    if not duplicates:
        sorted_path.unlink()
        path.unlink()
    return duplicates, sorted_path


def verify_prepared(base, record, cfg, smoke):
    if record.get("source_hash") != cfg["source_archive_sha256"] and not smoke:
        raise Failure("prepared input provenance mismatch")
    for dist in cfg["distributions"]:
        if digest(base / (dist + ".bin")) != record["binary_hashes"][dist]:
            raise Failure("prepared data checksum mismatch")


def smoke_input(base):
    """Write the query fixture in the raw API format read by the bridge."""
    directory = base / "input"
    directory.mkdir(exist_ok=True)
    readings = [
        {
            "userId": e["user"],
            "lastUpdatedTime": e["ts"],
            "connection": {"ssid": e["ssid"], "rssi": e["rssi"]},
        }
        for e in fixture()
    ]
    atomic(directory / "readings_0.json", {"response": readings})
    return directory


def archive_input(base, cfg):
    archive = ROOT / "data.tar.xz"
    if digest(archive) != cfg["source_archive_sha256"]:
        raise Failure("source archive does not match frozen checksum")
    cmd(["xz", "-t", archive])
    extract = base / "extracted"
    if extract.exists():
        shutil.rmtree(extract)
    safe_extract(archive, extract)
    return extract / "data/readings"


def prepare_data(cfg, smoke=False):
    """Convert raw readings into verified binary datasets, once per data directory."""
    base = ROOT / "study-data" / ("smoke" if smoke else "full")
    base.mkdir(parents=True, exist_ok=True)
    ready = base / "ready.json"
    if ready.exists():
        verify_prepared(base, json.loads(ready.read_text()), cfg, smoke)
        return base

    source = smoke_input(base) if smoke else archive_input(base, cfg)
    cmd([HERE / "bin/bridge", "--mode", "prepare", "--input", source, "--output", base])

    hashes = {}
    for dist in cfg["distributions"]:
        duplicates, evidence = duplicate_check(base / (dist + ".keys"))
        if duplicates:
            raise Failure(
                f"{dist}: {duplicates} duplicate event keys; choose an explicit "
                f"common deduplication policy before proceeding. Sorted keys "
                f"retained at {evidence}"
            )
        hashes[dist] = digest(base / (dist + ".bin"))
        manifest_path = base / (dist + ".json")
        manifest = json.loads(manifest_path.read_text())
        manifest["duplicate_keys"] = duplicates
        atomic(manifest_path, manifest)

    atomic(
        ready,
        {
            "source_hash": cfg["source_archive_sha256"],
            "binary_hashes": hashes,
            "protocol": cfg["protocol"],
        },
    )
    return base


def expected_rows(manifest, level, limit):
    if level == "S":
        expected = manifest["small_count"]
    elif level == "L":
        expected = manifest["count"] * 2
    else:
        expected = manifest["count"]
    return min(limit, expected) if limit else expected


def load_dataset(engine, cell, data, cfg, directory, mixed=False):
    """Bulk-load one dataset and return its observed time bounds and row count."""
    dist = cell.get("distribution", "base")
    manifest = json.loads((data / (dist + ".json")).read_text())
    level = cell.get("level", "B")
    limit = cell.get("limit", 0)
    # Mixed trials end loaded history 901 s before the origin; the workload fills
    # the final 900 s with generated recent events.
    shift = VIRTUAL_ORIGIN - 901 - manifest["max"] if mixed else 0

    request = {
        "path": str(data / (dist + ".bin")),
        "level": level,
        "limit": limit,
        "shift": shift,
        "batch": cfg["batch_rows"],
        "timeout": cfg["load_timeout_seconds"],
        "log": str(directory / "ingestion.jsonl"),
    }
    loaded = require(
        call("/load", request, cfg["load_timeout_seconds"] + LOAD_GRACE_SECONDS),
        "dataset load",
    )
    expected = expected_rows(manifest, level, limit)
    if loaded["count"] != expected:
        raise Failure("loader count differs from manifest")

    lo = manifest["min"] + shift
    hi = manifest["max"] + shift + 1
    visible(engine, expected, lo, hi, cfg)

    # Actual small/limited bounds can differ; validate and use observed bounds.
    bounds = require(
        query(engine, "1", lo, hi, cfg["query_timeout_seconds"]), "loaded bounds"
    )
    lo, maximum = map(int, bounds["rows"][0])
    hi = maximum + 1

    if engine in ("postgres", "timescaledb"):
        require(call("/exec", {"statements": ["ANALYZE user_events"]}), "analyze")

    atomic(
        directory / "load.json",
        {
            **loaded,
            "manifest": manifest,
            "level": level,
            "limit": limit,
            "shift": shift,
            "lo": lo,
            "hi": hi,
            "expected": expected,
        },
    )
    return lo, hi, expected, VIRTUAL_ORIGIN


def validation_sample(path: Path, limit: int = 512) -> list[dict]:
    """Read a bounded real-data sample without decoding a multi-gigabyte JSON chunk.

    Record layout (little-endian): u16 user length, user, u16 SSID length, SSID,
    i64 timestamp, f64 RSSI. It must match writeEvent in bridge/data.go.
    """
    events = []
    with path.open("rb") as stream:
        while len(events) < limit:
            size = stream.read(2)
            if not size:
                break
            user = stream.read(struct.unpack("<H", size)[0]).decode()
            ssid_length = struct.unpack("<H", stream.read(2))[0]
            ssid = stream.read(ssid_length).decode()
            timestamp, rssi = struct.unpack("<qd", stream.read(16))
            events.append({"user": user, "ssid": ssid, "ts": timestamp, "rssi": rssi})
    if not events:
        raise Failure("No events available for real-data validation")
    return events
