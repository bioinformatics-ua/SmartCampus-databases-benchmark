"""Shared I/O, timing, and bridge client primitives."""

import datetime as dt
import hashlib
import json
import os
import subprocess
import threading
import time
import urllib.request
from pathlib import Path

from queries import definition

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
STOP = threading.Event()

BRIDGE_URL = "http://127.0.0.1:18999"

REQUEST_GRACE_SECONDS = 15
LOAD_GRACE_SECONDS = 30
DEFAULT_CALL_TIMEOUT = 300 + LOAD_GRACE_SECONDS

# InfluxDB returns no table, rather than a zero count, when nothing matches.
INFLUX_ZERO_WHEN_EMPTY = ("2", "3", "5", "6", "7", "10", "11", "15", "16", "probe")


class Failure(RuntimeError):
    pass


def digest(path):
    sha = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(8 << 20), b""):
            sha.update(chunk)
    return sha.hexdigest()


def atomic_text(path, text):
    """Write text through a synced temporary file so readers never see partial data."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w") as stream:
        stream.write(text)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)


def atomic(path, value):
    """Write a JSON document atomically."""
    atomic_text(path, json.dumps(value, indent=2, allow_nan=False) + "\n")


def cmd(args, **kwargs):
    return subprocess.run([str(arg) for arg in args], check=True, **kwargs)


def log(text):
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    print(now, text, flush=True)


class Journal:
    """Thread-safe, line-buffered JSON Lines writer."""

    def __init__(self, path):
        self.file = open(path, "a", buffering=1)
        self.lock = threading.Lock()

    def add(self, value):
        with self.lock:
            self.file.write(json.dumps(value, allow_nan=False) + "\n")

    def close(self):
        self.file.flush()
        os.fsync(self.file.fileno())
        self.file.close()


def call(path, payload=None, timeout=DEFAULT_CALL_TIMEOUT):
    request = urllib.request.Request(
        BRIDGE_URL + path,
        data=json.dumps(payload or {}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def query(engine, qid, lo, hi, timeout=300, now=None, event=None):
    spec = definition(engine, str(qid), lo, hi, now, event)
    start = time.monotonic()
    try:
        result = call(
            "/query", {**spec, "timeout": timeout}, timeout + REQUEST_GRACE_SECONDS
        )
    except Exception as e:
        result = {
            "status": "infrastructure_error",
            "error": str(e),
            "rows": [],
            "duration_us": round((time.monotonic() - start) * 1e6),
        }
    if (
        engine == "influxdb"
        and str(qid) in INFLUX_ZERO_WHEN_EMPTY
        and result.get("status") == "ok"
        and result.get("rows") == []
    ):
        result["rows"] = [[0]]
    return {**spec, **result}


def require(result, label):
    if result["status"] != "ok":
        raise Failure(f"{label}: {result.get('error', result['status'])}")
    return result
