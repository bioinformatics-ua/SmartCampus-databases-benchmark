"""Docker isolation, readiness, resource logging, and VM checks."""

import json
import os
import shutil
import socket
import subprocess
import threading
import time
import urllib.request
from pathlib import Path

from common import (
    BRIDGE_URL,
    HERE,
    ROOT,
    STOP,
    Failure,
    atomic,
    call,
    cmd,
    query,
    require,
)

# The bridge port plus every host port published by `compose`.
HOST_PORTS = (18999, 15432, 19001, 18812, 19000, 14200, 18086)

DATA_DIRECTORIES = {
    "postgres": "/var/lib/postgresql/data",
    "timescaledb": "/var/lib/postgresql/data",
    "clickhouse": "/var/lib/clickhouse",
    "questdb": "/var/lib/questdb",
    "cratedb": "/data",
    "influxdb": "/var/lib/influxdb2",
}

SCHEMAS = {
    "postgres": [
        "CREATE TABLE user_events(user_id TEXT NOT NULL,ssid TEXT NOT NULL,"
        "ts TIMESTAMPTZ NOT NULL,rssi DOUBLE PRECISION NOT NULL)",
        "CREATE INDEX events_ts ON user_events(ts)",
    ],
    "timescaledb": [
        "CREATE TABLE user_events(user_id TEXT NOT NULL,ssid TEXT NOT NULL,"
        "ts TIMESTAMPTZ NOT NULL,rssi DOUBLE PRECISION NOT NULL)",
        "CREATE EXTENSION IF NOT EXISTS timescaledb",
        "SELECT create_hypertable('user_events','ts',"
        "chunk_time_interval => INTERVAL '4 hours')",
    ],
    "clickhouse": [
        "CREATE TABLE user_events(user_id String,ssid String,"
        "ts DateTime64(6,'UTC'),rssi Float64) ENGINE=MergeTree ORDER BY ts",
    ],
    "questdb": [
        "CREATE TABLE user_events(user_id SYMBOL,ssid SYMBOL,ts TIMESTAMP,rssi DOUBLE) "
        "TIMESTAMP(ts) PARTITION BY DAY WAL",
    ],
    "cratedb": [
        "CREATE TABLE user_events(user_id TEXT,ssid TEXT,ts TIMESTAMP WITH TIME "
        "ZONE,rssi DOUBLE PRECISION) CLUSTERED INTO 4 SHARDS WITH "
        "(number_of_replicas=0)",
    ],
    "influxdb": [],  # Schemaless.
}


def compose(engine, image, cfg):
    """Build a single-service Compose file with fixed CPU and memory limits."""
    memory = cfg["db_memory_gib"]
    cpus = cfg["db_cpus"]
    service = {
        "image": image,
        "cpus": cpus,
        "mem_limit": f"{memory}g",
        "memswap_limit": f"{memory}g",
        "labels": {"campus.study": "true"},
        "stop_grace_period": "60s",
        "environment": {"TZ": "UTC"},
    }
    cores = sorted(os.sched_getaffinity(0))
    if len(cores) >= cpus + 2:
        service["cpuset"] = ",".join(map(str, cores[:cpus]))
    service["volumes"] = ["dbdata:" + DATA_DIRECTORIES[engine]]
    environment = service["environment"]

    if engine in ("postgres", "timescaledb"):
        service["ports"] = ["127.0.0.1:15432:5432"]
        environment.update(
            POSTGRES_USER="bench", POSTGRES_PASSWORD="bench", POSTGRES_DB="bench"
        )
        settings = {
            "shared_buffers": f"{max(128, memory * 1024 // 4)}MB",
            "work_mem": "32MB",
            "effective_cache_size": f"{memory * 3 // 4}GB",
            "max_connections": "100",
            "timezone": "UTC",
            "max_parallel_workers": cpus,
            "max_parallel_workers_per_gather": min(cpus, 4),
        }
        service["command"] = ["postgres"]
        for name, value in settings.items():
            service["command"] += ["-c", f"{name}={value}"]

    elif engine == "clickhouse":
        service["ports"] = ["127.0.0.1:19001:9000"]
        environment.update(
            CLICKHOUSE_USER="bench",
            CLICKHOUSE_PASSWORD="bench",
            CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT="1",
        )
        service["ulimits"] = {"nofile": {"soft": 262144, "hard": 262144}}

    elif engine == "questdb":
        service["ports"] = ["127.0.0.1:19000:9000", "127.0.0.1:18812:8812"]
        environment.update(
            QDB_SHARED_WORKER_COUNT=str(min(cpus, 8)),
            QDB_HTTP_WORKER_COUNT="2",
            QDB_PG_WORKER_COUNT="2",
            QDB_CAIRO_COMMIT_LAG="0",
        )

    elif engine == "cratedb":
        service["ports"] = ["127.0.0.1:15432:5432", "127.0.0.1:14200:4200"]
        environment["CRATE_HEAP_SIZE"] = f"{max(1, min(28, memory // 2))}g"
        service["command"] = [
            "crate",
            "-Cnetwork.host=0.0.0.0",
            "-Cdiscovery.type=single-node",
            "-Ccluster.name=campus-benchmark",
        ]

    else:  # influxdb
        service["ports"] = ["127.0.0.1:18086:8086"]
        environment.update(
            DOCKER_INFLUXDB_INIT_MODE="setup",
            DOCKER_INFLUXDB_INIT_USERNAME="bench",
            DOCKER_INFLUXDB_INIT_PASSWORD="benchmark-local-password",
            DOCKER_INFLUXDB_INIT_ORG="study",
            DOCKER_INFLUXDB_INIT_BUCKET="bench",
            DOCKER_INFLUXDB_INIT_ADMIN_TOKEN="study-token-local-only",
        )

    return {"services": {"db": service}, "volumes": {"dbdata": {}}}


class Engine:
    """Context manager for one database container and its bridge process."""

    def __init__(self, engine, image, cfg, directory, project):
        self.engine = engine
        self.cfg = cfg
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.project = project
        self.bridge = None
        self.bridge_log = None
        self.container_id = None
        self.stop = threading.Event()
        self.monitor = None
        self.composefile = self.directory / "compose.json"
        atomic(self.composefile, compose(engine, image, cfg))
        self.args = ["docker", "compose", "-p", project, "-f", str(self.composefile)]

    def compose_command(self, *args):
        cmd(self.args + list(args), stdout=self.docker_log, stderr=self.docker_log)

    def __enter__(self):
        try:
            return self.start()
        except Exception:
            if hasattr(self, "docker_log"):
                self.__exit__(Exception, None, None)
            raise

    def start(self):
        # The same study project is reused only sequentially; cleanup is scoped to it.
        self.docker_log = open(self.directory / "docker.log", "a")
        self.compose_command("down", "--volumes", "--remove-orphans")
        self.compose_command("up", "-d", "--pull", "never")
        self.bridge_log = open(self.directory / "bridge.log", "a")

        deadline = time.monotonic() + 240
        while time.monotonic() < deadline and not STOP.is_set():
            self.bridge = subprocess.Popen(
                [str(HERE / "bin/bridge"), "--engine", self.engine],
                stdout=self.bridge_log,
                stderr=self.bridge_log,
            )
            for _ in range(10):
                if self.bridge.poll() is not None:
                    break  # The bridge exited, usually because the database is not up.
                try:
                    if self.bridge_healthy():
                        self.record_container()
                        self.monitor = threading.Thread(target=self.watch, daemon=True)
                        self.monitor.start()
                        return self
                except Exception:
                    pass
                time.sleep(0.5)
            if self.bridge.poll() is None:
                self.bridge.terminate()
                self.bridge.wait(timeout=15)
            time.sleep(1)

        raise Failure(f"{self.engine} did not become ready; see {self.directory}")

    def bridge_healthy(self):
        with urllib.request.urlopen(BRIDGE_URL + "/health", timeout=1) as response:
            return json.load(response)["engine"] == self.engine

    def record_container(self):
        self.container_id = cmd(
            self.args + ["ps", "-q", "db"], capture_output=True, text=True
        ).stdout.strip()
        inspect = ["docker", "inspect", self.container_id]
        info = json.loads(cmd(inspect, capture_output=True, text=True).stdout)
        atomic(self.directory / "container.json", info)

    def sample(self):
        stats = subprocess.run(
            [
                "docker",
                "stats",
                "--no-stream",
                "--format",
                "{{json .}}",
                self.container_id,
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if stats.returncode == 0 and stats.stdout.strip():
            container = json.loads(stats.stdout)
        else:
            container = {"error": stats.stderr}
        return {
            "at": time.time(),
            "container": container,
            "proc_stat": Path("/proc/stat").read_text().splitlines()[0],
            "meminfo": Path("/proc/meminfo").read_text(),
            "diskstats": Path("/proc/diskstats").read_text(),
            "loadavg": Path("/proc/loadavg").read_text(),
            "disk_free": shutil.disk_usage(self.directory).free,
        }

    def watch(self):
        with open(self.directory / "metrics.jsonl", "a", buffering=1) as out:
            while not self.stop.is_set():
                try:
                    out.write(json.dumps(self.sample()) + "\n")
                except Exception as e:
                    error = {"at": time.time(), "monitor_error": str(e)}
                    out.write(json.dumps(error) + "\n")
                self.stop.wait(self.cfg["monitor_interval_seconds"])

    def __exit__(self, typ, val, tb):
        self.stop.set()
        if self.monitor:
            self.monitor.join(timeout=15)
        if self.bridge and self.bridge.poll() is None:
            self.bridge.terminate()
            try:
                self.bridge.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.bridge.kill()
                self.bridge.wait(timeout=10)
        if self.container_id is not None:
            inspect = subprocess.run(
                ["docker", "inspect", self.container_id], capture_output=True, text=True
            )
            final = self.directory / "container-final.json"
            final.write_text(inspect.stdout or inspect.stderr)
        to_log = {"stdout": self.docker_log, "stderr": self.docker_log}
        subprocess.run(self.args + ["logs", "--no-color"], **to_log)
        # Always stop; retain failed volumes for inspection until a deliberate resume.
        cleanup = ["down", "--remove-orphans"] + (["--volumes"] if typ is None else [])
        subprocess.run(self.args + cleanup, **to_log)
        self.docker_log.close()
        if self.bridge_log:
            self.bridge_log.close()


def setup(engine):
    statements = SCHEMAS[engine]
    if statements:
        require(call("/exec", {"statements": statements}), "schema setup")


def visible(engine, expected, lo, hi, cfg):
    """Wait until exactly `expected` rows are visible in [lo, hi)."""
    deadline = time.monotonic() + cfg["visibility_timeout_seconds"]
    last = {}
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        timeout = min(cfg["query_timeout_seconds"], remaining)
        if timeout <= 0:
            break
        last = query(engine, "2", lo, hi, timeout=timeout)
        if last["status"] == "timeout":
            raise Failure(
                f"row-count validation timed out; expected {expected} rows, "
                f"actual count unknown: {last}"
            )
        if last["status"] in ("error", "infrastructure_error"):
            raise Failure(f"row-count validation failed: {last}")
        if last["status"] == "ok":
            if last["rows"] == [[expected]]:
                return
            if last["rows"] and last["rows"][0][0] > expected:
                break  # Too many rows will never converge.
        if STOP.wait(1):
            break
    raise Failure(f"visible row count mismatch: expected {expected}, got {last}")


def meminfo_line(field):
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith(field + ":"):
            return line
    raise Failure(f"/proc/meminfo has no {field} field")


def check_tools():
    for tool in ("docker", "go", "sort", "xz", "git"):
        if not shutil.which(tool):
            raise Failure(
                f"{tool} is required; install it before launching "
                "(no packages are installed by this script)"
            )
    cmd(["docker", "info"], stdout=subprocess.DEVNULL)
    cmd(["docker", "compose", "version"], stdout=subprocess.DEVNULL)


def check_resources(cfg):
    if len(os.sched_getaffinity(0)) < cfg["db_cpus"] + 2:
        raise Failure(
            "VM needs db_cpus + 2 available vCPUs; adjust config before first launch"
        )
    memory_bytes = int(meminfo_line("MemTotal").split()[1]) * 1024
    if memory_bytes < (cfg["db_memory_gib"] + 8) * 1024**3:
        raise Failure("VM needs DB memory plus 8 GiB client/OS headroom")


def check_machine_identity(root):
    """Refuse to mix results from different VMs or resource allocations."""
    cpu_models = {
        line
        for line in Path("/proc/cpuinfo").read_text().splitlines()
        if line.startswith("model name")
    }
    fingerprint = {
        "machine_id": Path("/etc/machine-id").read_text().strip(),
        "cpu_affinity": sorted(os.sched_getaffinity(0)),
        "memory_total": meminfo_line("MemTotal"),
        "cpu_models": sorted(cpu_models),
    }
    path = root / "environment" / "machine.json"
    if path.exists() and json.loads(path.read_text()) != fingerprint:
        raise Failure(
            "VM identity or resource allocation changed; use a new output directory"
        )
    atomic(path, fingerprint)


def check_ports():
    for port in HOST_PORTS:
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(("127.0.0.1", port))
            except OSError:
                raise Failure(
                    f"port {port} is occupied; "
                    "do not stop unrelated services automatically"
                )


def capture_environment(root):
    captures = {
        "uname": ["uname", "-a"],
        "lscpu": ["lscpu"],
        "docker": ["docker", "info"],
        "docker-version": ["docker", "version"],
        "compose": ["docker", "compose", "version"],
        "go": ["go", "version"],
        "disks": ["lsblk", "-J"],
        "mounts": ["findmnt", "-J"],
        "git": ["git", "rev-parse", "HEAD"],
    }
    snapshot = root / "environment" / f"launch-{time.time_ns()}"
    snapshot.mkdir()
    for name, args in captures.items():
        result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True)
        (snapshot / f"{name}.txt").write_text(result.stdout + result.stderr)
    for name in ("meminfo", "cpuinfo"):
        (snapshot / f"{name}.txt").write_text(Path("/proc", name).read_text())


def preflight(cfg, root, smoke):
    check_tools()
    if not smoke:
        check_resources(cfg)
    check_machine_identity(root)
    check_disk(cfg, root)
    check_ports()
    capture_environment(root)
    cmd(
        ["go", "build", "-mod=readonly", "-o", str(HERE / "bin/bridge"), "."],
        cwd=HERE / "bridge",
    )


def check_disk(cfg, root):
    usage = shutil.disk_usage(root)
    if usage.free < max(cfg["min_free_gib"] * 1024**3, usage.total * 0.2):
        raise Failure("disk reserve reached; free space before resuming")
