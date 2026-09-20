"""Bounded paced reads, writes, and visibility probes."""

import math
import queue
import random
import threading
import time

from common import REQUEST_GRACE_SECONDS, STOP, Failure, Journal, call, query, require
from infrastructure import visible
from schedule import PATTERNS

RECENT_HISTORY_SECONDS = 900


def service_for(qid):
    """Latency objective class of a mixed-workload query."""
    if qid in ("3", "19"):
        return "periodic"
    if qid == "A1":
        return "alert"
    return "dashboard"


class Mixed:
    """One open-loop trial: paced reads and writes against a shared virtual clock."""

    def __init__(self, engine, cell, lo, hi, count, origin, cfg, directory):
        self.engine = engine
        self.cell = cell
        self.lo = lo
        self.hi = hi
        self.count = count
        self.origin = origin
        self.cfg = cfg
        self.directory = directory

        self.journal = Journal(directory / "requests.jsonl")
        self.writes = Journal(directory / "writes.jsonl")
        self.fresh = Journal(directory / "freshness.jsonl")

        self.readq = queue.Queue(cfg["read_queue"])
        self.writeq = queue.Queue(cfg["write_queue_seconds"])
        self.samples = queue.Queue(1)  # At most one write waits for a freshness probe.
        self.done = threading.Event()
        self.errors = queue.Queue()
        self.threads = []

        self.uncertain = 0
        self.acked = 0
        self.accepted = 0
        self.rejected = 0
        self.lock = threading.Lock()

    def event_batch(self, index, rate):
        # Seconds and device IDs provide unique keys across every generated batch.
        ts = self.origin + index
        return [
            {
                "user": f"live-{(index * rate + i) % 30000:05d}",
                "ssid": f"live-ap-{(index * rate + i) % 100:03d}",
                "ts": ts,
                "rssi": float(-95 + (index + i) % 65),
            }
            for i in range(rate)
        ]

    def phase(self, offset):
        return "warmup" if offset < self.cfg["warmup_seconds"] else "measured"

    # ----------------------------------------------------------------- workers

    def read_worker(self):
        while True:
            job = self.readq.get()
            if job is None:
                self.readq.task_done()
                return
            qid, scheduled, virtual_now, phase = job
            dispatched = time.monotonic()
            remaining = self.cfg["query_timeout_seconds"] - (dispatched - scheduled)
            if remaining <= 0:
                result = {
                    "status": "timeout",
                    "error": "expired in client queue",
                    "id": qid,
                    "rows": [],
                }
            else:
                result = query(
                    self.engine,
                    qid,
                    self.lo,
                    virtual_now + 1,
                    remaining,
                    now=virtual_now,
                )
            finished = time.monotonic()
            result.update(
                phase=phase,
                scheduled_s=scheduled - self.start,
                dispatch_s=dispatched - self.start,
                finish_s=finished - self.start,
                latency_us=round((finished - scheduled) * 1e6),
                queue_us=round((dispatched - scheduled) * 1e6),
                service=service_for(qid),
            )
            if result["status"] in ("error", "infrastructure_error"):
                self.errors.put(result)
            self.journal.add(result)
            self.readq.task_done()

    def send_batch(self, events, remaining):
        if remaining <= 0:
            return {"status": "timeout", "error": "write expired in client queue"}
        try:
            return call(
                "/write",
                {"events": events, "timeout": max(0.001, remaining)},
                max(1, remaining) + REQUEST_GRACE_SECONDS,
            )
        except Exception as e:
            return {"status": "infrastructure_error", "error": str(e)}

    def write_worker(self):
        while True:
            job = self.writeq.get()
            if job is None:
                self.writeq.task_done()
                return
            index, scheduled, phase = job
            events = self.event_batch(index, self.cell["writes"])
            sent = time.monotonic()
            remaining = self.cfg["query_timeout_seconds"] - (sent - scheduled)
            result = self.send_batch(events, remaining)
            ack = time.monotonic()

            if result["status"] == "ok":
                with self.lock:
                    self.acked += len(events)
                if self.cell.get("freshness", True):
                    try:
                        self.samples.put_nowait((events[-1], sent, ack, phase))
                    except queue.Full:
                        pass  # A sample is already waiting; skip this batch.
            elif result["status"] == "timeout" and remaining > 0:
                # The server may still commit a batch whose request timed out.
                with self.lock:
                    self.uncertain += len(events)
            elif result["status"] in ("error", "infrastructure_error"):
                self.errors.put(result)

            self.writes.add(
                {
                    **result,
                    "phase": phase,
                    "offered_rows": len(events),
                    "scheduled_s": scheduled - self.start,
                    "sent_s": sent - self.start,
                    "ack_s": ack - self.start,
                    "queue_s": sent - scheduled,
                }
            )
            self.writeq.task_done()

    def probe_until_visible(self, event, ack):
        """Poll for one acknowledged event; returns (lower, upper, error, probes)."""
        lower = 0.0
        upper = None
        error = None
        probes = []
        timeout = self.cfg["freshness_timeout_seconds"]
        while time.monotonic() - ack < timeout and not STOP.is_set():
            wait = self.next_poll - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            probe_start = time.monotonic()
            result = query(
                self.engine,
                "probe",
                event["ts"],
                event["ts"] + 1,
                timeout=min(5, timeout),
                event=event,
            )
            seen = time.monotonic()
            self.next_poll = seen + self.cfg["freshness_poll_seconds"]
            probes.append(
                {
                    "start_s": probe_start - self.start,
                    "end_s": seen - self.start,
                    "status": result["status"],
                    "rows": result.get("rows"),
                }
            )
            if result["status"] != "ok":
                error = result.get("error")
                break
            if result["rows"] == [[1]]:
                upper = seen - ack
                break
            if result["rows"] != [[0]]:
                error = "non-unique probe event"
                break
            # A negative response only establishes absence at or after request start.
            lower = max(lower, probe_start - ack)
        return lower, upper, error, probes

    def freshness_worker(self):
        self.next_poll = 0.0
        while not self.done.is_set() or not self.samples.empty():
            try:
                event, sent, ack, phase = self.samples.get(timeout=0.2)
            except queue.Empty:
                continue
            lower, upper, error, probes = self.probe_until_visible(event, ack)
            self.fresh.add(
                {
                    "phase": phase,
                    "event": event,
                    "send_s": sent - self.start,
                    "ack_s": ack - self.start,
                    "lower_s": lower,
                    "upper_s": upper,
                    "censored": upper is None,
                    "error": error,
                    "probes": probes,
                }
            )
            if error:
                self.errors.put({"freshness_error": error})
            self.samples.task_done()

    # ------------------------------------------------------------------- trial

    def seed_recent_history(self):
        """Write the 15 minutes before the virtual origin that dashboards query."""
        cfg = self.cfg
        rate = self.cell["writes"] or 500
        batch = []
        for second in range(-RECENT_HISTORY_SECONDS, 0):
            if STOP.is_set():
                raise Failure("interrupted")
            batch.extend(self.event_batch(second, rate))
            if len(batch) >= cfg["batch_rows"] or second == -1:
                require(
                    call(
                        "/write",
                        {"events": batch, "timeout": cfg["query_timeout_seconds"]},
                        cfg["query_timeout_seconds"] + REQUEST_GRACE_SECONDS,
                    ),
                    "recent history",
                )
                batch = []
        self.count += RECENT_HISTORY_SECONDS * rate
        visible(self.engine, self.count, self.lo, self.origin, self.cfg)

    def start_workers(self):
        self.read_threads = [
            threading.Thread(target=self.read_worker, daemon=True)
            for _ in range(self.cfg["read_workers"])
        ]
        self.threads = self.read_threads + [
            threading.Thread(target=self.write_worker, daemon=True),
            threading.Thread(target=self.freshness_worker, daemon=True),
        ]
        for thread in self.threads:
            thread.start()

    def read_interval(self, offset):
        rate = self.cell["rate"]
        if self.cell["pattern"] == "burst":
            # Alternating 10 s high / 50 s low bursts, 1x mean offered rate.
            rate *= 4 if int(offset) % 60 < 10 else 0.4
        return 1 / rate

    def offer_read(self, qid, offset):
        phase = self.phase(offset)
        job = (qid, self.start + offset, self.origin + int(offset), phase)
        try:
            self.readq.put_nowait(job)
        except queue.Full:
            self.journal.add(
                {
                    "id": qid,
                    "status": "rejected",
                    "phase": phase,
                    "scheduled_s": offset,
                    "service": service_for(qid),
                }
            )

    def offer_write(self, offset):
        phase = self.phase(offset)
        rows = self.cell["writes"]
        try:
            self.writeq.put_nowait((offset, self.start + offset, phase))
            self.accepted += rows
        except queue.Full:
            self.rejected += rows
            self.writes.add(
                {
                    "status": "rejected",
                    "phase": phase,
                    "offered_rows": rows,
                    "scheduled_s": offset,
                }
            )

    def generate_load(self, duration):
        """Offer reads and writes on schedule; returns the number of reads offered."""
        choices = PATTERNS[self.cell["pattern"]].copy()
        random.Random(self.cfg["seed"] + self.cell.get("repeat", 0)).shuffle(choices)
        next_read = 0.0
        next_write = 0
        offered_reads = 0
        while time.monotonic() - self.start < duration:
            if STOP.is_set():
                raise Failure("interrupted")
            if not self.errors.empty():
                raise Failure(f"mixed workload error: {self.errors.get()}")
            elapsed = time.monotonic() - self.start
            while self.cell["rate"] and next_read <= elapsed and next_read < duration:
                self.offer_read(choices[offered_reads % len(choices)], next_read)
                offered_reads += 1
                next_read += self.read_interval(next_read)
            while (
                self.cell["writes"] and next_write <= elapsed and next_write < duration
            ):
                self.offer_write(next_write)
                next_write += 1
            time.sleep(0.01)
        return offered_reads

    def pending(self):
        return self.readq.unfinished_tasks or self.writeq.unfinished_tasks

    def drain(self):
        # Every queued operation carries its original deadline; no unbounded drain.
        deadline = time.monotonic() + self.cfg["query_timeout_seconds"] + 20
        while self.pending() and time.monotonic() < deadline:
            if not self.errors.empty():
                raise Failure(str(self.errors.get()))
            time.sleep(0.2)
        if self.pending():
            raise Failure("drain exceeded deadline")

    def stop_workers(self):
        self.done.set()
        for _ in self.read_threads:
            self.readq.put(None)
        self.writeq.put(None)
        for thread in self.threads:
            thread.join(timeout=self.cfg["freshness_timeout_seconds"] + 10)
        if any(thread.is_alive() for thread in self.threads):
            raise Failure("worker did not terminate")
        if not self.errors.empty():
            raise Failure(str(self.errors.get()))

    def final_row_count(self, duration):
        cfg = self.cfg
        upper = self.origin + math.ceil(duration) + 1
        acknowledged = self.count + self.acked
        # Timeouts make write delivery uncertain; do not assert exact counts then.
        if not self.uncertain:
            visible(self.engine, acknowledged, self.lo, upper, cfg)
        result = require(
            query(self.engine, "2", self.lo, upper, cfg["query_timeout_seconds"]),
            "final row count",
        )
        final = result["rows"][0][0]
        if not acknowledged <= final <= acknowledged + self.uncertain:
            raise Failure(
                f"final row count outside acknowledged/uncertain bounds: {final}"
            )
        return final

    def run(self):
        cfg = self.cfg
        self.seed_recent_history()
        self.start = time.monotonic()
        duration = cfg["warmup_seconds"] + cfg["measurement_seconds"]
        self.start_workers()
        try:
            offered_reads = self.generate_load(duration)
            self.drain()
            self.stop_workers()
            final_count = self.final_row_count(duration)
            return {
                "offered_reads": offered_reads,
                "offered_writes": self.accepted + self.rejected,
                "acknowledged_writes": self.acked,
                "uncertain_writes": self.uncertain,
                "final_visible_rows": final_count,
                "rejected_writes": self.rejected,
                "initial_visible_rows": self.count,
                "measurement_seconds": cfg["measurement_seconds"],
                "virtual_origin": self.origin,
            }
        finally:
            self.done.set()
            # A failing run exits the controller after engine cancellation. Keep journals
            # open until daemon workers cease or process exit, rather than racing closed
            # files.
            if not any(thread.is_alive() for thread in self.threads):
                self.journal.close()
                self.writes.close()
                self.fresh.close()
