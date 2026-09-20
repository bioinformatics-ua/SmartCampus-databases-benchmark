"""Inspectable JSON, Markdown and standalone HTML reports; no plotting dependencies."""

import html
import json
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path

from common import atomic, atomic_text
from queries import quantile_r7

LEVELS = ("S", "B", "L")
SERIES_COLORS = ["#2563eb", "#d97706", "#059669", "#7c3aed", "#dc2626", "#0891b2"]

CAUTIONS = [
    "Pilot and calibration cells are excluded from final comparisons.",
    "Exact and approximate quantiles have separate accuracy labels.",
    "Successful-only tail latency excludes failures; "
    "use violation rates and coverage alongside it.",
    "All-request p95 is withheld when requests fail or time out.",
    "Quantiles describe individual trials; requests are not independent repetitions.",
    "Data transformations and virtualized hardware limit external validity.",
]

STYLE = (
    "body{font:16px system-ui;max-width:1200px;margin:40px auto;padding:0 20px;"
    "color:#17212b}"
    "table{border-collapse:collapse;width:100%;font-size:13px;margin:24px 0}"
    "td,th{text-align:left;padding:8px;border-bottom:1px solid #ddd}"
    "th{background:#edf2f7}"
    "svg{width:100%;max-width:900px}"
    "h2{margin-top:36px}"
)

HTML_HEAD = (
    '<!doctype html><html lang="en"><meta charset="utf-8">'
    '<meta name="viewport" content="width=device-width">'
    f"<title>DBMS study report</title><style>{STYLE}</style>"
)

WRITING_HANDOFF = (
    "\n\n## Writing handoff\n\nUse the new measurements to answer "
    "population/distribution, mixed-load, and freshness questions. Cite the "
    "conference baseline separately. Update the novelty ledger with completed "
    "experiments only. Do not describe synthetic SSID counts as validated "
    "building occupancy or observed device IDs as people. Title, abstract, "
    "conclusions, reused-item captions, and the two-paragraph differences "
    "statement still require author review.\n"
)


# ------------------------------------------------------------------ statistics


def records(path):
    if not path.exists():
        return
    with open(path) as lines:
        for line in lines:
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue  # interrupted final line is never a completed result


def percentile(values, p):
    """R type 7 quantile of an unordered sample; None for an empty sample."""
    if not values:
        return None
    return quantile_r7(sorted(values), p)


def aggregate_requests(path):
    """Summarize measured requests per service, counting failures as violations."""
    groups = defaultdict(list)
    for request in records(path):
        if request.get("phase") == "measured":
            groups[request.get("service", "unknown")].append(request)

    summary = {}
    for service, requests in groups.items():
        completed = [
            r["latency_us"] / 1000 for r in requests if r.get("status") == "ok"
        ]
        objective_ms = 60000 if service == "periodic" else 1000
        failures = len(requests) - len(completed)
        violations = sum(
            r.get("status") != "ok"
            or r.get("latency_us", float("inf")) / 1000 > objective_ms
            for r in requests
        )
        summary[service] = {
            "offered": len(requests),
            "completed": len(completed),
            "statuses": dict(Counter(r.get("status") for r in requests)),
            "p50_successful_ms": percentile(completed, 0.5),
            "p95_successful_ms": percentile(completed, 0.95),
            "p99_successful_ms": (
                percentile(completed, 0.99) if len(completed) >= 1000 else None
            ),
            "p95_all_requests_ms": (
                percentile(completed, 0.95) if failures == 0 else None
            ),
            "objective_ms": objective_ms,
            "objective_violation_fraction": violations / len(requests),
            "failure_fraction": failures / len(requests),
            "max_client_queue_ms": max(
                (r.get("queue_us", 0) / 1000 for r in requests), default=0
            ),
        }
    return summary


def aggregate_static(trials):
    """Summarize independent static repetitions without hiding failed queries."""
    groups = defaultdict(list)
    for trial in trials:
        cell = trial["cell"]
        if cell["block"] not in ("static", "distribution"):
            continue
        key = (cell["engine"], cell["level"], cell["distribution"], trial["id"])
        groups[key].append(trial)

    summaries = []
    for (engine, level, distribution, query), group in sorted(groups.items()):
        values = [t["duration_us"] / 1000 for t in group if t["status"] == "ok"]
        mean = statistics.mean(values) if values else None
        deviation = statistics.stdev(values) if len(values) > 1 else None
        summaries.append(
            {
                "engine": engine,
                "level": level,
                "distribution": distribution,
                "query": query,
                "trials": len(group),
                "successful_trials": len(values),
                "statuses": dict(Counter(t["status"] for t in group)),
                "mean_successful_ms": mean,
                "stdev_successful_ms": deviation,
                "coefficient_of_variation": (
                    deviation / mean if deviation is not None and mean else None
                ),
                "cell_ids": [t["cell"]["id"] for t in group],
            }
        )
    return summaries


def aggregate_writes(path, duration):
    """Separate acknowledged throughput from offered or uncertain writes."""
    batches = [r for r in records(path) if r.get("phase") == "measured"]
    acknowledged = [b for b in batches if b["status"] == "ok"]
    rows = sum(b["offered_rows"] for b in acknowledged)
    latencies = [(b["ack_s"] - b["scheduled_s"]) * 1000 for b in acknowledged]
    return {
        "offered_rows": sum(b["offered_rows"] for b in batches),
        "acknowledged_rows": rows,
        "acknowledged_rows_per_measurement_second": rows / duration,
        "batch_statuses": dict(Counter(b["status"] for b in batches)),
        "p95_ack_latency_ms": percentile(latencies, 0.95),
        "includes_drain_completions": True,
    }


def summarize_freshness(cell, samples):
    known = [s["upper_s"] for s in samples if s.get("upper_s") is not None]
    return {
        "cell": cell,
        "samples": len(samples),
        "censored": sum(s.get("censored", False) for s in samples),
        "p95_observed_upper_s": percentile(known, 0.95),
        "definite_within_5s": sum(
            s.get("upper_s") is not None and s["upper_s"] <= 5 for s in samples
        ),
        "definite_over_5s": sum(s.get("lower_s", 0) > 5 for s in samples),
    }


# ---------------------------------------------------------------------- HTML


def svg_lines(points, title):
    """Log-scale line chart of {series: [(level index, value ms)]}."""
    if not points:
        return ""
    values = [v for series in points.values() for _, v in series if v > 0]
    if not values:
        return ""

    low = math.floor(math.log10(min(values)))
    high = max(low + 1, math.ceil(math.log10(max(values))))
    left, right, top, bottom = 70, 600, 40, 280

    def x_position(index):
        return left + index * (right - left) / max(1, len(LEVELS) - 1)

    def y_position(exponent):
        return bottom - (exponent - low) / (high - low) * (bottom - top)

    escaped = html.escape(title)
    parts = [
        f'<svg viewBox="0 0 800 340" role="img" aria-label="{escaped}">'
        '<rect width="800" height="340" fill="white"/>'
        f'<text x="20" y="22">{escaped}</text>'
    ]
    for exponent in range(low, high + 1):
        y = y_position(exponent)
        parts.append(
            f'<path d="M{left} {y} H{right}" stroke="#ddd"/>'
            f'<text x="5" y="{y + 4}" font-size="11">{10**exponent:g} ms</text>'
        )
    for index, label in enumerate(LEVELS):
        parts.append(f'<text x="{x_position(index)}" y="305">{label}</text>')
    for i, (series, series_points) in enumerate(sorted(points.items())):
        color = SERIES_COLORS[i % len(SERIES_COLORS)]
        coordinates = []
        for index, value in sorted(series_points):
            if value <= 0:
                continue
            x = x_position(index)
            y = y_position(math.log10(value))
            coordinates.append(f"{x},{y}")
            parts.append(f'<circle cx="{x}" cy="{y}" r="4" fill="{color}"/>')
        parts.append(
            f'<polyline points="{" ".join(coordinates)}" stroke="{color}" fill="none"/>'
            f'<text x="620" y="{55 + i * 24}" fill="{color}" font-size="12">'
            f"{html.escape(series)}</text>"
        )
    parts.append("</svg>")
    return "".join(parts)


def html_table(headers, rows):
    head = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(v))}</td>" for v in row) + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def render_html(summary, results, cells, chart):
    failures = summary["failed_cells"]
    mixed = summary["mixed"]

    body = (
        "<h1>Smart Campus DBMS study</h1>"
        f"<p>{len(results)} / {len(cells)} cells complete. "
        f"{len(failures)} unfinished cells have recorded failures.</p>"
        '<p><a href="summary.json">Machine-readable results</a> · '
        '<a href="protocol.json">Frozen protocol</a> · '
        '<a href="images.json">Image digests</a></p>'
    )
    body += (
        "<h2>Interpretation limits</h2><ul>"
        + "".join(f"<li>{html.escape(c)}</li>" for c in summary["cautions"])
        + "</ul>"
        + svg_lines(
            chart,
            "Distinct-user query Q3 by population level: mean across independent runs",
        )
    )

    if failures:
        body += "<h2>Failures requiring attention</h2>" + html_table(
            ["Cell", "Error"],
            [(f["cell"]["id"], f["details"]["error"]) for f in failures],
        )

    mixed_rows = [
        (
            trial["cell"]["id"],
            service,
            m["offered"],
            m["completed"],
            "n/a"
            if m["p95_successful_ms"] is None
            else round(m["p95_successful_ms"], 2),
            f"{m['objective_violation_fraction']:.1%}",
        )
        for trial in mixed
        for service, m in trial["services"].items()
    ]
    body += "<h2>Mixed workload trials</h2>" + html_table(
        [
            "Cell",
            "Service",
            "Offered",
            "Completed",
            "Successful-only p95, ms",
            "Objective violations",
        ],
        mixed_rows,
    )

    body += "<h2>Freshness</h2>" + html_table(
        ["Cell", "Samples", "Censored", "Observed p95 upper bound, s"],
        [
            (f["cell"]["id"], f["samples"], f["censored"], f["p95_observed_upper_s"])
            for f in summary["freshness"]
        ],
    )

    body += "<h2>Scale and distribution trials</h2>" + html_table(
        [
            "Engine",
            "Level",
            "Distribution",
            "Query",
            "Success/total",
            "Mean successful ms",
            "Std. deviation ms",
        ],
        [
            (
                s["engine"],
                s["level"],
                s["distribution"],
                s["query"],
                f"{s['successful_trials']}/{s['trials']}",
                s["mean_successful_ms"],
                s["stdev_successful_ms"],
            )
            for s in summary["static_summaries"]
        ],
    )

    body += (
        "<h2>Write throughput</h2>"
        "<p>Measured arrival cohort, including acknowledgements during drain.</p>"
    ) + html_table(
        [
            "Cell",
            "Offered rows",
            "Acknowledged rows",
            "Acknowledged rows / measurement second",
            "p95 acknowledgement ms",
        ],
        [
            (
                trial["cell"]["id"],
                trial["writes"]["offered_rows"],
                trial["writes"]["acknowledged_rows"],
                trial["writes"]["acknowledged_rows_per_measurement_second"],
                trial["writes"]["p95_ack_latency_ms"],
            )
            for trial in mixed
        ],
    )

    body += (
        "<h2>Targeted rerun review</h2>"
        f"<p>{len(summary['rerun_candidates'])} query groups need review. "
        "See rerun_candidates in summary.json. "
        "A high variance flag is not permission to discard a run.</p>"
    )

    body += "<h2>Completed cells</h2>" + html_table(
        ["Cell", "Engine", "Seconds"],
        [
            (r["cell"]["id"], r["cell"]["engine"], round(r["elapsed_seconds"], 1))
            for r in results
        ],
    )
    return HTML_HEAD + body + "</html>"


def render_markdown(summary, results, cells):
    return (
        f"# Study results\n\n{len(results)} of {len(cells)} cells complete. "
        "See [report](report.html) and [JSON](summary.json).\n\n"
        + "\n".join("- " + c for c in summary["cautions"])
        + WRITING_HANDOFF
    )


# -------------------------------------------------------------------- report


def generate_report(root):
    root = Path(root)
    protocol_path = root / "protocol.json"
    protocol = json.loads(protocol_path.read_text()) if protocol_path.exists() else {}
    cells = protocol.get("schedule", [])

    results = []
    failures = []
    static = []
    mixed = []
    fresh = []
    for cell in cells:
        folder = root / "runs" / cell["id"]
        complete = folder / "complete.json"
        attempts = sorted(folder.glob("attempt-*"))
        latest_failure = attempts[-1] / "failure.json" if attempts else None
        if latest_failure and latest_failure.exists():
            details = json.loads(latest_failure.read_text())
            failures.append({"cell": cell, "details": details})
            continue  # An earlier success must not hide a failed rerun.
        if not complete.exists():
            continue

        result = json.loads(complete.read_text())
        attempt = root / result["attempt"]
        results.append(result)

        for trial in records(attempt / "queries.jsonl"):
            if trial.get("phase") == "measured":
                static.append({"cell": cell, **trial})

        if (attempt / "requests.jsonl").exists():
            mixed.append(
                {
                    "cell": cell,
                    "services": aggregate_requests(attempt / "requests.jsonl"),
                    "summary": result,
                    "writes": aggregate_writes(
                        attempt / "writes.jsonl", result["measurement_seconds"]
                    ),
                }
            )

        samples = [
            s
            for s in records(attempt / "freshness.jsonl")
            if s.get("phase") == "measured"
        ]
        if samples:
            fresh.append(summarize_freshness(cell, samples))

    # Mean Q3 latency per engine and population level, for the headline chart.
    q3_durations = defaultdict(list)
    for trial in static:
        cell = trial["cell"]
        if (
            cell["block"] == "static"
            and trial["id"] == "Q3"
            and trial["status"] == "ok"
        ):
            q3_durations[(cell["engine"], cell["level"])].append(
                trial["duration_us"] / 1000
            )
    chart = defaultdict(list)
    for (engine, level), durations in q3_durations.items():
        chart[engine].append((LEVELS.index(level), statistics.mean(durations)))

    static_summaries = aggregate_static(static)
    rerun_candidates = [
        {
            **row,
            "reason": "incomplete success coverage or coefficient of variation above 20%",
        }
        for row in static_summaries
        if row["successful_trials"] != row["trials"]
        or (row["coefficient_of_variation"] or 0) > 0.2
    ]

    summary = {
        "complete": len(results),
        "planned": len(cells),
        "failed_cells": failures,
        "static": static,
        "static_summaries": static_summaries,
        "rerun_candidates": rerun_candidates,
        "mixed": mixed,
        "freshness": fresh,
        "cautions": CAUTIONS,
    }
    atomic(root / "summary.json", summary)
    atomic_text(root / "report.html", render_html(summary, results, cells, chart))
    atomic_text(root / "RESULTS.md", render_markdown(summary, results, cells))
