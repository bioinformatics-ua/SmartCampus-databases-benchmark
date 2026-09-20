# Extended paper benchmark

Run the study from the benchmark repository on the VM:

```sh
./run-study.sh
```

This prepares the data, checks query results, runs the frozen experiment matrix,
and updates `study-results/full/report.html` after every experiment. It runs
one database at a time in Docker. The full study has 432 experiment cells;
completion time depends heavily on bulk-load speed. Full-data calibration
estimates the remaining runtime and stops if the estimate exceeds 35 days.
That stop requires reviewing the scope, not silently reducing repetitions.

For an SSH session, keep the controller alive after disconnection:

```sh
nohup ./run-study.sh > study.log 2>&1 < /dev/null &
tail -f study.log
```

## VM preparation

Install the tools yourself before launching: Linux, Python 3.10 or later, Go
compatible with `bridge/go.mod`, Docker Engine with Compose v2, Git, GNU sort,
and xz. The script installs no host packages. It downloads the configured Docker
images and the Go modules needed to compile the database client.

The default resource settings target a 16-vCPU, 64-GiB VM. Each database gets
12 vCPUs and a 48-GiB memory limit. Reserve at least 50 GiB and 20% of the
filesystem; these are safety reserves, not estimates of total storage needed.
Allow ample NVMe space for extracted data, three transformed datasets, database
storage, and raw results. Keep host activity low and keep the VM allocation
unchanged throughout the study. Record the hypervisor and physical host
configuration alongside the automatically collected guest metadata.

Copy this entire repository, including the untracked `study/` directory,
`run-study.sh`, and `data.tar.xz`, to the VM. A plain Git clone does not contain
these new files until you commit them. The optional `study-data/full/` cache
avoids repeated preparation; its binary checksums are verified before use.
The original dataset and benchmark source are retained.

Check the complete schedule without starting databases:

```sh
./run-study.sh --plan > planned-study.json
```

A short fixture-based end-to-end test uses smaller resource limits:

```sh
./run-study.sh --smoke
```

The smoke dataset is generated locally. Its timings are not paper results.

## Experiment coverage

| Stage | Measurements |
| --- | --- |
| Audit and correctness | Historical artifact audit, 24 query contracts against an independent oracle, real-input sample, invalid queries, deadline classification and recovery |
| Pilot and calibration | Bounded pilot loads, complete baseline loads, runtime estimate |
| Scale | All six engines, S/B/L populations, five independent runs, all 20 inherited query contracts |
| Distribution | Hotspot and fragmented-identity transformations at baseline size, all six engines, five independent runs |
| Mixed workloads | Dashboard, balanced, reporting-heavy, alert-heavy and burst patterns; paced arrivals, queue delay, completed latency, errors and rejection rates |
| Freshness | Visibility after acknowledged writes, interval bounds, censored samples and observer-disabled controls |
| Controls | Read-only and write-only trials; screening trials for CrateDB and InfluxDB |
| Diagnostics | Query plans or Flux profiler output plus Docker and guest CPU, memory and I/O logs |

The primary mixed-workload matrix covers PostgreSQL, TimescaleDB, ClickHouse,
and QuestDB. CrateDB and InfluxDB receive a smaller screening matrix. This scope
choice is explicit in `config.json` and the frozen `protocol.json`.

S selects user identifiers by a deterministic hash, approximately 10% of the
source population. B uses all source events. L makes two copies with disjoint
identifier prefixes. The time range is held constant. Hotspot and fragmented
identities are synthetic sensitivity experiments, not observed campus behavior.

Mixed experiments shift the historical data to a fixed virtual timeline and
prefill a recent 15-minute window. D1/D2 dashboard queries and A1 alerts query
newly ingested data. Reads and writes are scheduled independently of completion;
bounded queues record rejection rather than silently lowering offered load.
The default trial has 120 seconds of warmup and 600 seconds of measurement.

## Stop, resume and investigate

Interrupt with Ctrl-C for a controlled stop. Run the same command to resume.
Completed cells are skipped. Failed experiments are recorded and the study
continues with the next cell. An interrupted cell restarts in a new attempt
folder. A study lock prevents concurrent controllers. Database cleanup is
scoped to the study's Compose project; successful database volumes are
removed. The next experiment cleans the previous database volume; failure logs
and raw artifacts remain outside that volume.

The code, configuration and schedule are frozen on first launch. Configuration or
schedule changes require a new output directory, such as `--output study-results/revised`.
For a deliberate code-only repair, `--accept-source-update` archives the old and
new protocol in `protocol-history/`, retains completed runs and retries unfinished
cells. New attempts save their protocol snapshot. Review whether a code repair
changes measurement semantics before combining revisions in a paper.
Do not combine different protocols or hardware as repetitions of one experiment.
Image digests are recorded and reused. Do not edit raw results.

Useful commands:

```sh
# Regenerate the report from saved results, without starting a database.
./run-study.sh --report-only

# Run only the next two pending cells and then stop.
./run-study.sh --max-cells 2

# Repeat selected cells while preserving previous attempt directories.
./run-study.sh --rerun 0012-pilot-postgres
```

Use actual cell IDs from your `protocol.json`. `--only postgres,questdb` creates
a different protocol and needs its own output directory. Investigate failures,
variance and resource logs before choosing targeted reruns. Do not rerun only
unfavorable measurements or silently discard failures.

## Outputs and interpretation

Each attempt retains the cell definition, Compose settings, image/container
metadata, logs, raw query or request records, ingestion records, and metrics.
`summary.json`, `report.html` and `RESULTS.md` provide the writing handoff.
Pilot and calibration timings are labeled separately from comparative trials.
Inspect `plans.json` for unsupported diagnostic statements.

Latency summaries explicitly separate successful requests from all offered
requests. p99 requires at least 1,000 successful observations within a trial.
The configured one-second interactive and 60-second periodic objectives are
experimental targets, not proven application requirements. Freshness polling
bounds visibility time and can affect the database. Censored samples and probe
errors must remain visible when interpreting results. Approximate quantiles
have a separate accuracy contract; they are not interchangeable with exact R7
quantiles. Repetitions, rather than individual requests, are the experimental
units for inference.

The old hardware is unavailable. Treat these VM runs as a new controlled
comparison and keep conference timings separate. These scripts support the
experimental weeks. The final claims, statistical interpretation, figure
selection and publisher-compliance review still require author review.

## Code layout and checks

`run.py` coordinates the study. `schedule.py` defines the experimental matrix;
`queries.py` contains query contracts and the independent oracle;
`infrastructure.py` manages Docker; `datasets.py` handles input provenance;
`workloads.py` schedules mixed traffic; `experiments.py` runs correctness and
static trials; `audit.py` checks historical artifacts and runtime budget;
`report.py` summarizes saved results; `common.py` contains shared I/O helpers.
`bridge/` contains the Go database adapters and streaming data preparation.

```sh
python3 -m unittest discover -s study -p 'test_*.py'
(cd study/bridge && go test ./... && go vet ./...)
# Optional development tools, if available:
ruff check study
ruff format --check study
gofmt -l study/bridge/*.go
```

Keep functions focused, use blank lines between logical steps, format Python
with Ruff and Go with gofmt, and add tests for query semantics or scheduling
behavior when changing them.

## Failure behavior

An individual cell failure writes `failure.json`, cleans up its worker and moves
on. The report shows the latest failed attempt, including failed reruns of earlier
successes. A failed diagnostic query marks the diagnostic cell failed. A failed
correctness check blocks that engine's dependent cells; other engines continue.
Blocked cells are explicitly marked and are never counted as measurements.

`study-status.json` records how many cells were attempted and failed during
that launch. The process returns nonzero after finishing the requested schedule
if cells failed or were blocked; it does not abort at the first cell failure.
Performance timeouts and queue rejections remain recorded workload outcomes.
Manual interruption and study-wide failures, such as invalid input, changed
hardware or unavailable disk space, can still stop execution.
