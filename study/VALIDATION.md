# Local validation record

16 September 2026. These checks ran on the development machine, using isolated
Docker containers with 2-CPU and 4-GiB limits. They establish functional behavior
on small inputs. They do not establish full-scale performance or target-VM
readiness.

- Ruff formatting and configured lint checks pass.
- Fourteen Python unit tests pass, including query contracts, empty intervals,
  deterministic scheduling, resource settings and report failure denominators.
- Go unit tests and `go vet ./...` pass; Go sources are formatted with gofmt.
- The complete 30-cell smoke run passes across PostgreSQL, TimescaleDB,
  ClickHouse, QuestDB, CrateDB and InfluxDB. Each engine executes correctness,
  sample validation, pilot, calibration and a paced mixed trial.
- All six engines pass the 24-query oracle comparison on a separate 512-event
  sample from the downloaded archive.
- InfluxDB query/operator profiling passes for all four diagnostic queries.
- All five workload patterns plus read-only, write-only and observer-disabled
  controls pass short live PostgreSQL trials.
- Resume skips completed cells; explicit reruns preserve earlier attempts. These
  behaviors passed a three-launch PostgreSQL integration check.
- The complete source dataset and both distribution transformations have been
  prepared and checked for duplicate event keys. No duplicates were found.

The final smoke artifacts are at `/tmp/campus-final-smoke/` on this development
machine. Earlier checks are in `/tmp/campus-full-smoke-1/` and
`/tmp/campus-real-validation/`. Temporary paths are local evidence and are not
required inputs for tomorrow's VM run. Later formatting changes preserve Python
syntax trees and were checked again with the unit suite.

Known validation limits: the real-input oracle uses a bounded sample; performance
repetitions on the full data have not run; expired-deadline classification does
not prove cancellation of every expensive query after execution begins; profiler
and resource data still need interpretation; VM host contention cannot be ruled
out from guest metrics alone.

## 17 September 2026 readability refactor

The Python controller and Go bridge were restructured for legibility without
intended behavior changes. The source hash therefore differs from earlier runs.

- Query text, oracle results, schedules, Compose files, generated workloads and
  reports were compared byte for byte before and after. The only difference is
  whitespace inside the InfluxDB Q1 Flux reducer.
- `bridge --mode prepare` output is byte-identical on the smoke input and on the
  first 250,000 archive records.
- Unit tests, Ruff, gofmt and `go vet ./...` pass.
- The complete 30-cell smoke run passes across all six engines. A second
  launch verifies the completed artifacts and skips every cell.

## InfluxDB timeout correction, 17 September 2026

The VM's full-data validation exposed the InfluxDB SDK's default 20-second HTTP
request timeout. The bridge now disables that secondary timer and retains the
per-request context deadline. A delayed local HTTP server test passes after
21 seconds, and a second request confirms context cancellation still works.
Row-count checks now use the configured budget rather than a 60-second cap;
a timed-out check fails immediately and reports that the actual count is unknown.
Sixteen Python tests and the Go test/vet suite pass. Full-data validation on the
VM must be rerun; these checks do not establish that the full count completes
within the configured deadline. Source changes require a new output directory.

## Continue after cell failure, 17 September 2026

A live Docker test deliberately failed PostgreSQL correctness, then completed
TimescaleDB correctness, both real-sample checks and the TimescaleDB pilot. The
PostgreSQL pilot was marked blocked by its failed correctness prerequisite.
The controller finished all six requested cells before returning nonzero, with
four completions and two failed/blocked cells in the report. Each cell now has
an isolated worker process to prevent leftover mixed-workload threads from
interfering with later cells. Twenty Python tests pass, including diagnostic
error marking, failed rerun visibility and explicit source-revision history.

## Rename and quality audit, 20 September 2026

"Campaign" was renamed to "study" throughout the controller, the bridge, the
configuration and the documentation. The renamed paths are `study/`,
`run-study.sh`, `study-data/`, `study-results/`, `study-results.lock` and
`study-status.json`; the configuration key is `study_budget_days`. The InfluxDB
organisation and local admin token were renamed in the same commit in both
`infrastructure.py` and `bridge/main.go`, which must always agree. The prepared
13-GiB `study-data/full/` cache was moved, not regenerated, and its `ready.json`
checksums still verify.

Audit repairs in the same change:

- `summary.json`, `report.html` and `RESULTS.md` are now written through the
  synced temporary-file helper. The report is regenerated after every cell, so
  an interrupt during the write previously truncated the file being monitored.
- A duplicate-key failure keeps the sorted key file and names it in the error,
  instead of deleting the only evidence before raising.
- The R type 7 quantile had two independent implementations; `report.py` now
  calls the one in `queries.py`.
- The population-level axis in the report chart is derived from the level list
  rather than assuming exactly three levels.
- A missing `/proc/meminfo` field raises a stop reason instead of an attribute
  error; `uncertain` write rows are counted under the same lock as `acked`;
  the result lock file is opened for append; the bridge HTTP grace periods are
  named constants; the unused QuestDB client requirement was dropped and the Go
  module renamed from `src` to `bridge`.

Ten tests were added for burst pacing, queue rejection accounting, population
levels and the runtime budget: thirty Python tests, the Go test and vet suites,
Ruff check/format and gofmt all pass. The source hash therefore differs from
every earlier run, so this code needs a new output directory. Nothing in this
change was executed against a database; no full-scale or smoke run has been
repeated since the rename.
