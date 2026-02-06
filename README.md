# Smart Campus Databases Benchmark

A comprehensive benchmarking framework for evaluating six database systems on time-series IoT data from a smart campus WiFi scenario. The benchmark measures both ingestion throughput and query performance across 20 diverse queries, covering simple lookups, aggregations, percentiles, and advanced time-series analytics.

## Databases Under Test

| Database | Type | Version |
|----------|------|---------|
| **PostgreSQL** | Relational (baseline) | latest |
| **TimescaleDB** | Time-series (PG extension) | latest-pg17 |
| **QuestDB** | Time-series | 8.3.3 |
| **CrateDB** | Distributed SQL | latest |
| **ClickHouse** | Columnar OLAP | latest |
| **InfluxDB** | Time-series | 2.7 |

## Key Results

Averaged over 5 runs on ~58M records of WiFi connectivity data:

### Ingestion Throughput

| Database | Median Rate (records/s) | Speedup vs CrateDB |
|----------|------------------------|---------------------|
| ClickHouse | 1,628,462 | **43.4x** |
| PostgreSQL | 767,813 | 20.5x |
| TimescaleDB | 539,628 | 14.4x |
| QuestDB | 83,988 | 2.2x |
| InfluxDB | 70,241 | 1.9x |
| CrateDB | 37,480 | 1.0x |

### Query Performance (Median Speedup vs CrateDB)

| Database | Median Speedup | Queries Supported |
|----------|---------------|-------------------|
| ClickHouse | **23.82x** | 20/20 |
| QuestDB | 8.76x | 20/20 |
| TimescaleDB | 0.44x | 20/20 |
| PostgreSQL | 0.01x | 14/20 |
| InfluxDB | -- | 2/20 |

## Project Structure

```
.
├── src/
│   ├── entrypoint.go           # Benchmark engine (Go)
│   ├── benchmark.sh            # Orchestration script
│   ├── docker-compose.yaml     # Database containers
│   ├── go.mod / go.sum         # Go dependencies
│   ├── README.md               # Query documentation
│   └── benchmarks/             # JSON result files
├── data/
│   └── readings/               # Input data (27 JSON files, ~5.7 GB)
├── generate_speedup_report.py  # Performance report generator
├── plot_query_comparison.py    # Visualization script
├── report.md                   # Generated results summary
└── query_plots/                # Generated comparison charts
```

## Prerequisites

- **Docker** & **Docker Compose**
- **Go** 1.24+
- **Python** 3 with `matplotlib` and `numpy`
- ~60 GB disk space (data + database volumes)

## Quick Start

### 1. Prepare the data

Place the input data files in `data/readings/` (27 JSON files named `readings_1.json` through `readings_27.json`).

### 2. Run the full benchmark suite

```bash
cd src/
./benchmark.sh
```

This will:
- Spin up all six databases via Docker Compose
- Compile the Go benchmark binary
- Run 5 complete iterations (tear down and recreate containers between each)
- Output JSON results to `src/benchmarks/`

### 3. Run specific databases only

```bash
./benchmark.sh postgres,timescaledb,clickhouse
```

### 4. Generate the report and plots

```bash
python3 generate_speedup_report.py src/benchmarks/*.json
python3 plot_query_comparison.py src/benchmarks/*.json -o query_plots/
```

## Data Format

Each input file contains an array of WiFi connectivity events:

```json
{
  "userId": "simuser-42",
  "lastUpdatedTime": 1750795783,
  "connection": {
    "ssid": "AP-101",
    "rssi": -26.59
  }
}
```

These are loaded into a `user_events` table with the schema:

| Column | Type | Description |
|--------|------|-------------|
| `user_id` | VARCHAR | User identifier |
| `timestamp` | TIMESTAMPTZ | Event time |
| `rssi` | REAL | Signal strength (dBm) |
| `ssid` | VARCHAR | Access point name |

## Benchmark Queries

The suite includes 20 queries organized into categories:

| # | Category | Description |
|---|----------|-------------|
| 1 | Time bounds | MIN/MAX timestamp |
| 2-3 | Counting | Total records, distinct users |
| 4 | Aggregation | Average RSSI |
| 5-7 | Time filtering | Before/after/around midpoint |
| 8 | Time-series | 24h hourly aggregation |
| 9 | Top-K | Most active users |
| 10-11 | Filtering | Strong/weak signal counts |
| 12 | Top-K | Most common SSIDs |
| 13 | Per-user stats | RSSI min/max/avg by user |
| 14 | Percentiles | RSSI Q1/median/Q3 |
| 15-16 | Time splitting | First/second half counts |
| 17 | Patterns | Hourly activity distribution |
| 18 | Variance | Daily RSSI variance |
| 19 | Peak detection | Top 5 busiest hours |
| 20 | Sessions | User session duration analysis |

See [`src/README.md`](src/README.md) for the full SQL/Flux query implementations per database.

## How It Works

1. **Setup** -- Docker Compose starts all six database containers.
2. **Ingestion** -- The Go binary loads 27 data chunks sequentially, measuring throughput per batch.
3. **Querying** -- After ingestion completes, all 20 queries execute and their durations are recorded.
4. **Output** -- Results are written as JSON (`{database}Benchmark_{run}.json`).
5. **Analysis** -- Python scripts aggregate results across runs and generate comparison tables and charts.

## Technology Stack

- **Go 1.24** -- Benchmark engine with native drivers for each database
- **Python 3** -- Post-processing, statistics, and visualization
- **Docker Compose** -- Reproducible database deployment
- **Drivers** -- `pgx` (PostgreSQL/TimescaleDB/CrateDB), `clickhouse-go`, `go-questdb-client`, `influxdb-client-go`
