#!/bin/bash

# Database Benchmark Script
# Usage: ./benchmark.sh [database_list]
# 
# Parameters:
#   database_list: Comma-separated list of databases to benchmark
#                  Options: postgres,timescaledb,questdb,cratedb,influxdb
#                  Default: all databases if no parameter provided
#
# Examples:
#   ./benchmark.sh                           # Run all databases
#   ./benchmark.sh postgres,timescaledb      # Run only PostgreSQL and TimescaleDB
#   ./benchmark.sh cratedb                   # Run only CrateDB

# Default list of all available databases
ALL_DBS="postgres,timescaledb,questdb,cratedb,influxdb,clickhouse"

# Get database list from command line parameter or use default
DBS_TO_TEST="${1:-$ALL_DBS}"

# Convert comma-separated list to array
IFS=',' read -ra DB_ARRAY <<< "$DBS_TO_TEST"

# Function to display usage
show_usage() {
    echo "Usage: $0 [database_list]"
    echo ""
    echo "Parameters:"
    echo "  database_list: Comma-separated list of databases to benchmark"
    echo "                 Options: postgres,timescaledb,questdb,cratedb,influxdb,clickhouse"
    echo "                 Default: all databases if no parameter provided"
    echo ""
    echo "Examples:"
    echo "  $0                           # Run all databases"
    echo "  $0 postgres,timescaledb      # Run only PostgreSQL and TimescaleDB"
    echo "  $0 cratedb                   # Run only CrateDB"
    echo ""
    echo "Available databases:"
    echo "  - postgres    : PostgreSQL database"
    echo "  - timescaledb : TimescaleDB (PostgreSQL extension for time-series)"
    echo "  - questdb     : QuestDB (time-series database)"
    echo "  - cratedb     : CrateDB (distributed SQL database)"
    echo "  - influxdb    : InfluxDB (time-series database)"
    echo "  - clickhouse  : ClickHouse (columnar database for analytics)"
}

# Function to check if database is valid
is_valid_db() {
    local db="$1"
    case "$db" in
        postgres|timescaledb|questdb|cratedb|influxdb|clickhouse)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

# Function to run specific database benchmark
run_benchmark() {
    local db_type="$1"
    local iteration="$2"
    
    case "$db_type" in
        postgres)
            echo "Running PostgreSQL benchmark..."
            ./entrypoint -conn "postgres://postgres:example@localhost:5433/pdb" -type postgres -o postgresBenchmark_${iteration}.json
            ;;
        timescaledb)
            echo "Running TimescaleDB benchmark..."
            ./entrypoint -conn "postgres://postgres:example@localhost:5432/tsdb" -type timescaledb -o timescaledbBenchmark_${iteration}.json
            ;;
        questdb)
            echo "Running QuestDB benchmark..."
            ./entrypoint -conn "http::addr=localhost:9000;username=admin;password=quest;:::postgres://admin:quest@localhost:8812/qdb" -type questdb -o qdbBenchmark_${iteration}.json
            ;;
        cratedb)
            echo "Running CrateDB benchmark..."
            ./entrypoint -conn "postgres://crate@localhost:5434/crate" -type cratedb -o cratedbBenchmark_${iteration}.json
            ;;
        influxdb)
            echo "Running InfluxDB benchmark..."
            ./entrypoint -conn "http://localhost:8086" -type influxdb -o influxdbBenchmark_${iteration}.json
            ;;
        clickhouse)
            echo "Running ClickHouse benchmark..."
            ./entrypoint -conn "localhost:9001" -type clickhouse -o clickhouseBenchmark_${iteration}.json
            ;;
        *)
            echo "Error: Unknown database type '$db_type'"
            return 1
            ;;
    esac
}

# Check for help flag
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    show_usage
    exit 0
fi

# Validate all requested databases
echo "Validating requested databases..."
for db in "${DB_ARRAY[@]}"; do
    # Trim whitespace
    db=$(echo "$db" | xargs)
    if ! is_valid_db "$db"; then
        echo "Error: Invalid database '$db'"
        echo "Valid options: postgres, timescaledb, questdb, cratedb, influxdb, clickhouse"
        exit 1
    fi
done

# Display what will be benchmarked
echo "Databases to benchmark: ${DBS_TO_TEST}"
echo ""

# SETUP
for iter in {1..5}; do
  echo "Setting up environment..."
  docker compose down -v
  docker compose up -d
  go build ./entrypoint.go

  echo "Waiting for services to start..."
  sleep 5

  # BENCHMARKING
  echo ""
  echo "Starting benchmarks..."
  echo "======================"

  for db in "${DB_ARRAY[@]}"; do
      # Trim whitespace
      db=$(echo "$db" | xargs)
      echo ""
      echo "--- Benchmarking $db ---"

      if run_benchmark "$db" "$iter"; then
          echo "✓ $db benchmark completed successfully"
      else
          echo "✗ $db benchmark failed"
      fi
  done

  echo ""
  echo "======================"
  echo "One benchmark round completed"
done

# CLEANUP
echo ""
echo "Cleaning up..."
docker compose down -v

echo "Benchmark script finished."