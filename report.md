# Database Performance Speedup Report (Averaged Results)

**Baseline Database:** cratedb
**Analysis Method:** Results averaged across multiple benchmark runs per database type

## Ingestion Performance

### Ingestion Statistics (Averaged)

| Database | Median Duration (ms) | Median Records/Batch | Median Rate (records/s) | Avg Total Records | Avg Total Duration (ms) | Files |
|----------|-------------------|-------------------|---------------------|---------------|---------------------|-------|
| clickhouse | 1236.0 | 2,012,437 | 1,628,462 | 57,957,124 | 36,059 | 5 |
| cratedb | 53664.0 | 2,012,437 | 37,480 | 57,957,124 | 1,546,474 | 5 |
| influxdb | 28629.0 | 2,012,437 | 70,241 | 57,957,124 | 970,268 | 5 |
| postgres | 2620.0 | 2,012,437 | 767,813 | 57,957,124 | 77,733 | 5 |
| questdb | 23956.0 | 2,012,437 | 83,988 | 57,957,124 | 689,981 | 5 |
| timescaledb | 3717.0 | 2,012,437 | 539,628 | 57,957,124 | 110,002 | 5 |

### Ingestion Speedups

| Database | Duration Speedup | Rate Improvement |
|----------|------------------|------------------|
| cratedb | 1.00x | 1.00x |
| clickhouse | 43.42x | 43.45x |
| influxdb | 1.87x | 1.87x |
| postgres | 20.48x | 20.49x |
| questdb | 2.24x | 2.24x |
| timescaledb | 14.44x | 14.40x |

## Query Performance

### Query Execution Times (Averaged)

| Query ID | Description | clickhouse (ms) | cratedb (ms) | influxdb (ms) | postgres (ms) | questdb (ms) | timescaledb (ms) |
|----------|-------------|------------|------------|------------|------------|------------|------------|
| 1 | Get time bounds | 5.4ms | 5.8s | N/A | 1.0ms | 45.4ms | 2.0ms |
| 2 | Count all records | 2.0ms | 17.4ms | N/A | 1.2s | 2.6ms | 701.6ms |
| 3 | Count distinct users | 205.8ms | 1.9s | N/A | 124.0s | 33.6ms | 13.7s |
| 4 | Average RSSI | 25.0ms | 235.8ms | N/A | 1.0s | 28.2ms | 562.0ms |
| 5 | Records before middle time | 5.2ms | 20.4ms | N/A | 423.0ms | 7.6ms | 256.0ms |
| 6 | Records after middle time | 16.4ms | 6.2ms | N/A | 583.4ms | 1.8ms | 324.8ms |
| 7 | Records around middle time (±1 hour) | 5.0ms | 57.6ms | 4.6ms | 48.6ms | 2.2ms | 48.8ms |
| 8 | 24 hours aggregation from middle time | 4.0ms | 494.0ms | 3.8ms | N/A | 161.6ms | 1.1s |
| 9 | Top 10 users by activity | 11.0ms | 1.7s | N/A | 2.3s | 71.8ms | 1.2s |
| 10 | Records with strong signal | 106.2ms | 17.4ms | N/A | 925.0ms | 84.6ms | 609.6ms |
| 11 | Records with weak signal | 77.2ms | 4.8ms | N/A | 691.2ms | 27.2ms | 421.2ms |
| 12 | Top SSIDs | 11.2ms | 610.4ms | N/A | 2.2s | 32.2ms | 1.1s |
| 13 | RSSI statistics by user | 19.6ms | 2.0s | N/A | 3.0s | 213.4ms | 1.6s |
| 14 | RSSI percentiles | 251.6ms | 171.9s | N/A | N/A | 2.1s | 30.6s |
| 15 | Records in first half | 20.2ms | 4.8ms | N/A | 445.8ms | 2.0ms | 295.4ms |
| 16 | Records in second half | 20.6ms | 2.0ms | N/A | 591.4ms | 0.0ms | 377.2ms |
| 17 | Hourly user activity patterns | 15.8ms | 722.6ms | N/A | N/A | 39.8ms | 2.0s |
| 18 | Daily RSSI variance | 25.2ms | 910.2ms | N/A | N/A | 155.4ms | 1.4s |
| 19 | Peak usage hours | 28.6ms | 1.1s | N/A | N/A | 166.0ms | 1.7s |
| 20 | User session duration analysis | 16.4ms | 1.7s | N/A | N/A | 111.8ms | 1.3s |

### Query Speedups

| Query ID | Description | clickhouse Speedup | cratedb Speedup | influxdb Speedup | postgres Speedup | questdb Speedup | timescaledb Speedup |
|----------|-------------|------------|------------|------------|------------|------------|------------|
| 1 | Get time bounds | 1068.67x | 1.00x | N/A | 5770.80x | 127.11x | 2885.40x |
| 2 | Count all records | 8.70x | 1.00x | N/A | 0.01x | 6.69x | 0.02x |
| 3 | Count distinct users | 9.18x | 1.00x | N/A | 0.02x | 56.25x | 0.14x |
| 4 | Average RSSI | 9.43x | 1.00x | N/A | 0.24x | 8.36x | 0.42x |
| 5 | Records before middle time | 3.92x | 1.00x | N/A | 0.05x | 2.68x | 0.08x |
| 6 | Records after middle time | 0.38x | 1.00x | N/A | 0.01x | 3.44x | 0.02x |
| 7 | Records around middle time (±1 hour) | 11.52x | 1.00x | 12.52x | 1.19x | 26.18x | 1.18x |
| 8 | 24 hours aggregation from middle time | 123.50x | 1.00x | 130.00x | N/A | 3.06x | 0.46x |
| 9 | Top 10 users by activity | 151.22x | 1.00x | N/A | 0.72x | 23.17x | 1.34x |
| 10 | Records with strong signal | 0.16x | 1.00x | N/A | 0.02x | 0.21x | 0.03x |
| 11 | Records with weak signal | 0.06x | 1.00x | N/A | 0.01x | 0.18x | 0.01x |
| 12 | Top SSIDs | 54.50x | 1.00x | N/A | 0.27x | 18.96x | 0.54x |
| 13 | RSSI statistics by user | 99.69x | 1.00x | N/A | 0.65x | 9.16x | 1.25x |
| 14 | RSSI percentiles | 683.41x | 1.00x | N/A | N/A | 80.64x | 5.62x |
| 15 | Records in first half | 0.24x | 1.00x | N/A | 0.01x | 2.40x | 0.02x |
| 16 | Records in second half | 0.10x | 1.00x | N/A | 0.00x | 20.00x | 0.01x |
| 17 | Hourly user activity patterns | 45.73x | 1.00x | N/A | N/A | 18.16x | 0.36x |
| 18 | Daily RSSI variance | 36.12x | 1.00x | N/A | N/A | 5.86x | 0.66x |
| 19 | Peak usage hours | 37.11x | 1.00x | N/A | N/A | 6.39x | 0.64x |
| 20 | User session duration analysis | 104.96x | 1.00x | N/A | N/A | 15.40x | 1.28x |

### Median Query Speedups

| Database | Median Speedup | Min Speedup | Max Speedup | Queries Analyzed |
|----------|-----------------|-------------|-------------|------------------|
| cratedb | 1.00x | 1.00x | 1.00x | 20 |
| clickhouse | 23.82x | 0.06x | 1068.67x | 20 |
| influxdb | 0.00x | 12.52x | 130.00x | 2 |
| postgres | 0.01x | 0.00x | 5770.80x | 14 |
| questdb | 8.76x | 0.18x | 127.11x | 20 |
| timescaledb | 0.44x | 0.01x | 2885.40x | 20 |

## Summary

- **Best Ingestion Performance:** clickhouse (43.42x faster than cratedb)
- **Best Median Query Performance:** clickhouse (23.82x faster than cratedb)

---
*Report generated from 30 benchmark files, averaged by database type*

**File Count Summary:**
- clickhouse: 5 files
- cratedb: 5 files
- influxdb: 5 files
- postgres: 5 files
- questdb: 5 files
- timescaledb: 5 files