# Database Benchmark Queries

This document describes the 20 benchmark queries used to evaluate database performance across PostgreSQL, TimescaleDB, QuestDB, CrateDB, ClickHouse, and InfluxDB in a multitude of scenarios.
Mixing time series and relational queries, these queries are designed to test the capabilities of each database system in handling time-based data, aggregations, and user-specific queries.

## Query List

### Query 1: Get Time Bounds
**SQL (PostgreSQL/TimescaleDB/QuestDB/CrateDB/ClickHouse):**
```sql
SELECT MIN(timestamp), MAX(timestamp) FROM user_events
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: -30y)
  |> filter(fn: (r) => r._measurement == "user_events")
  |> keep(columns: ["_time"])
  |> min(column: "_time")
```
**Description:** Retrieves the minimum and maximum timestamps from the dataset to establish time boundaries.

### Query 2: Count All Records
**SQL (PostgreSQL/TimescaleDB/QuestDB/CrateDB/ClickHouse):**
```sql
SELECT COUNT(*) FROM user_events
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: -30y)
  |> filter(fn: (r) => r._measurement == "user_events")
  |> keep(columns: ["_time"])
  |> count()
```
**Description:** Counts the total number of records in the user_events table.

### Query 3: Count Distinct Users
**SQL (PostgreSQL/TimescaleDB/QuestDB/CrateDB/ClickHouse):**
```sql
SELECT COUNT(DISTINCT user_id) FROM user_events
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: -30y)
  |> filter(fn: (r) => r._measurement == "user_events")
  |> distinct(column: "user_id")
  |> count()
```
**Description:** Counts the number of unique users in the dataset.

### Query 4: Average RSSI
**SQL (PostgreSQL/TimescaleDB/QuestDB/CrateDB/ClickHouse):**
```sql
SELECT AVG(rssi) FROM user_events
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: -30y)
  |> filter(fn: (r) => r._measurement == "user_events" and r._field == "rssi")
  |> mean()
```
**Description:** Calculates the average RSSI (Received Signal Strength Indicator) value across all records.

### Query 5: Records Before Middle Time
**SQL (PostgreSQL/TimescaleDB/QuestDB/CrateDB/ClickHouse):**
```sql
SELECT COUNT(*) FROM user_events WHERE timestamp < $1
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: -30y, stop: {middleTime})
  |> filter(fn: (r) => r._measurement == "user_events")
  |> count()
```
**Description:** Counts records that occurred before the calculated middle timestamp.

### Query 6: Records After Middle Time
**SQL (PostgreSQL/TimescaleDB/QuestDB/CrateDB/ClickHouse):**
```sql
SELECT COUNT(*) FROM user_events WHERE timestamp > $1
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: {middleTime})
  |> filter(fn: (r) => r._measurement == "user_events")
  |> count()
```
**Description:** Counts records that occurred after the calculated middle timestamp.

### Query 7: Records Around Middle Time (±1 Hour)
**SQL (PostgreSQL/TimescaleDB/CrateDB/ClickHouse):**
```sql
SELECT COUNT(*) FROM user_events WHERE timestamp BETWEEN $1 AND $2
```
**QuestDB:**
```sql
SELECT COUNT(*) FROM user_events WHERE timestamp BETWEEN dateadd('h', -1, $1) AND dateadd('h', 1, $1)
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: {hourBefore}, stop: {hourAfter})
  |> filter(fn: (r) => r._measurement == "user_events")
  |> count()
```
**Description:** Counts records within a 2-hour window centered on the middle timestamp.

### Query 8: 24 Hours Aggregation from Middle Time
**PostgreSQL:** Not supported

**TimescaleDB/CrateDB/ClickHouse:**
```sql
SELECT date_trunc('hour', timestamp) as hour, COUNT(*) 
FROM user_events 
WHERE timestamp BETWEEN $1 AND $2 
GROUP BY hour 
ORDER BY hour
```
**QuestDB:**
```sql
SELECT timestamp, COUNT(*) 
FROM user_events 
WHERE timestamp BETWEEN $1 AND dateadd('h', 24, $1) 
SAMPLE BY 1h 
LIMIT 24
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: {middleTime}, stop: {dayAfter})
  |> filter(fn: (r) => r._measurement == "user_events")
  |> aggregateWindow(every: 1h, fn: count)
```
**Description:** Aggregates data by hour for a 24-hour period starting from the middle timestamp.

### Query 9: Top 10 Users by Activity
**SQL (PostgreSQL/TimescaleDB/CrateDB/ClickHouse):**
```sql
SELECT user_id, COUNT(*) as count 
FROM user_events 
GROUP BY user_id 
ORDER BY count DESC 
LIMIT 10
```
**QuestDB:**
```sql
SELECT user_id, COUNT(*) as count 
FROM user_events 
ORDER BY count DESC 
LIMIT 10
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: -30y)
  |> filter(fn: (r) => r._measurement == "user_events")
  |> group(columns: ["user_id"])
  |> count()
  |> top(n: 10)
```
**Description:** Identifies the 10 most active users based on record count.

### Query 10: Records with Strong Signal
**SQL (PostgreSQL/TimescaleDB/QuestDB/CrateDB/ClickHouse):**
```sql
SELECT COUNT(*) FROM user_events WHERE rssi > -50
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: -30y)
  |> filter(fn: (r) => r._measurement == "user_events" and r._field == "rssi" and r._value > -50.0)
  |> count()
```
**Description:** Counts records with strong signal strength (RSSI > -50 dBm).

### Query 11: Records with Weak Signal
**SQL (PostgreSQL/TimescaleDB/QuestDB/CrateDB/ClickHouse):**
```sql
SELECT COUNT(*) FROM user_events WHERE rssi < -80
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: -30y)
  |> filter(fn: (r) => r._measurement == "user_events" and r._field == "rssi" and r._value < -80.0)
  |> count()
```
**Description:** Counts records with weak signal strength (RSSI < -80 dBm).

### Query 12: Top SSIDs
**SQL (PostgreSQL/TimescaleDB/CrateDB/ClickHouse):**
```sql
SELECT ssid, COUNT(*) as count 
FROM user_events 
GROUP BY ssid 
ORDER BY count DESC 
LIMIT 10
```
**QuestDB:**
```sql
SELECT ssid, COUNT(*) as count 
FROM user_events 
ORDER BY count DESC 
LIMIT 10
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: -30y)
  |> filter(fn: (r) => r._measurement == "user_events")
  |> group(columns: ["ssid"])
  |> count()
  |> top(n: 10)
```
**Description:** Identifies the 10 most frequently encountered WiFi network names (SSIDs).

### Query 13: RSSI Statistics by User
**SQL (PostgreSQL/TimescaleDB/CrateDB/ClickHouse):**
```sql
SELECT user_id, AVG(rssi), MIN(rssi), MAX(rssi) 
FROM user_events 
GROUP BY user_id 
ORDER BY AVG(rssi) DESC 
LIMIT 100
```
**QuestDB:**
```sql
SELECT user_id, avg(rssi), min(rssi), max(rssi) 
FROM user_events 
ORDER BY avg DESC 
LIMIT 100
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: -30y)
  |> filter(fn: (r) => r._measurement == "user_events" and r._field == "rssi")
  |> group(columns: ["user_id"])
  |> aggregateWindow(every: inf, fn: mean)
  |> top(n: 100)
```
**Description:** Computes RSSI statistics (average, minimum, maximum) for each user, ordered by average RSSI.

### Query 14: RSSI Percentiles
**PostgreSQL:** Not supported

**TimescaleDB:**
```sql
SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY rssi) as q1, 
       percentile_cont(0.5) WITHIN GROUP (ORDER BY rssi) as median, 
       percentile_cont(0.75) WITHIN GROUP (ORDER BY rssi) as q3 
FROM user_events
```
**CrateDB:**
```sql
SELECT percentile(rssi, 0.25), percentile(rssi, 0.5), percentile(rssi, 0.75) 
FROM user_events
```
**ClickHouse:**
```sql
SELECT quantile(0.25)(rssi) as q1, quantile(0.5)(rssi) as median, quantile(0.75)(rssi) as q3 
FROM user_events
```
**QuestDB:**
```sql
SELECT -approx_percentile(-rssi, 1.0-0.25) as q1, 
       -approx_percentile(-rssi, 1.0-0.5) as median, 
       -approx_percentile(-rssi, 1.0-0.75) as q3 
FROM user_events
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: -30y)
  |> filter(fn: (r) => r._measurement == "user_events" and r._field == "rssi")
  |> quantile(q: 0.25, method: "estimate_tdigest")
```
**Description:** Calculates RSSI percentiles (25th, 50th, 75th percentiles).

### Query 15: Records in First Half
**SQL (PostgreSQL/TimescaleDB/QuestDB/CrateDB/ClickHouse):**
```sql
SELECT COUNT(*) FROM user_events WHERE timestamp BETWEEN $1 AND $2
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: {minTime}, stop: {middleTime})
  |> filter(fn: (r) => r._measurement == "user_events")
  |> count()
```
**Description:** Counts records in the first half of the time range (from minimum to middle timestamp).

### Query 16: Records in Second Half
**SQL (PostgreSQL/TimescaleDB/QuestDB/CrateDB/ClickHouse):**
```sql
SELECT COUNT(*) FROM user_events WHERE timestamp BETWEEN $1 AND $2
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: {middleTime}, stop: {maxTime})
  |> filter(fn: (r) => r._measurement == "user_events")
  |> count()
```
**Description:** Counts records in the second half of the time range (from middle to maximum timestamp).

### Query 17: Hourly User Activity Patterns
**PostgreSQL:** Not supported (returns -1)
**TimescaleDB/CrateDB:**
```sql
SELECT EXTRACT(hour FROM timestamp) as hour, COUNT(*) as count 
FROM user_events 
GROUP BY hour 
ORDER BY hour
```
**ClickHouse:**
```sql
SELECT toHour(timestamp) as hour, COUNT(*) as count 
FROM user_events 
GROUP BY hour 
ORDER BY hour
```
**QuestDB:**
```sql
SELECT hour(timestamp) as hour, COUNT(*) as count 
FROM user_events 
ORDER BY hour
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: -30y)
  |> filter(fn: (r) => r._measurement == "user_events")
  |> group(columns: ["_time"])
  |> aggregateWindow(every: 1h, fn: count)
  |> group(columns: ["hour"])
  |> sum()
```
**Description:** Analyzes user activity patterns by hour of the day to identify peak usage times.

### Query 18: Daily RSSI Variance
**PostgreSQL:** Not supported (returns -1)
**TimescaleDB/CrateDB:**
```sql
SELECT DATE(timestamp) as day, VARIANCE(rssi) as rssi_variance 
FROM user_events 
GROUP BY day 
ORDER BY day 
LIMIT 30
```
**ClickHouse:**
```sql
SELECT toStartOfDay(timestamp) as day, varSamp(rssi) as rssi_variance 
FROM user_events 
GROUP BY day 
ORDER BY day 
LIMIT 30
```
**QuestDB:**
```sql
SELECT timestamp, variance(rssi) as rssi_variance 
FROM user_events 
SAMPLE BY 1d 
LIMIT 30
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: -30y)
  |> filter(fn: (r) => r._measurement == "user_events" and r._field == "rssi")
  |> aggregateWindow(every: 1d, fn: stddev)
  |> limit(n: 30)
```
**Description:** Calculates daily variance in RSSI values to analyze signal quality consistency over time.

### Query 19: Peak Usage Hours
**PostgreSQL:** Not supported (returns -1)
**TimescaleDB/CrateDB:**
```sql
SELECT date_trunc('hour', timestamp) as hour, COUNT(*) as count 
FROM user_events 
GROUP BY hour 
ORDER BY count DESC 
LIMIT 5
```
**ClickHouse:**
```sql
SELECT toStartOfHour(timestamp) as hour, COUNT(*) as count 
FROM user_events 
GROUP BY hour 
ORDER BY count DESC 
LIMIT 5
```
**QuestDB:**
```sql
SELECT timestamp, count 
FROM (SELECT timestamp, COUNT(*) as count FROM user_events SAMPLE BY 1h) 
ORDER BY count DESC 
LIMIT 5
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: -30y)
  |> filter(fn: (r) => r._measurement == "user_events")
  |> aggregateWindow(every: 1h, fn: count)
  |> top(n: 5)
```
**Description:** Identifies the top 5 hours with the highest user activity across the entire dataset.

### Query 20: User Session Duration Analysis
**PostgreSQL:** Not supported (returns -1)
**TimescaleDB/CrateDB:**
```sql
SELECT user_id, MAX(timestamp) - MIN(timestamp) as session_duration 
FROM user_events 
GROUP BY user_id 
ORDER BY session_duration DESC 
LIMIT 10
```
**ClickHouse:**
```sql
SELECT user_id, MAX(timestamp) - MIN(timestamp) as session_duration 
FROM user_events 
GROUP BY user_id 
ORDER BY session_duration DESC 
LIMIT 10
```
**QuestDB:**
```sql
SELECT user_id, max(timestamp) - min(timestamp) as session_duration 
FROM user_events 
ORDER BY session_duration DESC 
LIMIT 10
```
**InfluxDB (Flux):**
```flux
from(bucket: "benchmark")
  |> range(start: -30y)
  |> filter(fn: (r) => r._measurement == "user_events")
  |> group(columns: ["user_id"])
  |> aggregateWindow(every: inf, fn: spread)
  |> top(n: 10)
```
**Description:** Analyzes user session durations by calculating the time span between first and last activity for each user.

## Table Schema

The `user_events` table/measurement contains the following fields:

- `id`: Auto-incrementing primary key (PostgreSQL/TimescaleDB only)
- `user_id`: String identifier for the user
- `timestamp`: Timestamp with timezone information
- `rssi`: Real/Float value representing signal strength
- `ssid`: String representing the WiFi network name

## Database-Specific Notes

### PostgreSQL
- Uses standard PostgreSQL syntax
- Queries 8, 14, 17, 18, 19, and 20 are not supported and return -1

### TimescaleDB
- Extends PostgreSQL with time-series optimizations
- Creates hypertables with 4-hour partitioning
- Implements all 20 queries including percentile calculations and advanced time-series analytics

### QuestDB
- Time-series database with SQL-like syntax
- Uses specific functions like `dateadd()`, `SAMPLE BY`, and `hour()`
- Implements custom percentile calculations using `approx_percentile()`

### CrateDB
- Distributed SQL database with time-series optimizations
- Uses standard PostgreSQL-compatible SQL syntax
- Supports clustering and sharding for horizontal scaling
- Implements all 20 queries including percentile calculations and advanced analytics
- Compatible with PostgreSQL wire protocol via pgx driver

### ClickHouse
- Column-oriented database with SQL syntax
- Uses MergeTree engine for optimal performance
- Time-specific functions like `toHour()`, `toStartOfHour()`, `toStartOfDay()`
- Uses `quantile()` functions for percentiles instead of `percentile_cont()`
- Uses `varSamp()` for variance calculations
- Implements all 20 queries with native SQL support
- High-performance analytics database designed for OLAP workloads

### InfluxDB
- Uses Flux query language instead of SQL
- Data is stored with tags (user_id, ssid) and fields (rssi)
- Different approach to aggregations and time-based queries
