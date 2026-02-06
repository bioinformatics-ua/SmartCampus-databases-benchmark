package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	qdb "github.com/questdb/go-questdb-client/v3"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Reading struct {
	UserId          string `json:"userId"`
	LastUpdatedTime int    `json:"lastUpdatedTime"`
	Connection      struct {
		Ssid string  `json:"ssid"`
		Rssi float64 `json:"rssi"`
	} `json:"connection"`
}

type ReadingFile struct {
	Response []Reading `json:"response"`
}

type QueryResult struct {
	QueryId     int    `json:"queryId"`
	DurationMs  int64  `json:"durationMs"`
	Description string `json:"description"`
}

type BenchmarkResults struct {
	DbType    string `json:"dbType"`
	Ingestion []struct {
		DurationMs int64 `json:"durationMs"`
		NRecords   int   `json:"nRecords"`
	} `json:"ingestion"`
	Queries []QueryResult `json:"queries"`
}

func loadDataChunk(currentChunk int) (bool, ReadingFile, error) {
	fmt.Printf("[INFO] Loading data chunk %d\n", currentChunk)
	fd, err := os.Open("../data/readings/readings_" + strconv.Itoa(currentChunk) + ".json")
	if err != nil {
		return false, ReadingFile{}, err
	}

	defer fd.Close()
	var data ReadingFile
	if err := json.NewDecoder(fd).Decode(&data); err != nil {
		return false, ReadingFile{}, err
	}

	filesInDirectory, err := os.ReadDir("../data/readings")
	if err != nil {
		return false, ReadingFile{}, err
	}

	if currentChunk+1 < len(filesInDirectory) {
		return true, data, nil
	}

	return false, data, nil
}

func benchmarkPostgres(connStr string, outFile string) error {
	pool, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		return err
	}

	// Create the table if it doesn't exist
	_, err = pool.Exec(context.Background(), `
		CREATE TABLE user_events (
			id BIGSERIAL,
			user_id VARCHAR(255) NOT NULL,
			timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
			rssi REAL NOT NULL,
			ssid VARCHAR(255) NOT NULL
		); CREATE INDEX IF NOT EXISTS idx_user_events_timestamp ON user_events (timestamp);`)
	if err != nil {
		return err
	}

	currentChunk := 0
	results := BenchmarkResults{}
	nRecords := 0

	// Ingestion benchmark
	for {
		hasNext, data, err := loadDataChunk(currentChunk)
		if err != nil {
			return err
		}

		start := time.Now()

		rows := make([][]interface{}, len(data.Response))
		for i, reading := range data.Response {
			rows[i] = []interface{}{
				reading.UserId,
				time.Unix(int64(reading.LastUpdatedTime), 0),
				reading.Connection.Rssi,
				reading.Connection.Ssid,
			}
		}

		_, err = pool.CopyFrom(
			context.Background(),
			pgx.Identifier{"user_events"},
			[]string{"user_id", "timestamp", "rssi", "ssid"},
			pgx.CopyFromRows(rows),
		)

		if err != nil {
			return err
		}

		nRecords += len(data.Response)
		duration := time.Since(start).Milliseconds()
		results.Ingestion = append(results.Ingestion, struct {
			DurationMs int64 `json:"durationMs"`
			NRecords   int   `json:"nRecords"`
		}{
			DurationMs: duration,
			NRecords:   nRecords,
		})

		currentChunk++
		if !hasNext {
			break
		}
	}

	// Query benchmarks
	var minTime, maxTime, middleTime time.Time

	// Query 1: Get time bounds
	fmt.Println("[INFO] Running query 1: Get time bounds")
	start := time.Now()
	err = pool.QueryRow(context.Background(), "SELECT MIN(timestamp), MAX(timestamp) FROM user_events").Scan(&minTime, &maxTime)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     1,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Get time bounds",
	})
	fmt.Println("[INFO] Done with query 1")

	middleTime = minTime.Add(maxTime.Sub(minTime) / 2)

	// Query 2: Count all records
	fmt.Println("[INFO] Running query 2: Count all records")
	start = time.Now()
	var totalCount int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events").Scan(&totalCount)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     2,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Count all records",
	})
	fmt.Println("[INFO] Done with query 2")

	// Query 3: Count distinct users
	fmt.Println("[INFO] Running query 3: Count distinct users")
	start = time.Now()
	var distinctUsers int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(DISTINCT user_id) FROM user_events").Scan(&distinctUsers)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     3,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Count distinct users",
	})
	fmt.Println("[INFO] Done with query 3")

	// Query 4: Average RSSI
	fmt.Println("[INFO] Running query 4: Average RSSI")
	start = time.Now()
	var avgRssi float64
	err = pool.QueryRow(context.Background(), "SELECT AVG(rssi) FROM user_events").Scan(&avgRssi)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     4,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Average RSSI",
	})
	fmt.Println("[INFO] Done with query 4")

	// Query 5: Records before middle time
	fmt.Println("[INFO] Running query 5: Records before middle time")
	start = time.Now()
	var beforeMiddle int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE timestamp < $1", middleTime).Scan(&beforeMiddle)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     5,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records before middle time",
	})
	fmt.Println("[INFO] Done with query 5")

	// Query 6: Records after middle time
	fmt.Println("[INFO] Running query 6: Records after middle time")
	start = time.Now()
	var afterMiddle int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE timestamp > $1", middleTime).Scan(&afterMiddle)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     6,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records after middle time",
	})
	fmt.Println("[INFO] Done with query 6")

	// Query 7: Records around middle time (±1 hour)
	fmt.Println("[INFO] Running query 7: Records around middle time (±1 hour)")
	start = time.Now()
	var aroundMiddle int
	hourBefore := middleTime.Add(-time.Hour)
	hourAfter := middleTime.Add(time.Hour)
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE timestamp BETWEEN $1 AND $2", hourBefore, hourAfter).Scan(&aroundMiddle)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     7,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records around middle time (±1 hour)",
	})
	fmt.Println("[INFO] Done with query 7")

	// Query 8: 24 hours aggregation from middle time
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     8,
		DurationMs:  -1,
		Description: "24 hours aggregation from middle time",
	})

	// Query 9: Top 10 users by activity
	fmt.Println("[INFO] Running query 9: Top 10 users by activity")
	start = time.Now()
	_, err = pool.Query(context.Background(), "SELECT user_id, COUNT(*) as count FROM user_events GROUP BY user_id ORDER BY count DESC LIMIT 10")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     9,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Top 10 users by activity",
	})
	fmt.Println("[INFO] Done with query 9")

	// Query 10: Records with strong signal
	fmt.Println("[INFO] Running query 10: Records with strong signal")
	start = time.Now()
	var strongSignal int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE rssi > -50").Scan(&strongSignal)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     10,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records with strong signal",
	})
	fmt.Println("[INFO] Done with query 10")

	// Query 11: Records with weak signal
	fmt.Println("[INFO] Running query 11: Records with weak signal")
	start = time.Now()
	var weakSignal int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE rssi < -80").Scan(&weakSignal)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     11,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records with weak signal",
	})
	fmt.Println("[INFO] Done with query 11")

	// Query 12: Top SSIDs
	fmt.Println("[INFO] Running query 12: Top SSIDs")
	start = time.Now()
	_, err = pool.Query(context.Background(), "SELECT ssid, COUNT(*) as count FROM user_events GROUP BY ssid ORDER BY count DESC LIMIT 10")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     12,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Top SSIDs",
	})
	fmt.Println("[INFO] Done with query 12")

	// Query 13: RSSI statistics by user
	fmt.Println("[INFO] Running query 13: RSSI statistics by user")
	start = time.Now()
	_, err = pool.Query(context.Background(), "SELECT user_id, AVG(rssi), MIN(rssi), MAX(rssi) FROM user_events GROUP BY user_id ORDER BY AVG(rssi) DESC LIMIT 100")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     13,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "RSSI statistics by user",
	})
	fmt.Println("[INFO] Done with query 13")

	// Query 14: RSSI percentiles
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     14,
		DurationMs:  -1,
		Description: "RSSI percentiles",
	})

	// Query 15: Records in first half
	fmt.Println("[INFO] Running query 15: Records in first half")
	start = time.Now()
	var firstHalf int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE timestamp BETWEEN $1 AND $2", minTime, middleTime).Scan(&firstHalf)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     15,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records in first half",
	})
	fmt.Println("[INFO] Done with query 15")

	// Query 16: Records in second half
	fmt.Println("[INFO] Running query 16: Records in second half")
	start = time.Now()
	var secondHalf int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE timestamp BETWEEN $1 AND $2", middleTime, maxTime).Scan(&secondHalf)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     16,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records in second half",
	})
	fmt.Println("[INFO] Done with query 16")

	// Query 17: Hourly user activity patterns
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     17,
		DurationMs:  -1,
		Description: "Hourly user activity patterns",
	})

	// Query 18: Daily RSSI variance
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     18,
		DurationMs:  -1,
		Description: "Daily RSSI variance",
	})

	// Query 19: Peak usage hours
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     19,
		DurationMs:  -1,
		Description: "Peak usage hours",
	})

	// Query 20: User session duration analysis
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     20,
		DurationMs:  -1,
		Description: "User session duration analysis",
	})

	results.DbType = "postgres"
	out, err := os.Create(outFile)
	if err != nil {
		return err
	}

	defer out.Close()
	if err := json.NewEncoder(out).Encode(results); err != nil {
		return err
	}
	return nil
}

func benchmarkTimescaleDb(connStr string, outFile string) error {
	pool, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		return err
	}

	// Create the table if it doesn't exist
	_, err = pool.Exec(context.Background(), `
		CREATE TABLE user_events (
			id BIGSERIAL,
			user_id VARCHAR(255) NOT NULL,
			timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
			rssi REAL NOT NULL,
			ssid VARCHAR(255) NOT NULL
		) WITH (
			tsdb.hypertable,
			tsdb.partition_column='timestamp'
		);SELECT create_hypertable('user_events', by_range('time', INTERVAL '4 hours'), if_not_exists => TRUE);`)
	if err != nil {
		return err
	}

	currentChunk := 0
	results := BenchmarkResults{}
	nRecords := 0

	// Ingestion benchmark
	for {
		hasNext, data, err := loadDataChunk(currentChunk)
		if err != nil {
			return err
		}

		start := time.Now()

		rows := make([][]interface{}, len(data.Response))
		for i, reading := range data.Response {
			rows[i] = []interface{}{
				reading.UserId,
				time.Unix(int64(reading.LastUpdatedTime), 0),
				reading.Connection.Rssi,
				reading.Connection.Ssid,
			}
		}

		_, err = pool.CopyFrom(
			context.Background(),
			pgx.Identifier{"user_events"},
			[]string{"user_id", "timestamp", "rssi", "ssid"},
			pgx.CopyFromRows(rows),
		)

		if err != nil {
			return err
		}

		nRecords += len(data.Response)
		duration := time.Since(start).Milliseconds()
		results.Ingestion = append(results.Ingestion, struct {
			DurationMs int64 `json:"durationMs"`
			NRecords   int   `json:"nRecords"`
		}{
			DurationMs: duration,
			NRecords:   nRecords,
		})

		currentChunk++
		if !hasNext {
			break
		}
	}

	// Query benchmarks
	var minTime, maxTime, middleTime time.Time

	// Query 1: Get time bounds
	fmt.Println("[INFO] Running query 1: Get time bounds")
	start := time.Now()
	err = pool.QueryRow(context.Background(), "SELECT MIN(timestamp), MAX(timestamp) FROM user_events").Scan(&minTime, &maxTime)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     1,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Get time bounds",
	})
	fmt.Println("[INFO] Done with query 1")

	middleTime = minTime.Add(maxTime.Sub(minTime) / 2)

	// Query 2: Count all records
	fmt.Println("[INFO] Running query 2: Count all records")
	start = time.Now()
	var totalCount int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events").Scan(&totalCount)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     2,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Count all records",
	})
	fmt.Println("[INFO] Done with query 2")

	// Query 3: Count distinct users
	fmt.Println("[INFO] Running query 3: Count distinct users")
	start = time.Now()
	var distinctUsers int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(DISTINCT user_id) FROM user_events").Scan(&distinctUsers)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     3,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Count distinct users",
	})
	fmt.Println("[INFO] Done with query 3")

	// Query 4: Average RSSI
	fmt.Println("[INFO] Running query 4: Average RSSI")
	start = time.Now()
	var avgRssi float64
	err = pool.QueryRow(context.Background(), "SELECT AVG(rssi) FROM user_events").Scan(&avgRssi)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     4,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Average RSSI",
	})
	fmt.Println("[INFO] Done with query 4")

	// Query 5: Records before middle time
	fmt.Println("[INFO] Running query 5: Records before middle time")
	start = time.Now()
	var beforeMiddle int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE timestamp < $1", middleTime).Scan(&beforeMiddle)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     5,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records before middle time",
	})
	fmt.Println("[INFO] Done with query 5")

	// Query 6: Records after middle time
	fmt.Println("[INFO] Running query 6: Records after middle time")
	start = time.Now()
	var afterMiddle int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE timestamp > $1", middleTime).Scan(&afterMiddle)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     6,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records after middle time",
	})
	fmt.Println("[INFO] Done with query 6")

	// Query 7: Records around middle time (±1 hour)
	fmt.Println("[INFO] Running query 7: Records around middle time (±1 hour)")
	start = time.Now()
	var aroundMiddle int
	hourBefore := middleTime.Add(-time.Hour)
	hourAfter := middleTime.Add(time.Hour)
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE timestamp BETWEEN $1 AND $2", hourBefore, hourAfter).Scan(&aroundMiddle)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     7,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records around middle time (±1 hour)",
	})
	fmt.Println("[INFO] Done with query 7")

	// Query 8: 24 hours aggregation from middle time
	fmt.Println("[INFO] Running query 8: 24 hours aggregation from middle time")
	start = time.Now()
	dayAfter := middleTime.Add(24 * time.Hour)
	_, err = pool.Query(context.Background(), "SELECT date_trunc('hour', timestamp) as hour, COUNT(*) FROM user_events WHERE timestamp BETWEEN $1 AND $2 GROUP BY hour ORDER BY hour", middleTime, dayAfter)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     8,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "24 hours aggregation from middle time",
	})
	fmt.Println("[INFO] Done with query 8")

	// Query 9: Top 10 users by activity
	fmt.Println("[INFO] Running query 9: Top 10 users by activity")
	start = time.Now()
	_, err = pool.Query(context.Background(), "SELECT user_id, COUNT(*) as count FROM user_events GROUP BY user_id ORDER BY count DESC LIMIT 10")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     9,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Top 10 users by activity",
	})
	fmt.Println("[INFO] Done with query 9")

	// Query 10: Records with strong signal
	fmt.Println("[INFO] Running query 10: Records with strong signal")
	start = time.Now()
	var strongSignal int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE rssi > -50").Scan(&strongSignal)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     10,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records with strong signal",
	})
	fmt.Println("[INFO] Done with query 10")

	// Query 11: Records with weak signal
	fmt.Println("[INFO] Running query 11: Records with weak signal")
	start = time.Now()
	var weakSignal int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE rssi < -80").Scan(&weakSignal)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     11,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records with weak signal",
	})
	fmt.Println("[INFO] Done with query 11")

	// Query 12: Top SSIDs
	fmt.Println("[INFO] Running query 12: Top SSIDs")
	start = time.Now()
	_, err = pool.Query(context.Background(), "SELECT ssid, COUNT(*) as count FROM user_events GROUP BY ssid ORDER BY count DESC LIMIT 10")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     12,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Top SSIDs",
	})
	fmt.Println("[INFO] Done with query 12")

	// Query 13: RSSI statistics by user
	fmt.Println("[INFO] Running query 13: RSSI statistics by user")
	start = time.Now()
	_, err = pool.Query(context.Background(), "SELECT user_id, AVG(rssi), MIN(rssi), MAX(rssi) FROM user_events GROUP BY user_id ORDER BY AVG(rssi) DESC LIMIT 100")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     13,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "RSSI statistics by user",
	})
	fmt.Println("[INFO] Done with query 13")

	// Query 14: RSSI percentiles
	fmt.Println("[INFO] Running query 14: RSSI percentiles")
	start = time.Now()
	var q1, median, q3 float64
	err = pool.QueryRow(context.Background(), "SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY rssi) as q1, percentile_cont(0.5) WITHIN GROUP (ORDER BY rssi) as median, percentile_cont(0.75) WITHIN GROUP (ORDER BY rssi) as q3 FROM user_events").Scan(&q1, &median, &q3)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     14,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "RSSI percentiles",
	})
	fmt.Println("[INFO] Done with query 14")

	// Query 15: Records in first half
	fmt.Println("[INFO] Running query 15: Records in first half")
	start = time.Now()
	var firstHalf int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE timestamp BETWEEN $1 AND $2", minTime, middleTime).Scan(&firstHalf)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     15,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records in first half",
	})
	fmt.Println("[INFO] Done with query 15")

	// Query 16: Records in second half
	fmt.Println("[INFO] Running query 16: Records in second half")
	start = time.Now()
	var secondHalf int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE timestamp BETWEEN $1 AND $2", middleTime, maxTime).Scan(&secondHalf)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     16,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records in second half",
	})
	fmt.Println("[INFO] Done with query 16")

	// Query 17: Hourly user activity patterns
	fmt.Println("[INFO] Running query 17: Hourly user activity patterns")
	start = time.Now()
	_, err = pool.Query(context.Background(), "SELECT EXTRACT(hour FROM timestamp) as hour, COUNT(*) as count FROM user_events GROUP BY hour ORDER BY hour")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     17,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Hourly user activity patterns",
	})
	fmt.Println("[INFO] Done with query 17")

	// Query 18: Daily RSSI variance
	fmt.Println("[INFO] Running query 18: Daily RSSI variance")
	start = time.Now()
	_, err = pool.Query(context.Background(), "SELECT DATE(timestamp) as day, VARIANCE(rssi) as rssi_variance FROM user_events GROUP BY day ORDER BY day LIMIT 30")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     18,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Daily RSSI variance",
	})
	fmt.Println("[INFO] Done with query 18")

	// Query 19: Peak usage hours
	fmt.Println("[INFO] Running query 19: Peak usage hours")
	start = time.Now()
	_, err = pool.Query(context.Background(), "SELECT date_trunc('hour', timestamp) as hour, COUNT(*) as count FROM user_events GROUP BY hour ORDER BY count DESC LIMIT 5")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     19,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Peak usage hours",
	})
	fmt.Println("[INFO] Done with query 19")

	// Query 20: User session duration analysis
	fmt.Println("[INFO] Running query 20: User session duration analysis")
	start = time.Now()
	_, err = pool.Query(context.Background(), "SELECT user_id, MAX(timestamp) - MIN(timestamp) as session_duration FROM user_events GROUP BY user_id ORDER BY session_duration DESC LIMIT 10")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     20,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "User session duration analysis",
	})
	fmt.Println("[INFO] Done with query 20")

	results.DbType = "timescaledb"
	out, err := os.Create(outFile)
	if err != nil {
		return err
	}

	defer out.Close()
	if err := json.NewEncoder(out).Encode(results); err != nil {
		return err
	}
	return nil
}

func benchmarkQuestDb(connStr string, outFile string) error {
	connParts := strings.Split(connStr, ":::")
	if len(connParts) != 2 {
		return fmt.Errorf("invalid connection string format, expected 'ingestUrl:::queryUrl'")
	}

	ingestUrl := connParts[0]
	queryUrl := connParts[1]

	ingestPool, err := qdb.LineSenderFromConf(context.Background(), ingestUrl)
	if err != nil {
		return err
	}

	queryPool, err := pgxpool.New(context.Background(), queryUrl)
	if err != nil {
		return err
	}

	currentChunk := 0
	results := BenchmarkResults{}
	nRecords := 0
	ctx := context.Background()

	for {
		hasNext, data, err := loadDataChunk(currentChunk)
		if err != nil {
			return err
		}

		start := time.Now()

		for _, reading := range data.Response {
			err := ingestPool.Table("user_events").
				Symbol("ssid", reading.Connection.Ssid).
				Symbol("user_id", reading.UserId).
				Float64Column("rssi", reading.Connection.Rssi).
				At(ctx, time.Unix(int64(reading.LastUpdatedTime), 0))
			if err != nil {
				return err
			}
		}

		if err := ingestPool.Flush(ctx); err != nil {
			return err
		}

		nRecords += len(data.Response)
		duration := time.Since(start).Milliseconds()
		results.Ingestion = append(results.Ingestion, struct {
			DurationMs int64 `json:"durationMs"`
			NRecords   int   `json:"nRecords"`
		}{
			DurationMs: duration,
			NRecords:   nRecords,
		})

		currentChunk++
		if !hasNext {
			break
		}
	}

	// Query benchmarks
	var minTime, maxTime, middleTime time.Time

	// Query 1: Get time bounds
	fmt.Println("[INFO] Running query 1: Get time bounds")
	start := time.Now()
	err = queryPool.QueryRow(context.Background(), "SELECT MIN(timestamp), MAX(timestamp) FROM user_events").Scan(&minTime, &maxTime)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     1,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Get time bounds",
	})
	fmt.Println("[INFO] Done with query 1")

	middleTime = minTime.Add(maxTime.Sub(minTime) / 2)

	// Query 2: Count all records
	fmt.Println("[INFO] Running query 2: Count all records")
	start = time.Now()
	var totalCount int
	err = queryPool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events").Scan(&totalCount)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     2,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Count all records",
	})
	fmt.Println("[INFO] Done with query 2")

	// Query 3: Count distinct users
	fmt.Println("[INFO] Running query 3: Count distinct users")
	start = time.Now()
	var distinctUsers int
	err = queryPool.QueryRow(context.Background(), "SELECT COUNT(DISTINCT user_id) FROM user_events").Scan(&distinctUsers)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     3,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Count distinct users",
	})
	fmt.Println("[INFO] Done with query 3")

	// Query 4: Average RSSI
	fmt.Println("[INFO] Running query 4: Average RSSI")
	start = time.Now()
	var avgRssi float64
	err = queryPool.QueryRow(context.Background(), "SELECT AVG(rssi) FROM user_events").Scan(&avgRssi)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     4,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Average RSSI",
	})
	fmt.Println("[INFO] Done with query 4")

	// Query 5: Records before middle time
	fmt.Println("[INFO] Running query 5: Records before middle time")
	start = time.Now()
	var beforeMiddle int
	err = queryPool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE timestamp < $1", middleTime).Scan(&beforeMiddle)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     5,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records before middle time",
	})
	fmt.Println("[INFO] Done with query 5")

	// Query 6: Records after middle time
	fmt.Println("[INFO] Running query 6: Records after middle time")
	start = time.Now()
	var afterMiddle int
	err = queryPool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE timestamp > $1", middleTime).Scan(&afterMiddle)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     6,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records after middle time",
	})
	fmt.Println("[INFO] Done with query 6")

	// Query 7: Records around middle time (±1 hour) - QuestDB syntax
	fmt.Println("[INFO] Running query 7: Records around middle time (±1 hour)")
	start = time.Now()
	var aroundMiddle int
	err = queryPool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE timestamp BETWEEN dateadd('h', -1, $1) AND dateadd('h', 1, $1)", middleTime).Scan(&aroundMiddle)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     7,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records around middle time (±1 hour)",
	})
	fmt.Println("[INFO] Done with query 7")

	// Query 8: 24 hours aggregation from middle time - QuestDB syntax
	fmt.Println("[INFO] Running query 8: 24 hours aggregation from middle time")
	start = time.Now()
	_, err = queryPool.Query(context.Background(), "SELECT timestamp, COUNT(*) FROM user_events WHERE timestamp BETWEEN $1 AND dateadd('h', 24, $1) SAMPLE BY 1h LIMIT 24", middleTime)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     8,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "24 hours aggregation from middle time",
	})
	fmt.Println("[INFO] Done with query 8")

	// Query 9: Top 10 users by activity
	fmt.Println("[INFO] Running query 9: Top 10 users by activity")
	start = time.Now()
	_, err = queryPool.Query(context.Background(), "SELECT user_id, COUNT(*) as count FROM user_events ORDER BY count DESC LIMIT 10")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     9,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Top 10 users by activity",
	})
	fmt.Println("[INFO] Done with query 9")

	// Query 10: Records with strong signal
	fmt.Println("[INFO] Running query 10: Records with strong signal")
	start = time.Now()
	var strongSignal int
	err = queryPool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE rssi > -50").Scan(&strongSignal)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     10,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records with strong signal",
	})
	fmt.Println("[INFO] Done with query 10")

	// Query 11: Records with weak signal
	fmt.Println("[INFO] Running query 11: Records with weak signal")
	start = time.Now()
	var weakSignal int
	err = queryPool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE rssi < -80").Scan(&weakSignal)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     11,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records with weak signal",
	})
	fmt.Println("[INFO] Done with query 11")

	// Query 12: Top SSIDs
	fmt.Println("[INFO] Running query 12: Top SSIDs")
	start = time.Now()
	_, err = queryPool.Query(context.Background(), "SELECT ssid, COUNT(*) as count FROM user_events ORDER BY count DESC LIMIT 10")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     12,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Top SSIDs",
	})
	fmt.Println("[INFO] Done with query 12")

	// Query 13: RSSI statistics by user
	fmt.Println("[INFO] Running query 13: RSSI statistics by user")
	start = time.Now()
	_, err = queryPool.Query(context.Background(), "SELECT user_id, avg(rssi), min(rssi), max(rssi) FROM user_events ORDER BY avg DESC LIMIT 100")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     13,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "RSSI statistics by user",
	})
	fmt.Println("[INFO] Done with query 13")

	// Query 14: RSSI percentiles - QuestDB syntax
	fmt.Println("[INFO] Running query 14: RSSI percentiles")
	start = time.Now()
	var q1, median, q3 float64
	err = queryPool.QueryRow(context.Background(), "SELECT -approx_percentile(-rssi, 1.0-0.25) as q1, -approx_percentile(-rssi, 1.0-0.5) as median, -approx_percentile(-rssi, 1.0-0.75) as q3 FROM user_events").Scan(&q1, &median, &q3)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     14,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "RSSI percentiles",
	})
	fmt.Println("[INFO] Done with query 14")

	// Query 15: Records in first half
	fmt.Println("[INFO] Running query 15: Records in first half")
	start = time.Now()
	var firstHalf int
	err = queryPool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE timestamp BETWEEN $1 AND $2", minTime, middleTime).Scan(&firstHalf)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     15,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records in first half",
	})
	fmt.Println("[INFO] Done with query 15")

	// Query 16: Records in second half
	fmt.Println("[INFO] Running query 16: Records in second half")
	start = time.Now()
	var secondHalf int
	err = queryPool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE timestamp BETWEEN $1 AND $2", middleTime, maxTime).Scan(&secondHalf)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     16,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records in second half",
	})
	fmt.Println("[INFO] Done with query 16")

	// Query 17: Hourly user activity patterns
	fmt.Println("[INFO] Running query 17: Hourly user activity patterns")
	start = time.Now()
	_, err = queryPool.Query(context.Background(), "SELECT hour(timestamp) as hour, COUNT(*) as count FROM user_events ORDER BY hour")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     17,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Hourly user activity patterns",
	})
	fmt.Println("[INFO] Done with query 17")

	// Query 18: Daily RSSI variance
	fmt.Println("[INFO] Running query 18: Daily RSSI variance")
	start = time.Now()
	_, err = queryPool.Query(context.Background(), "SELECT timestamp, variance(rssi) as rssi_variance FROM user_events SAMPLE BY 1d LIMIT 30")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     18,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Daily RSSI variance",
	})
	fmt.Println("[INFO] Done with query 18")

	// Query 19: Peak usage hours
	fmt.Println("[INFO] Running query 19: Peak usage hours")
	start = time.Now()
	_, err = queryPool.Query(context.Background(), "SELECT timestamp, count FROM (SELECT timestamp, COUNT(*) as count FROM user_events SAMPLE BY 1h) ORDER BY count DESC LIMIT 5")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     19,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Peak usage hours",
	})
	fmt.Println("[INFO] Done with query 19")

	// Query 20: User session duration analysis
	fmt.Println("[INFO] Running query 20: User session duration analysis")
	start = time.Now()
	_, err = queryPool.Query(context.Background(), "SELECT user_id, max(timestamp) - min(timestamp) as session_duration FROM user_events ORDER BY session_duration DESC LIMIT 10")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     20,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "User session duration analysis",
	})
	fmt.Println("[INFO] Done with query 20")

	results.DbType = "questdb"
	out, err := os.Create(outFile)
	if err != nil {
		return err
	}

	defer out.Close()
	if err := json.NewEncoder(out).Encode(results); err != nil {
		return err
	}
	return nil
}

func benchmarkInfluxDB(connStr string, outFile string) error {
	client := influxdb2.NewClientWithOptions("http://localhost:8086", "mytoken123", influxdb2.DefaultOptions())
	defer client.Close()

	org := "myorg"
	bucket := "benchmark"
	writeAPI := client.WriteAPI(org, bucket)
	queryAPI := client.QueryAPI(org)

	currentChunk := 0
	results := BenchmarkResults{}
	nRecords := 0

	// Ingestion benchmark
	for {
		hasNext, data, err := loadDataChunk(currentChunk)
		if err != nil {
			return err
		}

		start := time.Now()

		// Convert data to InfluxDB points and write in batch
		for _, reading := range data.Response {
			p := influxdb2.NewPointWithMeasurement("user_events").
				AddTag("user_id", reading.UserId).
				AddTag("ssid", reading.Connection.Ssid).
				AddField("rssi", reading.Connection.Rssi).
				SetTime(time.Unix(int64(reading.LastUpdatedTime), 0))

			writeAPI.WritePoint(p)
		}

		// Flush the batch
		writeAPI.Flush()

		nRecords += len(data.Response)
		duration := time.Since(start).Milliseconds()
		results.Ingestion = append(results.Ingestion, struct {
			DurationMs int64 `json:"durationMs"`
			NRecords   int   `json:"nRecords"`
		}{
			DurationMs: duration,
			NRecords:   nRecords,
		})

		currentChunk++
		if !hasNext {
			break
		}
	}

	// Query benchmarks
	var minTime, maxTime, middleTime time.Time

	// Query 1: Get time bounds
	fmt.Println("[INFO] Running query 1: Get time bounds")
	start := time.Now()
	query1 := `from(bucket: "benchmark")
		|> range(start: -30y)
		|> filter(fn: (r) => r._measurement == "user_events")
		|> keep(columns: ["_time"])
		|> limit(n: 1)
		|> min(column: "_time")`
	result, err := queryAPI.Query(context.Background(), query1)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     1,
			DurationMs:  -1,
			Description: "Get time bounds",
		})
	} else {
		for result.Next() {
			minTime = result.Record().Time()
		}
		result.Close()

		query1Max := `from(bucket: "benchmark")
		|> range(start: -30y)
		|> filter(fn: (r) => r._measurement == "user_events")
		|> keep(columns: ["_time"])
		|> limit(n: 1)
		|> max(column: "_time")`
		result, err = queryAPI.Query(context.Background(), query1Max)
		if err != nil {
			results.Queries = append(results.Queries, QueryResult{
				QueryId:     1,
				DurationMs:  -1,
				Description: "Get time bounds",
			})
		} else {
			for result.Next() {
				maxTime = result.Record().Time()
			}
			result.Close()

			results.Queries = append(results.Queries, QueryResult{
				QueryId:     1,
				DurationMs:  time.Since(start).Milliseconds(),
				Description: "Get time bounds",
			})
		}
	}
	fmt.Println("[INFO] Done with query 1")

	middleTime = minTime.Add(maxTime.Sub(minTime) / 2)

	// Query 2: Count all records
	fmt.Println("[INFO] Running query 2: Count all records")
	start = time.Now()
	query2 := `from(bucket: "benchmark")
		|> range(start: -30y)
		|> filter(fn: (r) => r._measurement == "user_events")
		|> keep(columns: ["_time"])
		|> count()`
	result, err = queryAPI.Query(context.Background(), query2)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     2,
			DurationMs:  -1,
			Description: "Count all records",
		})
	} else {
		for result.Next() {
			// Just consume the result
		}
		result.Close()
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     2,
			DurationMs:  time.Since(start).Milliseconds(),
			Description: "Count all records",
		})
	}
	fmt.Println("[INFO] Done with query 2")

	// Query 3: Count distinct users
	fmt.Println("[INFO] Running query 3: Count distinct users")
	start = time.Now()
	query3 := `from(bucket: "benchmark")
		|> range(start: -30y)
		|> filter(fn: (r) => r._measurement == "user_events")
		|> distinct(column: "user_id")
		|> count()`
	result, err = queryAPI.Query(context.Background(), query3)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     3,
			DurationMs:  -1,
			Description: "Count distinct users",
		})
	} else {
		for result.Next() {
			// Just consume the result
		}
		result.Close()
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     3,
			DurationMs:  time.Since(start).Milliseconds(),
			Description: "Count distinct users",
		})
	}
	fmt.Println("[INFO] Done with query 3")

	// Query 4: Average RSSI
	fmt.Println("[INFO] Running query 4: Average RSSI")
	start = time.Now()
	query4 := `from(bucket: "benchmark")
		|> range(start: -30y)
		|> filter(fn: (r) => r._measurement == "user_events" and r._field == "rssi")
		|> mean()`
	result, err = queryAPI.Query(context.Background(), query4)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     4,
			DurationMs:  -1,
			Description: "Average RSSI",
		})
	} else {
		for result.Next() {
			// Just consume the result
		}
		result.Close()
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     4,
			DurationMs:  time.Since(start).Milliseconds(),
			Description: "Average RSSI",
		})
	}
	fmt.Println("[INFO] Done with query 4")

	// Query 5: Records before middle time
	fmt.Println("[INFO] Running query 5: Records before middle time")
	start = time.Now()
	query5 := fmt.Sprintf(`from(bucket: "benchmark")
		|> range(start: -30y, stop: %s)
		|> filter(fn: (r) => r._measurement == "user_events")
		|> count()`, middleTime.Format(time.RFC3339))
	result, err = queryAPI.Query(context.Background(), query5)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     5,
			DurationMs:  -1,
			Description: "Records before middle time",
		})
	} else {
		for result.Next() {
			// Just consume the result
		}
		result.Close()
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     5,
			DurationMs:  time.Since(start).Milliseconds(),
			Description: "Records before middle time",
		})
	}
	fmt.Println("[INFO] Done with query 5")

	// Query 6: Records after middle time
	fmt.Println("[INFO] Running query 6: Records after middle time")
	start = time.Now()
	query6 := fmt.Sprintf(`from(bucket: "benchmark")
		|> range(start: %s)
		|> filter(fn: (r) => r._measurement == "user_events")
		|> count()`, middleTime.Format(time.RFC3339))
	result, err = queryAPI.Query(context.Background(), query6)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     6,
			DurationMs:  -1,
			Description: "Records after middle time",
		})
	} else {
		for result.Next() {
			// Just consume the result
		}
		result.Close()
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     6,
			DurationMs:  time.Since(start).Milliseconds(),
			Description: "Records after middle time",
		})
	}
	fmt.Println("[INFO] Done with query 6")

	// Query 7: Records around middle time (±1 hour)
	fmt.Println("[INFO] Running query 7: Records around middle time (±1 hour)")
	start = time.Now()
	hourBefore := middleTime.Add(-time.Hour)
	hourAfter := middleTime.Add(time.Hour)
	query7 := fmt.Sprintf(`from(bucket: "benchmark")
		|> range(start: %s, stop: %s)
		|> filter(fn: (r) => r._measurement == "user_events")
		|> count()`, hourBefore.Format(time.RFC3339), hourAfter.Format(time.RFC3339))
	result, err = queryAPI.Query(context.Background(), query7)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     7,
			DurationMs:  -1,
			Description: "Records around middle time (±1 hour)",
		})
	} else {
		for result.Next() {
			// Just consume the result
		}
		result.Close()
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     7,
			DurationMs:  time.Since(start).Milliseconds(),
			Description: "Records around middle time (±1 hour)",
		})
	}
	fmt.Println("[INFO] Done with query 7")

	// Query 8: 24 hours aggregation from middle time
	fmt.Println("[INFO] Running query 8: 24 hours aggregation from middle time")
	start = time.Now()
	dayAfter := middleTime.Add(24 * time.Hour)
	query8 := fmt.Sprintf(`from(bucket: "benchmark")
		|> range(start: %s, stop: %s)
		|> filter(fn: (r) => r._measurement == "user_events")
		|> aggregateWindow(every: 1h, fn: count)`, middleTime.Format(time.RFC3339), dayAfter.Format(time.RFC3339))
	result, err = queryAPI.Query(context.Background(), query8)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     8,
			DurationMs:  -1,
			Description: "24 hours aggregation from middle time",
		})
	} else {
		for result.Next() {
			// Just consume the result
		}
		result.Close()
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     8,
			DurationMs:  time.Since(start).Milliseconds(),
			Description: "24 hours aggregation from middle time",
		})
	}
	fmt.Println("[INFO] Done with query 8")

	// Query 9: Top 10 users by activity
	fmt.Println("[INFO] Running query 9: Top 10 users by activity")
	start = time.Now()
	query9 := `from(bucket: "benchmark")
		|> range(start: -30y)
		|> filter(fn: (r) => r._measurement == "user_events")
		|> group(columns: ["user_id"])
		|> count()
		|> top(n: 10)`
	result, err = queryAPI.Query(context.Background(), query9)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     9,
			DurationMs:  -1,
			Description: "Top 10 users by activity",
		})
	} else {
		for result.Next() {
			// Just consume the result
		}
		result.Close()
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     9,
			DurationMs:  time.Since(start).Milliseconds(),
			Description: "Top 10 users by activity",
		})
	}
	fmt.Println("[INFO] Done with query 9")

	// Query 10: Records with strong signal
	fmt.Println("[INFO] Running query 10: Records with strong signal")
	start = time.Now()
	query10 := `from(bucket: "benchmark")
		|> range(start: -30y)
		|> filter(fn: (r) => r._measurement == "user_events" and r._field == "rssi" and r._value > -50.0)
		|> count()`
	result, err = queryAPI.Query(context.Background(), query10)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     10,
			DurationMs:  -1,
			Description: "Records with strong signal",
		})
	} else {
		for result.Next() {
			// Just consume the result
		}
		result.Close()
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     10,
			DurationMs:  time.Since(start).Milliseconds(),
			Description: "Records with strong signal",
		})
	}
	fmt.Println("[INFO] Done with query 10")

	// Query 11: Records with weak signal
	fmt.Println("[INFO] Running query 11: Records with weak signal")
	start = time.Now()
	query11 := `from(bucket: "benchmark")
		|> range(start: -30y)
		|> filter(fn: (r) => r._measurement == "user_events" and r._field == "rssi" and r._value < -80.0)
		|> count()`
	result, err = queryAPI.Query(context.Background(), query11)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     11,
			DurationMs:  -1,
			Description: "Records with weak signal",
		})
	} else {
		for result.Next() {
			// Just consume the result
		}
		result.Close()
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     11,
			DurationMs:  time.Since(start).Milliseconds(),
			Description: "Records with weak signal",
		})
	}
	fmt.Println("[INFO] Done with query 11")

	// Query 12: Top SSIDs
	fmt.Println("[INFO] Running query 12: Top SSIDs")
	start = time.Now()
	query12 := `from(bucket: "benchmark")
		|> range(start: -30y)
		|> filter(fn: (r) => r._measurement == "user_events")
		|> group(columns: ["ssid"])
		|> count()
		|> top(n: 10)`
	result, err = queryAPI.Query(context.Background(), query12)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     12,
			DurationMs:  -1,
			Description: "Top SSIDs",
		})
	} else {
		for result.Next() {
			// Just consume the result
		}
		result.Close()
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     12,
			DurationMs:  time.Since(start).Milliseconds(),
			Description: "Top SSIDs",
		})
	}
	fmt.Println("[INFO] Done with query 12")

	// Query 13: RSSI statistics by user
	fmt.Println("[INFO] Running query 13: RSSI statistics by user")
	start = time.Now()
	query13 := `from(bucket: "benchmark")
		|> range(start: -30y)
		|> filter(fn: (r) => r._measurement == "user_events" and r._field == "rssi")
		|> group(columns: ["user_id"])
		|> aggregateWindow(every: inf, fn: mean)
		|> top(n: 100)`
	result, err = queryAPI.Query(context.Background(), query13)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     13,
			DurationMs:  -1,
			Description: "RSSI statistics by user",
		})
	} else {
		for result.Next() {
			// Just consume the result
		}
		result.Close()
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     13,
			DurationMs:  time.Since(start).Milliseconds(),
			Description: "RSSI statistics by user",
		})
	}
	fmt.Println("[INFO] Done with query 13")

	// Query 14: RSSI percentiles
	fmt.Println("[INFO] Running query 14: RSSI percentiles")
	start = time.Now()
	query14 := `from(bucket: "benchmark")
		|> range(start: -30y)
		|> filter(fn: (r) => r._measurement == "user_events" and r._field == "rssi")
		|> quantile(q: 0.25, method: "estimate_tdigest")
		|> yield(name: "q1")`
	result, err = queryAPI.Query(context.Background(), query14)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     14,
			DurationMs:  -1,
			Description: "RSSI percentiles",
		})
	} else {
		for result.Next() {
			// Just consume the result
		}
		result.Close()
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     14,
			DurationMs:  time.Since(start).Milliseconds(),
			Description: "RSSI percentiles",
		})
	}
	fmt.Println("[INFO] Done with query 14")

	// Query 15: Records in first half
	fmt.Println("[INFO] Running query 15: Records in first half")
	start = time.Now()
	query15 := fmt.Sprintf(`from(bucket: "benchmark")
		|> range(start: %s, stop: %s)
		|> filter(fn: (r) => r._measurement == "user_events")
		|> count()`, minTime.Format(time.RFC3339), middleTime.Format(time.RFC3339))
	result, err = queryAPI.Query(context.Background(), query15)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     15,
			DurationMs:  -1,
			Description: "Records in first half",
		})
	} else {
		for result.Next() {
			// Just consume the result
		}
		result.Close()
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     15,
			DurationMs:  time.Since(start).Milliseconds(),
			Description: "Records in first half",
		})
	}
	fmt.Println("[INFO] Done with query 15")

	// Query 16: Records in second half
	fmt.Println("[INFO] Running query 16: Records in second half")
	start = time.Now()
	query16 := fmt.Sprintf(`from(bucket: "benchmark")
		|> range(start: %s, stop: %s)
		|> filter(fn: (r) => r._measurement == "user_events")
		|> count()`, middleTime.Format(time.RFC3339), maxTime.Format(time.RFC3339))
	result, err = queryAPI.Query(context.Background(), query16)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     16,
			DurationMs:  -1,
			Description: "Records in second half",
		})
	} else {
		for result.Next() {
			// Just consume the result
		}
		result.Close()
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     16,
			DurationMs:  time.Since(start).Milliseconds(),
			Description: "Records in second half",
		})
	}
	fmt.Println("[INFO] Done with query 16")

	// Query 17: Hourly user activity patterns
	fmt.Println("[INFO] Running query 17: Hourly user activity patterns")
	start = time.Now()
	query17 := `from(bucket: "benchmark")
		|> range(start: -30y)
		|> filter(fn: (r) => r._measurement == "user_events")
		|> group(columns: ["_time"])
		|> aggregateWindow(every: 1h, fn: count)
		|> group(columns: ["hour"])
		|> sum()`
	result, err = queryAPI.Query(context.Background(), query17)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     17,
			DurationMs:  -1,
			Description: "Hourly user activity patterns",
		})
	} else {
		for result.Next() {
			// Just consume the result
		}
		result.Close()
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     17,
			DurationMs:  time.Since(start).Milliseconds(),
			Description: "Hourly user activity patterns",
		})
	}
	fmt.Println("[INFO] Done with query 17")

	// Query 18: Daily RSSI variance
	fmt.Println("[INFO] Running query 18: Daily RSSI variance")
	start = time.Now()
	query18 := `from(bucket: "benchmark")
		|> range(start: -30y)
		|> filter(fn: (r) => r._measurement == "user_events" and r._field == "rssi")
		|> aggregateWindow(every: 1d, fn: stddev)
		|> limit(n: 30)`
	result, err = queryAPI.Query(context.Background(), query18)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     18,
			DurationMs:  -1,
			Description: "Daily RSSI variance",
		})
	} else {
		for result.Next() {
			// Just consume the result
		}
		result.Close()
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     18,
			DurationMs:  time.Since(start).Milliseconds(),
			Description: "Daily RSSI variance",
		})
	}
	fmt.Println("[INFO] Done with query 18")

	// Query 19: Peak usage hours
	fmt.Println("[INFO] Running query 19: Peak usage hours")
	start = time.Now()
	query19 := `from(bucket: "benchmark")
		|> range(start: -30y)
		|> filter(fn: (r) => r._measurement == "user_events")
		|> aggregateWindow(every: 1h, fn: count)
		|> top(n: 5)`
	result, err = queryAPI.Query(context.Background(), query19)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     19,
			DurationMs:  -1,
			Description: "Peak usage hours",
		})
	} else {
		for result.Next() {
			// Just consume the result
		}
		result.Close()
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     19,
			DurationMs:  time.Since(start).Milliseconds(),
			Description: "Peak usage hours",
		})
	}
	fmt.Println("[INFO] Done with query 19")

	// Query 20: User session duration analysis
	fmt.Println("[INFO] Running query 20: User session duration analysis")
	start = time.Now()
	query20 := `from(bucket: "benchmark")
		|> range(start: -30y)
		|> filter(fn: (r) => r._measurement == "user_events")
		|> group(columns: ["user_id"])
		|> aggregateWindow(every: inf, fn: spread)
		|> top(n: 10)`
	result, err = queryAPI.Query(context.Background(), query20)
	if err != nil {
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     20,
			DurationMs:  -1,
			Description: "User session duration analysis",
		})
	} else {
		for result.Next() {
			// Just consume the result
		}
		result.Close()
		results.Queries = append(results.Queries, QueryResult{
			QueryId:     20,
			DurationMs:  time.Since(start).Milliseconds(),
			Description: "User session duration analysis",
		})
	}
	fmt.Println("[INFO] Done with query 20")

	results.DbType = "influxdb"
	out, err := os.Create(outFile)
	if err != nil {
		return err
	}

	defer out.Close()
	if err := json.NewEncoder(out).Encode(results); err != nil {
		return err
	}
	return nil
}

func benchmarkCrateDB(connStr string, outFile string) error {
	pool, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		return err
	}

	// Create the table if it doesn't exist
	_, err = pool.Exec(context.Background(), `
		CREATE TABLE IF NOT EXISTS user_events (
			user_id TEXT NOT NULL,
			ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
			rssi FLOAT NOT NULL,
			ssid TEXT NOT NULL
		) CLUSTERED BY (ts) INTO 4 SHARDS`)
	if err != nil {
		return err
	}

	currentChunk := 0
	results := BenchmarkResults{}
	nRecords := 0

	// Ingestion benchmark
	for {
		hasNext, data, err := loadDataChunk(currentChunk)
		if err != nil {
			return err
		}

		start := time.Now()

		// Use batch INSERT for CrateDB instead of CopyFrom
		batch := &pgx.Batch{}
		for _, reading := range data.Response {
			batch.Queue(
				"INSERT INTO user_events (user_id, ts, rssi, ssid) VALUES ($1, $2, $3, $4)",
				reading.UserId,
				time.Unix(int64(reading.LastUpdatedTime), 0),
				reading.Connection.Rssi,
				reading.Connection.Ssid,
			)
		}

		batchResults := pool.SendBatch(context.Background(), batch)
		err = batchResults.Close()
		if err != nil {
			return err
		}

		nRecords += len(data.Response)
		duration := time.Since(start).Milliseconds()
		results.Ingestion = append(results.Ingestion, struct {
			DurationMs int64 `json:"durationMs"`
			NRecords   int   `json:"nRecords"`
		}{
			DurationMs: duration,
			NRecords:   nRecords,
		})

		currentChunk++
		if !hasNext {
			break
		}
	}

	// Query benchmarks
	var minTime, maxTime, middleTime time.Time

	// Query 1: Get time bounds
	fmt.Println("[INFO] Running query 1: Get time bounds")
	start := time.Now()
	err = pool.QueryRow(context.Background(), "SELECT MIN(ts), MAX(ts) FROM user_events").Scan(&minTime, &maxTime)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     1,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Get time bounds",
	})
	fmt.Println("[INFO] Done with query 1")

	middleTime = minTime.Add(maxTime.Sub(minTime) / 2)

	// Query 2: Count all records
	fmt.Println("[INFO] Running query 2: Count all records")
	start = time.Now()
	var totalCount int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events").Scan(&totalCount)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     2,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Count all records",
	})
	fmt.Println("[INFO] Done with query 2")

	// Query 3: Count distinct users
	fmt.Println("[INFO] Running query 3: Count distinct users")
	start = time.Now()
	var distinctUsers int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(DISTINCT user_id) FROM user_events").Scan(&distinctUsers)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     3,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Count distinct users",
	})
	fmt.Println("[INFO] Done with query 3")

	// Query 4: Average RSSI
	fmt.Println("[INFO] Running query 4: Average RSSI")
	start = time.Now()
	var avgRssi float64
	err = pool.QueryRow(context.Background(), "SELECT AVG(rssi) FROM user_events").Scan(&avgRssi)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     4,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Average RSSI",
	})
	fmt.Println("[INFO] Done with query 4")

	// Query 5: Records before middle time
	fmt.Println("[INFO] Running query 5: Records before middle time")
	start = time.Now()
	var beforeMiddle int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE ts < $1", middleTime).Scan(&beforeMiddle)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     5,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records before middle time",
	})
	fmt.Println("[INFO] Done with query 5")

	// Query 6: Records after middle time
	fmt.Println("[INFO] Running query 6: Records after middle time")
	start = time.Now()
	var afterMiddle int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE ts > $1", middleTime).Scan(&afterMiddle)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     6,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records after middle time",
	})
	fmt.Println("[INFO] Done with query 6")

	// Query 7: Records around middle time (±1 hour)
	fmt.Println("[INFO] Running query 7: Records around middle time (±1 hour)")
	start = time.Now()
	var aroundMiddle int
	hourBefore := middleTime.Add(-time.Hour)
	hourAfter := middleTime.Add(time.Hour)
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE ts BETWEEN $1 AND $2", hourBefore, hourAfter).Scan(&aroundMiddle)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     7,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records around middle time (±1 hour)",
	})
	fmt.Println("[INFO] Done with query 7")

	// Query 8: 24 hours aggregation from middle time
	fmt.Println("[INFO] Running query 8: 24 hours aggregation from middle time")
	start = time.Now()
	dayAfter := middleTime.Add(24 * time.Hour)
	_, err = pool.Query(context.Background(), "SELECT date_trunc('hour', ts) as hour, COUNT(*) FROM user_events WHERE ts BETWEEN $1 AND $2 GROUP BY hour ORDER BY hour", middleTime, dayAfter)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     8,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "24 hours aggregation from middle time",
	})
	fmt.Println("[INFO] Done with query 8")

	// Query 9: Top 10 users by activity
	fmt.Println("[INFO] Running query 9: Top 10 users by activity")
	start = time.Now()
	_, err = pool.Query(context.Background(), "SELECT user_id, COUNT(*) as count FROM user_events GROUP BY user_id ORDER BY count DESC LIMIT 10")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     9,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Top 10 users by activity",
	})
	fmt.Println("[INFO] Done with query 9")

	// Query 10: Records with strong signal
	fmt.Println("[INFO] Running query 10: Records with strong signal")
	start = time.Now()
	var strongSignal int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE rssi > -50").Scan(&strongSignal)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     10,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records with strong signal",
	})
	fmt.Println("[INFO] Done with query 10")

	// Query 11: Records with weak signal
	fmt.Println("[INFO] Running query 11: Records with weak signal")
	start = time.Now()
	var weakSignal int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE rssi < -80").Scan(&weakSignal)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     11,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records with weak signal",
	})
	fmt.Println("[INFO] Done with query 11")

	// Query 12: Top SSIDs
	fmt.Println("[INFO] Running query 12: Top SSIDs")
	start = time.Now()
	_, err = pool.Query(context.Background(), "SELECT ssid, COUNT(*) as count FROM user_events GROUP BY ssid ORDER BY count DESC LIMIT 10")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     12,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Top SSIDs",
	})
	fmt.Println("[INFO] Done with query 12")

	// Query 13: RSSI statistics by user
	fmt.Println("[INFO] Running query 13: RSSI statistics by user")
	start = time.Now()
	_, err = pool.Query(context.Background(), "SELECT user_id, AVG(rssi), MIN(rssi), MAX(rssi) FROM user_events GROUP BY user_id ORDER BY AVG(rssi) DESC LIMIT 100")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     13,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "RSSI statistics by user",
	})
	fmt.Println("[INFO] Done with query 13")

	// Query 14: RSSI percentiles
	fmt.Println("[INFO] Running query 14: RSSI percentiles")
	start = time.Now()
	var q25, q50, q75 float64
	err = pool.QueryRow(context.Background(), "SELECT percentile(rssi, 0.25), percentile(rssi, 0.5), percentile(rssi, 0.75) FROM user_events").Scan(&q25, &q50, &q75)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     14,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "RSSI percentiles",
	})
	fmt.Println("[INFO] Done with query 14")

	// Query 15: Records in first half
	fmt.Println("[INFO] Running query 15: Records in first half")
	start = time.Now()
	var firstHalf int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE ts BETWEEN $1 AND $2", minTime, middleTime).Scan(&firstHalf)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     15,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records in first half",
	})
	fmt.Println("[INFO] Done with query 15")

	// Query 16: Records in second half
	fmt.Println("[INFO] Running query 16: Records in second half")
	start = time.Now()
	var secondHalf int
	err = pool.QueryRow(context.Background(), "SELECT COUNT(*) FROM user_events WHERE ts BETWEEN $1 AND $2", middleTime, maxTime).Scan(&secondHalf)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     16,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records in second half",
	})
	fmt.Println("[INFO] Done with query 16")

	// Query 17: Hourly user activity patterns
	fmt.Println("[INFO] Running query 17: Hourly user activity patterns")
	start = time.Now()
	_, err = pool.Query(context.Background(), "SELECT extract(hour from ts) as hour, COUNT(*) as count FROM user_events GROUP BY hour ORDER BY hour")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     17,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Hourly user activity patterns",
	})
	fmt.Println("[INFO] Done with query 17")

	// Query 18: Daily RSSI variance
	fmt.Println("[INFO] Running query 18: Daily RSSI variance")
	start = time.Now()
	_, err = pool.Query(context.Background(), "SELECT date_trunc('day', ts) as day, variance(rssi) as rssi_variance FROM user_events GROUP BY day ORDER BY day LIMIT 30")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     18,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Daily RSSI variance",
	})
	fmt.Println("[INFO] Done with query 18")

	// Query 19: Peak usage hours
	fmt.Println("[INFO] Running query 19: Peak usage hours")
	start = time.Now()
	_, err = pool.Query(context.Background(), "SELECT date_trunc('hour', ts) as hour, COUNT(*) as count FROM user_events GROUP BY hour ORDER BY count DESC LIMIT 5")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     19,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Peak usage hours",
	})
	fmt.Println("[INFO] Done with query 19")

	// Query 20: User session duration analysis
	fmt.Println("[INFO] Running query 20: User session duration analysis")
	start = time.Now()
	_, err = pool.Query(context.Background(), "SELECT user_id, MAX(ts) - MIN(ts) as session_duration FROM user_events GROUP BY user_id ORDER BY session_duration DESC LIMIT 10")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     20,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "User session duration analysis",
	})
	fmt.Println("[INFO] Done with query 20")

	results.DbType = "cratedb"
	out, err := os.Create(outFile)
	if err != nil {
		return err
	}

	defer out.Close()
	if err := json.NewEncoder(out).Encode(results); err != nil {
		return err
	}
	return nil
}

func benchmarkClickHouse(connStr string, outFile string) error {
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{connStr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
	})
	defer conn.Close()

	if err := conn.Ping(); err != nil {
		return err
	}

	// Create the table if it doesn't exist
	_, err := conn.Exec(`
		CREATE TABLE IF NOT EXISTS user_events (
			id UInt64,
			user_id String,
			timestamp DateTime,
			rssi Float32,
			ssid String
		) ENGINE = MergeTree()
		ORDER BY timestamp`)
	if err != nil {
		return err
	}

	currentChunk := 0
	results := BenchmarkResults{}
	nRecords := 0

	// Ingestion benchmark
	for {
		hasNext, data, err := loadDataChunk(currentChunk)
		if err != nil {
			return err
		}

		start := time.Now()

		// Prepare batch insert
		tx, err := conn.Begin()
		if err != nil {
			return err
		}

		stmt, err := tx.Prepare("INSERT INTO user_events (id, user_id, timestamp, rssi, ssid) VALUES (?, ?, ?, ?, ?)")
		if err != nil {
			return err
		}

		for i, reading := range data.Response {
			_, err = stmt.Exec(
				uint64(nRecords+i+1),
				reading.UserId,
				time.Unix(int64(reading.LastUpdatedTime), 0),
				reading.Connection.Rssi,
				reading.Connection.Ssid,
			)
			if err != nil {
				return err
			}
		}

		if err = tx.Commit(); err != nil {
			return err
		}

		nRecords += len(data.Response)
		duration := time.Since(start).Milliseconds()
		results.Ingestion = append(results.Ingestion, struct {
			DurationMs int64 `json:"durationMs"`
			NRecords   int   `json:"nRecords"`
		}{
			DurationMs: duration,
			NRecords:   nRecords,
		})

		currentChunk++
		if !hasNext {
			break
		}
	}

	// Query benchmarks
	var minTime, maxTime, middleTime time.Time

	// Query 1: Get time bounds
	fmt.Println("[INFO] Running query 1: Get time bounds")
	start := time.Now()
	err = conn.QueryRow("SELECT MIN(timestamp), MAX(timestamp) FROM user_events").Scan(&minTime, &maxTime)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     1,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Get time bounds",
	})
	fmt.Println("[INFO] Done with query 1")

	middleTime = minTime.Add(maxTime.Sub(minTime) / 2)

	// Query 2: Count all records
	fmt.Println("[INFO] Running query 2: Count all records")
	start = time.Now()
	var totalCount int
	err = conn.QueryRow("SELECT COUNT(*) FROM user_events").Scan(&totalCount)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     2,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Count all records",
	})
	fmt.Println("[INFO] Done with query 2")

	// Query 3: Count distinct users
	fmt.Println("[INFO] Running query 3: Count distinct users")
	start = time.Now()
	var distinctUsers int
	err = conn.QueryRow("SELECT COUNT(DISTINCT user_id) FROM user_events").Scan(&distinctUsers)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     3,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Count distinct users",
	})
	fmt.Println("[INFO] Done with query 3")

	// Query 4: Average RSSI
	fmt.Println("[INFO] Running query 4: Average RSSI")
	start = time.Now()
	var avgRssi float64
	err = conn.QueryRow("SELECT AVG(rssi) FROM user_events").Scan(&avgRssi)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     4,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Average RSSI",
	})
	fmt.Println("[INFO] Done with query 4")

	// Query 5: Records before middle time
	fmt.Println("[INFO] Running query 5: Records before middle time")
	start = time.Now()
	var beforeMiddle int
	err = conn.QueryRow("SELECT COUNT(*) FROM user_events WHERE timestamp < ?", middleTime).Scan(&beforeMiddle)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     5,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records before middle time",
	})
	fmt.Println("[INFO] Done with query 5")

	// Query 6: Records after middle time
	fmt.Println("[INFO] Running query 6: Records after middle time")
	start = time.Now()
	var afterMiddle int
	err = conn.QueryRow("SELECT COUNT(*) FROM user_events WHERE timestamp > ?", middleTime).Scan(&afterMiddle)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     6,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records after middle time",
	})
	fmt.Println("[INFO] Done with query 6")

	// Query 7: Records around middle time (±1 hour)
	fmt.Println("[INFO] Running query 7: Records around middle time (±1 hour)")
	start = time.Now()
	var aroundMiddle int
	hourBefore := middleTime.Add(-time.Hour)
	hourAfter := middleTime.Add(time.Hour)
	err = conn.QueryRow("SELECT COUNT(*) FROM user_events WHERE timestamp BETWEEN ? AND ?", hourBefore, hourAfter).Scan(&aroundMiddle)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     7,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records around middle time (±1 hour)",
	})
	fmt.Println("[INFO] Done with query 7")

	// Query 8: 24 hours aggregation from middle time
	fmt.Println("[INFO] Running query 8: 24 hours aggregation from middle time")
	start = time.Now()
	dayAfter := middleTime.Add(24 * time.Hour)
	_, err = conn.Query("SELECT toStartOfHour(timestamp) as hour, COUNT(*) FROM user_events WHERE timestamp BETWEEN ? AND ? GROUP BY hour ORDER BY hour", middleTime, dayAfter)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     8,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "24 hours aggregation from middle time",
	})
	fmt.Println("[INFO] Done with query 8")

	// Query 9: Top 10 users by activity
	fmt.Println("[INFO] Running query 9: Top 10 users by activity")
	start = time.Now()
	_, err = conn.Query("SELECT user_id, COUNT(*) as count FROM user_events GROUP BY user_id ORDER BY count DESC LIMIT 10")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     9,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Top 10 users by activity",
	})
	fmt.Println("[INFO] Done with query 9")

	// Query 10: Records with strong signal
	fmt.Println("[INFO] Running query 10: Records with strong signal")
	start = time.Now()
	var strongSignal int
	err = conn.QueryRow("SELECT COUNT(*) FROM user_events WHERE rssi > -50").Scan(&strongSignal)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     10,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records with strong signal",
	})
	fmt.Println("[INFO] Done with query 10")

	// Query 11: Records with weak signal
	fmt.Println("[INFO] Running query 11: Records with weak signal")
	start = time.Now()
	var weakSignal int
	err = conn.QueryRow("SELECT COUNT(*) FROM user_events WHERE rssi < -80").Scan(&weakSignal)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     11,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records with weak signal",
	})
	fmt.Println("[INFO] Done with query 11")

	// Query 12: Top SSIDs
	fmt.Println("[INFO] Running query 12: Top SSIDs")
	start = time.Now()
	_, err = conn.Query("SELECT ssid, COUNT(*) as count FROM user_events GROUP BY ssid ORDER BY count DESC LIMIT 10")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     12,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Top SSIDs",
	})
	fmt.Println("[INFO] Done with query 12")

	// Query 13: RSSI statistics by user
	fmt.Println("[INFO] Running query 13: RSSI statistics by user")
	start = time.Now()
	_, err = conn.Query("SELECT user_id, AVG(rssi), MIN(rssi), MAX(rssi) FROM user_events GROUP BY user_id ORDER BY AVG(rssi) DESC LIMIT 100")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     13,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "RSSI statistics by user",
	})
	fmt.Println("[INFO] Done with query 13")

	// Query 14: RSSI percentiles
	fmt.Println("[INFO] Running query 14: RSSI percentiles")
	start = time.Now()
	var q1, median, q3 float64
	err = conn.QueryRow("SELECT quantile(0.25)(rssi) as q1, quantile(0.5)(rssi) as median, quantile(0.75)(rssi) as q3 FROM user_events").Scan(&q1, &median, &q3)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     14,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "RSSI percentiles",
	})
	fmt.Println("[INFO] Done with query 14")

	// Query 15: Records in first half
	fmt.Println("[INFO] Running query 15: Records in first half")
	start = time.Now()
	var firstHalf int
	err = conn.QueryRow("SELECT COUNT(*) FROM user_events WHERE timestamp BETWEEN ? AND ?", minTime, middleTime).Scan(&firstHalf)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     15,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records in first half",
	})
	fmt.Println("[INFO] Done with query 15")

	// Query 16: Records in second half
	fmt.Println("[INFO] Running query 16: Records in second half")
	start = time.Now()
	var secondHalf int
	err = conn.QueryRow("SELECT COUNT(*) FROM user_events WHERE timestamp BETWEEN ? AND ?", middleTime, maxTime).Scan(&secondHalf)
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     16,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Records in second half",
	})
	fmt.Println("[INFO] Done with query 16")

	// Query 17: Hourly user activity patterns
	fmt.Println("[INFO] Running query 17: Hourly user activity patterns")
	start = time.Now()
	_, err = conn.Query("SELECT toHour(timestamp) as hour, COUNT(*) as count FROM user_events GROUP BY hour ORDER BY hour")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     17,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Hourly user activity patterns",
	})
	fmt.Println("[INFO] Done with query 17")

	// Query 18: Daily RSSI variance
	fmt.Println("[INFO] Running query 18: Daily RSSI variance")
	start = time.Now()
	_, err = conn.Query("SELECT toStartOfDay(timestamp) as day, varSamp(rssi) as rssi_variance FROM user_events GROUP BY day ORDER BY day LIMIT 30")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     18,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Daily RSSI variance",
	})
	fmt.Println("[INFO] Done with query 18")

	// Query 19: Peak usage hours
	fmt.Println("[INFO] Running query 19: Peak usage hours")
	start = time.Now()
	_, err = conn.Query("SELECT toStartOfHour(timestamp) as hour, COUNT(*) as count FROM user_events GROUP BY hour ORDER BY count DESC LIMIT 5")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     19,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "Peak usage hours",
	})
	fmt.Println("[INFO] Done with query 19")

	// Query 20: User session duration analysis
	fmt.Println("[INFO] Running query 20: User session duration analysis")
	start = time.Now()
	_, err = conn.Query("SELECT user_id, MAX(timestamp) - MIN(timestamp) as session_duration FROM user_events GROUP BY user_id ORDER BY session_duration DESC LIMIT 10")
	if err != nil {
		return err
	}
	results.Queries = append(results.Queries, QueryResult{
		QueryId:     20,
		DurationMs:  time.Since(start).Milliseconds(),
		Description: "User session duration analysis",
	})
	fmt.Println("[INFO] Done with query 20")

	results.DbType = "clickhouse"
	out, err := os.Create(outFile)
	if err != nil {
		return err
	}

	defer out.Close()
	if err := json.NewEncoder(out).Encode(results); err != nil {
		return err
	}
	return nil
}

func main() {
	connStr := flag.String("conn", "", "Database connection string")
	outputFile := flag.String("o", "", "Output file name")
	dbType := flag.String("type", "", "Database type: postgres, timescaledb, questdb, cratedb, clickhouse, or influxdb")
	flag.Parse()

	if *connStr == "" || *dbType == "" || *outputFile == "" {
		flag.Usage()
		return
	}

	if *dbType == "postgres" {
		if err := benchmarkPostgres(*connStr, *outputFile); err != nil {
			panic(err)
		}
	} else if *dbType == "timescaledb" {
		if err := benchmarkTimescaleDb(*connStr, *outputFile); err != nil {
			panic(err)
		}
	} else if *dbType == "questdb" {
		if err := benchmarkQuestDb(*connStr, *outputFile); err != nil {
			panic(err)
		}
	} else if *dbType == "cratedb" {
		if err := benchmarkCrateDB(*connStr, *outputFile); err != nil {
			panic(err)
		}
	} else if *dbType == "clickhouse" {
		if err := benchmarkClickHouse(*connStr, *outputFile); err != nil {
			panic(err)
		}
	} else if *dbType == "influxdb" {
		if err := benchmarkInfluxDB(*connStr, *outputFile); err != nil {
			panic(err)
		}
	} else {
		panic("Unsupported database type: " + *dbType)
	}
}
