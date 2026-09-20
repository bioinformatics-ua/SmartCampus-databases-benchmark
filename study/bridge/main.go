// Study bridge: bounded ingestion and fully consumed query results.
//
// The Python controller talks to one bridge per database over local HTTP, so
// every engine is measured through the same native Go client path.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	influx "github.com/influxdata/influxdb-client-go/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	maxConnections = 72
	defaultTimeout = 300 * time.Second
	maxRequestSize = 64 << 20

	influxURL    = "http://127.0.0.1:18086"
	influxToken  = "study-token-local-only"
	influxOrg    = "study"
	influxBucket = "bench"

	questDBWriteURL = "http://127.0.0.1:19000/write?precision=n"
	crateDBSQLURL   = "http://127.0.0.1:14200/_sql"
)

// PostgreSQL wire protocol endpoints.
var postgresDSN = map[string]string{
	"postgres":    "postgres://bench:bench@127.0.0.1:15432/bench?sslmode=disable",
	"timescaledb": "postgres://bench:bench@127.0.0.1:15432/bench?sslmode=disable",
	"questdb":     "postgres://admin:quest@127.0.0.1:18812/qdb?sslmode=disable",
	"cratedb":     "postgres://crate@127.0.0.1:15432/doc?sslmode=disable",
}

type Event struct {
	User string  `json:"user"`
	SSID string  `json:"ssid"`
	TS   int64   `json:"ts"`
	RSSI float64 `json:"rssi"`
}

type Server struct {
	engine string
	pg     *pgxpool.Pool
	ch     chdriver.Conn
	inf    influx.Client
	http   *http.Client
}

type Request struct {
	Query      string   `json:"query"`
	Columns    int      `json:"columns"`
	Timeout    float64  `json:"timeout"`
	Events     []Event  `json:"events"`
	Path       string   `json:"path"`
	Level      string   `json:"level"`
	Limit      int64    `json:"limit"`
	Shift      int64    `json:"shift"`
	Batch      int      `json:"batch"`
	Log        string   `json:"log"`
	Statements []string `json:"statements"`
}

type Result struct {
	Status   string  `json:"status"`
	Error    string  `json:"error,omitempty"`
	Rows     [][]any `json:"rows"`
	Duration int64   `json:"duration_us"`
	Count    int64   `json:"count,omitempty"`
}

func main() {
	mode := flag.String("mode", "serve", "serve or prepare")
	engine := flag.String("engine", "postgres", "engine")
	addr := flag.String("listen", "127.0.0.1:18999", "listen")
	input := flag.String("input", "", "input directory")
	output := flag.String("output", "", "prepared directory")
	limit := flag.Int64("limit", 0, "maximum source records for preparation")
	flag.Parse()

	if *mode == "prepare" {
		if err := prepare(*input, *output, *limit); err != nil {
			log.Fatal(err)
		}
		return
	}

	s := &Server{engine: *engine, http: &http.Client{Timeout: 0}}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := s.connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer s.close()

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"engine": s.engine, "status": "ok"})
	})
	for _, path := range []string{"/query", "/write", "/load", "/exec"} {
		http.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			s.handle(path, w, r)
		})
	}
	log.Printf("bridge ready: %s", s.engine)
	server := &http.Server{Addr: *addr, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(server.ListenAndServe())
}

// ---------------------------------------------------------------- connections

// connect opens the engine client and runs one trivial query to prove readiness.
func (s *Server) connect(ctx context.Context) error {
	switch s.engine {
	case "postgres", "timescaledb", "questdb", "cratedb":
		return s.connectPostgresWire(ctx)
	case "clickhouse":
		return s.connectClickHouse(ctx)
	case "influxdb":
		return s.connectInflux(ctx)
	default:
		return fmt.Errorf("unknown engine %s", s.engine)
	}
}

func (s *Server) connectPostgresWire(ctx context.Context) error {
	cfg, err := pgxpool.ParseConfig(postgresDSN[s.engine])
	if err != nil {
		return err
	}
	cfg.MaxConns = maxConnections
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	if s.engine == "questdb" || s.engine == "cratedb" {
		cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	}
	if s.pg, err = pgxpool.NewWithConfig(ctx, cfg); err != nil {
		return err
	}
	var one int
	return s.pg.QueryRow(ctx, "SELECT 1").Scan(&one)
}

func (s *Server) connectClickHouse(ctx context.Context) error {
	var err error
	s.ch, err = clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:19001"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "bench",
			Password: "bench",
		},
		MaxOpenConns: maxConnections,
		MaxIdleConns: maxConnections,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  time.Hour,
	})
	if err != nil {
		return err
	}
	return s.ch.Ping(ctx)
}

func newInfluxClient(url string) influx.Client {
	// Each bridge request already has a context deadline. The SDK's default
	// 20-second HTTP timeout must not override that experiment deadline.
	options := influx.DefaultOptions().SetHTTPRequestTimeout(0)
	return influx.NewClientWithOptions(url, influxToken, options)
}

func (s *Server) connectInflux(ctx context.Context) error {
	s.inf = newInfluxClient(influxURL)
	if _, err := s.inf.Ready(ctx); err != nil {
		return err
	}
	result, err := s.inf.QueryAPI(influxOrg).Query(ctx, "buckets() |> limit(n:1)")
	if err != nil {
		return err
	}
	defer result.Close()
	for result.Next() {
	}
	return result.Err()
}

func (s *Server) close() {
	if s.pg != nil {
		s.pg.Close()
	}
	if s.ch != nil {
		s.ch.Close()
	}
	if s.inf != nil {
		s.inf.Close()
	}
}

// ------------------------------------------------------------------- requests

func (s *Server) handle(path string, w http.ResponseWriter, r *http.Request) {
	var req Request
	if err := json.NewDecoder(io.LimitReader(r.Body, maxRequestSize)).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	timeout := defaultTimeout
	if req.Timeout > 0 {
		timeout = time.Duration(req.Timeout * float64(time.Second))
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	start := time.Now()
	res := Result{Status: "ok", Rows: [][]any{}}
	var err error
	switch path {
	case "/query":
		res.Rows, err = s.query(ctx, req.Query, req.Columns)
	case "/exec":
		for _, statement := range req.Statements {
			if err = s.exec(ctx, statement); err != nil {
				break
			}
		}
	case "/write":
		if err = s.write(ctx, req.Events); err == nil {
			res.Count = int64(len(req.Events))
		}
	case "/load":
		res.Count, err = s.load(ctx, req)
	}
	res.Duration = time.Since(start).Microseconds()

	if err != nil {
		res.Status = "error"
		res.Error = err.Error()
		if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
			res.Status = "timeout"
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(res); err != nil {
		log.Print(err)
	}
}

// normalize converts driver values to JSON-friendly types; NaN and ±Inf become null.
func normalize(v any) any {
	switch x := v.(type) {
	case time.Time:
		return x.Unix()
	case []byte:
		return string(x)
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil
		}
		return x
	case float32:
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return nil
		}
		return float64(x)
	default:
		return v
	}
}

// query reads every result row so timings include full result transfer.
func (s *Server) query(ctx context.Context, q string, columns int) ([][]any, error) {
	switch {
	case s.inf != nil:
		return s.queryInflux(ctx, q, columns)
	case s.pg != nil:
		return s.queryPostgresWire(ctx, q)
	default:
		return s.queryClickHouse(ctx, q)
	}
}

// queryInflux reads columns c0..c(n-1); with n == 0 it returns whole records (profiler output).
func (s *Server) queryInflux(ctx context.Context, q string, columns int) ([][]any, error) {
	result, err := s.inf.QueryAPI(influxOrg).Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	out := make([][]any, 0)
	for result.Next() {
		record := result.Record()
		if columns == 0 {
			values := map[string]any{}
			for key, value := range record.Values() {
				values[key] = normalize(value)
			}
			out = append(out, []any{values})
			continue
		}
		row := make([]any, columns)
		for i := range row {
			row[i] = normalize(record.ValueByKey(fmt.Sprintf("c%d", i)))
		}
		out = append(out, row)
	}
	return out, result.Err()
}

func (s *Server) queryPostgresWire(ctx context.Context, q string) ([][]any, error) {
	rows, err := s.pg.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([][]any, 0)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		for i := range values {
			values[i] = normalize(values[i])
		}
		out = append(out, values)
	}
	return out, rows.Err()
}

func (s *Server) queryClickHouse(ctx context.Context, q string) ([][]any, error) {
	rows, err := s.ch.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([][]any, 0)
	for rows.Next() {
		types := rows.ColumnTypes()
		pointers := make([]any, len(types))
		for i := range pointers {
			pointers[i] = reflect.New(types[i].ScanType()).Interface()
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, err
		}
		values := make([]any, len(types))
		for i := range values {
			values[i] = normalize(reflect.ValueOf(pointers[i]).Elem().Interface())
		}
		out = append(out, values)
	}
	return out, rows.Err()
}

func (s *Server) exec(ctx context.Context, q string) error {
	if s.pg != nil {
		_, err := s.pg.Exec(ctx, q)
		return err
	}
	if s.ch != nil {
		return s.ch.Exec(ctx, q)
	}
	return fmt.Errorf("exec not supported for %s", s.engine)
}

// --------------------------------------------------------------------- writes

func escapeTag(x string) string {
	return strings.NewReplacer("\\", "\\\\", " ", "\\ ", ",", "\\,", "=", "\\=").Replace(x)
}

// ilp encodes events as InfluxDB line protocol with nanosecond timestamps.
func ilp(events []Event) string {
	var b strings.Builder
	for _, e := range events {
		fmt.Fprintf(&b, "user_events,user_id=%s,ssid=%s rssi=%s %d\n",
			escapeTag(e.User),
			escapeTag(e.SSID),
			strconv.FormatFloat(e.RSSI, 'g', -1, 64),
			e.TS*1e9,
		)
	}
	return b.String()
}

// write inserts one batch with each engine's native bulk path.
func (s *Server) write(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	switch s.engine {
	case "postgres", "timescaledb":
		return s.writePostgres(ctx, events)
	case "clickhouse":
		return s.writeClickHouse(ctx, events)
	case "influxdb":
		return s.inf.WriteAPIBlocking(influxOrg, influxBucket).WriteRecord(ctx, ilp(events))
	case "questdb":
		_, err := s.post(ctx, "QuestDB", questDBWriteURL, "", strings.NewReader(ilp(events)))
		return err
	case "cratedb":
		return s.writeCrateDB(ctx, events)
	}
	return fmt.Errorf("unknown engine")
}

func (s *Server) writePostgres(ctx context.Context, events []Event) error {
	rows := pgx.CopyFromSlice(len(events), func(i int) ([]any, error) {
		e := events[i]
		return []any{e.User, e.SSID, time.Unix(e.TS, 0).UTC(), e.RSSI}, nil
	})
	_, err := s.pg.CopyFrom(
		ctx,
		pgx.Identifier{"user_events"},
		[]string{"user_id", "ssid", "ts", "rssi"},
		rows,
	)
	return err
}

func (s *Server) writeClickHouse(ctx context.Context, events []Event) error {
	batch, err := s.ch.PrepareBatch(ctx, "INSERT INTO user_events (user_id, ssid, ts, rssi)")
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, e := range events {
		if err = batch.Append(e.User, e.SSID, time.Unix(e.TS, 0).UTC(), e.RSSI); err != nil {
			return err
		}
	}
	return batch.Send()
}

// writeCrateDB uses the HTTP bulk endpoint and requires one acknowledged row per event.
func (s *Server) writeCrateDB(ctx context.Context, events []Event) error {
	args := make([][]any, len(events))
	for i, e := range events {
		args[i] = []any{e.User, e.SSID, e.TS * 1000, e.RSSI}
	}
	data, _ := json.Marshal(map[string]any{
		"stmt":      "INSERT INTO user_events (user_id,ssid,ts,rssi) VALUES (?,?,?,?)",
		"bulk_args": args,
	})
	body, err := s.post(ctx, "CrateDB", crateDBSQLURL, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}

	var response struct {
		Results []struct {
			Rowcount int             `json:"rowcount"`
			Error    json.RawMessage `json:"error"`
		} `json:"results"`
	}
	if err = json.Unmarshal(body, &response); err != nil {
		return err
	}
	if len(response.Results) != len(events) {
		return fmt.Errorf("CrateDB incomplete bulk acknowledgement")
	}
	for _, r := range response.Results {
		if r.Rowcount != 1 || len(r.Error) > 0 {
			return fmt.Errorf("CrateDB bulk failure %s", body)
		}
	}
	return nil
}

// post sends an HTTP write and returns the response body, failing on non-2xx statuses.
func (s *Server) post(ctx context.Context, name, url, contentType string, payload io.Reader) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", url, payload)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s write %d: %s", name, resp.StatusCode, body)
	}
	return body, nil
}
