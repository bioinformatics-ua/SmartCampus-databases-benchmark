package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	bufferSize    = 1 << 20
	maxIdentifier = 65535

	// Accepted source timestamps: 2000-01-01 to 2100-01-01 UTC.
	minTimestamp = 946684800
	maxTimestamp = 4102444800
)

var distributions = []string{"base", "hotspot", "fragmented"}

func userHash(user string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(user))
	return h.Sum64()
}

// inSmallPopulation selects roughly 10% of users for the S level.
func inSmallPopulation(user string) bool {
	return userHash(user)%10 == 0
}

// ------------------------------------------------------------- binary format
//
// Each record is: u16 user length, user bytes, u16 SSID length, SSID bytes,
// i64 timestamp, f64 RSSI, all little-endian. datasets.validation_sample in
// Python reads the same layout.

func writeEvent(w io.Writer, e Event) error {
	for _, s := range []string{e.User, e.SSID} {
		if len(s) > maxIdentifier {
			return fmt.Errorf("identifier too long")
		}
		if err := binary.Write(w, binary.LittleEndian, uint16(len(s))); err != nil {
			return err
		}
		if _, err := io.WriteString(w, s); err != nil {
			return err
		}
	}
	if err := binary.Write(w, binary.LittleEndian, e.TS); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, e.RSSI)
}

func readString(r io.Reader) (string, error) {
	var length uint16
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return "", err
	}
	b := make([]byte, length)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

func readEvent(r io.Reader) (Event, error) {
	e := Event{}
	user, err := readString(r)
	if err != nil {
		return e, err
	}
	ssid, err := readString(r)
	if err != nil {
		return e, err
	}
	e.User = user
	e.SSID = ssid
	if err := binary.Read(r, binary.LittleEndian, &e.TS); err != nil {
		return e, err
	}
	err = binary.Read(r, binary.LittleEndian, &e.RSSI)
	return e, err
}

// transform applies a synthetic distribution to one source event.
func transform(e Event, dist string) Event {
	switch dist {
	case "hotspot":
		// About 80% of users are moved onto a single access point.
		if userHash(e.User)%10 < 8 {
			e.SSID = "synthetic-hotspot"
		}
	case "fragmented":
		// Each user appears as four identities that rotate every minute.
		e.User = fmt.Sprintf("%s#%d", e.User, (uint64(e.TS)/60)%4)
	}
	return e
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	_, err = io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), err
}

// ---------------------------------------------------------------- preparation

type DataManifest struct {
	Version      int               `json:"version"`
	Count        int64             `json:"count"`
	SmallCount   int64             `json:"small_count"`
	Min          int64             `json:"min"`
	Max          int64             `json:"max"`
	Users        int               `json:"users"`
	SSIDCounts   map[string]int64  `json:"ssid_counts"`
	HourCounts   map[string]int64  `json:"hour_counts"`
	Sources      map[string]string `json:"sources"`
	SHA          string            `json:"sha256"`
	Distribution string            `json:"distribution"`
	RSSIMin      float64           `json:"rssi_min"`
	RSSIMax      float64           `json:"rssi_max"`
}

// datasetWriter streams one distribution to <dist>.bin and its event keys to
// <dist>.keys, both as .partial files until finish renames them.
type datasetWriter struct {
	dist                 string
	binFile, keyFile     *os.File
	binWriter, keyWriter *bufio.Writer
	manifest             DataManifest
	users                map[string]bool
}

func newDatasetWriter(output, dist string, sources map[string]string) (*datasetWriter, error) {
	binFile, err := os.Create(filepath.Join(output, dist+".bin.partial"))
	if err != nil {
		return nil, err
	}
	keyFile, err := os.Create(filepath.Join(output, dist+".keys.partial"))
	if err != nil {
		binFile.Close()
		return nil, err
	}
	return &datasetWriter{
		dist:      dist,
		binFile:   binFile,
		keyFile:   keyFile,
		binWriter: bufio.NewWriterSize(binFile, bufferSize),
		keyWriter: bufio.NewWriterSize(keyFile, bufferSize),
		users:     map[string]bool{},
		manifest: DataManifest{
			Version:      1,
			Min:          math.MaxInt64,
			Max:          math.MinInt64,
			RSSIMin:      math.Inf(1),
			RSSIMax:      math.Inf(-1),
			Sources:      sources,
			Distribution: dist,
			SSIDCounts:   map[string]int64{},
			HourCounts:   map[string]int64{},
		},
	}, nil
}

func (d *datasetWriter) add(source Event) error {
	e := transform(source, d.dist)
	if err := writeEvent(d.binWriter, e); err != nil {
		return err
	}
	// Keys are checked for duplicates by an external sort in datasets.py.
	key := sha256.Sum256([]byte(fmt.Sprintf("%q\x00%q\x00%d", e.User, e.SSID, e.TS)))
	if _, err := fmt.Fprintf(d.keyWriter, "%x\n", key); err != nil {
		return err
	}

	m := &d.manifest
	m.Count++
	if inSmallPopulation(e.User) {
		m.SmallCount++
	}
	d.users[e.User] = true
	m.SSIDCounts[e.SSID]++
	m.HourCounts[strconv.FormatInt(e.TS/3600*3600, 10)]++
	m.Min = min(m.Min, e.TS)
	m.Max = max(m.Max, e.TS)
	if e.RSSI < m.RSSIMin {
		m.RSSIMin = e.RSSI
	}
	if e.RSSI > m.RSSIMax {
		m.RSSIMax = e.RSSI
	}
	return nil
}

func (d *datasetWriter) close() {
	d.binFile.Close()
	d.keyFile.Close()
}

// finish flushes both files, renames them into place, and writes <dist>.json.
func (d *datasetWriter) finish(output string) error {
	if err := d.binWriter.Flush(); err != nil {
		return err
	}
	if err := d.keyWriter.Flush(); err != nil {
		return err
	}
	if err := d.binFile.Sync(); err != nil {
		return err
	}
	d.close()

	bin := filepath.Join(output, d.dist+".bin")
	if err := os.Rename(bin+".partial", bin); err != nil {
		return err
	}
	keys := filepath.Join(output, d.dist+".keys")
	if err := os.Rename(keys+".partial", keys); err != nil {
		return err
	}

	sha, err := hashFile(bin)
	if err != nil {
		return err
	}
	d.manifest.SHA = sha
	d.manifest.Users = len(d.users)
	b, _ := json.MarshalIndent(d.manifest, "", "  ")
	return os.WriteFile(filepath.Join(output, d.dist+".json"), b, 0644)
}

// chunkPaths returns readings_0.json, readings_1.json, ... and rejects gaps.
func chunkPaths(input string) ([]string, error) {
	paths, err := filepath.Glob(filepath.Join(input, "readings_*.json"))
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no readings_*.json in %s", input)
	}
	number := func(p string) int {
		n, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(filepath.Base(p), "readings_"), ".json"))
		return n
	}
	sort.Slice(paths, func(i, j int) bool { return number(paths[i]) < number(paths[j]) })
	for i, p := range paths {
		if number(p) != i {
			return nil, fmt.Errorf("missing/noncontiguous chunk at %d", i)
		}
	}
	return paths, nil
}

func validEvent(e Event) bool {
	validIdentifier := e.User != "" && e.SSID != "" &&
		!strings.ContainsAny(e.User+e.SSID, "\n\r\x00")
	validTimestamp := e.TS >= minTimestamp && e.TS <= maxTimestamp
	validRSSI := !math.IsNaN(e.RSSI) && !math.IsInf(e.RSSI, 0)
	return validIdentifier && validTimestamp && validRSSI
}

// sourceReading is one element of a chunk's "response" array.
type sourceReading struct {
	User       string `json:"userId"`
	TS         int64  `json:"lastUpdatedTime"`
	Connection struct {
		SSID string   `json:"ssid"`
		RSSI *float64 `json:"rssi"`
	} `json:"connection"`
}

// readChunk streams {"response": [reading, ...], ...} without loading the whole
// file. It stops early once done() reports that the record limit is reached.
func readChunk(path string, done func() bool, consume func(Event) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(bufio.NewReaderSize(f, bufferSize))

	if tok, err := d.Token(); err != nil || tok != json.Delim('{') {
		return fmt.Errorf("%s must contain object with response array", path)
	}
	found := false
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return err
		}
		if key != "response" {
			var discard json.RawMessage
			if err := d.Decode(&discard); err != nil {
				return err
			}
			continue
		}

		found = true
		if tok, err := d.Token(); err != nil || tok != json.Delim('[') {
			return fmt.Errorf("response is not array")
		}
		for d.More() && !done() {
			var raw sourceReading
			if err := d.Decode(&raw); err != nil {
				return err
			}
			if raw.Connection.RSSI == nil {
				return fmt.Errorf("missing rssi")
			}
			event := Event{raw.User, raw.Connection.SSID, raw.TS, *raw.Connection.RSSI}
			if err := consume(event); err != nil {
				return err
			}
		}
		if done() {
			break
		}
		if _, err := d.Token(); err != nil { // closing ]
			return err
		}
	}

	// A partially read chunk cannot be checked for trailing content.
	if !done() {
		if _, err := d.Token(); err != nil { // closing }
			return err
		}
		var extra any
		if err := d.Decode(&extra); err != io.EOF {
			return fmt.Errorf("trailing/invalid JSON in %s", path)
		}
	}
	if !found {
		return fmt.Errorf("response missing in %s", path)
	}
	return nil
}

// prepare converts raw JSON chunks into the three binary distributions, with
// per-distribution manifests and key files. limit > 0 caps the source records.
func prepare(input, output string, limit int64) error {
	if err := os.MkdirAll(output, 0755); err != nil {
		return err
	}
	paths, err := chunkPaths(input)
	if err != nil {
		return err
	}
	sources := map[string]string{}
	for _, p := range paths {
		h, err := hashFile(p)
		if err != nil {
			return err
		}
		sources[filepath.Base(p)] = h
	}

	writers := make([]*datasetWriter, 0, len(distributions))
	defer func() {
		for _, w := range writers {
			w.close()
		}
	}()
	for _, dist := range distributions {
		w, err := newDatasetWriter(output, dist, sources)
		if err != nil {
			return err
		}
		writers = append(writers, w)
	}

	var total int64
	done := func() bool { return limit > 0 && total >= limit }
	consume := func(e Event) error {
		if !validEvent(e) {
			return fmt.Errorf("invalid event at index %d: %+v", total, e)
		}
		// One canonical numeric representation for every engine.
		e.RSSI = float64(float32(e.RSSI))
		for _, w := range writers {
			if err := w.add(e); err != nil {
				return err
			}
		}
		total++
		return nil
	}

	for _, p := range paths {
		if done() {
			break
		}
		if err := readChunk(p, done, consume); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "prepared %d records after %s\n", total, filepath.Base(p))
	}
	if total == 0 {
		return fmt.Errorf("empty source dataset")
	}

	for _, w := range writers {
		if err := w.finish(output); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------- load

// batchLogger appends one JSON line per bulk-load batch.
type batchLogger struct {
	encoder *json.Encoder
}

func (l *batchLogger) record(rows int, total int64, started time.Time, err error) error {
	if l == nil {
		return nil
	}
	status, message := "ok", ""
	if err != nil {
		status, message = "error", err.Error()
	}
	return l.encoder.Encode(map[string]any{
		"batch_rows":         rows,
		"acknowledged_total": total,
		"duration_us":        time.Since(started).Microseconds(),
		"status":             status,
		"error":              message,
		"at":                 time.Now().UTC(),
	})
}

// load bulk-inserts a prepared .bin file at population level S, B or L.
// S keeps the small-population users, L inserts two copies with distinct user
// prefixes, and Shift moves every timestamp by a fixed number of seconds.
func (s *Server) load(ctx context.Context, req Request) (int64, error) {
	if req.Batch <= 0 {
		req.Batch = 10000
	}
	f, err := os.Open(req.Path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var logger *batchLogger
	if req.Log != "" {
		logfile, err := os.OpenFile(req.Log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return 0, err
		}
		defer logfile.Close()
		logger = &batchLogger{json.NewEncoder(logfile)}
	}

	copies := 1
	if req.Level == "L" {
		copies = 2
	}

	var total int64
	batch := make([]Event, 0, req.Batch)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		started := time.Now()
		err := s.write(ctx, batch)
		if err == nil {
			total += int64(len(batch))
		}
		if logErr := logger.record(len(batch), total, started, err); logErr != nil {
			return logErr
		}
		batch = batch[:0]
		return err
	}

	for copyID := 0; copyID < copies; copyID++ {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return total, err
		}
		r := bufio.NewReaderSize(f, bufferSize)
		for {
			if err := ctx.Err(); err != nil {
				return total, err
			}
			e, err := readEvent(r)
			if err == io.EOF {
				break
			}
			if err != nil {
				return total, err
			}
			if req.Level == "S" && !inSmallPopulation(e.User) {
				continue
			}
			if req.Level == "L" {
				e.User = fmt.Sprintf("replica%d:%s", copyID, e.User)
			}
			e.TS += req.Shift
			batch = append(batch, e)

			if len(batch) == req.Batch {
				if err := flush(); err != nil {
					return total, err
				}
			}
			if req.Limit > 0 && total+int64(len(batch)) >= req.Limit {
				if err := flush(); err != nil {
					return total, err
				}
				return total, nil
			}
		}
		if err := flush(); err != nil {
			return total, err
		}
	}
	return total, nil
}
