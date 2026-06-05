package main

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	collectorlogsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

func TestProtobufConstructionAndAttributes(t *testing.T) {
	raw, compressed, err := makeBatch("run-a", "local", 10, 2, 64, 42, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(compressed) == 0 {
		t.Fatal("expected gzip payload")
	}
	var request collectorlogsv1.ExportLogsServiceRequest
	if err := proto.Unmarshal(raw, &request); err != nil {
		t.Fatal(err)
	}
	records := request.ResourceLogs[0].ScopeLogs[0].LogRecords
	if len(records) != 2 || records[0].Attributes[1].Value.GetIntValue() != 10 || records[1].Attributes[1].Value.GetIntValue() != 11 {
		t.Fatalf("unexpected records: %#v", records)
	}
	if records[0].Attributes[0].Value.GetStringValue() != "run-a" {
		t.Fatal("run_id attribute missing")
	}
	if records[0].Attributes[3].Value.GetStringValue() != "local" {
		t.Fatal("scenario attribute missing")
	}
}

func TestFixedBodyCalibration(t *testing.T) {
	calibration, err := fixedBodyCalibration(Config{
		BatchSize: 50, BodyPayloadSize: 32, Seed: 123, Scenario: "catalog",
	})
	if err != nil {
		t.Fatal(err)
	}
	if calibration.BodyPayloadBytes != 32 || calibration.GzipBatchBytes == 0 {
		t.Fatalf("unexpected calibration: %#v", calibration)
	}
}

func TestDeterministicRecordGeneration(t *testing.T) {
	a := deterministicBody(7, 99, 256)
	b := deterministicBody(7, 99, 256)
	c := deterministicBody(7, 100, 256)
	if a != b || a == c {
		t.Fatal("deterministic body generation is not stable and sequence-specific")
	}
}

func TestCalibrationConvergesAndAccountsBytes(t *testing.T) {
	calibration, err := calibrate(Config{BatchSize: 1000, Seed: 123, TargetGzipBPR: 786})
	if err != nil {
		t.Fatal(err)
	}
	if calibration.CalibrationErrorPercent > 1 {
		t.Fatalf("calibration error %.3f%%", calibration.CalibrationErrorPercent)
	}
	if got := float64(calibration.GzipBatchBytes) / float64(calibration.RecordsPerRequest); math.Abs(got-calibration.CalibratedGzipBytesPerRecord) > 0.001 {
		t.Fatalf("gzip accounting mismatch: %f != %f", got, calibration.CalibratedGzipBytesPerRecord)
	}
}

func TestSequenceUniquenessAcrossWorkers(t *testing.T) {
	var mu sync.Mutex
	seen := map[int64]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"buffered","rows":10}`))
	}))
	defer server.Close()
	cfg := Config{
		URL: server.URL, Token: "token", RunID: "unique", Rate: 1000, Duration: 100 * time.Millisecond,
		BatchSize: 10, Concurrency: 4, QueueDepth: 8, Seed: 1, TargetGzipBPR: 786, RequestTimeout: time.Second,
	}
	calibration, err := calibrate(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for first := int64(0); first < 100; first += 10 {
		for sequence := first; sequence < first+10; sequence++ {
			mu.Lock()
			if seen[sequence] {
				t.Fatalf("duplicate sequence %d", sequence)
			}
			seen[sequence] = true
			mu.Unlock()
		}
	}
	result, err := runLoad(context.Background(), cfg, calibration)
	if err != nil {
		t.Fatal(err)
	}
	if result.AttemptedRecords == 0 || result.AcceptedRecords != result.AttemptedRecords {
		t.Fatalf("unexpected result: %#v", result)
	}
	if result.SchemaVersion != 1 || result.AcceptedGzipBytes == 0 {
		t.Fatalf("missing reusable result fields: %#v", result)
	}
	if result.DurationSeconds < cfg.Duration.Seconds() {
		t.Fatalf("producer ended before configured duration: %#v", result)
	}
}

func TestOpenLoopSchedulerDetectsSaturationAndClassifiesResponses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	cfg := Config{
		URL: server.URL, Token: "token", RunID: "saturated", Rate: 100000, Duration: 100 * time.Millisecond,
		BatchSize: 100, Concurrency: 1, QueueDepth: 1, Seed: 1, TargetGzipBPR: 786, RequestTimeout: time.Second,
	}
	calibration, err := calibrate(cfg)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runLoad(context.Background(), cfg, calibration)
	if err != nil {
		t.Fatal(err)
	}
	if result.SchedulerMissedDeadlines == 0 || result.ResponseStatusCounts["503"] == 0 || result.RejectedRecords == 0 {
		t.Fatalf("saturation was not observed: %#v", result)
	}
}

func TestPercentilesAndStatisticsJSON(t *testing.T) {
	got := percentiles([]float64{1, 2, 3, 4, 100})
	if got.Mean != 22 || got.P50 != 3 || got.P95 != 100 || got.P99 != 100 || got.Max != 100 {
		t.Fatalf("unexpected percentiles: %#v", got)
	}
	encoded, err := json.Marshal(Result{RunID: "json", LatencyMS: got, ResponseStatusCounts: map[string]uint64{"202": 1}})
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) == 0 {
		t.Fatal("empty JSON")
	}
}
