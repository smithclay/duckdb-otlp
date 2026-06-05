package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	collectorlogsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

const calibrationTolerance = 0.01

type Config struct {
	URL             string
	Token           string
	RunID           string
	Scenario        string
	Rate            int
	Duration        time.Duration
	BatchSize       int
	Concurrency     int
	QueueDepth      int
	Seed            uint64
	TargetGzipBPR   float64
	BodyPayloadSize int
	RequestTimeout  time.Duration
}

type Calibration struct {
	TargetGzipBytesPerRecord     float64 `json:"target_gzip_bytes_per_record"`
	CalibratedGzipBytesPerRecord float64 `json:"calibrated_gzip_bytes_per_record"`
	CalibrationErrorPercent      float64 `json:"calibration_error_percent"`
	BodyPayloadBytes             int     `json:"body_payload_bytes"`
	RecordsPerRequest            int     `json:"records_per_request"`
	ProtobufBatchBytes           int     `json:"protobuf_batch_bytes"`
	GzipBatchBytes               int     `json:"gzip_batch_bytes"`
	ProtobufBytesPerRecord       float64 `json:"protobuf_bytes_per_record"`
	CompressionRatio             float64 `json:"compression_ratio"`
	RandomSeed                   uint64  `json:"random_seed"`
	CalibrationIterations        int     `json:"calibration_iterations"`
}

type Result struct {
	SchemaVersion             int               `json:"schema_version"`
	RunID                     string            `json:"run_id"`
	Scenario                  string            `json:"scenario,omitempty"`
	StartedAt                 time.Time         `json:"started_at"`
	FinishedAt                time.Time         `json:"finished_at"`
	DurationSeconds           float64           `json:"duration_seconds"`
	ConfiguredRecordsPerSec   int               `json:"configured_offered_records_per_second"`
	ActualAttemptedPerSec     float64           `json:"actual_attempted_records_per_second"`
	AcceptedRecordsPerSec     float64           `json:"accepted_records_per_second"`
	RejectedRecordsPerSec     float64           `json:"rejected_records_per_second"`
	AttemptedRecords          uint64            `json:"attempted_records"`
	AcceptedRecords           uint64            `json:"accepted_records"`
	RejectedRecords           uint64            `json:"rejected_records"`
	FailedRecords             uint64            `json:"failed_records"`
	AmbiguousTransportRecords uint64            `json:"ambiguous_transport_failure_records"`
	SchedulerMissedRecords    uint64            `json:"scheduler_missed_records"`
	SchedulerMissedDeadlines  uint64            `json:"scheduler_missed_deadlines"`
	RequestsAttempted         uint64            `json:"requests_attempted"`
	ResponseStatusCounts      map[string]uint64 `json:"response_status_counts"`
	ResponseErrorSamples      map[string]string `json:"response_error_samples"`
	TransportErrorCounts      map[string]uint64 `json:"transport_error_counts"`
	ProtobufBytes             uint64            `json:"protobuf_bytes"`
	GzipBytes                 uint64            `json:"gzip_bytes"`
	AcceptedProtobufBytes     uint64            `json:"accepted_protobuf_bytes"`
	AcceptedGzipBytes         uint64            `json:"accepted_gzip_bytes"`
	ProtobufBytesPerSec       float64           `json:"protobuf_bytes_per_second"`
	GzipBytesPerSec           float64           `json:"gzip_bytes_per_second"`
	GzipGbps                  float64           `json:"gzip_gigabits_per_second"`
	GzipBytesPerRecord        float64           `json:"gzip_bytes_per_record"`
	LatencyMS                 Percentiles       `json:"request_latency_ms"`
	SchedulerLagMS            Percentiles       `json:"scheduler_lag_ms"`
	Calibration               Calibration       `json:"calibration"`
	GoMaxProcs                int               `json:"go_max_procs"`
	PeakHeapBytes             uint64            `json:"peak_heap_bytes"`
	Completed                 bool              `json:"completed"`
	StopReason                string            `json:"stop_reason,omitempty"`
}

type Percentiles struct {
	Count int     `json:"count"`
	Mean  float64 `json:"mean"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
}

type requestJob struct {
	sequence uint64
}

type counters struct {
	attempted             atomic.Uint64
	accepted              atomic.Uint64
	rejected              atomic.Uint64
	failed                atomic.Uint64
	ambiguous             atomic.Uint64
	missedRecords         atomic.Uint64
	missedDeadlines       atomic.Uint64
	requests              atomic.Uint64
	protobufBytes         atomic.Uint64
	gzipBytes             atomic.Uint64
	acceptedProtobufBytes atomic.Uint64
	acceptedGzipBytes     atomic.Uint64
	peakHeapBytes         atomic.Uint64
	mu                    sync.Mutex
	status                map[string]uint64
	responseErrors        map[string]string
	transportErrors       map[string]uint64
	latencies             []float64
	schedulerLag          []float64
}

func deterministicBody(seed, sequence uint64, size int) string {
	if size <= 0 {
		return ""
	}
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
	x := seed + sequence*0x9e3779b97f4a7c15 + 0xd1b54a32d192ed03
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x ^= x >> 31
	out := make([]byte, size)
	for i := range out {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		x *= 0x2545f4914f6cdd1d
		out[i] = alphabet[x&63]
	}
	return string(out)
}

func makeBatch(
	runID string,
	scenario string,
	firstSequence uint64,
	batchSize, bodySize int,
	seed uint64,
	generatedAt uint64,
) ([]byte, []byte, error) {
	records := make([]*logsv1.LogRecord, batchSize)
	for i := 0; i < batchSize; i++ {
		sequence := firstSequence + uint64(i)
		timestamp := generatedAt + uint64(i)
		records[i] = &logsv1.LogRecord{
			TimeUnixNano:         timestamp,
			ObservedTimeUnixNano: timestamp,
			SeverityNumber:       logsv1.SeverityNumber_SEVERITY_NUMBER_INFO,
			SeverityText:         "INFO",
			Body: &commonv1.AnyValue{
				Value: &commonv1.AnyValue_StringValue{StringValue: deterministicBody(seed, sequence, bodySize)},
			},
			Attributes: []*commonv1.KeyValue{
				stringAttribute("benchmark.run_id", runID),
				intAttribute("benchmark.sequence", int64(sequence)),
				intAttribute("benchmark.generated_at_unix_nano", int64(timestamp)),
				stringAttribute("benchmark.scenario", scenario),
			},
		}
	}
	payload := &collectorlogsv1.ExportLogsServiceRequest{
		ResourceLogs: []*logsv1.ResourceLogs{{
			Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{
				stringAttribute("service.name", "duckdb-otlp-benchmark"),
				stringAttribute("benchmark.scenario", scenario),
			}},
			ScopeLogs: []*logsv1.ScopeLogs{{
				Scope:      &commonv1.InstrumentationScope{Name: "duckdb-otlp-benchmark-producer", Version: "1"},
				LogRecords: records,
			}},
		}},
	}
	protobufBytes, err := proto.Marshal(payload)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal OTLP payload: %w", err)
	}
	var compressed bytes.Buffer
	writer, err := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
	if err != nil {
		return nil, nil, fmt.Errorf("create gzip writer: %w", err)
	}
	if _, err := writer.Write(protobufBytes); err != nil {
		return nil, nil, fmt.Errorf("gzip OTLP payload: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, nil, fmt.Errorf("finish gzip OTLP payload: %w", err)
	}
	return protobufBytes, compressed.Bytes(), nil
}

func stringAttribute(key, value string) *commonv1.KeyValue {
	return &commonv1.KeyValue{Key: key, Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: value}}}
}

func intAttribute(key string, value int64) *commonv1.KeyValue {
	return &commonv1.KeyValue{Key: key, Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: value}}}
}

func calibrate(cfg Config) (Calibration, error) {
	if cfg.BatchSize <= 0 {
		return Calibration{}, errors.New("batch size must be positive")
	}
	if cfg.TargetGzipBPR <= 0 {
		return Calibration{}, errors.New("target gzip bytes per record must be positive")
	}
	low, high := 0, 4096
	iterations := 0
	var best Calibration
	bestError := math.MaxFloat64
	bestBodySize := 0
	evaluate := func(bodySize int) (Calibration, float64, error) {
		iterations++
		var protobufSize, gzipSize int
		for _, firstSequence := range []uint64{0, uint64(cfg.BatchSize * 100), uint64(cfg.BatchSize * 5000)} {
			protobufBytes, gzipBytes, err := makeBatch(
				"calibration",
				"calibration",
				firstSequence,
				cfg.BatchSize,
				bodySize,
				cfg.Seed,
				1_700_000_000_000_000_000+firstSequence,
			)
			if err != nil {
				return Calibration{}, 0, err
			}
			protobufSize += len(protobufBytes)
			gzipSize += len(gzipBytes)
		}
		records := cfg.BatchSize * 3
		actual := float64(gzipSize) / float64(records)
		errorFraction := math.Abs(actual-cfg.TargetGzipBPR) / cfg.TargetGzipBPR
		return Calibration{
			TargetGzipBytesPerRecord:     cfg.TargetGzipBPR,
			CalibratedGzipBytesPerRecord: actual,
			CalibrationErrorPercent:      errorFraction * 100,
			BodyPayloadBytes:             bodySize,
			RecordsPerRequest:            cfg.BatchSize,
			ProtobufBatchBytes:           int(math.Round(float64(protobufSize) / 3)),
			GzipBatchBytes:               int(math.Round(float64(gzipSize) / 3)),
			ProtobufBytesPerRecord:       float64(protobufSize) / float64(records),
			CompressionRatio:             float64(gzipSize) / float64(protobufSize),
			RandomSeed:                   cfg.Seed,
		}, errorFraction, nil
	}
	for low <= high && iterations < 32 {
		bodySize := low + (high-low)/2
		candidate, errorFraction, err := evaluate(bodySize)
		if err != nil {
			return Calibration{}, err
		}
		if errorFraction < bestError {
			best, bestError = candidate, errorFraction
			bestBodySize = bodySize
		}
		if candidate.CalibratedGzipBytesPerRecord < cfg.TargetGzipBPR {
			low = bodySize + 1
		} else {
			high = bodySize - 1
		}
	}
	// Deflate block choices make compressed size locally non-monotonic. Search a
	// small neighborhood around the binary-search winner before enforcing tolerance.
	for bodySize := max(0, bestBodySize-24); bodySize <= min(4096, bestBodySize+24); bodySize++ {
		candidate, errorFraction, err := evaluate(bodySize)
		if err != nil {
			return Calibration{}, err
		}
		if errorFraction < bestError {
			best, bestError = candidate, errorFraction
		}
	}
	best.CalibrationIterations = iterations
	if bestError > calibrationTolerance {
		return best, fmt.Errorf("calibration did not converge within 1%%: target %.3f, got %.3f gzip bytes/record", cfg.TargetGzipBPR, best.CalibratedGzipBytesPerRecord)
	}
	return best, nil
}

func fixedBodyCalibration(cfg Config) (Calibration, error) {
	if cfg.BatchSize <= 0 {
		return Calibration{}, errors.New("batch size must be positive")
	}
	if cfg.BodyPayloadSize <= 0 {
		return Calibration{}, errors.New("body payload size must be positive")
	}
	var protobufSize, gzipSize int
	for _, firstSequence := range []uint64{0, uint64(cfg.BatchSize * 100), uint64(cfg.BatchSize * 5000)} {
		protobufBytes, gzipBytes, err := makeBatch(
			"fixed-body-calibration",
			cfg.Scenario,
			firstSequence,
			cfg.BatchSize,
			cfg.BodyPayloadSize,
			cfg.Seed,
			1_700_000_000_000_000_000+firstSequence,
		)
		if err != nil {
			return Calibration{}, err
		}
		protobufSize += len(protobufBytes)
		gzipSize += len(gzipBytes)
	}
	records := cfg.BatchSize * 3
	actual := float64(gzipSize) / float64(records)
	return Calibration{
		TargetGzipBytesPerRecord:     actual,
		CalibratedGzipBytesPerRecord: actual,
		BodyPayloadBytes:             cfg.BodyPayloadSize,
		RecordsPerRequest:            cfg.BatchSize,
		ProtobufBatchBytes:           int(math.Round(float64(protobufSize) / 3)),
		GzipBatchBytes:               int(math.Round(float64(gzipSize) / 3)),
		ProtobufBytesPerRecord:       float64(protobufSize) / float64(records),
		CompressionRatio:             float64(gzipSize) / float64(protobufSize),
		RandomSeed:                   cfg.Seed,
		CalibrationIterations:        3,
	}, nil
}

func runLoad(ctx context.Context, cfg Config, calibration Calibration) (Result, error) {
	if cfg.Concurrency < 1 {
		return Result{}, errors.New("concurrency must be positive")
	}
	if cfg.Rate <= 0 || cfg.Duration <= 0 || cfg.BatchSize <= 0 {
		return Result{}, errors.New("rate, duration, and batch size must be positive")
	}
	if cfg.QueueDepth <= 0 {
		cfg.QueueDepth = cfg.Concurrency * 2
	}
	if cfg.BodyPayloadSize <= 0 {
		cfg.BodyPayloadSize = calibration.BodyPayloadBytes
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          cfg.Concurrency,
		MaxIdleConnsPerHost:   cfg.Concurrency,
		MaxConnsPerHost:       cfg.Concurrency,
		IdleConnTimeout:       90 * time.Second,
		DisableCompression:    true,
		ForceAttemptHTTP2:     false,
		ResponseHeaderTimeout: cfg.RequestTimeout,
	}
	client := &http.Client{Transport: transport, Timeout: cfg.RequestTimeout}
	defer transport.CloseIdleConnections()

	stats := &counters{
		status: map[string]uint64{}, responseErrors: map[string]string{}, transportErrors: map[string]uint64{},
	}
	jobs := make(chan requestJob, cfg.QueueDepth)
	started := time.Now()
	var workers sync.WaitGroup
	for i := 0; i < cfg.Concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobs {
				if ctx.Err() != nil {
					return
				}
				sendBatch(ctx, client, cfg, calibration, job, stats)
			}
		}()
	}

	monitorDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-monitorDone:
				return
			case <-ticker.C:
				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)
				for {
					old := stats.peakHeapBytes.Load()
					if mem.HeapAlloc <= old || stats.peakHeapBytes.CompareAndSwap(old, mem.HeapAlloc) {
						break
					}
				}
			}
		}
	}()

	requestInterval := time.Duration(float64(time.Second) * float64(cfg.BatchSize) / float64(cfg.Rate))
	totalRequests := int(math.Ceil(cfg.Duration.Seconds() * float64(cfg.Rate) / float64(cfg.BatchSize)))
	nextDeadline := started
	var nextSequence uint64
	completedSchedule := true
	stopReason := ""
	for i := 0; i < totalRequests; i++ {
		if i > 0 {
			nextDeadline = nextDeadline.Add(requestInterval)
		}
		delay := time.Until(nextDeadline)
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				completedSchedule = false
				stopReason = ctx.Err().Error()
				i = totalRequests
				continue
			case <-timer.C:
			}
		}
		lag := time.Since(nextDeadline).Seconds() * 1000
		stats.mu.Lock()
		stats.schedulerLag = append(stats.schedulerLag, math.Max(0, lag))
		stats.mu.Unlock()
		job := requestJob{sequence: nextSequence}
		nextSequence += uint64(cfg.BatchSize)
		select {
		case jobs <- job:
		default:
			stats.missedDeadlines.Add(1)
			stats.missedRecords.Add(uint64(cfg.BatchSize))
		}
	}
	close(jobs)
	workers.Wait()
	if remaining := time.Until(started.Add(cfg.Duration)); remaining > 0 {
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()
			completedSchedule = false
			stopReason = ctx.Err().Error()
		case <-timer.C:
		}
	}
	close(monitorDone)
	finished := time.Now()
	elapsed := finished.Sub(started).Seconds()
	if stopReason == "" && ctx.Err() != nil {
		completedSchedule = false
		stopReason = ctx.Err().Error()
	}
	result := snapshotResult(cfg, calibration, stats, started, finished, elapsed)
	result.Completed = completedSchedule
	result.StopReason = stopReason
	return result, nil
}

func sendBatch(ctx context.Context, client *http.Client, cfg Config, calibration Calibration, job requestJob, stats *counters) {
	generatedAt := uint64(time.Now().UnixNano())
	protobufBytes, gzipBytes, err := makeBatch(
		cfg.RunID,
		cfg.Scenario,
		job.sequence,
		cfg.BatchSize,
		calibration.BodyPayloadBytes,
		cfg.Seed,
		generatedAt,
	)
	if err != nil {
		stats.failed.Add(uint64(cfg.BatchSize))
		stats.mu.Lock()
		stats.transportErrors["payload_generation"]++
		stats.mu.Unlock()
		return
	}
	stats.attempted.Add(uint64(cfg.BatchSize))
	stats.requests.Add(1)
	stats.protobufBytes.Add(uint64(len(protobufBytes)))
	stats.gzipBytes.Add(uint64(len(gzipBytes)))
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, bytes.NewReader(gzipBytes))
	if err != nil {
		stats.failed.Add(uint64(cfg.BatchSize))
		return
	}
	request.Header.Set("Authorization", "Bearer "+cfg.Token)
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set("Content-Encoding", "gzip")
	request.Header.Set("Connection", "keep-alive")
	start := time.Now()
	response, err := client.Do(request)
	latency := time.Since(start).Seconds() * 1000
	stats.mu.Lock()
	stats.latencies = append(stats.latencies, latency)
	stats.mu.Unlock()
	if err != nil {
		stats.failed.Add(uint64(cfg.BatchSize))
		stats.ambiguous.Add(uint64(cfg.BatchSize))
		stats.mu.Lock()
		stats.transportErrors[classifyTransportError(err)]++
		stats.mu.Unlock()
		return
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	status := fmt.Sprintf("%d", response.StatusCode)
	stats.mu.Lock()
	stats.status[status]++
	stats.mu.Unlock()
	if readErr != nil {
		stats.failed.Add(uint64(cfg.BatchSize))
		stats.ambiguous.Add(uint64(cfg.BatchSize))
		return
	}
	if response.StatusCode == http.StatusAccepted {
		var payload struct {
			Rows uint64 `json:"rows"`
		}
		if err := json.Unmarshal(body, &payload); err != nil || payload.Rows != uint64(cfg.BatchSize) {
			stats.failed.Add(uint64(cfg.BatchSize))
			stats.ambiguous.Add(uint64(cfg.BatchSize))
			return
		}
		stats.accepted.Add(payload.Rows)
		stats.acceptedProtobufBytes.Add(uint64(len(protobufBytes)))
		stats.acceptedGzipBytes.Add(uint64(len(gzipBytes)))
		return
	}
	stats.rejected.Add(uint64(cfg.BatchSize))
	stats.mu.Lock()
	if _, exists := stats.responseErrors[status]; !exists {
		stats.responseErrors[status] = string(body)
	}
	stats.mu.Unlock()
}

func classifyTransportError(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return "transport_error"
}

func snapshotResult(cfg Config, calibration Calibration, stats *counters, started, finished time.Time, elapsed float64) Result {
	stats.mu.Lock()
	status := cloneMap(stats.status)
	responseErrors := cloneStringMap(stats.responseErrors)
	transportErrors := cloneMap(stats.transportErrors)
	latencies := append([]float64(nil), stats.latencies...)
	lags := append([]float64(nil), stats.schedulerLag...)
	stats.mu.Unlock()
	attempted := stats.attempted.Load()
	accepted := stats.accepted.Load()
	rejected := stats.rejected.Load()
	protobufBytes := stats.protobufBytes.Load()
	gzipBytes := stats.gzipBytes.Load()
	return Result{
		SchemaVersion:             1,
		RunID:                     cfg.RunID,
		Scenario:                  cfg.Scenario,
		StartedAt:                 started.UTC(),
		FinishedAt:                finished.UTC(),
		DurationSeconds:           elapsed,
		ConfiguredRecordsPerSec:   cfg.Rate,
		ActualAttemptedPerSec:     divide(float64(attempted), elapsed),
		AcceptedRecordsPerSec:     divide(float64(accepted), elapsed),
		RejectedRecordsPerSec:     divide(float64(rejected), elapsed),
		AttemptedRecords:          attempted,
		AcceptedRecords:           accepted,
		RejectedRecords:           rejected,
		FailedRecords:             stats.failed.Load(),
		AmbiguousTransportRecords: stats.ambiguous.Load(),
		SchedulerMissedRecords:    stats.missedRecords.Load(),
		SchedulerMissedDeadlines:  stats.missedDeadlines.Load(),
		RequestsAttempted:         stats.requests.Load(),
		ResponseStatusCounts:      status,
		ResponseErrorSamples:      responseErrors,
		TransportErrorCounts:      transportErrors,
		ProtobufBytes:             protobufBytes,
		GzipBytes:                 gzipBytes,
		AcceptedProtobufBytes:     stats.acceptedProtobufBytes.Load(),
		AcceptedGzipBytes:         stats.acceptedGzipBytes.Load(),
		ProtobufBytesPerSec:       divide(float64(protobufBytes), elapsed),
		GzipBytesPerSec:           divide(float64(gzipBytes), elapsed),
		GzipGbps:                  divide(float64(gzipBytes)*8, elapsed*1e9),
		GzipBytesPerRecord:        divide(float64(gzipBytes), float64(attempted)),
		LatencyMS:                 percentiles(latencies),
		SchedulerLagMS:            percentiles(lags),
		Calibration:               calibration,
		GoMaxProcs:                runtime.GOMAXPROCS(0),
		PeakHeapBytes:             stats.peakHeapBytes.Load(),
	}
}

func cloneStringMap(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneMap(input map[string]uint64) map[string]uint64 {
	output := make(map[string]uint64, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func divide(numerator, denominator float64) float64 {
	if denominator == 0 {
		return 0
	}
	return numerator / denominator
}

func percentiles(values []float64) Percentiles {
	if len(values) == 0 {
		return Percentiles{}
	}
	sort.Float64s(values)
	var total float64
	for _, value := range values {
		total += value
	}
	at := func(p float64) float64 {
		index := int(math.Ceil(p*float64(len(values)))) - 1
		if index < 0 {
			index = 0
		}
		if index >= len(values) {
			index = len(values) - 1
		}
		return values[index]
	}
	return Percentiles{
		Count: len(values),
		Mean:  total / float64(len(values)),
		P50:   at(0.50),
		P95:   at(0.95),
		P99:   at(0.99),
		Max:   values[len(values)-1],
	}
}
