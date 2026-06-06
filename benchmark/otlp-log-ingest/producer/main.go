package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	var cfg Config
	var output string
	var mode string
	var duration time.Duration
	flag.StringVar(&mode, "mode", "run", "calibrate or run")
	flag.StringVar(&cfg.URL, "url", "http://127.0.0.1:4318/v1/logs", "OTLP logs endpoint")
	flag.StringVar(&cfg.Token, "token", "", "bearer token")
	flag.StringVar(&cfg.RunID, "run-id", "", "benchmark run identifier")
	flag.StringVar(&cfg.Scenario, "scenario", "", "benchmark scenario recorded in generated attributes")
	flag.IntVar(&cfg.Rate, "rate", 175000, "offered records per second")
	flag.DurationVar(&duration, "duration", 180*time.Second, "load duration")
	flag.IntVar(&cfg.BatchSize, "batch-size", 1000, "records per request")
	flag.IntVar(&cfg.Concurrency, "concurrency", 4, "active request workers")
	flag.IntVar(&cfg.QueueDepth, "queue-depth", 8, "bounded request queue depth")
	flag.Uint64Var(&cfg.Seed, "seed", 20260605, "deterministic random seed")
	flag.Float64Var(&cfg.TargetGzipBPR, "target-gzip-bytes-per-record", 786, "calibration target")
	flag.IntVar(&cfg.BodyPayloadSize, "body-payload-bytes", 0, "calibrated body payload size")
	flag.DurationVar(&cfg.RequestTimeout, "request-timeout", 30*time.Second, "per-request timeout")
	flag.StringVar(&output, "output", "", "JSON output path (stdout when empty)")
	flag.Parse()
	cfg.Duration = duration

	var calibration Calibration
	var err error
	if cfg.BodyPayloadSize > 0 {
		calibration, err = fixedBodyCalibration(cfg)
	} else {
		calibration, err = calibrate(cfg)
	}
	if err != nil {
		writeFailure(output, err, calibration)
		os.Exit(1)
	}
	if mode == "calibrate" {
		writeJSON(output, calibration)
		return
	}
	if mode != "run" {
		fmt.Fprintf(os.Stderr, "unsupported mode %q\n", mode)
		os.Exit(2)
	}
	if cfg.Token == "" || cfg.RunID == "" {
		fmt.Fprintln(os.Stderr, "--token and --run-id are required in run mode")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	result, err := runLoad(ctx, cfg, calibration)
	if err != nil {
		writeFailure(output, err, calibration)
		os.Exit(1)
	}
	writeJSON(output, result)
	if !result.Completed {
		os.Exit(3)
	}
}

func writeFailure(output string, err error, calibration Calibration) {
	writeJSON(output, map[string]any{"error": err.Error(), "calibration": calibration})
}

func writeJSON(output string, value any) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "encode JSON: %v\n", err)
		os.Exit(1)
	}
	data = append(data, '\n')
	if output == "" {
		_, _ = os.Stdout.Write(data)
		return
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "create output directory: %v\n", err)
		os.Exit(1)
	}
	tmp := output + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write JSON: %v\n", err)
		os.Exit(1)
	}
	if err := os.Rename(tmp, output); err != nil {
		fmt.Fprintf(os.Stderr, "commit JSON: %v\n", err)
		os.Exit(1)
	}
}
