# Local Mac Studio capacity benchmark

This benchmark drives the native `duckdb-otlp-server` image with a compiled Go
OTLP/HTTP protobuf producer. It calibrates complete gzip-compressed requests to
approximately 786 bytes per log record, then runs:

1. 15 seconds at 10,000 records/s.
2. 60 seconds of warm-up at 175,000 records/s.
3. 180 measured seconds at 175,000 records/s.

The consumer is pinned to the host CPU architecture and limited to 4 CPUs and
8 GiB RAM. The producer uses four request workers and a bounded eight-request
queue.

```bash
uv run --with pytest --with black python benchmark/otlp-log-ingest/run_local.py plan
uv run --with pytest --with black pytest benchmark/otlp-log-ingest/test_run_local.py
uv run python benchmark/otlp-log-ingest/run_local.py smoke
uv run python benchmark/otlp-log-ingest/run_local.py run
uv run python benchmark/otlp-log-ingest/run_local.py status
uv run python benchmark/otlp-log-ingest/run_local.py cleanup
```

All generated data lives under `benchmark/otlp-log-ingest/output/<run-id>`.
Warm-up and smoke storage are deleted before the next phase. Unless
`--keep-data` is passed, measured DuckLake data is also removed after storage
statistics and reports are written.

This is a shared-host local capacity gate. OrbStack localhost networking is not
isolated EC2 networking, local disk is not S3, and the compressed-size
calibration is only a proxy for the unpublished source workload.

The Go module in `producer/` is also the canonical workload generator for
`scripts/benchmark_catalog_ingest.py`. The catalog harness uses a fixed small
record body while this capacity suite uses compressed-size calibration. Python
owns provisioning, observation, reconciliation, and reporting; Go owns OTLP
payload generation, pacing, HTTP transport, and producer-side metrics.
