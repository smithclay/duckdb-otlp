# OTLP log ingest producer

`producer/` is a Go OTLP/HTTP protobuf load generator: it builds gzip-compressed
log batches, paces them at a target rate across a bounded pool of request
workers, and prints producer-side metrics (accepted/rejected records, request
latency percentiles) as JSON.

It is the workload generator for `scripts/benchmark_catalog_ingest.py`, which
starts the daemon image in `local-ducklake` mode, drives this producer against
it, samples Docker and `otlp_server_list()` counters over Quack, and writes a
JSON/Markdown report:

```bash
python3 scripts/benchmark_catalog_ingest.py \
  --image duckdb-otlp:daemon --duration 60 --rate 50000 \
  --output-dir output/catalog-benchmarks
```

The harness builds the producer itself (`go build`), or reuses one passed with
`--producer-bin`. Run the producer's own tests with `go test ./...` from
`producer/`.
