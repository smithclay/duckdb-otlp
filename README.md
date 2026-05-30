# DuckDB OpenTelemetry (OTLP) Extension

Query OpenTelemetry traces, logs, and metrics with SQL, or run an embedded OTLP/HTTP endpoint that streams live telemetry into DuckDB or DuckLake.

The extension reads OTLP JSON, JSONL, and protobuf file exports from the OpenTelemetry Collector, with row schemas inspired by the [OpenTelemetry ClickHouse exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/clickhouseexporter/README.md). Native builds also include live HTTP ingest for `/v1/logs`, `/v1/traces`, and `/v1/metrics`.

## Quickstart

Install and load the extension:

```sql
INSTALL otlp FROM community;
LOAD otlp;
```

Read OTLP data from files:

```sql
SELECT timestamp, service_name, severity_text, body
FROM read_otlp_logs('test/data/logs_simple.jsonl');

SELECT trace_id, span_name, duration / 1000000 AS duration_ms
FROM read_otlp_traces('test/data/traces_simple.jsonl')
ORDER BY duration DESC;
```

Stream one OTLP log into DuckDB over HTTP:

```sql
SELECT listen_url
FROM otlp_serve('otlp:localhost:4318', token := 'dev-token-123456');
```

```bash
curl -sS http://localhost:4318/v1/logs -H 'Authorization: Bearer dev-token-123456' -H 'Content-Type: application/json' -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"curl-demo"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1704067200000000000","severityText":"INFO","body":{"stringValue":"hello from curl"}}]}]}]}'
```

```sql
SELECT * FROM otlp_flush('otlp:localhost:4318');
SELECT timestamp, service_name, severity_text, body FROM otlp_logs;
SELECT status FROM otlp_stop('otlp:localhost:4318');
```

For a full walkthrough, including streaming into DuckLake/Parquet, see [Get Started](docs/get-started.md) and the [Live Ingest Quickstart](docs/quickstart/serve.md).

## What You Can Do

- Read OTLP traces, logs, gauges, sums/counters, histograms, and exponential histograms from files.
- Stream live OTLP/HTTP exports into the default DuckDB catalog or an attached [DuckLake](https://ducklake.select) lakehouse.
- Convert telemetry exports to Parquet with DuckDB `COPY`.
- Query local files, globs, S3, HTTP(S), Azure Blob, and GCS paths through DuckDB file systems.
- Use the browser demo for JSON-only exploration with DuckDB-WASM: [Interactive Demo](https://smithclay.github.io/duckdb-otlp/).

## Documentation

- [Documentation Hub](docs/README.md)
- [Get Started](docs/get-started.md)
- [Live Ingest Quickstart](docs/quickstart/serve.md)
- [Cookbook](docs/guides/cookbook.md)
- [API Reference](docs/reference/api.md)
- [Schema Reference](docs/reference/schemas.md)
- [Live Ingest Reference](docs/reference/serve.md)
- [Architecture](docs/architecture.md)

## API at a Glance

| Function | What it does |
| --- | --- |
| `read_otlp_traces(path)` | Read trace spans with identifiers, attributes, events, links, and duration |
| `read_otlp_logs(path)` | Read log records with severity, body, attributes, and trace correlation |
| `read_otlp_metrics_gauge(path)` | Read gauge metrics |
| `read_otlp_metrics_sum(path)` | Read sum/counter metrics |
| `read_otlp_metrics_histogram(path)` | Read standard histogram metrics |
| `read_otlp_metrics_exp_histogram(path)` | Read exponential histogram metrics |
| `otlp_serve([uri], ...)` | Start a native OTLP/HTTP ingest server |
| `otlp_flush(uri)` | Force buffered ingest rows to seal into the target catalog |
| `otlp_stop(uri)` | Stop a server after sealing remaining rows |
| `otlp_server_list()` | Inspect running servers and ingest counters |

`read_otlp_metrics` and `read_otlp_metrics_summary` are registered but intentionally unsupported until the extension has stable schemas for those shapes. See the [API Reference](docs/reference/api.md) for details.

## Installation

```sql
INSTALL otlp FROM community;
LOAD otlp;
```

For source builds, development commands, and WASM builds, see [CONTRIBUTING.md](CONTRIBUTING.md). WASM supports JSON/JSONL file reads, but not protobuf or the live ingest server.

## Limits

Individual file reads are limited to **100 MB** to prevent memory exhaustion. Live ingest accepts request bodies up to `max_body_bytes` and applies buffered-ingest backpressure through `max_buffered_bytes`; see the [Live Ingest Reference](docs/reference/serve.md#responses-and-status-codes).

## Need Help?

- [GitHub Discussions](https://github.com/smithclay/duckdb-otlp/discussions)
- [Open an issue](https://github.com/smithclay/duckdb-otlp/issues)
- [Contributing guide](CONTRIBUTING.md)

## License

MIT. See [LICENSE](LICENSE) for details.
