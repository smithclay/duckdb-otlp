# DuckDB OpenTelemetry Extension

DuckDB extension for querying and storing OpenTelemetry traces, logs, and metrics with SQL.

The embedded HTTP server streams live telemetry, including traces from [Claude Code or Codex](https://smithclay.github.io/duckdb-otlp/guides/store-agent-traces-local-ducklake/), into local DuckDB files, [DuckLake](https://smithclay.github.io/duckdb-otlp/guides/stream-to-ducklake/), or Iceberg catalogs such as [Amazon S3 Tables](https://smithclay.github.io/duckdb-otlp/guides/stream-to-s3-tables/) and [Cloudflare R2 Data Catalog](https://smithclay.github.io/duckdb-otlp/guides/stream-to-r2-data-catalog/).

## Quickstart

Install and load the extension in a `duckdb` v1.5.3 or higher:

```sql
INSTALL otlp FROM community;
LOAD otlp;
```

Read OTLP protobuf/JSON data from public URLs, local files, or object storage buckets:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT timestamp, service_name, severity_text, body FROM read_otlp_logs('https://github.com/smithclay/duckdb-otlp/raw/refs/heads/main/test/data/otlp_logs.pb');

SELECT trace_id, span_name, duration / 1000000 AS duration_ms FROM read_otlp_traces('https://github.com/smithclay/duckdb-otlp/raw/refs/heads/main/test/data/otlp_traces.pb') ORDER BY duration DESC;
```

You can start a server that accepts OpenTelemetry data from instrumented code, AI agents such as [Claude Code or Codex](https://smithclay.github.io/duckdb-otlp/guides/store-agent-traces-local-ducklake/), or OpenTelemetry Collectors:

```sql
-- Start the server
otlp_serve('otlp:localhost:4318', token := 'dev-token-123456');
```

Send one hello-world log in OTLP/HTTP format with cURL:

```bash
curl -sS http://localhost:4318/v1/logs -H 'Authorization: Bearer dev-token-123456' -H 'Content-Type: application/json' -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"curl-demo"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1704067200000000000","severityText":"INFO","body":{"stringValue":"hello from curl"}}]}]}]}'
```

Back in DuckDB:

```sql
-- Stop server, flush all buffered data.
SELECT status FROM otlp_stop('otlp:localhost:4318');
SELECT timestamp, service_name, severity_text, body FROM otlp_logs;
```

Live ingest commits buffered rows in the background after about 5 seconds for the oldest buffered row or about 64 MiB of admitted request-body bytes. Use `otlp_flush` when readers need accepted rows durable and queryable while the server keeps running.

For a full walkthrough, including lakehouse ingest, see the [documentation site](https://smithclay.github.io/duckdb-otlp/).

## Schema

The schemas align with a normalized version of the [OpenTelemetry Arrow Data model](https://github.com/open-telemetry/otel-arrow/blob/main/docs/data_model.md) as of extension release `v0.5.0`. Release `v0.5.0` includes breaking schema changes from `v0.4.0`.

## What You Can Do

- Read OTLP traces, logs, gauges, sums/counters, histograms, and exponential histograms from files.
- Stream live OTLP/HTTP exports into the default DuckDB catalog, an attached [DuckLake](https://ducklake.select) lakehouse, or an Iceberg REST catalog such as Amazon S3 Tables or Cloudflare R2 Data Catalog.
- Convert telemetry exports to Parquet with DuckDB `COPY`.
- Query local files, globs, S3, HTTP(S), Azure Blob, and GCS paths through DuckDB file systems.
- Use the browser demo for JSON, JSONL, and protobuf exploration with DuckDB-WASM: [Interactive Demo](https://smithclay.github.io/duckdb-otlp/demo/).

## Documentation

- [Documentation Site](https://smithclay.github.io/duckdb-otlp/)
- [Get Started](https://smithclay.github.io/duckdb-otlp/get-started/)
- [Live Ingest Quickstart](https://smithclay.github.io/duckdb-otlp/quickstart/serve/)
- [Stream to DuckLake](https://smithclay.github.io/duckdb-otlp/guides/stream-to-ducklake/)
- [Stream to Amazon S3 Tables](https://smithclay.github.io/duckdb-otlp/guides/stream-to-s3-tables/)
- [Stream to Cloudflare R2 Data Catalog](https://smithclay.github.io/duckdb-otlp/guides/stream-to-r2-data-catalog/)
- [Store Claude Code or Codex Traces in Local DuckLake](https://smithclay.github.io/duckdb-otlp/guides/store-agent-traces-local-ducklake/)
- [How-to Guides](https://smithclay.github.io/duckdb-otlp/guides/)
- [API Reference](https://smithclay.github.io/duckdb-otlp/reference/api/)
- [Schema Reference](https://smithclay.github.io/duckdb-otlp/reference/schemas/)
- [Live Ingest Reference](https://smithclay.github.io/duckdb-otlp/reference/serve/)
- [Architecture](https://smithclay.github.io/duckdb-otlp/architecture/)

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
| `otlp_flush(uri)` | Optionally force buffered ingest rows to commit to the target catalog now |
| `otlp_stop(uri)` | Stop a server after committing remaining rows |
| `otlp_server_list()` | Inspect running servers and ingest counters |

The extension registers `read_otlp_metrics` and `read_otlp_metrics_summary`, but those functions remain unsupported until the project defines stable schemas for those shapes. See the [API Reference](https://smithclay.github.io/duckdb-otlp/reference/api/) for details.

## Installation

```sql
INSTALL otlp FROM community;
LOAD otlp;
```

For source builds, development commands, and WASM builds, see [CONTRIBUTING.md](CONTRIBUTING.md). WASM supports JSON, JSONL, and protobuf file reads, but not the live ingest server.

## Limits

The file readers limit individual files to **100 MB** to prevent memory exhaustion. Live ingest accepts request bodies up to `max_body_bytes` and applies buffered-ingest backpressure through `max_buffered_bytes`; see the [Live Ingest Reference](https://smithclay.github.io/duckdb-otlp/reference/serve/#responses-and-status-codes).

## Need Help?

- [GitHub Discussions](https://github.com/smithclay/duckdb-otlp/discussions)
- [Open an issue](https://github.com/smithclay/duckdb-otlp/issues)
- [Contributing guide](CONTRIBUTING.md)

## License

MIT. See [LICENSE](LICENSE) for details.
