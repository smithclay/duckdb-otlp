# DuckDB OpenTelemetry Extension

[![Build](https://github.com/smithclay/duckdb-otlp/actions/workflows/MainDistributionPipeline.yml/badge.svg)](https://github.com/smithclay/duckdb-otlp/actions/workflows/MainDistributionPipeline.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![DuckDB](https://img.shields.io/badge/DuckDB-%E2%89%A5%201.5.4-FFF000?logo=duckdb&logoColor=black)](https://duckdb.org)
[![Docs](https://img.shields.io/badge/docs-online-4c9aff)](https://smithclay.github.io/duckdb-otlp/)

DuckDB extension for querying and storing OpenTelemetry traces, logs, and metrics with SQL.

As of v0.6, the extension has an embedded HTTP/gRPC server that lets you stream live telemetry using the OpenTelemetry Protocol (including the [new OTel-Arrow protocol](https://github.com/open-telemetry/otel-arrow#what-is-otap)) into [parquet files](https://smithclay.github.io/duckdb-otlp/guides/stream-to-parquet/), [DuckLake](https://smithclay.github.io/duckdb-otlp/guides/stream-to-ducklake/), or Iceberg catalogs like [Amazon S3 Tables](https://smithclay.github.io/duckdb-otlp/guides/stream-to-s3-tables/) and [Cloudflare R2 Data Catalog](https://smithclay.github.io/duckdb-otlp/guides/stream-to-r2-data-catalog/).

## Quickstart: Read OpenTelemetry data

Install and load the extension in `duckdb` v1.5.4 or higher:

```sql
-- Run commands inside the duckdb shell, install via `curl https://install.duckdb.org | sh` or `brew install duckdb`
-- See "Install pre-release extension via GitHub" to use a nightly builds
INSTALL otlp FROM community;
LOAD otlp;
```

<details>
<summary>Install pre-release extension via GitHub</summary>

If you want to use a pre-release that's not published on the duckdb community site, you can install it (unsigned) via GitHub:

```sql
-- Install unsigned extenstion from GitHub
-- You must start duckdb with `-unsigned` to allow this
INSTALL otlp from 'https://smithclay.github.io/duckdb-otlp';
LOAD otlp;
```

</details>

Read OTLP protobuf/JSON data from public URLs, local files, or object storage buckets:

```sql
-- Install extension to support reading over HTTP(S)
INSTALL httpfs; LOAD httpfs;

-- Read logs exported from the OpenTelemetry Collector
SELECT time_unix_nano, service_name, severity_text, body FROM read_otlp_logs('https://github.com/smithclay/duckdb-otlp/raw/refs/heads/main/test/data/otlp_logs.pb');

-- Read traces exported from the OpenTelemetry Collector
SELECT trace_id, name, duration_time_unix_nano FROM read_otlp_traces('https://github.com/smithclay/duckdb-otlp/raw/refs/heads/main/test/data/otlp_traces.pb') ORDER BY duration_time_unix_nano DESC;
```

Read the columnar [OpenTelemetry Arrow Protocol (OTAP)](https://github.com/open-telemetry/otel-arrow) with the `read_otap_*` readers. They emit the same schemas as `read_otlp_*`; pick the reader that matches your input encoding:

```sql
-- Decode an OTAP (BatchArrowRecords) file into the same flattened log schema
SELECT time_unix_nano, service_name, severity_text, body FROM read_otap_logs('https://github.com/smithclay/duckdb-otlp/raw/refs/heads/main/test/data/otap/logs-initial.bar');
```

## Quickstart: Stream OpenTelemetry data

You can start a server that accepts OpenTelemetry data from instrumented code, AI agents such as [Claude Code or Codex](https://smithclay.github.io/duckdb-otlp/guides/store-agent-traces-local-ducklake/), or OpenTelemetry Collectors. 

You can either run a Docker image that runs the extension as a daemon, or type some short commands in DuckDB shell.

<details>
<summary>Start server in the DuckDB shell</summary>

```sql
-- See instructions above for loading otlp extension
-- use otap_serve() for OpenTelemetry Arrow
-- Listens for OTLP/HTTP on :4318 for metrics, logs, traces
FROM otlp_serve(
    'otlp:localhost:4318',
    token := 'dev-token-123456'
);
```

</details>

<details>
<summary>Run server as a daemon with Docker</summary>

```sh
# Bootstraps an embedded DuckDB instance with the server running
# Writes data to a local DuckLake file
mkdir -p data 
export DUCKDB_OTLP_TOKEN=dev-token-123456

docker run --rm --name duckdb-otlp \
    -p 4318:4318 \
    -e DUCKDB_OTLP_TOKEN \
    -v "$(pwd)/data:/data" \
    ghcr.io/smithclay/duckdb-otlp:latest
```

To query the running daemon using Quack protocol, [see docs here](https://smithclay.github.io/duckdb-otlp/guides/query-with-quack/).

</details>
Send one hello-world log in OTLP/HTTP format with cURL:

```bash
curl -sS http://localhost:4318/v1/logs -H 'Authorization: Bearer dev-token-123456' -H 'Content-Type: application/json' -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"curl-demo"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1704067200000000000","severityText":"INFO","body":{"stringValue":"hello from curl"}}]}]}]}'
```

Query the data after ~5 seconds for the buffer to flush:

```sql
SELECT time_unix_nano, service_name, severity_text, body FROM otlp_logs;
```

Live ingest commits buffered rows in the background after about 5 seconds for the oldest buffered row or about 128 MiB of admitted request-body bytes. Use `otlp_flush` when readers need accepted rows durable and queryable while the server keeps running.

For a full walkthrough, including lakehouse ingest, see the [docs](https://smithclay.github.io/duckdb-otlp/).

## Schema

The schemas align with a normalized version of the [OpenTelemetry Arrow Data model](https://github.com/open-telemetry/otel-arrow/blob/main/docs/data_model.md) as of extension release `v0.5.0`. Release `v0.5.0` includes breaking schema changes from `v0.4.0`.

## What You Can Do

- Read OTLP traces, logs, gauges, sums/counters, histograms, and exponential histograms from files.
- Read the columnar OpenTelemetry Arrow Protocol (OTAP) with the `read_otap_*` functions, which produce the same schemas as `read_otlp_*`.
- Stream live telemetry over **OTLP** (`otlp_serve` — OTLP/HTTP, or standard OTLP/gRPC unary with `transport := 'grpc'`) or the **OpenTelemetry Arrow Protocol** (`otap_serve` — OTAP/Arrow bidirectional gRPC streaming) into the default DuckDB catalog, an attached [DuckLake](https://ducklake.select) lakehouse, or an Iceberg REST catalog such as Amazon S3 Tables or Cloudflare R2 Data Catalog.
- Convert telemetry to Parquet files and save to cloud storage.
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
| `read_otap_traces/logs/metrics_*(path)` | Read OpenTelemetry Arrow Protocol (OTAP) files into the same schemas as the `read_otlp_*` readers |
| `otlp_serve([uri], ...)` | Start a native OTLP ingest server (`otlp:` scheme): OTLP/HTTP, or OTLP/gRPC unary with `transport := 'grpc'` |
| `otap_serve([uri], ...)` | Start a native OTAP/Arrow gRPC streaming ingest server (`otap:` scheme) |
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

Early-stage and **single-node** (one daemon, one writer — no HA or horizontal scaling). Ingest has been benchmarked at ~100k logs/s on a 4-vCPU node; **querying at volume is unproven**, so test on your own data.

- **Durability is the seal, not the `202`.** Live ingest buffers in memory and commits on a periodic group-commit ("seal"); a `202` means *accepted*, not durable. Call `otlp_flush`/`otlp_stop` before shutting down — a hard kill drops un-sealed rows (there is no WAL).
- **Keep queries time-bounded.** Data lands roughly time-ordered, so queries scoped by `timestamp` (and `service_name`) prune well; unbounded scans are slow. There is **no full-text index** — `body` substring/regex search and `trace_id` point lookups are brute-force scans: cheap over a short window, expensive over a wide one.
- **File reads** cap individual files at **100 MB**. **Live ingest** runs via `otlp_serve` (OTLP/HTTP, or OTLP/gRPC unary with `transport := 'grpc'`) and `otap_serve` (OTAP/Arrow gRPC streaming); both are native-only (not in the WASM build), bound request bodies, and apply `max_buffered_bytes` backpressure (`503` / `RESOURCE_EXHAUSTED`); see the [Live Ingest Reference](https://smithclay.github.io/duckdb-otlp/reference/serve/#responses-and-status-codes).

## Need Help?

- [GitHub Discussions](https://github.com/smithclay/duckdb-otlp/discussions)
- [Open an issue](https://github.com/smithclay/duckdb-otlp/issues)
- [Contributing guide](CONTRIBUTING.md)

## License

MIT. See [LICENSE](LICENSE) for details.
