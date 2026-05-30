# DuckDB OpenTelemetry (OTLP) Extension

Query OpenTelemetry traces, logs, and metrics with SQL. Works with OTLP file exports from any OpenTelemetry Collector, uses a row-based schema inspired by the [Clickhouse OpenTelemetry exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/clickhouseexporter/README.md).

```sql
-- Install from DuckDB community extensions
INSTALL otlp FROM community;
LOAD otlp;

-- Query slow traces
SELECT
    trace_id,
    span_name,
    duration / 1000000 AS duration_ms
FROM read_otlp_traces('traces.jsonl')
WHERE duration > 1000000000  -- over 1 second
ORDER BY duration DESC
LIMIT 5;
```

**Output:**
```
┌─────────────────────────────────┬──────────────────┬──────────────┐
│            trace_id             │    span_name     │ duration_ms  │
├─────────────────────────────────┼──────────────────┼──────────────┤
│ 7a3f92e8b4c1d6f0a9e2...         │ POST /checkout   │    1523.4    │
│ 8b1e45c9f2a7d3e6b0f1...         │ GET /search      │    1205.7    │
│ 3c2d19f8e4b6a0c7d1f9...         │ PUT /cart/items  │    1089.2    │
└─────────────────────────────────┴──────────────────┴──────────────┘
```

> Want to _stream_ OTLP data directly to duckdb / Parquet in cloud storage? Check out https://github.com/smithclay/otlp2parquet

## What you can do

- **Analyze production telemetry** - Query OTLP file exports with familiar SQL syntax
- **Stream live telemetry in** - Run an OTLP/HTTP server and POST exports straight into DuckDB tables
- **Archive to your data lake** - Convert OpenTelemetry data to Parquet with schemas intact
- **Debug faster** - Filter logs by severity, find slow traces, aggregate metrics
- **Integrate with data tools** - Use DuckDB's ecosystem (MotherDuck, Jupyter, DBT, etc.)

## Get started in 3 minutes

**[→ Quick Start Guide](docs/get-started.md)** - Install, load sample data, run your first query

## Try it in your browser

**[→ Interactive Demo](https://smithclay.github.io/duckdb-otlp/)** - Query OTLP data directly in your browser using DuckDB-WASM

The browser demo lets you:
- Load sample OTLP traces, logs, and metrics
- Run SQL queries without installing anything
- Upload your own JSONL files for analysis

**Note:** The WASM demo supports JSON format only. For protobuf support, install the native extension.

## Common use cases

### Find slow requests
```sql
SELECT span_name, AVG(duration) / 1000000 AS avg_ms
FROM read_otlp_traces('prod-traces/*.jsonl')
WHERE span_kind = 2  -- SERVER
GROUP BY span_name
HAVING AVG(duration) > 1000000000
ORDER BY avg_ms DESC;
```

### Export telemetry to Parquet
```sql
COPY (
  SELECT * FROM read_otlp_traces('otel-export/*.jsonl')
) TO 'data-lake/daily_traces.parquet' (FORMAT PARQUET);
```

### Filter logs by severity
```sql
SELECT timestamp, service_name, body
FROM read_otlp_logs('app-logs/*.jsonl')
WHERE severity_text IN ('ERROR', 'FATAL')
ORDER BY timestamp DESC;
```

### Build metrics dashboards
```sql
CREATE TABLE metrics_gauge AS
SELECT timestamp, service_name, metric_name, value
FROM read_otlp_metrics_gauge('metrics/*.jsonl');
```

**[→ See more examples in the Cookbook](docs/guides/cookbook.md)**

### Stream live OTLP exports into DuckDB

```sql
-- Start an OTLP/HTTP server (native builds only)
SELECT listen_url, auth_token
FROM otlp_serve('otlp:localhost:4318', token := 'dev-token-123456');
```

```bash
# Point any OpenTelemetry exporter at it, or POST directly:
curl -sS http://localhost:4318/v1/logs \
  -H 'Content-Type: application/x-ndjson' \
  -H 'Authorization: Bearer dev-token-123456' \
  --data-binary @test/data/logs_simple.jsonl
```

```sql
SELECT count(*) FROM otlp_logs;  -- rows landed live
```

**[→ Live Ingest Quickstart](docs/quickstart/serve.md)**

## Limits

Individual files are limited to **100 MB** to prevent memory exhaustion. This applies to entire protobuf files or individual documents in JSONL files.

## What's inside

**Table Functions**

| Function | What it does |
|----------|-------------|
| `read_otlp_traces(path)` | Stream trace spans (25 columns) with identifiers, attributes, events, and links |
| `read_otlp_logs(path)` | Read log records (15 columns) with severity, body, and trace correlation |
| `read_otlp_metrics_gauge(path)` | Read gauge metrics (16 columns) |
| `read_otlp_metrics_sum(path)` | Read sum/counter metrics (18 columns) with aggregation temporality |

**Live Ingest** (native builds only)

| Function | What it does |
|----------|-------------|
| `otlp_serve([uri], ...)` | Start an OTLP/HTTP server that appends `/v1/logs`, `/v1/traces`, `/v1/metrics` POSTs into DuckDB tables |
| `otlp_stop(uri)` | Stop a running server |
| `otlp_server_list()` | List running servers with live request/row counters |

**[→ Full API Reference](docs/reference/api.md)** · **[→ Live Ingest Server](docs/reference/serve.md)**

**Features**

- **Automatic format detection** - Works with JSON, JSONL, and protobuf OTLP files (protobuf requires native extension)
- **DuckDB file systems** - Read from local files, S3, HTTP(S), Azure Blob, GCS
- **ClickHouse-inspired schema** - Row-based schema inspired by OpenTelemetry ClickHouse exporter
- **Browser support** - Run queries in-browser with DuckDB-WASM (JSON only)

## Installation

**Option 1: Install from community (recommended)**

```sql
INSTALL otlp FROM community;
LOAD otlp;
```

**Option 2: Build from source**

See **[CONTRIBUTING.md](CONTRIBUTING.md)** for build instructions.

## Documentation

**[Documentation Hub →](docs/)**

📚 **Guides** - Task-based tutorials with real examples
📖 **Reference** - Schemas, API signatures, error handling
⚙️  **Setup** - Installation, collector configuration, sample data

## How it works

Generally speaking: the idea is you load files created using the OpenTelemetry Collector file exporter.

```
OpenTelemetry     File         DuckDB OTLP          SQL
Collector      Exporter       Extension          Results
   │              │               │                 │
   │  OTLP/gRPC   │               │                 │
   ├─────────────►│  .jsonl/.pb   │                 │
   │              ├──────────────►│  read_otlp_*()  │
   │              │               ├────────────────►│
   │              │               │                 │
```

The extension reads OTLP files (JSON or protobuf), detects the format automatically, and streams strongly-typed rows into DuckDB tables. Schemas match the ClickHouse exporter format for compatibility.

**[→ Learn more in Architecture Guide](docs/architecture.md)**

## Schemas

All table functions emit strongly-typed columns with `snake_case` naming:

- **Traces**: 25 columns - identifiers, timestamps, attributes, events, links
- **Logs**: 15 columns - severity, body, trace correlation, attributes
- **Metrics**: 16-18 columns depending on metric type (gauge, sum)

**[→ Schema Reference](docs/reference/schemas.md)**

## Need help?

- **Getting started?** Read the **[Quick Start Guide](docs/get-started.md)**
- **Have a question?** Check **[Discussions](https://github.com/smithclay/duckdb-otlp/discussions)**
- **Found a bug?** **[Open an issue](https://github.com/smithclay/duckdb-otlp/issues)**
- **Want to contribute?** See **[CONTRIBUTING.md](CONTRIBUTING.md)**

## License

MIT - See [LICENSE](LICENSE) for details

---

**Learn more**: [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/) | [ClickHouse Exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter) | [DuckDB Extensions](https://duckdb.org/docs/extensions/overview)
