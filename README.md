# DuckDB OpenTelemetry (OTLP) Extension

Query OpenTelemetry traces, logs, and metrics with SQL. Works with OTLP file exports from any OpenTelemetry Collector.

```sql
-- Install from DuckDB community extensions
INSTALL otlp FROM community;
LOAD otlp;

-- Query slow traces
SELECT
    TraceId,
    SpanName,
    Duration / 1000000 AS duration_ms
FROM read_otlp_traces('traces.jsonl')
WHERE Duration > 1000000000  -- over 1 second
ORDER BY Duration DESC
LIMIT 5;
```

**Output:**
```
┌─────────────────────────────────┬──────────────────┬──────────────┐
│            TraceId              │    SpanName      │ duration_ms  │
├─────────────────────────────────┼──────────────────┼──────────────┤
│ 7a3f92e8b4c1d6f0a9e2...         │ POST /checkout   │    1523.4    │
│ 8b1e45c9f2a7d3e6b0f1...         │ GET /search      │    1205.7    │
│ 3c2d19f8e4b6a0c7d1f9...         │ PUT /cart/items  │    1089.2    │
└─────────────────────────────────┴──────────────────┴──────────────┘
```

## What you can do

✅ **Analyze production telemetry** - Query OTLP file exports with familiar SQL syntax
✅ **Archive to your data lake** - Convert OpenTelemetry data to Parquet with schemas intact
✅ **Debug faster** - Filter logs by severity, find slow traces, aggregate metrics
✅ **Integrate with data tools** - Use DuckDB's ecosystem (MotherDuck, Jupyter, DBT, etc.)

## Get started in 3 minutes

**[→ Quick Start Guide](docs/get-started.md)** - Install, load sample data, run your first query

## Common use cases

### Find slow requests
```sql
SELECT SpanName, AVG(Duration) / 1000000 AS avg_ms
FROM read_otlp_traces('prod-traces/*.jsonl')
WHERE SpanKind = 'SERVER'
GROUP BY SpanName
HAVING AVG(Duration) > 1000000000
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
SELECT Timestamp, ServiceName, Body
FROM read_otlp_logs('app-logs/*.jsonl')
WHERE SeverityText IN ('ERROR', 'FATAL')
ORDER BY Timestamp DESC;
```

### Build metrics dashboards
```sql
CREATE TABLE metrics_gauge AS
SELECT Timestamp, ServiceName, MetricName, Value
FROM read_otlp_metrics_gauge('metrics/*.jsonl');
```

**[→ See more examples in the Guides](docs/guides/)**

## Configuration Options

### Error Handling

Control how the extension handles malformed or invalid OTLP data:

```sql
-- Default: fail on parse errors
SELECT * FROM read_otlp_traces('traces.jsonl');

-- Skip invalid records and continue processing
SELECT * FROM read_otlp_traces('traces.jsonl', on_error := 'skip');

-- Emit NULL rows for invalid records (preserves row count)
SELECT * FROM read_otlp_traces('traces.jsonl', on_error := 'nullify');

-- Check error statistics after scan
SELECT * FROM read_otlp_scan_stats();
```

### Size Limits

Individual JSON/Protobuf documents are limited to **100 MB by default** to prevent memory exhaustion:

```sql
-- Use default 100MB limit
SELECT * FROM read_otlp_traces('traces.jsonl');

-- Override for larger documents (value in bytes)
SELECT * FROM read_otlp_traces('huge_traces.jsonl', max_document_bytes := 500000000);

-- Combine with error handling
SELECT * FROM read_otlp_metrics('metrics.pb',
                                max_document_bytes := 200000000,
                                on_error := 'skip');
```

**Note**: This limit applies to individual documents in JSONL files, or entire protobuf files. It does not limit total file size for streaming JSONL.

### Discover All Options

```sql
-- View all available configuration options
SELECT * FROM read_otlp_options();
```

## What's inside

**Table Functions**

| Function | What it does |
|----------|-------------|
| `read_otlp_traces(path, ...)` | Stream trace spans with identifiers, attributes, events, and links |
| `read_otlp_logs(path, ...)` | Read log records with severity, body, and trace correlation |
| `read_otlp_metrics(path, ...)` | Query metrics (gauge, sum, histogram, exponential histogram, summary) |
| `read_otlp_metrics_gauge(path, ...)` | Typed helper for gauge metrics |
| `read_otlp_metrics_sum(path, ...)` | Typed helper for sum/counter metrics |
| `read_otlp_metrics_histogram(path, ...)` | Typed helper for histogram metrics |

**[→ Full API Reference](docs/reference/api.md)**

**Features**

- **Automatic format detection** - Works with JSON, JSONL, and protobuf OTLP files
- **DuckDB file systems** - Read from local files, S3, HTTP(S), Azure Blob, GCS
- **Error handling & safeguards** - `on_error` (fail/skip/nullify) plus `max_document_bytes` (per-file size cap)
- **ClickHouse compatible** - Matches OpenTelemetry ClickHouse exporter schema
- **Scan diagnostics** - Review parser stats with `read_otlp_scan_stats()`

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

All table functions emit strongly-typed columns (no JSON extraction required):

- **Traces**: 22 columns - identifiers, timestamps, attributes, events, links
- **Logs**: 15 columns - severity, body, trace correlation, attributes
- **Metrics**: 27 columns (union schema) or typed helpers for each metric shape

**[→ Schema Reference](docs/reference/)**

## Need help?

- **Getting started?** Read the **[Quick Start Guide](docs/get-started.md)**
- **Have a question?** Check **[Discussions](https://github.com/smithclay/duckdb-otlp/discussions)**
- **Found a bug?** **[Open an issue](https://github.com/smithclay/duckdb-otlp/issues)**
- **Want to contribute?** See **[CONTRIBUTING.md](CONTRIBUTING.md)**

## License

MIT - See [LICENSE](LICENSE) for details

---

**Learn more**: [OpenTelemetry Protocol (OTLP)](https://opentelemetry.io/docs/specs/otlp/) | [ClickHouse Exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter) | [DuckDB Extensions](https://duckdb.org/docs/extensions/overview)
