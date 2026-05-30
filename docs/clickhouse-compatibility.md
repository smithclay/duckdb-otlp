# ClickHouse Compatibility

The schemas are inspired by the OpenTelemetry ClickHouse exporter, but they are not byte-for-byte identical. This extension uses `snake_case` names, hex-string trace/span IDs, JSON-string attributes, and shape-specific metric readers.

## Main Differences

| Area | DuckDB OTLP extension |
| --- | --- |
| Column names | `snake_case` (`trace_id`, `span_name`, `service_name`) |
| Trace/span IDs | Hex strings |
| Attributes | JSON strings such as `resource_attributes`, `span_attributes`, `log_attributes` |
| Metrics | Typed readers for gauge, sum, histogram, and exponential histogram |
| Summary metrics | Not implemented yet |

## Common Query Translation

ClickHouse:

```sql
SELECT TraceId, SpanName, Duration / 1000000 AS duration_ms
FROM otel_traces
WHERE Duration > 1000000000;
```

DuckDB:

```sql
SELECT trace_id, span_name, duration / 1000000 AS duration_ms
FROM read_otlp_traces('traces.jsonl')
WHERE duration > 1000000000;
```

ClickHouse:

```sql
SELECT Timestamp, ServiceName, Body
FROM otel_logs
WHERE SeverityText = 'ERROR';
```

DuckDB:

```sql
SELECT timestamp, service_name, body
FROM read_otlp_logs('logs.jsonl')
WHERE severity_text = 'ERROR';
```

## Metrics

Use one reader per metric shape:

```sql
CREATE TABLE metrics_gauge AS
SELECT * FROM read_otlp_metrics_gauge('metrics.jsonl');

CREATE TABLE metrics_sum AS
SELECT * FROM read_otlp_metrics_sum('metrics.jsonl');

CREATE TABLE metrics_histogram AS
SELECT * FROM read_otlp_metrics_histogram('metrics.jsonl');

CREATE TABLE metrics_exp_histogram AS
SELECT * FROM read_otlp_metrics_exp_histogram('metrics.jsonl');
```

See [Schema Reference](reference/schemas.md) and the [OpenTelemetry ClickHouse exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter).
