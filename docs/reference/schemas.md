# Schema Reference

Duckspan emits strongly-typed tables that mirror the [OpenTelemetry ClickHouse exporter schema](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter). Every column is a native DuckDB type: hugeints for trace identifiers, maps for attributes, structs and lists for nested data.

| Table function | Columns | Notes |
| --- | --- | --- |
| `read_otlp_traces` | 22 | Spans with identifiers, scope metadata, attributes, events, and links. |
| `read_otlp_logs` | 15 | Log records with severity, body, resource attributes, and trace correlation. |
| `read_otlp_metrics` | 27 | Union schema that covers gauge, sum, histogram, exponential histogram, and summary metrics. |
| `read_otlp_metrics_{gauge,sum,histogram,exp_histogram,summary}` | varies | Typed projections of the union schema (see below). |

## Traces (`read_otlp_traces`)

- **Identifiers**: `TraceId`, `SpanId`, `ParentSpanId`, `TraceState`
- **Span metadata**: `SpanName`, `SpanKind`, `ServiceName`
- **Timing**: `Timestamp`, `EndTimestamp`, `Duration` (nanoseconds)
- **Status**: `StatusCode`, `StatusMessage`
- **Attributes**: `ResourceAttributes`, `Attributes`, `SpanAttributes`
- **Instrumentation**: `ScopeName`, `ScopeVersion`, `ResourceSchemaUrl`, `ScopeSchemaUrl`
- **Structured data**: `Events` (list of structs), `Links` (list of structs)

Each span is emitted once, even if multiple files are scanned. Use standard DuckDB SQL to join traces back to logs or metrics via `TraceId`/`SpanId`.

## Logs (`read_otlp_logs`)

- **Timing**: `Timestamp`, `ObservedTimestamp`
- **Severity**: `SeverityText`, `SeverityNumber`
- **Content**: `Body`
- **Correlation**: `TraceId`, `SpanId`
- **Service**: `ServiceName`
- **Attributes**: `ResourceAttributes`, `Attributes`
- **Instrumentation**: `ScopeName`, `ScopeVersion`, `ResourceSchemaUrl`, `ScopeSchemaUrl`

Logs keep the same attribute map types as traces, so filters such as `ResourceAttributes['deployment.environment'] = 'prod'` work across signals.

## Metrics (`read_otlp_metrics`)

`read_otlp_metrics` returns a 27-column union schema. Every row includes the base columns plus the fields associated with the metric’s type.

### Common columns

- `Timestamp`, `ServiceName`, `MetricName`, `MetricDescription`, `MetricUnit`
- `MetricType` – one of `gauge`, `sum`, `histogram`, `exp_histogram`, `summary`
- `ResourceAttributes`, `ScopeName`, `ScopeVersion`, `Attributes`

### Type-specific columns

- **Gauge** (`MetricType = 'gauge'`)
  - `Value`

- **Sum** (`MetricType = 'sum'`)
  - `Value`, `AggregationTemporality`, `IsMonotonic`

- **Histogram** (`MetricType = 'histogram'`)
  - `Count`, `Sum`, `BucketCounts`, `ExplicitBounds`, `Min`, `Max`

- **Exponential histogram** (`MetricType = 'exp_histogram'`)
  - `Count`, `Sum`, `Scale`, `ZeroCount`
  - `PositiveOffset`, `PositiveBucketCounts`
  - `NegativeOffset`, `NegativeBucketCounts`
  - `Min`, `Max`

- **Summary** (`MetricType = 'summary'`)
  - `Count`, `Sum`, `QuantileValues`, `QuantileQuantiles`

The union strategy makes it easy to split metrics into typed archive tables:

```sql
CREATE TABLE metrics_sum AS
SELECT *
FROM read_otlp_metrics('otel-export/telemetry.jsonl')
WHERE MetricType = 'sum';
```

See the [cookbook](../guides/cookbook.md#build-typed-metrics-tables) for more recipes.

## Metric helper functions

The helper scans project the union schema into typed layouts. They expose the same base columns and only the fields that apply to the requested metric type.

| Helper | Columns returned (in order) |
| --- | --- |
| `read_otlp_metrics_gauge` | Base columns + `Value` |
| `read_otlp_metrics_sum` | Base columns + `Value`, `AggregationTemporality`, `IsMonotonic` |
| `read_otlp_metrics_histogram` | Base columns + `Count`, `Sum`, `BucketCounts`, `ExplicitBounds`, `Min`, `Max` |
| `read_otlp_metrics_exp_histogram` | Base columns + `Count`, `Sum`, `Scale`, `ZeroCount`, positive and negative bucket structs, `Min`, `Max` |
| `read_otlp_metrics_summary` | Base columns + `Count`, `Sum`, `QuantileValues`, `QuantileQuantiles` |

All helper scans accept the same named parameters as `read_otlp_metrics`:

```sql
SELECT *
FROM read_otlp_metrics_gauge('otel-export/telemetry.jsonl', on_error := 'skip');
```

## Type system notes

- `TraceId` and `SpanId` are DuckDB `HUGEINT`s (128-bit). Convert to hex with `format('{:032x}', TraceId)`.
- Attribute maps use DuckDB’s `MAP<VARCHAR, VARCHAR>` type. Cast to JSON with `to_json(ResourceAttributes)`.
- Histogram buckets are `LIST`s (`BucketCounts`, `ExplicitBounds`, `QuantileValues`, etc.).
- Exponential histogram buckets are stored as structs containing offset and count arrays.

## Diagnostics

Use `read_otlp_options()` to enumerate available named parameters and defaults (e.g., `on_error`, `max_document_bytes`). Inspect `read_otlp_scan_stats()` after a scan to review parse errors, skipped rows, and format detection details.

---

Looking for end-to-end workflows? Head back to the [cookbook](../guides/cookbook.md). Need help generating telemetry? The [collector setup guide](../setup/collector.md) covers the OpenTelemetry Collector setup.
