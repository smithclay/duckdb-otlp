# Schema Reference

All table functions in duckspan use strongly-typed columns compatible with the [OpenTelemetry ClickHouse exporter schema](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter).

## Traces Table

The `read_otlp_traces()` function returns **22 columns**:

### Core Identifiers
- `TraceId` - Unique trace identifier
- `SpanId` - Unique span identifier
- `ParentSpanId` - Parent span identifier for hierarchy

### Span Metadata
- `SpanName` - Name of the span operation
- `SpanKind` - Type of span (CLIENT, SERVER, INTERNAL, etc.)
- `ServiceName` - Extracted from resource attributes
- `Duration` - Calculated from start/end timestamps (nanoseconds)

### Status Information
- `StatusCode` - Span status code
- `StatusMessage` - Human-readable status message

### Temporal Data
- `Timestamp` - Start time of the span
- `EndTimestamp` - End time of the span

### Attributes and Context
- `ResourceAttributes` - Key-value map of resource-level attributes
- `Attributes` - Key-value map of span-level attributes
- `ScopeName` - Name of the instrumentation scope
- `ScopeVersion` - Version of the instrumentation scope

### Structured Data
- `Events` - Nested array of span events
- `Links` - Nested array of span links to other traces

### Additional Fields
- `TraceState` - W3C trace state
- `ResourceSchemaUrl` - Schema URL for resource attributes
- `ScopeSchemaUrl` - Schema URL for scope
- `SpanAttributes` - Additional span-level attributes

## Logs Table

The `read_otlp_logs()` function returns **15 columns**:

### Temporal Data
- `Timestamp` - When the log occurred
- `ObservedTimestamp` - When the log was observed/collected

### Severity Information
- `SeverityText` - Human-readable severity (ERROR, INFO, etc.)
- `SeverityNumber` - Numeric severity level

### Content
- `Body` - Log message content

### Service Context
- `ServiceName` - Extracted from resource attributes

### Trace Correlation
- `TraceId` - Associated trace identifier for correlation
- `SpanId` - Associated span identifier for correlation

### Attributes and Context
- `ResourceAttributes` - Key-value map of resource-level attributes
- `Attributes` - Key-value map of log-level attributes
- `ScopeName` - Name of the instrumentation scope
- `ScopeVersion` - Version of the instrumentation scope

### Schema Information
- `ResourceSchemaUrl` - Schema URL for resource attributes
- `ScopeSchemaUrl` - Schema URL for scope

## Metrics Tables

Duckspan provides two approaches for working with metrics:

### Union Schema (read_otlp_metrics)

The `read_otlp_metrics()` function returns a **union schema with 27 columns** containing all metric types. A `MetricType` discriminator column identifies the type of each row.

#### Common Base Columns (All Metric Types)
- `Timestamp` - When the metric was recorded
- `ServiceName` - Extracted from resource attributes
- `MetricName` - Name of the metric
- `MetricDescription` - Description of what the metric measures
- `MetricUnit` - Unit of measurement
- `MetricType` - Discriminator: `gauge`, `sum`, `histogram`, `exp_histogram`, or `summary`
- `ResourceAttributes` - Key-value map of resource-level attributes
- `ScopeName` - Name of the instrumentation scope
- `ScopeVersion` - Version of the instrumentation scope
- `Attributes` - Key-value map of metric-specific attributes

#### Type-Specific Columns
Different columns are populated based on the `MetricType`:

**Gauge metrics** (`MetricType = 'gauge'`):
- `Value` - The gauge value

**Sum metrics** (`MetricType = 'sum'`):
- `Value` - The sum value
- `AggregationTemporality` - DELTA or CUMULATIVE
- `IsMonotonic` - Whether the sum only increases

**Histogram metrics** (`MetricType = 'histogram'`):
- `Count` - Number of measurements
- `Sum` - Sum of all measurements
- `BucketCounts` - Array of counts per bucket
- `ExplicitBounds` - Array of bucket boundaries
- `Min` - Minimum value (optional)
- `Max` - Maximum value (optional)

**Exponential Histogram metrics** (`MetricType = 'exp_histogram'`):
- `Count` - Number of measurements
- `Sum` - Sum of all measurements
- `Scale` - Scale factor for exponential buckets
- `ZeroCount` - Count of zero values
- `PositiveOffset`, `PositiveBucketCounts` - Positive value buckets
- `NegativeOffset`, `NegativeBucketCounts` - Negative value buckets
- `Min`, `Max` - Value range

**Summary metrics** (`MetricType = 'summary'`):
- `Count` - Number of measurements
- `Sum` - Sum of all measurements
- `QuantileValues` - Array of quantile values
- `QuantileQuantiles` - Array of quantile levels

### Typed Helper Functions

For convenience, duckspan provides typed helper functions that return only the relevant columns for each metric type:

#### read_otlp_metrics_gauge()
Returns **10 columns** for gauge metrics:
- Base columns (Timestamp, ServiceName, MetricName, etc.)
- `Value` - The gauge value

#### read_otlp_metrics_sum()
Returns **12 columns** for sum/counter metrics:
- Base columns
- `Value` - The sum value
- `AggregationTemporality` - DELTA or CUMULATIVE
- `IsMonotonic` - Boolean flag

#### read_otlp_metrics_histogram()
Returns **15 columns** for histogram metrics:
- Base columns
- `Count`, `Sum` - Aggregate statistics
- `BucketCounts`, `ExplicitBounds` - Distribution data
- `Min`, `Max` - Value range

#### read_otlp_metrics_exp_histogram()
Returns **19 columns** for exponential histogram metrics:
- Base columns
- `Count`, `Sum` - Aggregate statistics
- `Scale`, `ZeroCount` - Exponential bucket parameters
- Positive/Negative bucket data
- `Min`, `Max` - Value range

#### read_otlp_metrics_summary()
Returns **13 columns** for summary metrics:
- Base columns
- `Count`, `Sum` - Aggregate statistics
- `QuantileValues`, `QuantileQuantiles` - Percentile data

## Schema Compatibility

All schemas are designed to be compatible with the OpenTelemetry ClickHouse exporter, enabling:
- Direct migration from ClickHouse to DuckDB
- Consistent query patterns across systems
- Standard OTLP field mappings

## Type System

Duckspan uses DuckDB's native types for optimal performance:
- **HUGEINT** - 128-bit integers for TraceId, SpanId
- **BIGINT** - 64-bit integers for timestamps (nanoseconds since epoch)
- **VARCHAR** - Variable-length strings
- **MAP** - Key-value pairs for attributes
- **LIST** - Arrays for events, links, buckets
- **STRUCT** - Nested structures for complex data

## Working with the Union Schema

When using `read_otlp_metrics()`, you can filter and project to specific metric types:

```sql
-- Create typed tables from union schema
CREATE TABLE metrics_gauge AS
SELECT Timestamp, ServiceName, MetricName, Value
FROM read_otlp_metrics('metrics.jsonl')
WHERE MetricType = 'gauge';

CREATE TABLE metrics_histogram AS
SELECT Timestamp, ServiceName, MetricName, Count, Sum, BucketCounts, ExplicitBounds
FROM read_otlp_metrics('metrics.jsonl')
WHERE MetricType = 'histogram';
```

Alternatively, use the helper functions directly:
```sql
-- Use typed helper function
SELECT * FROM read_otlp_metrics_gauge('metrics.jsonl');
SELECT * FROM read_otlp_metrics_histogram('metrics.jsonl');
```

See [metrics_helpers.md](metrics_helpers.md) for more examples and cookbook patterns.
