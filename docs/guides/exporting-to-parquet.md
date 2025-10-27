# Exporting to Parquet

Learn how to export OpenTelemetry data to Parquet format for long-term storage and analysis.

## Why Export to Parquet?

Parquet provides several benefits:
- **Compression** - Significantly smaller file sizes compared to JSON
- **Columnar storage** - Efficient for analytical queries
- **Schema preservation** - Retains all DuckDB type information
- **Universal compatibility** - Works with Spark, Pandas, Arrow, and other tools
- **No extension required** - Read Parquet files directly with DuckDB without loading the OTLP extension

## Export Traces to Parquet

Convert OTLP trace files to Parquet:

```sql
LOAD otlp;

COPY (
  SELECT * FROM read_otlp_traces('otel-export/*.jsonl')
) TO 'warehouse/daily_traces.parquet' (FORMAT PARQUET);
```

Later, query the Parquet file without the extension:

```sql
SELECT TraceId, SpanName, Duration / 1000000 AS duration_ms
FROM read_parquet('warehouse/daily_traces.parquet')
WHERE Duration >= 1000000000
ORDER BY Duration DESC;
```

## Export Logs to Parquet

Archive log data for compliance or analysis:

```sql
LOAD otlp;

COPY (
  SELECT * FROM read_otlp_logs('logs/*.jsonl')
  WHERE SeverityText IN ('ERROR', 'WARN', 'FATAL')
) TO 'warehouse/error_logs.parquet' (FORMAT PARQUET);
```

## Export Metrics to Parquet

Archive metrics with full schema preservation:

```sql
LOAD otlp;

-- Export all metrics (union schema)
COPY (
  SELECT * FROM read_otlp_metrics('metrics/*.jsonl')
) TO 'warehouse/daily_metrics.parquet' (FORMAT PARQUET);

-- Export specific metric types
COPY (
  SELECT * FROM read_otlp_metrics_gauge('metrics/*.jsonl')
) TO 'warehouse/gauge_metrics.parquet' (FORMAT PARQUET);
```

## Partitioned Exports

Export data partitioned by date or service:

```sql
-- Export traces partitioned by day
COPY (
  SELECT *, date_trunc('day', Timestamp) AS partition_date
  FROM read_otlp_traces('traces/*.jsonl')
) TO 'warehouse/traces' (FORMAT PARQUET, PARTITION_BY (partition_date));
```

## Incremental Exports

Append new data to existing Parquet datasets:

```sql
-- Create initial export
COPY (
  SELECT * FROM read_otlp_traces('day1/*.jsonl')
) TO 'warehouse/traces.parquet' (FORMAT PARQUET);

-- Append new data
INSERT INTO read_parquet('warehouse/traces.parquet')
SELECT * FROM read_otlp_traces('day2/*.jsonl');
```

## Combine Multiple Signals

Create a unified data warehouse:

```sql
-- Traces
COPY (
  SELECT * FROM read_otlp_traces('otel-export/*.jsonl')
) TO 'warehouse/traces.parquet' (FORMAT PARQUET);

-- Logs
COPY (
  SELECT * FROM read_otlp_logs('otel-export/*.jsonl')
) TO 'warehouse/logs.parquet' (FORMAT PARQUET);

-- Metrics
COPY (
  SELECT * FROM read_otlp_metrics('otel-export/*.jsonl')
) TO 'warehouse/metrics.parquet' (FORMAT PARQUET);
```

## Join Across Parquet Files

Query multiple Parquet files together:

```sql
SELECT
  t.SpanName,
  t.Duration / 1000000 AS duration_ms,
  COUNT(l.Body) AS error_count
FROM read_parquet('warehouse/traces.parquet') t
LEFT JOIN read_parquet('warehouse/logs.parquet') l
  ON t.TraceId = l.TraceId
  AND l.SeverityText = 'ERROR'
GROUP BY t.SpanName, t.Duration
ORDER BY error_count DESC;
```

## Export to Cloud Storage

Write directly to S3, Azure, or GCS:

```sql
-- S3 export
COPY (
  SELECT * FROM read_otlp_traces('local/*.jsonl')
) TO 's3://my-bucket/warehouse/traces.parquet' (FORMAT PARQUET);

-- Azure export
COPY (
  SELECT * FROM read_otlp_logs('local/*.jsonl')
) TO 'azure://my-container/logs.parquet' (FORMAT PARQUET);
```

## Compression Options

Control Parquet compression:

```sql
COPY (
  SELECT * FROM read_otlp_traces('traces/*.jsonl')
) TO 'warehouse/traces.parquet' (
  FORMAT PARQUET,
  COMPRESSION 'zstd',  -- Options: snappy, gzip, zstd, uncompressed
  ROW_GROUP_SIZE 100000
);
```

## More Examples

For additional recipes, see the [Cookbook](cookbook.md#export-telemetry-to-parquet-and-read-it-back).

## See Also

- [DuckDB Parquet Documentation](https://duckdb.org/docs/data/parquet/overview)
- [Building Dashboards](building-dashboards.md) - Visualize Parquet data
- [Working with Metrics](working-with-metrics.md) - Metrics-specific export patterns
