# Performance Tips

Optimize queries and scans when working with large OTLP datasets.

## File Format Considerations

### JSON vs Protobuf

- **JSON** - Human-readable, larger file sizes, slightly slower parsing
- **Protobuf** - Binary, smaller files, faster parsing (native builds only)

```sql
-- Both formats work identically
SELECT * FROM read_otlp_traces('traces.jsonl');  -- JSON
SELECT * FROM read_otlp_traces('traces.pb');     -- Protobuf
```

Use protobuf for production data pipelines when file size and parsing speed matter.

### Compress Source Files

GZIP-compressed files are supported and recommended:

```sql
-- Works with compressed files
SELECT * FROM read_otlp_traces('traces.jsonl.gz');
```

Compression reduces storage and network transfer costs with minimal CPU overhead.

## Query Optimization

### Projection Pushdown

Select only the columns you need:

```sql
-- Slow: reads all 22 columns
SELECT * FROM read_otlp_traces('traces.jsonl');

-- Fast: reads only 3 columns
SELECT TraceId, SpanName, Duration
FROM read_otlp_traces('traces.jsonl');
```

### Filter Pushdown

Apply filters early to reduce data transfer:

```sql
-- Good: filter applied during scan
SELECT * FROM read_otlp_traces('traces.jsonl')
WHERE ServiceName = 'api-gateway'
  AND Duration > 1000000000;

-- Better: combine with projection
SELECT TraceId, SpanName, Duration
FROM read_otlp_traces('traces.jsonl')
WHERE ServiceName = 'api-gateway'
  AND Duration > 1000000000;
```

### Use Parquet for Repeated Queries

Convert to Parquet for better query performance:

```sql
-- One-time conversion
COPY (
  SELECT * FROM read_otlp_traces('traces/*.jsonl')
) TO 'traces.parquet' (FORMAT PARQUET);

-- Subsequent queries are much faster
SELECT * FROM read_parquet('traces.parquet')
WHERE Duration > 1000000000;
```

Parquet provides:
- Column-based storage (better compression, faster scans)
- Built-in indexes and statistics
- Native DuckDB support (no extension needed)

## Glob Pattern Optimization

### Use Specific Patterns

```sql
-- Slow: scans all files
SELECT * FROM read_otlp_traces('data/**/*.jsonl');

-- Faster: targets specific date
SELECT * FROM read_otlp_traces('data/2024-01-15/*.jsonl');
```

### Partition Files by Date/Service

Organize files for efficient querying:

```
data/
  2024-01-15/
    service-a-traces.jsonl
    service-b-traces.jsonl
  2024-01-16/
    service-a-traces.jsonl
    service-b-traces.jsonl
```

```sql
-- Query single day
SELECT * FROM read_otlp_traces('data/2024-01-15/*.jsonl');

-- Query single service
SELECT * FROM read_otlp_traces('data/*/service-a-traces.jsonl');
```

## Materialize Common Queries

Create tables for frequently-accessed aggregations:

```sql
-- Create materialized view
CREATE TABLE hourly_stats AS
SELECT
  date_trunc('hour', Timestamp) AS hour,
  ServiceName,
  COUNT(*) AS span_count,
  AVG(Duration) / 1000000 AS avg_ms,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY Duration) / 1000000 AS p95_ms
FROM read_otlp_traces('traces/*.jsonl')
GROUP BY hour, ServiceName;

-- Query the table instead of raw files
SELECT * FROM hourly_stats
WHERE hour >= '2024-01-01'
ORDER BY avg_ms DESC;
```

## Batch Processing

Process large datasets in batches:

```sql
-- Process one day at a time
FOR day IN (SELECT generate_series(...)::DATE AS day)
LOOP
  INSERT INTO traces_archive
  SELECT * FROM read_otlp_traces('data/' || day || '/*.jsonl');
END LOOP;
```

## Error Handling Performance

### Skip vs Nullify

- **`skip`** - Faster (discards invalid records immediately)
- **`nullify`** - Slower (creates NULL rows)

```sql
-- Faster for production pipelines
SELECT * FROM read_otlp_traces('traces.jsonl', on_error := 'skip');

-- Use nullify only when debugging
SELECT * FROM read_otlp_traces('traces.jsonl', on_error := 'nullify');
```

## Memory Considerations

### Document Size Limits

Adjust `max_document_bytes` for your workload:

```sql
-- Default 100MB is safe for most workloads
SELECT * FROM read_otlp_traces('traces.jsonl');

-- Increase for large documents (uses more memory)
SELECT * FROM read_otlp_traces('traces.jsonl',
                                max_document_bytes := 500000000);
```

### Streaming vs Materialization

```sql
-- Streaming: low memory, slower
SELECT COUNT(*)
FROM read_otlp_traces('traces/*.jsonl')
WHERE Duration > 1000000000;

-- Materialized: high memory, faster for multiple queries
CREATE TABLE traces AS
SELECT * FROM read_otlp_traces('traces/*.jsonl');

SELECT COUNT(*) FROM traces WHERE Duration > 1000000000;
```

## Parallel Processing

DuckDB automatically parallelizes scans. Ensure enough threads:

```sql
-- Check thread count
PRAGMA threads;

-- Set thread count (default: CPU cores)
PRAGMA threads=8;
```

## Cloud Storage Performance

### S3/GCS/Azure Optimization

- Use region-local buckets to reduce latency
- Enable HTTP connection pooling
- Consider `s3://` vs `https://` URLs

```sql
-- Direct S3 access (recommended)
SELECT * FROM read_otlp_traces('s3://my-bucket/traces/*.jsonl');

-- HTTPS access (slower)
SELECT * FROM read_otlp_traces('https://my-bucket.s3.amazonaws.com/traces/*.jsonl');
```

## Profiling Queries

Use DuckDB's profiler to identify bottlenecks:

```sql
PRAGMA enable_profiling;
PRAGMA profiling_output = '/tmp/profile.json';

SELECT * FROM read_otlp_traces('traces.jsonl')
WHERE Duration > 1000000000;

-- View profile
SELECT * FROM '/tmp/profile.json';
```

## Benchmarking

Test different approaches:

```sql
-- Time a query
.timer on
SELECT COUNT(*) FROM read_otlp_traces('traces.jsonl');
.timer off
```

## See Also

- [Exporting to Parquet](../guides/exporting-to-parquet.md) - Convert for better performance
- [Error Handling](../guides/error-handling.md) - Error handling performance tips
- [DuckDB Performance Guide](https://duckdb.org/docs/guides/performance/overview)
