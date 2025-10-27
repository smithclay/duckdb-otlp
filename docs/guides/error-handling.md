# Error Handling

Learn how to handle malformed or invalid OTLP data using the DuckDB OTLP extension's error handling features.

## Error Handling Modes

The extension provides three error handling modes via the `on_error` parameter:

### 1. Fail (Default)

Stop processing and raise an error on the first parse failure:

```sql
SELECT * FROM read_otlp_traces('traces.jsonl');
-- Error: Failed to parse OTLP record at line 42
```

Use this mode when data quality is critical and you want to catch issues immediately.

### 2. Skip

Skip invalid records and continue processing:

```sql
SELECT * FROM read_otlp_traces('traces.jsonl', on_error := 'skip');
```

Invalid records are silently discarded. This is useful when:
- You have a large dataset with occasional corruption
- You want to extract as much valid data as possible
- Missing records are acceptable

### 3. Nullify

Emit NULL rows for invalid records, preserving row count:

```sql
SELECT * FROM read_otlp_traces('traces.jsonl', on_error := 'nullify');
```

Invalid records result in rows with NULL values in all columns. Use this when:
- You need to maintain row/line number alignment
- You want to count how many records failed
- You need to correlate errors with source data

## Check Scan Statistics

After processing data with error handling enabled, inspect scan diagnostics:

```sql
SELECT * FROM read_otlp_scan_stats();
```

Returns:
- `records_scanned` - Total records processed
- `parse_errors` - Number of parse failures
- `records_skipped` - Records discarded (skip mode)
- `records_nullified` - Records converted to NULL (nullify mode)
- `format_detected` - Detected file format (json/protobuf)
- `files_processed` - Number of files scanned

## Example: Handle Noisy Logs

Process a large log export that may contain malformed records:

```sql
-- Skip invalid records
SELECT *
FROM read_otlp_logs('s3://otel-bucket/logs/*.jsonl', on_error := 'skip')
WHERE SeverityText IN ('ERROR', 'FATAL');

-- Check how many records were skipped
SELECT
  records_scanned,
  parse_errors,
  records_scanned - parse_errors AS valid_records,
  (parse_errors * 100.0 / records_scanned) AS error_percentage
FROM read_otlp_scan_stats();
```

## Example: Identify Problem Files

Find which files have the most errors:

```sql
-- Process with nullify mode
CREATE TABLE traces_with_nulls AS
SELECT *, current_setting('filename') AS source_file
FROM read_otlp_traces('traces/*.jsonl', on_error := 'nullify');

-- Count nulls by file
SELECT
  source_file,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN TraceId IS NULL THEN 1 ELSE 0 END) AS null_rows
FROM traces_with_nulls
GROUP BY source_file
HAVING null_rows > 0
ORDER BY null_rows DESC;
```

## Example: Combine with Size Limits

Handle both parse errors and oversized documents:

```sql
SELECT *
FROM read_otlp_metrics('metrics.pb',
                       on_error := 'skip',
                       max_document_bytes := 200000000);  -- 200 MB limit

-- Check stats
SELECT * FROM read_otlp_scan_stats();
```

## Size Limits

Individual JSON/Protobuf documents are limited to **100 MB by default** to prevent memory exhaustion. Override this limit when needed:

```sql
-- Increase limit for large documents
SELECT * FROM read_otlp_traces('huge_traces.jsonl',
                                max_document_bytes := 500000000);  -- 500 MB
```

**Note**: This limit applies to:
- Individual documents in JSONL files (one per line)
- Entire protobuf files
- Does NOT limit total file size for streaming JSONL

## Best Practices

### Development

Use `fail` mode (default) during development to catch data quality issues:

```sql
SELECT * FROM read_otlp_traces('test_data.jsonl');
```

### Production

Use `skip` mode in production pipelines to handle occasional corruption:

```sql
INSERT INTO traces_archive
SELECT * FROM read_otlp_traces('prod/*.jsonl', on_error := 'skip');
```

### Debugging

Use `nullify` mode to investigate parsing failures:

```sql
-- Find records that failed to parse
SELECT *
FROM read_otlp_logs('logs.jsonl', on_error := 'nullify')
WHERE Timestamp IS NULL;
```

### Monitoring

Always check scan statistics after processing:

```sql
-- Alert if error rate exceeds threshold
SELECT
  CASE
    WHEN (parse_errors * 100.0 / records_scanned) > 5.0
    THEN 'High error rate detected!'
    ELSE 'OK'
  END AS status
FROM read_otlp_scan_stats();
```

## Common Error Causes

- **Invalid JSON syntax** - Malformed JSONL, truncated files
- **Missing required fields** - OTLP spec violations
- **Type mismatches** - Wrong data types for expected fields
- **Oversized documents** - Documents exceeding `max_document_bytes`
- **Corrupted protobuf** - Binary data corruption
- **Encoding issues** - Invalid UTF-8 sequences

## Discover All Options

View all available configuration options:

```sql
SELECT * FROM read_otlp_options();
```

## See Also

- [API Reference](../reference/api.md#configuration-options) - Complete parameter documentation
- [Cookbook](cookbook.md#filter-noisy-telemetry-during-ingest) - Additional error handling recipes
- [Filtering Logs](filtering-logs.md) - Apply filters with error handling
