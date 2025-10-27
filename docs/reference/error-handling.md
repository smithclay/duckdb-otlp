# Error Handling Reference

See the [Error Handling Guide](../guides/error-handling.md) for comprehensive documentation on handling malformed OTLP data.

## Quick Reference

### Error Modes

```sql
-- Fail on first error (default)
SELECT * FROM read_otlp_traces('traces.jsonl');

-- Skip invalid records
SELECT * FROM read_otlp_traces('traces.jsonl', on_error := 'skip');

-- Emit NULL for invalid records
SELECT * FROM read_otlp_traces('traces.jsonl', on_error := 'nullify');
```

### Scan Statistics

```sql
SELECT * FROM read_otlp_scan_stats();
```

Returns:
- `records_scanned` - Total records processed
- `parse_errors` - Number of failures
- `records_skipped` - Skipped records (skip mode)
- `records_nullified` - NULL records (nullify mode)
- `format_detected` - File format (json/protobuf)
- `files_processed` - Number of files

### Size Limits

```sql
-- Default 100MB limit
SELECT * FROM read_otlp_traces('traces.jsonl');

-- Custom limit (in bytes)
SELECT * FROM read_otlp_traces('traces.jsonl', max_document_bytes := 500000000);
```

## See Also

- [Error Handling Guide](../guides/error-handling.md) - Detailed examples and best practices
- [API Reference](api.md#configuration-options) - All available parameters
