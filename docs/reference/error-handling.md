# Error Handling Reference

File readers fail fast on invalid input:

```sql
SELECT * FROM read_otlp_traces('traces.jsonl');
```

Typical failures include unmatched globs, malformed OTLP payloads, unsupported metric shapes, and files larger than the 100 MB file-read limit.

The current file-reader API does not expose `on_error`, scan-statistics, or max-document-size parameters. Split or validate problematic files before querying them.

For live ingest HTTP status codes and response bodies, see [OTLP HTTP Ingest Server](serve.md#responses-and-status-codes).

## See Also

- [Error Handling Guide](../guides/error-handling.md)
- [API Reference](api.md)
- [Schema Reference](schemas.md)
