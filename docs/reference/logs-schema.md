# Logs Schema

The source of truth is [schemas.md#logs-read_otlp_logs](schemas.md#logs-read_otlp_logs).

`read_otlp_logs(path)` returns 15 `snake_case` columns for timestamps, severity, body, resource attributes, scope metadata, and trace/span correlation.

```sql
SELECT timestamp, service_name, severity_text, body
FROM read_otlp_logs('logs.jsonl')
WHERE severity_text IN ('ERROR', 'FATAL')
ORDER BY timestamp DESC;
```
