# Traces Schema

The source of truth is [schemas.md#traces-read_otlp_traces](schemas.md#traces-read_otlp_traces).

`read_otlp_traces(path)` returns 25 `snake_case` columns for span identity, timing, status, resource attributes, scope metadata, events, links, and drop counts.

```sql
SELECT trace_id, span_name, duration / 1000000 AS duration_ms
FROM read_otlp_traces('traces.jsonl')
WHERE duration > 1000000000;
```
