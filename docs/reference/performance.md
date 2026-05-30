# Performance Tips

Use these rules before reaching for deeper tuning.

## Prefer the Right Format

- JSON/JSONL is easiest to inspect and works in WASM.
- Protobuf is smaller; native builds usually parse it fastest.
- For repeated queries, convert OTLP exports to Parquet.

```sql
COPY (
  SELECT * FROM read_otlp_traces('traces/*.jsonl')
) TO 'warehouse/traces.parquet' (FORMAT PARQUET);

SELECT count(*)
FROM read_parquet('warehouse/traces.parquet')
WHERE duration > 1000000000;
```

## Read Less Data

Project only the columns you need and avoid broad globs:

```sql
SELECT trace_id, span_name, duration
FROM read_otlp_traces('data/2026-05-30/*.jsonl')
WHERE service_name = 'api-gateway'
  AND duration > 1000000000;
```

The file readers support projection pushdown. Filter pushdown is not currently enabled, so narrower files and Parquet conversion matter for large repeated scans.

## Keep Files Reasonable

Individual file reads are capped at 100 MB. Configure collector rotation so files stay small:

```yaml
rotation:
  max_megabytes: 50
  max_days: 1
```

## Materialize Common Views

```sql
CREATE TABLE hourly_stats AS
SELECT
  date_trunc('hour', timestamp) AS hour,
  service_name,
  count(*) AS span_count,
  avg(duration) / 1000000 AS avg_ms
FROM read_otlp_traces('traces/*.jsonl')
GROUP BY hour, service_name;
```

## Live Ingest

Live ingest buffers accepted rows and seals them in batches. Use `otlp_flush` when readers need fresh rows immediately:

```sql
SELECT * FROM otlp_flush('otlp:localhost:4318');
```

For DuckLake targets, each seal writes small Parquet files; use DuckLake maintenance separately when file counts need cleanup. See [Live Ingest Reference](serve.md).
