---
title: "Performance Tips"
---

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
WHERE duration_time_unix_nano > 1000000000;
```

## Read Less Data

Project only the columns you need and avoid broad globs:

```sql
SELECT trace_id, name, duration_time_unix_nano
FROM read_otlp_traces('data/2026-05-30/*.jsonl')
WHERE service_name = 'api-gateway'
  AND duration_time_unix_nano > 1000000000;
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
  date_trunc('hour', start_time_unix_nano) AS hour,
  service_name,
  count(*) AS span_count,
  avg(duration_time_unix_nano) / 1000000 AS avg_ms
FROM read_otlp_traces('traces/*.jsonl')
GROUP BY hour, service_name;
```

## Live Ingest

Live ingest buffers accepted rows and commits them in batches automatically. Current native builds commit when the oldest buffered row is about 5 seconds old, or when admitted request-body bytes reach about 64 MiB. `otlp_flush` is an optional low-latency read path for cases where readers need fresh rows immediately while the server keeps running.

For named catalog targets, successful automatic row-seals may occasionally be followed by best-effort catalog-native `CHECKPOINT <catalog>` outside the ingest transaction when recent ingest rate and pending bytes leave ample admission headroom. This is conservative internal scheduling: it is skipped for the default catalog, sustained high ingest, high pending buffered bytes, explicit `otlp_flush`, and shutdown drains. DuckLake uses this checkpoint hook for its own maintenance policy; unsupported catalog implementations are logged and disabled for that server. See [Live Ingest Reference](../serve/).
