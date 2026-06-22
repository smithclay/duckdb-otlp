---
title: "Performance Tips"
---

Use these rules before reaching for deeper tuning.

## Prefer the Right Format

- JSON/JSONL is easiest to inspect and works in WASM.
- Protobuf is smaller; native builds parse it fastest in most workloads.
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

The file readers support projection pushdown. They skip filter pushdown for OTLP scans today, so narrower files and Parquet conversion matter for large repeated scans.

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

## Promote Hot Attributes

Filtering an attribute that lives inside the `resource_attributes` / `scope_attributes` JSON blob is a full scan — `json_extract_string(...)` has no row-group statistics. If you repeatedly filter or group by a resource/scope attribute (`deployment.environment`, `k8s.namespace.name`, `cloud.region`, ...), have the live server promote it into a real column at ingest so the scanner can prune by min/max:

```sql
SELECT * FROM otlp_serve('otlp:0.0.0.0:4318', catalog := 'lake',
    promote_resource_attributes := 'deployment.environment, k8s.namespace.name');
```

This trades a little ingest work for much cheaper repeated reads. See [Attribute promotion](../serve/#attribute-promotion).

## Live Ingest

Live ingest buffers accepted rows and commits them in batches. Current native builds commit when the oldest buffered row is about 5 seconds old, or when admitted request-body bytes reach about 64 MiB. Use `otlp_flush` when readers need fresh rows while the server keeps running.

For named catalog targets, the writer may run best-effort catalog-native `CHECKPOINT <catalog>` outside the ingest transaction after successful automatic row-seals when recent ingest rate and pending bytes leave ample admission headroom. The writer skips the default catalog, sustained high ingest, high pending buffered bytes, explicit `otlp_flush`, and shutdown drains. DuckLake uses this checkpoint hook for its own maintenance policy; the server logs unsupported catalog implementations and disables maintenance for that server. See [Live Ingest Reference](../serve/).
