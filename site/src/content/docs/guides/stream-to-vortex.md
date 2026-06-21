---
title: "How to Stream to Vortex"
---

Convert OpenTelemetry files to [Vortex](https://duckdb.org/docs/current/core_extensions/vortex) — a compressed, columnar file format with a DuckDB core extension — entirely on your machine. Read OTLP with the `read_otlp_*` table functions, then `COPY` straight to a `.vortex` file. No daemon or catalog required.

This is the file-conversion counterpart to [How to stream to Parquet](../stream-to-parquet/): the same readers, a different output format.

## Prerequisites

- DuckDB 1.5.4 or later.
- OTLP files in protobuf, JSON, or NDJSON. The `read_otlp_*` readers accept all three, plus globs and remote URLs.

## Install the extensions

`otlp` is a community extension and `vortex` is a DuckDB core extension. Load both in the same session:

```sql
INSTALL otlp FROM community;
LOAD otlp;
INSTALL vortex;
LOAD vortex;
```

For a development build of `otlp`, start DuckDB with `-unsigned` and install it from GitHub instead — see [Get Started](../../get-started/).

## Export OTLP to Vortex

Each `COPY ... TO '<file>' (FORMAT vortex)` writes one query result to one Vortex file. Always pass `FORMAT vortex` — without it DuckDB writes CSV. Run one `COPY` per signal you want to keep:

```sql
COPY (SELECT * FROM read_otlp_traces('traces.pb'))
  TO 'traces.vortex' (FORMAT vortex);

COPY (SELECT * FROM read_otlp_logs('logs.pb'))
  TO 'logs.vortex' (FORMAT vortex);

COPY (SELECT * FROM read_otlp_metrics_gauge('metrics.pb'))
  TO 'metrics_gauge.vortex' (FORMAT vortex);
```

Because the readers accept globs, you can fold a directory of NDJSON into a single Vortex file:

```sql
COPY (SELECT * FROM read_otlp_logs('logs/*.jsonl'))
  TO 'logs.vortex' (FORMAT vortex);
```

`COPY` takes any query, so project or filter first to keep only what you need:

```sql
COPY (
  SELECT time_unix_nano, service_name, severity_text, body
  FROM read_otlp_logs('logs.pb')
  WHERE severity_text = 'ERROR'
) TO 'errors.vortex' (FORMAT vortex);
```

Column types are preserved through the round trip, including `TIMESTAMP_NS` timestamps. See the [Schema Reference](../../reference/schemas/) for every column each reader emits.

## Query the Vortex files

Read them back with `read_vortex()`. It accepts a single file or a glob:

```sql
SELECT trace_id, name, service_name, status_code
FROM read_vortex('traces.vortex');

SELECT service_name, severity_text, body
FROM read_vortex('logs.vortex')
WHERE severity_text = 'ERROR';

-- Read every Vortex file in a directory at once.
SELECT count(*) FROM read_vortex('*.vortex');
```

## Convert an existing Parquet export

If you already produce Parquet — for example from [How to stream to Parquet](../stream-to-parquet/) — re-encode it to Vortex without reparsing OTLP:

```sql
COPY (
  SELECT * FROM read_parquet(
    'data/otlp-parquet/otlp_traces/**/*.parquet',
    hive_partitioning = true
  )
) TO 'traces.vortex' (FORMAT vortex);
```

## See also

- [Vortex extension documentation](https://duckdb.org/docs/current/core_extensions/vortex)
- [How to stream to Parquet](../stream-to-parquet/)
- [How to analyze telemetry](../analyze-telemetry/)
- [Schema Reference](../../reference/schemas/)
