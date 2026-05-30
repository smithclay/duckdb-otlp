# How to Stream OTLP to Iceberg

Use `otlp_serve(..., catalog := 'iceberg_catalog')` to stream OTLP/HTTP exports into an attached Iceberg REST catalog. The DuckDB Iceberg extension must be attached with write support; plain metadata-file scans are not enough for live ingest.

> Requires the native extension. Live ingestion is HTTP-only and is not available in WASM builds.

## Attach an Iceberg REST catalog

Install and load the extensions, create credentials, and attach your Iceberg REST catalog:

```sql
INSTALL otlp FROM community;
LOAD otlp;

INSTALL iceberg;
LOAD iceberg;
LOAD httpfs;

CREATE SECRET iceberg_secret (
  TYPE iceberg,
  CLIENT_ID 'admin',
  CLIENT_SECRET 'password',
  OAUTH2_SERVER_URI 'https://iceberg.example.com/v1/oauth/tokens'
);

ATTACH 'warehouse' AS iceberg_catalog (
  TYPE iceberg,
  SECRET iceberg_secret,
  ENDPOINT 'https://iceberg.example.com'
);
```

If your catalog uses a bearer token instead of OAuth client credentials:

```sql
CREATE SECRET iceberg_secret (
  TYPE iceberg,
  TOKEN 'bearer-token'
);
```

DuckDB's Iceberg REST catalog support includes `CREATE TABLE`, `INSERT INTO`, and `SELECT` when the catalog is attached. See the [DuckDB Iceberg REST catalog docs](https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_rest_catalogs) for provider-specific options such as R2, Polaris, Lakekeeper, Glue, and S3 Tables.

## Create a schema

Create the schema that will hold the OTLP tables:

```sql
CREATE SCHEMA IF NOT EXISTS iceberg_catalog.otlp;
```

## Start the ingest server

```sql
SELECT listen_url, auth_token, catalog_name, schema_name
FROM otlp_serve(
  'otlp:localhost:4318',
  catalog := 'iceberg_catalog',
  schema := 'otlp',
  token := 'dev-token-123456'
);
```

`otlp_serve` creates these tables if they do not already exist:

- `iceberg_catalog.otlp.otlp_logs`
- `iceberg_catalog.otlp.otlp_traces`
- `iceberg_catalog.otlp.otlp_metrics_gauge`
- `iceberg_catalog.otlp.otlp_metrics_sum`
- `iceberg_catalog.otlp.otlp_metrics_histogram`
- `iceberg_catalog.otlp.otlp_metrics_exp_histogram`

Leave this DuckDB session running while clients send OTLP/HTTP requests.

## POST logs

In another terminal:

```bash
curl -sS http://localhost:4318/v1/logs \
  -H 'Authorization: Bearer dev-token-123456' \
  -H 'Content-Type: application/json' \
  -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"iceberg-demo"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1704067200000000000","severityText":"INFO","body":{"stringValue":"hello from iceberg"}}]}]}]}'
```

**Response:**

```json
{"status":"buffered","rows":1,"batches":1}
```

Rows are accepted before they are durable. They commit automatically in the background, on graceful `otlp_stop`, or immediately on optional `otlp_flush`.

## Query committed rows

To inspect buffer and commit counters:

```sql
SELECT
  catalog_name,
  schema_name,
  total_rows,
  buffered_rows,
  last_seal_age_ms AS last_commit_age_ms,
  seals_total AS commits_total
FROM otlp_server_list();
```

When `buffered_rows` returns to `0`, query the Iceberg table:

```sql
SELECT timestamp, service_name, severity_text, body
FROM iceberg_catalog.otlp.otlp_logs
ORDER BY timestamp DESC
LIMIT 20;
```

## Stop cleanly

```sql
SELECT status FROM otlp_stop('otlp:localhost:4318');
```

`otlp_stop` commits remaining buffered rows before returning. A plain database or connection close stops the server but does not commit buffered rows, so stop the server before closing DuckDB.

## Notes

- Use an Iceberg REST catalog for write support. `iceberg_scan` over local metadata files is read-oriented and is not a target catalog for `otlp_serve`.
- Create the target schema before starting the server.
- Use `create_tables := false` only when all six target tables already exist with the exact DuckDB-visible columns and types expected by `duckdb-otlp`.

## See also

- [DuckDB Iceberg REST catalog docs](https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_rest_catalogs)
- [Live Ingest Reference](../reference/serve.md)
- [Live Ingest Quickstart](../quickstart/serve.md)
