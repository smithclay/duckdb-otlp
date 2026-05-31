---
title: "How to Stream OTLP to Cloudflare R2 Data Catalog"
---

Use `otlp_serve(..., catalog := 'r2catalog')` to stream OTLP/HTTP exports into an Iceberg catalog hosted by **Cloudflare R2 Data Catalog**.

This guide is specifically for **R2 Data Catalog**, the managed Cloudflare service that exposes an Iceberg REST catalog for an R2 bucket. It is not the same as writing Iceberg metadata and Parquet files to an ordinary R2 bucket through the S3-compatible API.

> Requires the native extension. Live ingestion is HTTP-only and is not available in WASM builds.

## Create the R2 Data Catalog resources

Choose a bucket name and a Cloudflare account/token that can create R2 buckets and enable R2 Data Catalog:

```bash
export CLOUDFLARE_ACCOUNT_ID=<account-id>
export CLOUDFLARE_API_TOKEN=<r2-admin-read-write-token>
export R2_BUCKET_NAME="duckdb-otlp-r2catalog-${CLOUDFLARE_ACCOUNT_ID}"
```

Do not paste the API token into logs or source files. The token needs R2 storage read/write and R2 Data Catalog read/write permissions. Cloudflare's R2 Admin Read & Write token includes both.

Create the bucket:

```bash
wrangler r2 bucket create "$R2_BUCKET_NAME"
```

Enable R2 Data Catalog on the bucket:

```bash
wrangler r2 bucket catalog enable "$R2_BUCKET_NAME"
```

Wrangler prints the two values DuckDB needs:

```text
Catalog URI: 'https://catalog.cloudflarestorage.com/<account-id>/<bucket-name>'
Warehouse: '<account-id>_<bucket-name>'
```

Save them in your shell:

```bash
export R2_CATALOG_URI="https://catalog.cloudflarestorage.com/${CLOUDFLARE_ACCOUNT_ID}/${R2_BUCKET_NAME}"
export R2_WAREHOUSE="${CLOUDFLARE_ACCOUNT_ID}_${R2_BUCKET_NAME}"
```

You can inspect the catalog status later with:

```bash
wrangler r2 bucket catalog get "$R2_BUCKET_NAME"
```

## Start DuckDB with the Cloudflare token

Start DuckDB from the shell where `CLOUDFLARE_API_TOKEN` is set:

```bash
duckdb r2-data-catalog-otlp.duckdb
```

DuckDB reads the token from the environment in the next step. Keeping it in an environment variable avoids pasting the token into SQL history.

## Attach R2 Data Catalog as an Iceberg catalog

In DuckDB, install and load the extensions, create an Iceberg secret, and attach the R2 Data Catalog warehouse:

```sql
INSTALL otlp FROM community;
LOAD otlp;

INSTALL iceberg;
INSTALL httpfs;

LOAD iceberg;
LOAD httpfs;

SET VARIABLE r2_token = getenv('CLOUDFLARE_API_TOKEN');

CREATE OR REPLACE SECRET r2_secret (
  TYPE ICEBERG,
  TOKEN getvariable('r2_token')
);

ATTACH '<warehouse-name>' AS r2catalog (
  TYPE ICEBERG,
  ENDPOINT '<catalog-uri>'
);

CREATE SCHEMA IF NOT EXISTS r2catalog.otlp;
```

Replace `<warehouse-name>` and `<catalog-uri>` with the values from Wrangler, for example:

```sql
ATTACH '1234567890abcdef1234567890abcdef_duckdb-otlp-r2catalog-1234567890abcdef1234567890abcdef'
AS r2catalog (
  TYPE ICEBERG,
  ENDPOINT 'https://catalog.cloudflarestorage.com/1234567890abcdef1234567890abcdef/duckdb-otlp-r2catalog-1234567890abcdef1234567890abcdef'
);
```

The important pieces are:

- `TYPE ICEBERG` in the secret because DuckDB sends the Cloudflare token to the Iceberg REST catalog.
- `TYPE ICEBERG` in the `ATTACH` because R2 Data Catalog exposes an Iceberg REST catalog.
- `ENDPOINT` is the **Catalog URI** from Wrangler.
- The attach string is the **Warehouse** from Wrangler, not an `r2://` or `s3://` path.

## Start the ingest server

Start the OTLP/HTTP server and target the attached R2 Data Catalog:

```sql
SELECT listen_url, catalog_name, schema_name
FROM otlp_serve(
  'otlp:localhost:4318',
  catalog := 'r2catalog',
  schema := 'otlp',
  token := 'dev-token-123456'
);
```

`otlp_serve` creates these Iceberg tables in the R2 Data Catalog namespace if they do not already exist:

- `r2catalog.otlp.otlp_logs`
- `r2catalog.otlp.otlp_traces`
- `r2catalog.otlp.otlp_metrics_gauge`
- `r2catalog.otlp.otlp_metrics_sum`
- `r2catalog.otlp.otlp_metrics_histogram`
- `r2catalog.otlp.otlp_metrics_exp_histogram`

Leave this DuckDB session running while clients send OTLP/HTTP requests.

## POST a log record

In another terminal:

```bash
curl -sS http://localhost:4318/v1/logs \
  -H 'Authorization: Bearer dev-token-123456' \
  -H 'Content-Type: application/json' \
  -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"r2-data-catalog-demo"}},{"key":"deployment.environment","value":{"stringValue":"docs"}}]},"scopeLogs":[{"scope":{"name":"duckdb-otlp-guide"},"logRecords":[{"timeUnixNano":"1704067200000000000","observedTimeUnixNano":"1704067200123456789","severityNumber":9,"severityText":"INFO","body":{"stringValue":"hello from Cloudflare R2 Data Catalog"},"attributes":[{"key":"guide","value":{"stringValue":"stream-to-r2-data-catalog"}}]}]}]}]}'
```

**Response:**

```json
{"status":"buffered","rows":1,"batches":1}
```

Rows are accepted before they are durable. They commit automatically in the background, on graceful `otlp_stop`, or immediately on optional `otlp_flush`.

## Query committed rows

For a deterministic check, force a synchronous commit:

```sql
SELECT * FROM otlp_flush('otlp:localhost:4318');
```

Then query the Iceberg table hosted by R2 Data Catalog:

```sql
SELECT
  service_name,
  severity_text,
  body,
  resource_attributes,
  log_attributes
FROM r2catalog.otlp.otlp_logs
WHERE service_name = 'r2-data-catalog-demo'
ORDER BY time_unix_nano DESC
LIMIT 5;
```

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

## Stop cleanly

```sql
SELECT status FROM otlp_stop('otlp:localhost:4318');
```

`otlp_stop` commits remaining buffered rows before returning. A plain database or connection close stops the server but does not commit buffered rows, so stop the server before closing DuckDB.

## Clean up

Drop the Iceberg tables before disabling the catalog and deleting the R2 bucket:

```sql
DROP TABLE IF EXISTS r2catalog.otlp.otlp_logs;
DROP TABLE IF EXISTS r2catalog.otlp.otlp_traces;
DROP TABLE IF EXISTS r2catalog.otlp.otlp_metrics_gauge;
DROP TABLE IF EXISTS r2catalog.otlp.otlp_metrics_sum;
DROP TABLE IF EXISTS r2catalog.otlp.otlp_metrics_histogram;
DROP TABLE IF EXISTS r2catalog.otlp.otlp_metrics_exp_histogram;
DETACH r2catalog;
```

Then disable the catalog and delete the bucket:

```bash
wrangler r2 bucket catalog disable "$R2_BUCKET_NAME"
wrangler r2 bucket delete "$R2_BUCKET_NAME"
```

If bucket deletion reports that the bucket is not empty, delete the remaining catalog objects and retry. For a fresh bucket created only for this guide, those objects are the Iceberg metadata and data files under `__r2_data_catalog/`:

```bash
export R2_OBJECTS_API="https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/r2/buckets/${R2_BUCKET_NAME}/objects"

curl -fsS \
  -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
  "$R2_OBJECTS_API" |
  jq -r '.result[]?.key' |
  while IFS= read -r key; do
    encoded="$(node -e 'process.stdout.write(encodeURIComponent(process.argv[1]))' "$key")"
    curl -fsS -X DELETE \
      -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
      "${R2_OBJECTS_API}/${encoded}" >/dev/null
  done

wrangler r2 bucket delete "$R2_BUCKET_NAME"
```

## Notes

- R2 Data Catalog is currently a Cloudflare open beta.
- R2 Data Catalog does not currently support R2 buckets in a non-default jurisdiction.
- DuckDB uses a `TYPE ICEBERG` secret here for the Cloudflare API token. Do not use DuckDB's `TYPE s3` secret for R2 Data Catalog attachment.
- R2 Data Catalog accepts Iceberg `TIMESTAMP` columns, not DuckDB's `TIMESTAMP_NS` type. Live ingest tables therefore store OTLP timestamp columns as DuckDB `TIMESTAMP` with microsecond precision.
- OTLP count and flag fields use signed SQL integer types so the created tables stay compatible with Iceberg catalogs that do not accept DuckDB unsigned integer types.
- Use `create_tables := false` only when all six target tables already exist with the exact DuckDB-visible columns and types expected by `duckdb-otlp`.
- R2 Data Catalog has its own table maintenance features such as compaction and snapshot expiration. `duckdb-otlp` only probes the generic DuckDB `CHECKPOINT <catalog>` hook on its internal maintenance cadence; if the catalog reports checkpointing as unsupported, automatic maintenance is disabled for that server and normal ingest/flush/stop durability behavior is unchanged.

## See also

- [Cloudflare R2 Data Catalog DuckDB docs](https://developers.cloudflare.com/r2/data-catalog/config-examples/duckdb/)
- [Cloudflare R2 Data Catalog management docs](https://developers.cloudflare.com/r2/data-catalog/manage-catalogs/)
- [DuckDB Iceberg REST catalog docs](https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_rest_catalogs)
- [Live Ingest Reference](../../reference/serve/)
- [Live Ingest Quickstart](../../quickstart/serve/)
