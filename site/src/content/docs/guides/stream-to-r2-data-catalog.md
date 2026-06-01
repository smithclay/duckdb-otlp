---
title: "How to Stream OTLP to Cloudflare R2 Data Catalog"
---

Run the `duckdb-otlp` Docker image in `r2-data-catalog` mode to stream OTLP/HTTP exports into an Iceberg catalog hosted by **Cloudflare R2 Data Catalog**. The container initializes DuckDB, loads the required extensions, attaches the R2 Data Catalog warehouse, starts the ingest server, and commits accepted rows in batches.

This guide is specifically for **R2 Data Catalog**, the managed Cloudflare service that exposes an Iceberg REST catalog for an R2 bucket. It is not the same as writing Iceberg metadata and Parquet files to an ordinary R2 bucket through the S3-compatible API.

> Live ingestion is OTLP/HTTP on port `4318`. The ingest server is not available in WASM builds.

## Create R2 Data Catalog resources

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

Wrangler prints the catalog values DuckDB needs:

```text
Catalog URI: 'https://catalog.cloudflarestorage.com/<account-id>/<bucket-name>'
Warehouse: '<account-id>_<bucket-name>'
```

Save them in your shell:

```bash
export R2_CATALOG_URI="https://catalog.cloudflarestorage.com/${CLOUDFLARE_ACCOUNT_ID}/${R2_BUCKET_NAME}"
export R2_WAREHOUSE="${CLOUDFLARE_ACCOUNT_ID}_${R2_BUCKET_NAME}"
```

You also need an R2 S3-compatible access key pair that can write objects to the bucket. Save those values as `CLOUDFLARE_ACCESS_KEY_ID` and `CLOUDFLARE_SECRET_ACCESS_KEY` in the next step.

## Configure

Create `cloudflare.env`:

```ini
DUCKDB_MODE=r2-data-catalog
DUCKDB_OTLP_TOKEN=dev-token-123456

DUCKDB_CATALOG=r2catalog
DUCKDB_SCHEMA=otlp

CLOUDFLARE_ACCOUNT_ID=<account-id>
CLOUDFLARE_R2_BUCKET=<bucket-name>
CLOUDFLARE_ACCESS_KEY_ID=<r2-s3-access-key-id>
CLOUDFLARE_SECRET_ACCESS_KEY=<r2-s3-secret-access-key>
CLOUDFLARE_CATALOG_URI=https://catalog.cloudflarestorage.com/<account-id>/<bucket-name>
CLOUDFLARE_CATALOG_TOKEN=<r2-admin-read-write-token>
```

## Start the server

```bash
docker run --rm --name duckdb-otlp \
  --env-file cloudflare.env \
  -p 4318:4318 \
  ghcr.io/smithclay/duckdb-otlp:latest
```

The container creates these Iceberg tables in the R2 Data Catalog namespace if they do not already exist:

- `r2catalog.otlp.otlp_logs`
- `r2catalog.otlp.otlp_traces`
- `r2catalog.otlp.otlp_metrics_gauge`
- `r2catalog.otlp.otlp_metrics_sum`
- `r2catalog.otlp.otlp_metrics_histogram`
- `r2catalog.otlp.otlp_metrics_exp_histogram`

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

Rows are accepted before they are durable. They commit automatically in the background, on graceful shutdown, or immediately after an explicit flush.

## Query committed rows

Flush and query through the running container:

```bash
docker exec duckdb-otlp sh -c \
  "printf '%s\n' \
    \"SELECT * FROM otlp_flush('otlp:0.0.0.0:4318');\" \
    \"SELECT service_name, severity_text, body\" \
    \"FROM r2catalog.otlp.otlp_logs\" \
    \"WHERE service_name = 'r2-data-catalog-demo'\" \
    \"ORDER BY timestamp DESC\" \
    \"LIMIT 5;\" \
    > /tmp/duckdb-otlp.sql"

docker logs --tail 80 duckdb-otlp
```

## Stop cleanly

If you plan to delete the R2 Data Catalog resources immediately, skip this step and use [Clean up](#clean-up) instead.

```bash
docker stop duckdb-otlp
```

The image sends `otlp_stop('otlp:0.0.0.0:4318')` during shutdown, so remaining buffered rows are committed before the process exits.

## Clean up

Drop the Iceberg tables before disabling the catalog and deleting the R2 bucket:

```bash
docker exec duckdb-otlp sh -c \
  "printf '%s\n' \
    \"SELECT status FROM otlp_stop('otlp:0.0.0.0:4318');\" \
    \"DROP TABLE IF EXISTS r2catalog.otlp.otlp_logs;\" \
    \"DROP TABLE IF EXISTS r2catalog.otlp.otlp_traces;\" \
    \"DROP TABLE IF EXISTS r2catalog.otlp.otlp_metrics_gauge;\" \
    \"DROP TABLE IF EXISTS r2catalog.otlp.otlp_metrics_sum;\" \
    \"DROP TABLE IF EXISTS r2catalog.otlp.otlp_metrics_histogram;\" \
    \"DROP TABLE IF EXISTS r2catalog.otlp.otlp_metrics_exp_histogram;\" \
    \"DETACH r2catalog;\" \
    > /tmp/duckdb-otlp.sql"

docker logs --tail 80 duckdb-otlp
docker stop duckdb-otlp
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
- R2 Data Catalog has its own table maintenance features such as compaction and snapshot expiration. `duckdb-otlp` only probes the generic DuckDB `CHECKPOINT <catalog>` hook on its internal maintenance cadence; if the catalog reports checkpointing as unsupported, automatic maintenance is disabled for that server and normal ingest/flush/stop durability behavior is unchanged.

## See also

- [Cloudflare R2 Data Catalog DuckDB docs](https://developers.cloudflare.com/r2/data-catalog/config-examples/duckdb/)
- [Cloudflare R2 Data Catalog management docs](https://developers.cloudflare.com/r2/data-catalog/manage-catalogs/)
- [DuckDB Iceberg REST catalog docs](https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_rest_catalogs)
- [Live Ingest Reference](../../reference/serve/)
- [Live Ingest Quickstart](../../quickstart/serve/)
