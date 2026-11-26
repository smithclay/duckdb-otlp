# How to Stream OTLP to Remote DuckLake

Use the `duckdb-otlp` Docker image in `r2-neon-ducklake` mode to stream OTLP/HTTP exports into DuckLake with a Neon Postgres catalog and Cloudflare R2 data files.

The container initializes DuckDB, loads the required extensions, connects DuckLake to Neon, stores Parquet files in R2, starts the ingest server, and commits accepted rows in batches.

Live ingestion uses OTLP/HTTP on port `4318`. WASM builds do not include the ingest server.

## Prepare Neon and R2

Create or choose a Neon database for the DuckLake catalog. Copy the connection details from Neon as separate values:

- host
- port
- database
- user
- password
- SSL mode

Create or choose a Cloudflare R2 bucket for DuckLake data files:

```bash
export CLOUDFLARE_ACCOUNT_ID=<account-id>
export R2_BUCKET=duckdb-otlp-ducklake

wrangler r2 bucket create "$R2_BUCKET"
```

Create an R2 S3-compatible access key pair that can write objects to the bucket. Save the access key ID and secret access key for the next step.

## Configure

Create `remote-ducklake.env`:

```ini
DUCKDB_MODE=r2-neon-ducklake
DUCKDB_OTLP_TOKEN=dev-token-123456

DUCKDB_CATALOG=lake
DUCKDB_SCHEMA=otlp

DUCKDB_QUACK_PORT=9494
DUCKDB_QUACK_TOKEN=dev-quack-token-123456

CLOUDFLARE_ACCOUNT_ID=<account-id>
R2_BUCKET=<bucket-name>
R2_PREFIX=duckdb-otlp/
R2_ACCESS_KEY_ID=<r2-s3-access-key-id>
R2_SECRET_ACCESS_KEY=<r2-s3-secret-access-key>

PGHOST=<neon-host>
PGPORT=5432
PGDATABASE=<database>
PGUSER=<user>
PGPASSWORD=<password>
PGSSLMODE=require
```

If you use a custom R2 endpoint, add `R2_ENDPOINT=<endpoint-host>`.

## Start the server

Prefer to run manually in the DuckDB shell? See [Run manually](#run-manually).

```bash
docker run --rm --name duckdb-otlp \
  --env-file remote-ducklake.env \
  -p 4318:4318 \
  -p 9494:9494 \
  ghcr.io/smithclay/duckdb-otlp:latest
```

The container creates the target tables in `lake.otlp` if they do not exist:

- `otlp_logs`
- `otlp_traces`
- `otlp_metrics_gauge`
- `otlp_metrics_sum`
- `otlp_metrics_histogram`
- `otlp_metrics_exp_histogram`

Leave the container running while clients send OTLP/HTTP requests.

## POST a log record

In another terminal:

```bash
curl -sS http://localhost:4318/v1/logs \
  -H 'Authorization: Bearer dev-token-123456' \
  -H 'Content-Type: application/json' \
  -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"remote-ducklake-demo"}},{"key":"deployment.environment","value":{"stringValue":"docs"}}]},"scopeLogs":[{"scope":{"name":"duckdb-otlp-guide"},"logRecords":[{"timeUnixNano":"1704067200000000000","observedTimeUnixNano":"1704067200123456789","severityNumber":9,"severityText":"INFO","body":{"stringValue":"hello from remote DuckLake"},"attributes":[{"key":"guide","value":{"stringValue":"stream-to-remote-ducklake"}}]}]}]}]}'
```

**Response:**

```json
{"status":"buffered","rows":1,"batches":1}
```

Rows are accepted before they are durable. They commit automatically in the
background, on graceful shutdown, or immediately after an explicit flush.

## Query committed rows

Flush and query through Quack from a host DuckDB process. The server process owns the DuckLake catalog connection while it runs, and the distroless image has no shell or bundled DuckDB CLI.

The server image is distroless and has no shell or DuckDB CLI, so do not use
`docker exec ... sh -c` for inspection SQL. The examples in this guide enable
Quack, publish port `9494`, and use DuckDB 2.0's `CONNECT` statement for
ordinary server-side SQL. Replace `<fingerprint>` with the TLS certificate
fingerprint the container prints at startup (see
[Query with Quack](../query-with-quack/)).

```bash
duckdb <<'SQL'
INSTALL quack;
LOAD quack;

CREATE SECRET duckdb_otlp_quack (
  TYPE quack,
  SCOPE 'quack:localhost:9494',
  TOKEN 'dev-quack-token-123456'
);

ATTACH 'quack:localhost:9494' AS otel (TYPE quack, SSL_FINGERPRINT '<fingerprint>');
CONNECT otel;

SELECT * FROM otlp_flush();

SELECT service_name, severity_text, body
FROM lake.otlp.otlp_logs
WHERE service_name = 'remote-ducklake-demo'
ORDER BY time_unix_nano DESC
LIMIT 5;

DISCONNECT;
SQL
```

## Stop cleanly

If you plan to delete the Neon and R2 resources immediately, skip this
step and use [Clean up](#clean-up) instead.

```bash
docker stop duckdb-otlp
```

The image runs `otlp_stop()` during shutdown,
so remaining buffered rows are committed before the process exits.

## Clean up

Drop the DuckLake tables before deleting the Neon database or R2 bucket:

```bash
duckdb <<'SQL'
INSTALL quack;
LOAD quack;

CREATE SECRET duckdb_otlp_quack (
  TYPE quack,
  SCOPE 'quack:localhost:9494',
  TOKEN 'dev-quack-token-123456'
);

ATTACH 'quack:localhost:9494' AS otel (TYPE quack, SSL_FINGERPRINT '<fingerprint>'); -- printed by the container at startup
CONNECT otel;

SELECT status FROM otlp_stop('otlp:0.0.0.0:4318');
DROP TABLE IF EXISTS lake.otlp.otlp_logs;
DROP TABLE IF EXISTS lake.otlp.otlp_traces;
DROP TABLE IF EXISTS lake.otlp.otlp_metrics_gauge;
DROP TABLE IF EXISTS lake.otlp.otlp_metrics_sum;
DROP TABLE IF EXISTS lake.otlp.otlp_metrics_histogram;
DROP TABLE IF EXISTS lake.otlp.otlp_metrics_exp_histogram;
DETACH lake;

DISCONNECT;
SQL

docker stop duckdb-otlp
```

Then delete the R2 prefix or bucket and remove the Neon database or branch you created for this guide.

## Run manually

To run this configuration in a DuckDB 2.0+ shell instead of the daemon, execute the SQL below. Replace bracketed values with the corresponding values from this guide. Keep the shell open while clients send telemetry.

```bash
duckdb duckdb-otlp-control.duckdb
```

```sql
-- The daemon embeds otlp statically; the shell loads the extension explicitly.
INSTALL otlp FROM community;
LOAD otlp;

-- Configure R2 storage and the Neon-backed DuckLake catalog.
INSTALL ducklake;
INSTALL postgres;
INSTALL httpfs;
LOAD ducklake;
LOAD postgres;
LOAD httpfs;
CREATE OR REPLACE SECRET r2_storage (
  TYPE s3,
  KEY_ID '<r2-s3-access-key-id>',
  SECRET '<r2-s3-secret-access-key>',
  REGION 'auto',
  ENDPOINT '<account-id>.r2.cloudflarestorage.com',
  URL_STYLE 'path'
);
CREATE OR REPLACE SECRET postgres_secret (
  TYPE postgres,
  HOST '<neon-host>',
  PORT 5432,
  DATABASE '<database>',
  USER '<user>',
  PASSWORD '<password>',
  SSLMODE 'require'
);
CREATE OR REPLACE SECRET ducklake_secret (
  TYPE ducklake,
  METADATA_PATH '',
  DATA_PATH 's3://<bucket-name>/duckdb-otlp/',
  METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_secret'}
);
ATTACH 'ducklake:ducklake_secret' AS lake;

-- Create the target schema before starting ingestion.
CREATE SCHEMA IF NOT EXISTS lake.otlp;

-- Match the daemon's default-catalog setting for unqualified queries.
USE lake;

-- Start OTLP/HTTP. Seal cadence, file sizes, and buffer limits use defaults;
-- override only as needed — see the Live Ingest Reference:
-- https://smithclay.github.io/duckdb-otlp/reference/serve/
SELECT listen_url, catalog_name, schema_name
FROM otlp_serve(
  'otlp:0.0.0.0:4318',
  catalog := 'lake',
  schema := 'otlp',
  token := 'dev-token-123456',
  allow_other_hostname := true
);

-- The guide's daemon configuration also enables Quack.
INSTALL quack;
LOAD quack;
SELECT listen_uri
FROM quack_serve(
  'quack:0.0.0.0:9494',
  token := 'dev-quack-token-123456',
  allow_other_hostname := true
);
```

Before closing DuckDB, stop both listeners cleanly:

```sql
-- Stop Quack, then commit buffered telemetry and stop OTLP.
CALL quack_stop('quack:0.0.0.0:9494');
SELECT status, dropped_rows
FROM otlp_stop('otlp:0.0.0.0:4318');
```

## Troubleshooting

- If the container cannot connect to Neon at startup, confirm `PGHOST`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, and `PGSSLMODE=require`.
- If the container connects to Neon but cannot write data files, confirm the R2 access key can write objects to `R2_BUCKET`.
- If R2 paths use the wrong location, confirm `CLOUDFLARE_ACCOUNT_ID`, `R2_BUCKET`, and `R2_PREFIX`.
- If no rows appear after a `202` response, run the flush command before querying.
- `otlp_flush` seals buffered ingest rows. Run DuckDB or DuckLake maintenance commands when you need compaction.

## See also

- [DuckLake documentation](https://ducklake.select/)
- [Neon documentation](https://neon.com/docs)
- [Cloudflare R2 documentation](https://developers.cloudflare.com/r2/)
- [How to stream to local DuckLake](../stream-to-local-ducklake/)
- [Live Ingest Reference](../../reference/serve/)
