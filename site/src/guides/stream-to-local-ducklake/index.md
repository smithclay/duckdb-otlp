# How to Stream OTLP to Local DuckLake

Use the `duckdb-otlp` Docker image in `local-ducklake` mode to stream OTLP/HTTP exports into a local DuckLake lakehouse.

The container initializes DuckDB, loads the required extensions, attaches DuckLake, starts the ingest server, and commits accepted rows in batches.

Live ingestion uses OTLP/HTTP on port `4318`. WASM builds do not include the ingest server.

## Configure

Create `.env`:

```ini
DUCKDB_MODE=local-ducklake
DUCKDB_OTLP_TOKEN=dev-token-123456

DUCKDB_CATALOG=lake
DUCKLAKE_CATALOG_PATH=/data/ducklake/catalog.duckdb
DUCKLAKE_DATA_PATH=/data/ducklake/storage

DUCKDB_QUACK_PORT=9494
DUCKDB_QUACK_TOKEN=dev-quack-token-123456
```

`DUCKLAKE_CATALOG_PATH` stores DuckLake metadata. `DUCKLAKE_DATA_PATH` stores Parquet data files.

## Start the server

Prefer to run manually in the DuckDB shell? See [Run manually](#run-manually).

```bash
mkdir -p data

docker run --rm --name duckdb-otlp \
  --env-file .env \
  -p 4318:4318 \
  -p 9494:9494 \
  -v "$(pwd)/data:/data" \
  ghcr.io/smithclay/duckdb-otlp:latest
```

The container creates the target tables in `lake.main` if they do not exist:

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
  -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"local-ducklake-demo"}},{"key":"deployment.environment","value":{"stringValue":"docs"}}]},"scopeLogs":[{"scope":{"name":"duckdb-otlp-guide"},"logRecords":[{"timeUnixNano":"1704067200000000000","observedTimeUnixNano":"1704067200123456789","severityNumber":9,"severityText":"INFO","body":{"stringValue":"hello from local DuckLake"},"attributes":[{"key":"guide","value":{"stringValue":"stream-to-local-ducklake"}}]}]}]}]}'
```

**Response:**

```json
{"status":"buffered","rows":1,"batches":1}
```

Rows are accepted before they are durable. They commit automatically in the
background, on graceful shutdown, or immediately after an explicit flush.

## Query committed rows

Flush and query through Quack from a host DuckDB process. The server process owns the DuckLake catalog while it runs, and the distroless image has no shell or bundled DuckDB CLI.

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

SELECT time_unix_nano, service_name, severity_text, body
FROM lake.main.otlp_logs
WHERE service_name = 'local-ducklake-demo'
ORDER BY time_unix_nano DESC
LIMIT 5;

DISCONNECT;
SQL
```

## Stop cleanly

```bash
docker stop duckdb-otlp
```

The image runs `otlp_stop()` during shutdown,
so remaining buffered rows are committed before the process exits.

`otlp_flush` seals buffered ingest rows. Run DuckDB or DuckLake maintenance commands when you need compaction.

## Run manually

To run this configuration in a DuckDB 2.0+ shell instead of the daemon, execute the SQL below. Replace bracketed values with the corresponding values from this guide. Keep the shell open while clients send telemetry.

```bash
mkdir -p data/ducklake/storage
duckdb data/duckdb-otlp-control.duckdb
```

```sql
-- The daemon embeds otlp statically; the shell loads the extension explicitly.
INSTALL otlp FROM community;
LOAD otlp;

-- Attach the local DuckLake catalog used by the Docker guide.
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:data/ducklake/catalog.duckdb' AS lake (
  DATA_PATH 'data/ducklake/storage'
);

-- Create the target schema before starting ingestion.
CREATE SCHEMA IF NOT EXISTS lake.main;

-- Match the daemon's default-catalog setting for unqualified queries.
USE lake;

-- Start OTLP/HTTP. Seal cadence, file sizes, and buffer limits use defaults;
-- override only as needed — see the Live Ingest Reference:
-- https://smithclay.github.io/duckdb-otlp/reference/serve/
SELECT listen_url, catalog_name, schema_name
FROM otlp_serve(
  'otlp:0.0.0.0:4318',
  catalog := 'lake',
  schema := 'main',
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

## See also

- [DuckLake documentation](https://ducklake.select/)
- [How to stream to remote DuckLake](../stream-to-remote-ducklake/)
- [Live Ingest Reference](../../reference/serve/)
