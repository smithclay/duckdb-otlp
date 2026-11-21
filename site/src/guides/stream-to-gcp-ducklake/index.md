# How to Stream OTLP to DuckLake on GCP

Use `DUCKDB_MODE=gcp-ducklake` to store DuckLake metadata in PostgreSQL and
Parquet data in Google Cloud Storage. Cloud SQL can host the catalog in a new
database on an existing instance. The daemon uses the native `gcs` extension
with Application Default Credentials (ADC), so no GCS HMAC keys are needed.

## Prepare storage and access

- Create a dedicated GCS bucket or prefix for the lake.
- Give the receiver's service account `roles/storage.objectAdmin` on that bucket.
  DuckLake maintenance needs list and delete access as well as read and write.
- Create a PostgreSQL database and a dedicated catalog user. Grant `CONNECT` on
  the database and `USAGE, CREATE` on its `public` schema; DuckLake creates and
  manages its own metadata tables.
- Make PostgreSQL reachable from the receiver. For Cloud SQL, use a Cloud SQL
  Auth Proxy socket mounted into the container, or a reachable TCP endpoint with
  the appropriate TLS configuration. The image does not start a proxy itself.

On a GCP VM, attach the service account and enable the `cloud-platform` OAuth
scope; bucket IAM determines storage access. A proxy also needs the
`roles/cloudsql.client` role. In a local container, mount an ADC credential file
readable by uid `65532` and set `GOOGLE_APPLICATION_CREDENTIALS` to its container
path. Credentials on the host are not automatically available inside Docker.

The image includes the system CA bundle and the compatibility path needed by
the GCS extension for TLS verification; no CA certificate mount is required.
On Cloud Run, use an attached service account for ADC rather than mounting a
credential file or setting `GOOGLE_APPLICATION_CREDENTIALS`.

## Configure

Create `gcp-ducklake.env` with permissions restricted to its owner:

```ini
DUCKDB_MODE=gcp-ducklake
DUCKDB_OTLP_TOKEN=<private-token-at-least-16-characters>
DUCKDB_CATALOG=lake
DUCKDB_SCHEMA=otlp
DUCKLAKE_DATA_PATH=gcss://your-trace-bucket/ducklake/

PGHOST=/cloudsql/your-project:us-central1:your-instance
PGPORT=5432
PGDATABASE=tracelake
PGUSER=tracelake_catalog
PGPASSWORD=<catalog-user-password>
PGSSLMODE=disable

DUCKDB_QUACK_PORT=9494
DUCKDB_QUACK_TOKEN=<separate-private-admin-token>
```

`PGHOST`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, and `DUCKLAKE_DATA_PATH` are required.
`PGPORT` defaults to `5432`; `PGSSLMODE` defaults to `require`. The `disable` value
above is only for the local Unix socket hop to a Cloud SQL Auth Proxy, which
provides the encrypted connection to Cloud SQL. These are ordinary PostgreSQL
connection settings; the mode is not tied to Cloud SQL. It does not acquire or
refresh PostgreSQL IAM tokens.

The data path must use `gcss://` to select the native GCS filesystem even if
`httpfs` is loaded. GCS auth uses ADC independently of the PostgreSQL password.

The image loads a patched `gcs` extension
([smithclay/duckdb-gcs](https://github.com/smithclay/duckdb-gcs)) through
`DUCKDB_OTLP_GCS_EXTENSION_PATH`. The community build prints a failed upload
finalize instead of raising it, so DuckLake commits data files that were never
written ([#90](https://github.com/smithclay/duckdb-otlp/issues/90)). Outside the
image, unset that variable to use the community build, or point it at your own
`gcs.duckdb_extension`.

> [!WARNING]
> Neither the patched nor the community `gcs` extension has a DuckDB 2.0 build yet, so
> `gcp-ducklake` fails at startup on the current image, naming the missing extension.

## Run and verify

On a GCP VM with its service account attached and a running proxy writing sockets
under `/cloudsql`:

```bash
docker run --rm --name duckdb-otlp \
  --env-file gcp-ducklake.env \
  --mount type=bind,src=/cloudsql,dst=/cloudsql \
  -p 127.0.0.1:4318:4318 \
  -p 127.0.0.1:9494:9494 \
  ghcr.io/smithclay/duckdb-otlp:latest
```

Use a pinned image digest for deployed services. Both host ports bind to loopback
in this example. Put the ingest endpoint behind HTTPS for remote producers and
keep Quack private: it provides full SQL access. The proxy directory and sockets
must be accessible to the image's nonroot uid `65532`. Bind-mounted `/data`, if
used, must also be writable by that uid.

The daemon creates the OTLP tables in `lake.otlp`. Send a record with the ingest
token from your environment file:

## POST a log record

In another terminal:

```bash
curl -sS http://localhost:4318/v1/logs \
  -H 'Authorization: Bearer <private-token-at-least-16-characters>' \
  -H 'Content-Type: application/json' \
  -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"gcp-demo"}},{"key":"deployment.environment","value":{"stringValue":"docs"}}]},"scopeLogs":[{"scope":{"name":"duckdb-otlp-guide"},"logRecords":[{"timeUnixNano":"1704067200000000000","observedTimeUnixNano":"1704067200123456789","severityNumber":9,"severityText":"INFO","body":{"stringValue":"hello from GCP DuckLake"},"attributes":[{"key":"guide","value":{"stringValue":"stream-to-gcp-ducklake"}}]}]}]}]}'
```

**Response:**

```json
{"status":"buffered","rows":1,"batches":1}
```

Rows are accepted before they are durable. They commit automatically in the
background, on graceful shutdown, or immediately after an explicit flush.

## Query committed rows

Flush and query through Quack from a host DuckDB process:

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
  TOKEN '<separate-private-admin-token>'
);

ATTACH 'quack:localhost:9494' AS otel (TYPE quack, SSL_FINGERPRINT '<fingerprint>');
CONNECT otel;

SELECT * FROM otlp_flush('otlp:0.0.0.0:4318');

SELECT service_name, severity_text, body
FROM lake.otlp.otlp_logs
WHERE service_name = 'gcp-demo'
ORDER BY time_unix_nano DESC
LIMIT 5;

DISCONNECT;
SQL
```

## Stop cleanly

```bash
docker stop duckdb-otlp
```

The image sends `otlp_stop('otlp:0.0.0.0:4318')` during shutdown,
so remaining buffered rows are committed before the process exits.

For traces, send OTLP/HTTP protobuf or JSON to `/v1/traces`, then query
`lake.otlp.otlp_traces`. Configure exporters with the HTTPS endpoint and
`Authorization: Bearer <ingest-token>`.

## Operate the lake

A successful ingest response means buffered in memory. A crash can lose rows
accepted since the last commit; graceful shutdown flushes them. PostgreSQL-backed
DuckLake supports concurrent appenders, including overlap during a deployment.
Each receiver must use its own local control database; share the remote catalog
and GCS data path, not a local DuckDB file. Bootstrap the catalog/tables once
before scaling out, since concurrent schema creation can conflict. See
[DuckLake conflict resolution](https://ducklake.select/docs/stable/duckdb/advanced_features/conflict_resolution).

On Cloud Run, enable instance-based billing (`cpu_idle = false`) for background
commits and keep a minimum of one instance for prompt ingestion. Configure the
container port as `4318`. Cloud Run supplies HTTPS; make sure its ingress and
invoker policy admit your producers before the daemon validates their bearer
token. Configure Cloud SQL socket mounting and the receiver service account as
above, and keep Quack unexposed. Monitor commit failures and test a rolling
deployment before increasing the instance limit. See the
[live ingest reference](../../reference/serve/) for buffer limits, commit health,
and maintenance settings.

DuckLake snapshot/file maintenance does not impose a trace retention period.
Delete expired rows through DuckLake before expiring snapshots and cleaning
unused files. Do not let bucket lifecycle rules delete files still referenced by
the catalog.

Small commits are inlined into the PostgreSQL catalog until a DuckLake
`CHECKPOINT` flushes them to Parquet in GCS; the daemon runs one every 5 minutes
(`maintenance_interval_ms`) while rows are being committed and prints each outcome
(`catalog maintenance checkpoint succeeded` / `WARNING: catalog maintenance
checkpoint failed ...`) to stderr, so alert on the warning. Set
`DUCKLAKE_DATA_INLINING_ROW_LIMIT=0` to write Parquet on every commit instead.

If startup fails, check PostgreSQL connectivity and grants, ADC availability in
the container, and bucket IAM. If ingestion succeeds but rows do not appear,
inspect commit errors with `otlp_server_list()` through private Quack access.
