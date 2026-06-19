---
title: "Get Started"
---

Use the DuckDB OpenTelemetry Extension to query OpenTelemetry files, try the native OTLP/HTTP ingest server, then share live telemetry with other hosts over Quack.

## Prerequisites

- DuckDB 1.5.4 or later.
- OpenTelemetry data
- Docker (if you want to use the server daemon image)

## 1. Install and Load

```sql
-- Install from community repo
INSTALL otlp FROM community;
LOAD otlp;
```

Alternately, for development and pre-releases, start duckdb with `-unsigned` and you can install from GitHub:
```sql
-- Install unsigned from GitHub pages (nightly/pre-release)
INSTALL otlp from 'https://smithclay.github.io/duckdb-otlp';
LOAD otlp;
```

## 2. Read OTLP Files

The table functions accept local paths, globs, and DuckDB-supported remote file systems.

```sql
LOAD httpfs;

SELECT time_unix_nano, service_name, severity_text, body
FROM read_otlp_logs('https://github.com/smithclay/duckdb-otlp/raw/refs/heads/main/test/data/otlp_logs.pb')
WHERE severity_text = 'ERROR';

SELECT
    trace_id,
    name,
    duration_time_unix_nano / 1000000 AS duration_ms
FROM read_otlp_traces('https://github.com/smithclay/duckdb-otlp/raw/refs/heads/main/test/data/otlp_traces.pb')
ORDER BY duration_time_unix_nano DESC;
```

See the [Schema Reference](../reference/schemas/) for every column emitted by each reader.

## 3. Stream One Log over OTLP/HTTP

Native builds include the ingest server. You can POST OTLP/HTTP to it, and DuckDB buffers rows in memory before batch commits. When a POST returns `202`, DuckDB has accepted the rows, but it has not made them durable yet. DuckDB commits rows after about 5 seconds in the buffer, when admitted request-body bytes reach about 64 MiB, when you call `otlp_stop`, or when you call `otlp_flush`.

Start a local server:

```sql
SELECT listen_url
FROM otlp_serve('otlp:localhost:4318', token := 'dev-token-123456');
```

Send one OTLP log record:

```bash
curl -sS http://localhost:4318/v1/logs -H 'Authorization: Bearer dev-token-123456' -H 'Content-Type: application/json' -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"curl-demo"}}]},"scopeLogs":[{"logRecords":[{"timeUnixNano":"1704067200000000000","severityText":"INFO","body":{"stringValue":"hello from curl"}}]}]}]}'
```

Leave the server running for the next steps. The in-memory `otlp_logs` table it writes to is the same one the queries and the Quack server below read from.

For durable lakehouse ingest, pass `catalog` to an attached lakehouse catalog. See [Stream to Local DuckLake](../guides/stream-to-local-ducklake/), [Stream to Remote DuckLake](../guides/stream-to-remote-ducklake/), [Stream to Amazon S3 Tables](../guides/stream-to-s3-tables/), or [Stream to Cloudflare R2 Data Catalog](../guides/stream-to-r2-data-catalog/). To write partitioned Parquet files without a lakehouse catalog, see [Stream to Parquet](../guides/stream-to-parquet/).

## 4. Query the Data

The server buffers rows in memory and commits them in the background, so a freshly POSTed row may not be visible yet. Force a commit with `otlp_flush`, then read the table:

```sql
SELECT * FROM otlp_flush('otlp:localhost:4318');

SELECT time_unix_nano, service_name, severity_text, body
FROM otlp_logs;
```

Rows land in the default (in-memory) catalog: logs in `otlp_logs`, traces in `otlp_traces`, and metrics in `otlp_metrics_gauge`, `otlp_metrics_sum`, `otlp_metrics_histogram`, and `otlp_metrics_exp_histogram`. See the [Schema Reference](../reference/schemas/) for every column.

## 5. Share with Quack

[Quack](https://duckdb.org/docs/current/quack/overview.html) turns a running DuckDB process into a query server, so other hosts can read its tables over the network. Enable it in the same DuckDB session that is running the ingest server:

```sql
INSTALL quack;
LOAD quack;

CALL quack_serve(
    'quack:0.0.0.0:9494',
    token := 'dev-quack-token-123456',
    allow_other_hostname := true
);
```

Quack now serves this DuckDB instance — including `otlp_logs` and the other signal tables — on port `9494`. Use a token separate from the ingest token: `dev-token-123456` protects OTLP ingest on `4318`, and `dev-quack-token-123456` protects query access on `9494`. To accept only same-host connections, bind to `quack:localhost:9494` and drop `allow_other_hostname`.

Quack grants full SQL read/write access to this DuckDB connection, so treat the token as an administrative credential. Quack is experimental in DuckDB 1.5.4. Run it on trusted networks, and put it behind a TLS-terminating reverse proxy for remote access.

## 6. Query with Quack from Another Host

On another machine, install Quack, create a secret with the token, and attach the remote server. Replace `SERVER_HOST` with the hostname or IP of the machine running the ingest server:

```sql
INSTALL quack;
LOAD quack;

CREATE SECRET otlp_quack (
  TYPE quack,
  SCOPE 'quack:SERVER_HOST:9494',
  TOKEN 'dev-quack-token-123456'
);

ATTACH 'quack:SERVER_HOST:9494' AS otel (TYPE quack);
```

Point the session at the attached server, then query the signal tables directly:

```sql
USE otel;

SELECT time_unix_nano, service_name, severity_text, body
FROM otlp_logs
ORDER BY time_unix_nano DESC
LIMIT 5;
```

Quack runs each scan inside the remote DuckDB process and streams the result back. For heavy aggregations — or server-side functions such as `otlp_flush` — run SQL through the attached catalog's `query` macro instead. See [Query with Quack](../guides/query-with-quack/) for the full workflow, including the Docker daemon path.

## Stop the Servers

When you are done, stop Quack and the ingest server before closing the database. `otlp_stop` commits remaining buffered rows before it returns:

```sql
CALL quack_stop('quack:0.0.0.0:9494');
SELECT status FROM otlp_stop('otlp:localhost:4318');
```

## Next Steps

- [Live Ingest Quickstart](../quickstart/serve/) - POST one log record over OTLP/HTTP.
- [Query with Quack](../guides/query-with-quack/) - share stored telemetry and query it from a separate DuckDB process.
- [Stream to Local DuckLake](../guides/stream-to-local-ducklake/) - stream OTLP into local DuckLake/Parquet.
- [Stream to Remote DuckLake](../guides/stream-to-remote-ducklake/) - stream OTLP into DuckLake with Neon and R2.
- [Stream to Parquet](../guides/stream-to-parquet/) - stream OTLP into partitioned Parquet files on disk or S3.
- [Stream to Amazon S3 Tables](../guides/stream-to-s3-tables/) - stream OTLP into Amazon S3 Tables as an Iceberg catalog.
- [Stream to Cloudflare R2 Data Catalog](../guides/stream-to-r2-data-catalog/) - stream OTLP into Cloudflare R2 Data Catalog as an Iceberg catalog.
- [How to Configure the OpenTelemetry Collector](../setup/collector/) - export OTLP files from the OpenTelemetry Collector.
- [How-to Guides](../guides/) - common query and export tasks.
- [API Reference](../reference/api/) - function signatures and capability notes.
- [Schema Reference](../reference/schemas/) - column lists for every reader.

## Common Issues

**Extension `otlp` not found**

```sql
INSTALL otlp FROM community;
LOAD otlp;
```

**File does not exist**

Use a path relative to DuckDB's current working directory, or pass an absolute path:

```sql
SELECT * FROM read_otlp_traces('/full/path/to/traces.jsonl');
```

**Rows posted to the server are not visible yet**

Wait for the background commit, or stop the server with `otlp_stop('otlp:localhost:4318')` before closing the database. Use `otlp_flush('otlp:localhost:4318')` when the server should keep running and readers need the latest accepted rows before the next scheduled commit.
