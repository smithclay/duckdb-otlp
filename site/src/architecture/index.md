# Architecture

The extension reads OpenTelemetry files into DuckDB and runs a native receiver for live exports. Both paths decode telemetry to Arrow through the Rust `otlp2records` library and use DuckDB's Arrow importer to turn it into typed rows.

## File reads

The `read_otlp_*` functions detect JSON, JSONL, and protobuf. The `read_otap_*` functions decode OpenTelemetry Arrow `BatchArrowRecords` into the same schemas. Each reader loads one whole file, up to 100 MiB, decodes it on first scan, and emits rows in DuckDB chunks. DuckDB's multi-file framework handles file lists, globs, and parallel scans across files. Column projection and some filters reduce the rows and columns the decoder builds. DuckDB evaluates the pushed filters on the resulting chunks.

See the [Schema Reference](../reference/schemas/) for fields and types.

## Live ingestion

In native builds, `otlp_serve` starts one receiver with one or more listeners. The HTTP listener handles OTLP/HTTP. The gRPC listener handles both OTLP/gRPC unary exports and OTAP/Arrow streams. The standalone `duckdb-otlp` receiver embeds the same extension and starts the listeners inside its own DuckDB instance.

| Component | Role |
| --- | --- |
| HTTP listener | Routes logs, traces, and metrics requests. `/healthz` reports liveness and `/readyz` checks whether buffered rows are committing. |
| gRPC listener | Bridges the embedded Rust tonic server to the shared ingestion pipeline. |
| Ingest pipeline | Reserves capacity, converts incoming batches, appends to per-signal buffers, and reports acceptance or refusal to each transport. |
| Server registry | Tracks running servers in each database. Each listener's URI identifies its server, and each destination has at most one server per database instance. |
| Writer | Seals buffered rows to the default catalog, an attached writable catalog such as DuckLake, or partitioned Parquet files. |

All listeners on a server share the same buffers, admission budget, and writer. The default admission budget is 512 MiB of accounted request bytes. A request that exceeds the available budget gets HTTP `503` or gRPC `RESOURCE_EXHAUSTED`.

### OTLP HTTP ingest server

```text
POST /v1/logs
  → check credentials, body size, encoding, and content type
  → reserve admission bytes, decode to Arrow, and append to the log buffer
  → return HTTP 202 with buffered row and batch counts
```

Each listener maps ingestion refusals to its own protocol status. The [Live Ingest Reference](../reference/serve/) describes the SQL controls and wire responses.

### Batch commit model and durability

One writer serializes seals while receiver workers continue to buffer incoming batches. The default background seal triggers are 128 MiB in admitted request bytes and an age of about five seconds for the oldest buffered rows. `otlp_flush` triggers a synchronous seal while the receiver stays running.

A catalog seal uses one transaction for all signals. If it fails, the writer restores the buffered rows for retry. Direct Parquet export writes each signal separately and retries unfinished signals. A partially written export can produce duplicate rows on retry.

HTTP `202` and successful gRPC responses acknowledge buffering. A process crash or a database close can drop rows still in memory. Call `otlp_stop` before closing the database to stop the listeners and drain their buffers. Durability after a successful seal depends on the destination.

For a configured named catalog, the writer can run a best-effort `CHECKPOINT` outside the ingest transaction. The maintenance interval defaults to five minutes.
