# API Reference

The DuckDB OpenTelemetry Extension API includes file readers and live-ingest functions. The OTLP file readers accept a path, a glob pattern, or a list of them and detect OTLP JSON, JSONL, and protobuf; the separate OTAP readers decode the columnar Arrow encoding (`BatchArrowRecords`). Native builds also support live ingest.

## Table Functions

Use these table functions to read OTLP data:

### Traces

**`read_otlp_traces(path)`**

Streams trace spans with identifiers, attributes, events, and links. See the [schema reference](../schemas/#traces-read_otlp_traces) for all 24 columns.

**Parameters:**
- `path` (VARCHAR or VARCHAR[]): A file path, a glob pattern, or a list of either. DuckDB file systems provide local, S3, HTTP(S), Azure, and GCS access.

The readers are DuckDB multi-file readers, so they take the same file options as `read_parquet` and `read_csv`:

| Option | Effect |
|---|---|
| `filename` | Adds a `filename` column naming each row's file. The `filename` virtual column is selectable without the option. |
| `file_row_number` | Adds a `file_row_number` column (BIGINT), the row's position within its file, from 0. |
| `hive_partitioning` | Reads `key=value` directories in the path as columns. A filter on one skips other partitions' files without opening them. Detected automatically when every path is partitioned. |
| `hive_types`, `hive_types_autocast` | Type the partition columns. |
| `union_by_name` | Accepted. Every file of a signal has the same columns, so it changes nothing. |
| `allow_empty` | Returns an empty result instead of an error when no file matches. |

```sql
SELECT service, count(*)
FROM read_otlp_traces('s3://bucket/traces/service=*/*.pb', hive_partitioning = true, filename = true)
WHERE service = 'checkout'
GROUP BY ALL;
```

Files are read in parallel, one thread per file. A file is decoded as a whole, so a single large file does not parallelize.

### Logs

**`read_otlp_logs(path)`**

Reads log records with severity, body, and trace correlation. See the [schema reference](../schemas/#logs-read_otlp_logs) for all 18 columns.

**Parameters:** Same as `read_otlp_traces`

### Metrics

**`read_otlp_metrics_gauge(path)`**

Returns gauge metrics (17 columns). See the [schema reference](../schemas/#gauge-metrics-read_otlp_metrics_gauge) for details.

**`read_otlp_metrics_sum(path)`**

Returns sum/counter metrics (19 columns) with `int_value`, `double_value`, `aggregation_temporality`, and `is_monotonic`. See the [schema reference](../schemas/#sum-metrics-read_otlp_metrics_sum) for details.

**`read_otlp_metrics_histogram(path)`**

Returns standard histogram metrics (22 columns) with counts, sum, min/max, explicit bucket bounds, and bucket counts. See the [schema reference](../schemas/#histogram-metrics-read_otlp_metrics_histogram) for details.

**`read_otlp_metrics_exp_histogram(path)`**

Returns exponential histogram metrics (27 columns) with scale, zero bucket, positive buckets, negative buckets, and aggregation temporality. See the [schema reference](../schemas/#exponential-histogram-metrics-read_otlp_metrics_exp_histogram) for details.

**Parameters:** Same as `read_otlp_traces`

Summary metrics are not supported.

### OpenTelemetry Arrow Protocol (OTAP)

OTAP is the columnar Arrow encoding of OpenTelemetry data (canonical `BatchArrowRecords`), distinct from OTLP's protobuf/JSON wire format. The `read_otap_*` readers decode OTAP files into the **same flattened schemas** as their `read_otlp_*` counterparts — only the input encoding differs:

- **`read_otap_traces(path)`**
- **`read_otap_logs(path)`**
- **`read_otap_metrics_gauge(path)`**
- **`read_otap_metrics_sum(path)`**
- **`read_otap_metrics_histogram(path)`**
- **`read_otap_metrics_exp_histogram(path)`**

**Parameters:** Same as `read_otlp_traces` (a path, glob or list, and the multi-file options).

Because OTAP and OTLP mean different things, they are separate functions rather than a format flag — pick the reader that matches your input encoding. Each file must be a self-contained `BatchArrowRecords` message; one file is decoded with one decoder. Streams that span multiple files relying on cross-message Arrow dictionary reuse are not supported by the file readers.

**One signal per file, multiple metric shapes per file.** A canonical OTAP message carries one signal family — logs *or* traces *or* metrics — so call the reader that matches the file. A single metrics file can hold several metric shapes at once: `read_otap_metrics_gauge`, `read_otap_metrics_sum`, `read_otap_metrics_histogram`, and `read_otap_metrics_exp_histogram` each extract their shape from the same file, just like the `read_otlp_metrics_*` readers (summary points are skipped). Pointing a reader at a file of a different signal, or at an envelope that mixes incompatible payloads, is a hard error — it never returns partial or mis-typed rows.

**Compression:** OpenTelemetry producers default to Zstandard. Native builds decode uncompressed, LZ4, and Zstandard OTAP. The WebAssembly build decodes uncompressed and LZ4 only (no Zstandard); a Zstandard OTAP file there fails with an Arrow IPC error.

## Live Ingest

In native builds, you can run a server that accepts live telemetry over **HTTP** or **gRPC** and streams it into the default DuckDB catalog or an attached writable catalog such as DuckLake or an Iceberg REST catalog. The server buffers rows and commits them in batches: an accepted request returns immediately (HTTP `202 Accepted` / gRPC `OK`), and rows become durable at the next background commit or on graceful stop. Current native builds commit after about 5 seconds for the oldest buffered row, or when admitted request-body bytes reach about 128 MiB.

- **`otlp_serve([uri], transport := 'http'|'grpc', catalog := '<attached_db>', ...)`** starts an ingest server on the `otlp:` scheme and creates or validates the target tables. The default transport is OTLP/HTTP. A gRPC listener accepts OTLP unary exports and OTAP/Arrow streams on the same port and writes them to the same tables. Pass a URI list with a parallel transport list to feed one server from several listeners. See the [Live Ingest Reference](../serve/#grpc-transport).

- **`otlp_flush([listen_uri])`** - Commit buffered rows synchronously. A listener URI selects its server. With no argument, flush every server on this database.
- **`otlp_stop(listen_uri)`** - Stop the server that owns the listener and commit its remaining buffered rows.
- **`otlp_stop()`** - Stop **every** server on this database, committing each before returning. One row per server. Use this before closing a database when you did not start every server yourself.
- **`otlp_server_list()`** - List running listeners with their server's live counters, buffer state, and health.

See the [Serve Reference](../serve/) for parameters, catalog targeting, endpoints, auth, and buffered commit behavior. For task-oriented walkthroughs, start with the [Live Ingest Quickstart](../../quickstart/serve/), [Stream to Local DuckLake](../../guides/stream-to-local-ducklake/), [Stream to Remote DuckLake](../../guides/stream-to-remote-ducklake/), [Stream to Parquet](../../guides/stream-to-parquet/), [Stream to Amazon S3 Tables](../../guides/stream-to-s3-tables/), or [Stream to Cloudflare R2 Data Catalog](../../guides/stream-to-r2-data-catalog/).

## Discovering these functions from SQL

Every function this extension registers carries its own documentation in DuckDB's catalog, so a connected client — including an agent that has nothing but a SQL connection — can read it without leaving the database:

```sql
LOAD otlp;
SELECT function_name, description, parameters, examples, categories
FROM duckdb_functions()
WHERE list_contains(categories, 'opentelemetry');
```

The `categories` column separates the two wire protocols (`otlp` vs `otap`) from the live-ingest functions (`ingest`, `monitoring`).

## Examples

For task-oriented examples, see the [How-to Guides](../../guides/).

For complete schema details, see the [Schema Reference](../schemas/).
