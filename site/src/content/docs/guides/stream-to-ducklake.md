---
title: "How to Stream OTLP to DuckLake"
---

Run the `duckdb-otlp` Docker image in `local-ducklake` mode to stream OTLP/HTTP exports into a local DuckLake lakehouse. The container initializes DuckDB, loads the required extensions, attaches DuckLake, starts the ingest server, and commits accepted rows in batches.

> Live ingestion is OTLP/HTTP on port `4318`. The ingest server is not available in WASM builds.

## Configure

Create `.env`:

```ini
DUCKDB_MODE=local-ducklake
DUCKDB_OTLP_TOKEN=dev-token-123456

DUCKLAKE_NAME=lake
DUCKLAKE_CATALOG_PATH=/data/ducklake/catalog.duckdb
DUCKLAKE_DATA_PATH=/data/ducklake/storage
```

`DUCKLAKE_CATALOG_PATH` stores DuckLake metadata. `DUCKLAKE_DATA_PATH` stores Parquet data files.

## Start the server

```bash
mkdir -p data

docker run --rm --name duckdb-otlp \
  --env-file .env \
  -p 4318:4318 \
  -v "$(pwd)/data:/data" \
  ghcr.io/smithclay/duckdb-otlp:latest
```

The container creates the target tables in `lake.main` if they do not already exist:

- `otlp_logs`
- `otlp_traces`
- `otlp_metrics_gauge`
- `otlp_metrics_sum`
- `otlp_metrics_histogram`
- `otlp_metrics_exp_histogram`

Leave the container running while clients send OTLP/HTTP requests.

## POST logs

In another terminal, POST a sample OTLP/JSON logs file:

```bash
curl -sS http://localhost:4318/v1/logs \
  -H 'Content-Type: application/x-ndjson' \
  -H 'Authorization: Bearer dev-token-123456' \
  --data-binary @test/data/logs_simple.jsonl
```

**Response:**

```json
{"status":"buffered","rows":1,"batches":1}
```

`application/x-ndjson` is for newline-delimited OTLP/JSON. Use `application/json` for one JSON document or `application/x-protobuf` for protobuf.

## Query committed rows

Rows are accepted before they are durable. They commit automatically in the background, on graceful shutdown, or immediately after an explicit flush.

Flush and query through the running container:

```bash
docker exec duckdb-otlp sh -c \
  "printf '%s\n' \
    \"SELECT * FROM otlp_flush('otlp:0.0.0.0:4318');\" \
    \"SELECT count(*) AS logs FROM lake.main.otlp_logs;\" \
    \"SELECT timestamp, service_name, severity_text, body\" \
    \"FROM lake.main.otlp_logs\" \
    \"ORDER BY timestamp DESC\" \
    \"LIMIT 20;\" \
    > /tmp/duckdb-otlp.sql"

docker logs --tail 80 duckdb-otlp
```

## Stop cleanly

```bash
docker stop duckdb-otlp
```

The image sends `otlp_stop('otlp:0.0.0.0:4318')` during shutdown, so remaining buffered rows are committed before the process exits.

DuckLake maintenance is best-effort automatic for live ingest: after a conservative number of successful automatic row-seals, `duckdb-otlp` runs non-force `CHECKPOINT lake` outside the ingest transaction when recent ingest rate and pending bytes leave ample admission headroom, then lets DuckLake apply its own policy. `otlp_flush` only forces ingest durability; it does not promise immediate compaction.

## See also

- [DuckLake documentation](https://ducklake.select/)
- [Live Ingest Reference](../../reference/serve/)
- [Live Ingest Quickstart](../../quickstart/serve/)
