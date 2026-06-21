---
title: "How to Store Claude Code or Codex Traces in Local DuckLake"
description: "Export agent OpenTelemetry spans to the duckdb-otlp Docker image and persist them in a local DuckLake volume."
---

Run the `duckdb-otlp` server image locally and point Claude Code or Codex at it with OTLP/HTTP. The container listens on `localhost:4318` and writes traces into a DuckLake catalog stored in a Docker volume.

The examples follow the Claude Code [monitoring](https://code.claude.com/docs/en/monitoring-usage) docs and the Codex [observability](https://developers.openai.com/codex/config-advanced#observability-and-telemetry) and [configuration](https://developers.openai.com/codex/config-reference#configtoml) docs.

## Start the Local Writer

Prefer to run manually in the DuckDB shell? See [Run manually](#run-manually).

Create a Docker volume for DuckLake metadata and Parquet data:

```bash
docker volume create duckdb-otlp-ducklake
```

Create `.env`:

```ini
DUCKDB_MODE=local-ducklake
DUCKLAKE_NAME=lake
DUCKDB_OTLP_TOKEN=dev-otlp-token-123456

DUCKDB_QUACK_ENABLED=1
DUCKDB_QUACK_ADDR=0.0.0.0:9494
DUCKDB_QUACK_TOKEN=dev-quack-token-123456
```

Start the published server image:

```bash
docker run --rm --name duckdb-otlp \
  --env-file .env \
  -p 4318:4318 \
  -p 9494:9494 \
  -v duckdb-otlp-ducklake:/data \
  ghcr.io/smithclay/duckdb-otlp:latest
```

This starts `duckdb-otlp` at `http://localhost:4318`. Send OTLP/HTTP traces to:

```text
http://localhost:4318/v1/traces
```

The token in `.env` is:

```text
dev-otlp-token-123456
```

To use a different local token, change `DUCKDB_OTLP_TOKEN` and use the same value in the agent exporter headers.

## Export Claude Code Traces

Run Claude Code with tracing enabled and route spans to the local writer:

```bash
# To make this apply to every Claude Code session, add these to ~/.claude/settings.json
export CLAUDE_CODE_ENABLE_TELEMETRY=1
export CLAUDE_CODE_ENHANCED_TELEMETRY_BETA=1
export OTEL_TRACES_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:4318/v1/traces
export OTEL_EXPORTER_OTLP_HEADERS='Authorization=Bearer dev-otlp-token-123456'

claude -p "write one sentence about local trace storage"
```

To store Claude Code events in DuckLake, add the logs exporter before starting Claude Code:

```bash
export OTEL_LOGS_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_LOGS_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=http://localhost:4318/v1/logs
```

Leave `OTEL_LOG_USER_PROMPTS`, `OTEL_LOG_TOOL_DETAILS`, `OTEL_LOG_TOOL_CONTENT`, and `OTEL_LOG_RAW_API_BODIES` unset unless you intend to store prompt text, tool details, tool content, or raw API bodies.

## Export Codex Traces

Put telemetry routing in your user-level Codex config. Codex ignores `otel` in project-local `.codex/config.toml`.

Edit `~/.codex/config.toml`:

```toml
[otel]
environment = "local"
log_user_prompt = false
metrics_exporter = "none"
exporter = { otlp-http = {
  endpoint = "http://localhost:4318/v1/logs",
  protocol = "binary",
  headers = { "Authorization" = "Bearer dev-otlp-token-123456" }
}}
trace_exporter = { otlp-http = {
  endpoint = "http://localhost:4318/v1/traces",
  protocol = "binary",
  headers = { "Authorization" = "Bearer dev-otlp-token-123456" }
}}
```

Start a new Codex process after editing the config:

```bash
codex exec "write one sentence about local trace storage"
```

To collect spans without Codex event logs, set `exporter = "none"` and keep the `trace_exporter` block.

## Flush Buffered Telemetry

`duckdb-otlp` buffers accepted rows. Flush before you query a short local run:

```bash
duckdb <<'SQL'
INSTALL quack;
LOAD quack;

FROM quack_query(
  'quack:localhost:9494',
  'SELECT * FROM otlp_flush(''otlp:0.0.0.0:4318'')',
  token = 'dev-quack-token-123456'
);
SQL
```

## Inspect Stored Traces

Query through the running DuckDB process with Quack. The server process owns the DuckLake catalog lock while it runs, and the distroless image has no shell or bundled DuckDB CLI, so do not use `docker exec ... sh -c` for inspection SQL.

```bash
duckdb <<'SQL'
INSTALL quack;
LOAD quack;

FROM quack_query(
  'quack:localhost:9494',
  $$
  SELECT trace_id, name, service_name, duration_time_unix_nano
  FROM lake.main.otlp_traces
  ORDER BY start_time_unix_nano DESC
  LIMIT 20
  $$,
  token = 'dev-quack-token-123456'
);
SQL
```

If you enabled event logs, inspect recent log rows:

```bash
duckdb <<'SQL'
INSTALL quack;
LOAD quack;

FROM quack_query(
  'quack:localhost:9494',
  $$
  SELECT time_unix_nano, service_name, severity_text, body
  FROM lake.main.otlp_logs
  ORDER BY time_unix_nano DESC
  LIMIT 20
  $$,
  token = 'dev-quack-token-123456'
);
SQL
```

## Stop the Writer

Stop the container with `Ctrl-C` if your terminal is attached, or run:

```bash
docker stop duckdb-otlp
```

During shutdown, the image sends `otlp_stop('otlp:0.0.0.0:4318')` to DuckDB so the process commits remaining buffered rows before it exits.

## Run manually

To run this configuration in a DuckDB 1.5.4+ shell instead of the daemon, create the local DuckLake directories and open a control database:

```bash
mkdir -p data/ducklake/storage
duckdb data/duckdb-otlp-control.duckdb
```

Then execute the same setup and server commands used by the guide's daemon configuration. The daemon embeds `otlp` statically; the shell loads it explicitly.

```sql
-- Load the extensions and attach the local DuckLake catalog.
INSTALL otlp FROM community;
LOAD otlp;
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:data/ducklake/catalog.duckdb' AS lake (
  DATA_PATH 'data/ducklake/storage'
);

-- Create the target schema and make its catalog the default.
CREATE SCHEMA IF NOT EXISTS lake.main;
USE lake;

-- Start OTLP/HTTP. Seal cadence, file sizes, and buffer limits use defaults;
-- override only as needed — see the Live Ingest Reference:
-- https://smithclay.github.io/duckdb-otlp/reference/serve/
SELECT listen_url, catalog_name, schema_name
FROM otlp_serve(
  'otlp:0.0.0.0:4318',
  catalog := 'lake',
  schema := 'main',
  token := 'dev-otlp-token-123456',
  allow_other_hostname := true
);

-- This guide also enables Quack for inspection from another DuckDB process.
INSTALL quack;
LOAD quack;
SELECT listen_uri
FROM quack_serve(
  'quack:0.0.0.0:9494',
  token := 'dev-quack-token-123456',
  allow_other_hostname := true
);
```

Keep the shell open while the agents send telemetry. Before closing it, stop both listeners cleanly:

```sql
CALL quack_stop('quack:0.0.0.0:9494');
SELECT status, dropped_rows
FROM otlp_stop('otlp:0.0.0.0:4318');
```

## If No Traces Appear

- Start the agent process after you set the telemetry settings.
- Confirm the trace endpoint is `http://localhost:4318/v1/traces`.
- Confirm the Authorization header uses the same token as `DUCKDB_OTLP_TOKEN`.
- Flush the writer before querying short sessions.
- Keep the query window recent enough to include the agent run.

## Related

- [How to stream to local DuckLake](../stream-to-local-ducklake/)
- [Live Ingest Reference](../../reference/serve/)
