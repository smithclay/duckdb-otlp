---
title: "How to Store Claude Code or Codex Traces in Local DuckLake"
description: "Export agent OpenTelemetry spans to the duckdb-otlp Docker image and persist them in a local DuckLake volume."
---

This guide shows you how to run the `duckdb-otlp` server image locally and point Claude Code or Codex at it with OTLP/HTTP. The container listens on `localhost:4318` and writes traces into a DuckLake catalog stored in a Docker volume.

The examples follow the Claude Code [monitoring](https://code.claude.com/docs/en/monitoring-usage) docs and the Codex [observability](https://developers.openai.com/codex/config-advanced#observability-and-telemetry) and [configuration](https://developers.openai.com/codex/config-reference#configtoml) docs.

## Start the Local Writer

Create a Docker volume for DuckLake metadata and Parquet data:

```bash
docker volume create duckdb-otlp-ducklake
```

Create `.env`:

```ini
DUCKDB_MODE=local-ducklake
DUCKLAKE_NAME=lake
DUCKDB_OTLP_TOKEN=dev-otlp-token-123456
```

Start the published server image:

```bash
docker run --rm --name duckdb-otlp \
  --env-file .env \
  -p 4318:4318 \
  -v duckdb-otlp-ducklake:/data \
  ghcr.io/smithclay/duckdb-otlp:latest
```

This starts `duckdb-otlp` at `http://localhost:4318`. OTLP/HTTP traces are accepted at:

```text
http://localhost:4318/v1/traces
```

The token in `.env` is:

```text
dev-otlp-token-123456
```

For a different local token, change `DUCKDB_OTLP_TOKEN` and use the same value in the agent exporter headers.

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

If you also want Claude Code events in DuckLake, add the logs exporter before starting Claude Code:

```bash
export OTEL_LOGS_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_LOGS_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=http://localhost:4318/v1/logs
```

Leave `OTEL_LOG_USER_PROMPTS`, `OTEL_LOG_TOOL_DETAILS`, `OTEL_LOG_TOOL_CONTENT`, and `OTEL_LOG_RAW_API_BODIES` unset unless you intend to store prompt text, tool details, tool content, or raw API bodies.

## Export Codex Traces

Put telemetry routing in your user-level Codex config, not in a project-local `.codex/config.toml`. Codex ignores `otel` in project-local config.

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

If you only want spans and not Codex event logs, set `exporter = "none"` and keep the `trace_exporter` block.

## Flush Buffered Telemetry

`duckdb-otlp` buffers accepted rows. Flush before querying a short local run:

```bash
docker exec duckdb-otlp sh -c \
  "printf '%s\n' \"SELECT * FROM otlp_flush('otlp:0.0.0.0:4318');\" > /tmp/duckdb-otlp.sql"
```

## Inspect Stored Traces

Query through the running DuckDB process. The server process owns the DuckLake catalog lock while it is running, so send inspection SQL to the control FIFO instead of attaching the same DuckLake from a second DuckDB process.

```bash
docker exec duckdb-otlp sh -c \
  "printf '%s\n' \
    \"SELECT trace_id, name, service_name, duration_time_unix_nano\" \
    \"FROM lake.main.otlp_traces\" \
    \"ORDER BY start_time_unix_nano DESC\" \
    \"LIMIT 20;\" \
    > /tmp/duckdb-otlp.sql"

docker logs --tail 80 duckdb-otlp
```

If you enabled event logs, inspect recent log rows:

```bash
docker exec duckdb-otlp sh -c \
  "printf '%s\n' \
    \"SELECT time_unix_nano, service_name, severity_text, body\" \
    \"FROM lake.main.otlp_logs\" \
    \"ORDER BY time_unix_nano DESC\" \
    \"LIMIT 20;\" \
    > /tmp/duckdb-otlp.sql"

docker logs --tail 80 duckdb-otlp
```

## Stop the Writer

Stop the container with `Ctrl-C` if it is attached to your terminal, or run:

```bash
docker stop duckdb-otlp
```

The image sends `otlp_stop('otlp:0.0.0.0:4318')` to DuckDB during shutdown so remaining buffered rows are committed before the process exits.

## If No Traces Appear

- Confirm the agent process was started after the telemetry settings were set.
- Confirm the trace endpoint is `http://localhost:4318/v1/traces`.
- Confirm the Authorization header uses the same token as `DUCKDB_OTLP_TOKEN`.
- Flush the writer before querying short sessions.
- Keep the query window recent enough to include the agent run.

## See also

- [How to stream to local DuckLake](../stream-to-local-ducklake/)
- [Live Ingest Reference](../../reference/serve/)
