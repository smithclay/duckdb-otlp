---
title: "CLI Reference"
---

`duckdb-otlp` is a single binary that receives OpenTelemetry data, converts OTLP/OTAP files, and queries what it has stored. It runs on macOS and Linux, and is the same binary that ships in the Docker image.

With no arguments it starts a working local receiver — no configuration file, no environment variables, no credentials:

```sh
duckdb-otlp
```

That listens for OTLP/HTTP on `127.0.0.1:4318` and OTLP/gRPC on `127.0.0.1:4317`, and streams into a local DuckLake lakehouse under `$XDG_DATA_HOME/duckdb-otlp`.

## Commands

| Command | What it does |
|---------|--------------|
| `duckdb-otlp [serve]` | Start the OTLP listeners. The default when no subcommand is given. |
| `duckdb-otlp convert FILE...` | Convert OTLP/OTAP files to Parquet, CSV, or JSON. Needs no configuration. |
| `duckdb-otlp export` | Export the configured catalog's signal tables. |
| `duckdb-otlp query "SQL"` | Run one-shot SQL against the configured catalog. |
| `duckdb-otlp validate` | Resolve the configuration, print the effective settings and generated SQL, exit. |
| `duckdb-otlp healthcheck` | Probe every configured listener. Exit 0 means healthy. |
| `duckdb-otlp version` | Print the version. |
| `duckdb-otlp help [COMMAND]` | Show help for the tool or one command. |

### `convert` vs `export`

These do different jobs and have different prerequisites. `convert` is stateless — it reads files you name and needs no `DUCKDB_MODE`, no data directory, no catalog, and no backend credentials, so it works on a machine with nothing set up. `export` reads the catalog that `serve` writes to, so it needs the same mode configuration and credentials that `serve` does.

```sh
# Files on disk -> Parquet. Nothing configured.
duckdb-otlp convert traces.pb --signal traces --to out/

# The configured lakehouse -> Parquet.
duckdb-otlp export --signal logs --since -24h --to out/
```

## Configuration precedence

Command-line flag, then environment variable, then built-in default. Every flag that maps onto a setting overrides the matching variable, so `--http 4318` wins over `DUCKDB_OTLP_HTTP_PORT=9999`.

## `serve`

```sh
duckdb-otlp serve [flags]
```

| Flag | Variable | Default |
|------|----------|---------|
| `-m`, `--mode MODE` | `DUCKDB_MODE` | `local-ducklake` |
| `--host HOST` | `DUCKDB_OTLP_HOST` | `127.0.0.1` |
| `--http PORT` | `DUCKDB_OTLP_HTTP_PORT` | `4318` (`0` disables) |
| `--grpc PORT` | `DUCKDB_OTLP_GRPC_PORT` | `4317` (`0` disables) |
| `--otap PORT` | `DUCKDB_OTLP_OTAP_PORT` | off |
| `--data-dir DIR` | `DUCKDB_OTLP_DATA_DIR` | `$XDG_DATA_HOME/duckdb-otlp` |
| `--database PATH` | `DUCKDB_DATABASE` | `<data-dir>/duckdb-otlp-control.duckdb` |
| `--catalog NAME` | `DUCKDB_CATALOG` | mode-dependent |
| `--schema NAME` | `DUCKDB_SCHEMA` | mode-dependent |
| `--token TOKEN` | `DUCKDB_OTLP_TOKEN` | *(none)* |
| `--no-auth` | `DUCKDB_OTLP_DISABLE_AUTH` | `0` |
| `--quack PORT` | `DUCKDB_QUACK_PORT` | off |
| `--quack-token TOKEN` | `DUCKDB_QUACK_TOKEN` | *(none)* |
| `--startup-timeout SECS` | `DUCKDB_OTLP_STARTUP_TIMEOUT` | `60` |

Passing a token as a flag makes it visible in the process list; prefer `DUCKDB_OTLP_TOKEN` outside of local use.

### Selecting listeners

Transports are selected by which ports you set. A **non-zero** port narrows the set to what you named; `0` switches one transport off without un-defaulting the others:

```sh
duckdb-otlp --grpc 4317               # gRPC only
duckdb-otlp --grpc 0                  # HTTP only (gRPC off, HTTP still on its default)
duckdb-otlp --host 0.0.0.0 --token "$TOKEN"
```

OTAP/Arrow is a different protocol served by `otap_serve`, so it cannot be combined with standard OTLP listeners in one process:

```sh
duckdb-otlp --otap 4317 --http 0 --grpc 0
```

The startup banner prints which setting chose the listener set, so a narrowed or unexpected set is never silent.

### Authentication

There is **no built-in default token.** The rules are:

- **No token, every listener on loopback** → authentication is disabled automatically and a notice is printed. Only your machine can reach the listeners.
- **No token, any non-loopback bind** (including `0.0.0.0`) → startup fails, telling you to set a token or pass `--no-auth`.
- **A token** → it must be at least 16 characters, and it is passed to `otlp_serve` through a session variable, so it never appears in generated SQL or logs.
- **`--no-auth`** → unauthenticated traffic is accepted anywhere, with a warning.

## OpenTelemetry environment variables

OpenTelemetry specifies environment variables for **exporters**, not receivers. There is no receiver-side standard, so `duckdb-otlp` honors only the variables whose meaning transfers cleanly, and refuses the ones it cannot actually implement.

| Variable | Treatment |
|----------|-----------|
| `OTEL_EXPORTER_OTLP_PROTOCOL` | Selects the transport (`grpc`, `http/protobuf`, `http/json`) — but only when no port has been set explicitly. |
| `OTEL_EXPORTER_OTLP_HEADERS` | An `Authorization=Bearer <token>` entry supplies the expected token. Other entries are ignored. |
| `OTEL_EXPORTER_OTLP_TIMEOUT`, `OTEL_EXPORTER_OTLP_COMPRESSION` | Accepted and ignored (client-side only; compressed bodies are always accepted). |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | **Not read at all.** |
| `OTEL_EXPORTER_OTLP_{TRACES,LOGS,METRICS}_ENDPOINT` | **Rejected at startup.** |
| `OTEL_EXPORTER_OTLP_{CERTIFICATE,CLIENT_KEY,CLIENT_CERTIFICATE}` | **Rejected at startup.** |

`OTEL_EXPORTER_OTLP_ENDPOINT` is ignored deliberately. It is usually already set in a developer's shell, pointing at their real collector; treating it as a *bind* address would either fail confusingly or silently move the listener somewhere unintended. Use `--host` and `--http`/`--grpc`.

The TLS and per-signal-endpoint variables are rejected rather than ignored for the same reason in reverse: this server does not terminate TLS and serves every signal on one listener, so accepting those settings silently would leave you believing something untrue about your deployment. Put a TLS-terminating proxy in front instead.

## `convert`

```sh
duckdb-otlp convert FILE... [flags]
```

| Flag | Meaning |
|------|---------|
| `-s`, `--signal SIGNAL` | `traces`, `logs`, `metrics_gauge`, `metrics_sum`, `metrics_histogram`, `metrics_exp_histogram`, `metrics` (the four shapes), `all`, or `auto` (default) |
| `--otap` | Read OTAP (`BatchArrowRecords`) files instead of OTLP |
| `-o`, `--to PATH` | Output file, or a directory when several signals are written. Omit to stream to stdout. |
| `-f`, `--format FORMAT` | `parquet` (default with `--to`), `csv`, `json`, `ndjson` |
| `--overwrite` | Replace existing output files |

Several inputs are read as one dataset. Globs work, and are passed to the reader:

```sh
duckdb-otlp convert 'logs/*.jsonl' --signal logs --format ndjson | jq .
duckdb-otlp convert a.pb b.pb --signal traces --to out/traces.parquet
```

With `--signal auto`, every reader is tried and the ones that produce rows are used; the selection is always printed. A metrics file legitimately yields several shapes and writes one file per shape. Pass `--signal` explicitly when you want to be sure.

Parquet cannot be streamed to stdout — the format ends with a footer that needs a seekable file — so `--to` is required for it.

## `export`

```sh
duckdb-otlp export [flags]
```

| Flag | Meaning |
|------|---------|
| `-s`, `--signal SIGNAL` | Signal to export, or `metrics` / `all` (default) |
| `-o`, `--to DIR` | Output directory |
| `-f`, `--format FORMAT` | `parquet` (default), `csv`, `json`, `ndjson` |
| `--since TS` | Rows at or after this time |
| `--until TS` | Rows strictly before this time |
| `--where SQL` | Additional raw predicate |
| `--partition-by day` | Write `<table>/year=/month=/day=`, matching the layout the serve-side Parquet export writes |
| `--overwrite` | Replace existing output files |

`--since` and `--until` accept an absolute timestamp (`'2026-01-01'`) or a relative shorthand: a sign, a number, and a unit of `s`, `m`, `h`, `d`, or `w`.

```sh
duckdb-otlp export --signal logs --since -24h --to out/
duckdb-otlp export --partition-by day --to lake-dump/
duckdb-otlp export --signal traces --where "service_name = 'checkout'" --to out/ --format csv
```

## `query`

```sh
duckdb-otlp query "SELECT ..." [flags]
duckdb-otlp query --file script.sql [flags]
```

| Flag | Meaning |
|------|---------|
| `--file PATH` | Read SQL from a file |
| `-f`, `--format FORMAT` | `box` (default on a terminal), `csv` (default when piped), `json`, `ndjson`, `parquet` |
| `-o`, `--to PATH` | Write results to a file |
| `--readonly` | Open the database read-only |
| `--overwrite` | Replace an existing output file |

Unqualified table names resolve against the mode's catalog and schema, so `FROM otlp_logs` works directly:

```sh
duckdb-otlp query "SELECT service_name, count(*) FROM otlp_logs GROUP BY 1 ORDER BY 2 DESC"
duckdb-otlp query "FROM otlp_traces LIMIT 10" --format json
```

A script may contain several statements. Only a trailing `SELECT` can be redirected to a file or non-`box` format; earlier statements run as setup.

`--readonly` is opt-in rather than the default because lakehouse modes need write access to attach their catalog.

## Docker

The image runs the same binary, with container-shaped defaults set in the image rather than in the code:

```
DUCKDB_OTLP_DATA_DIR=/data
DUCKDB_OTLP_HOST=0.0.0.0
DUCKDB_OTLP_TRANSPORTS=http
DUCKDB_QUACK_ADDR=0.0.0.0:9494
```

Because the image binds `0.0.0.0`, it requires a token:

```sh
docker run --rm -p 4318:4318 \
  -e DUCKDB_MODE=local-ducklake \
  -e DUCKDB_OTLP_TOKEN=replace-with-a-private-token \
  -v otlp-data:/data ghcr.io/smithclay/duckdb-otlp:latest
```

`duckdb-otlp-server` remains available inside the image as an alias for `duckdb-otlp`.
