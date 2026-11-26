# CLI Reference

`duckdb-otlp` is a single binary that receives OpenTelemetry data and writes it to a DuckDB-attached catalog or Parquet. It runs on macOS and Linux, and is the same binary that ships in the Docker image.

With no arguments it starts a working local receiver — no configuration file, no environment variables, no credentials:

```sh
duckdb-otlp
```

That listens for OTLP/HTTP on `127.0.0.1:4318` and OTLP/gRPC on `127.0.0.1:4317`, and streams into a local DuckLake lakehouse under `$XDG_DATA_HOME/duckdb-otlp`.

## Install

### Install script

```sh
curl -fsSL https://smithclay.github.io/duckdb-otlp/install.sh | sh
```

That resolves the latest release, downloads the tarball for your platform, verifies it against the release's `SHA256SUMS`, and installs `duckdb-otlp` into `$HOME/.local/bin`. It needs `curl` or `wget` and `sha256sum` or `shasum`; nothing is installed if the checksum does not match. Pipe options through `sh -s --`:

```sh
# A specific release, installed system-wide
curl -fsSL https://smithclay.github.io/duckdb-otlp/install.sh | sudo sh -s -- \
  --version v0.7.2 --bin-dir /usr/local/bin
```

| Option | Environment variable | Default |
|--------|----------------------|---------|
| `--version <tag>` | `DUCKDB_OTLP_VERSION` | the latest release |
| `--bin-dir <dir>` | `DUCKDB_OTLP_BIN_DIR` | `$HOME/.local/bin` |
| `--platform <id>` | `DUCKDB_OTLP_PLATFORM` | detected from `uname` |

A flag always wins over its variable. Re-running the script upgrades in place: the new binary is staged next to the old one and renamed over it, so a running `duckdb-otlp` is never left half-written. Exit codes match the CLI's own — `0` installed, `1` the install failed, `2` the command line was wrong — and everything it prints goes to stderr.

### Homebrew

```sh
brew install smithclay/tap/duckdb-otlp
```

`brew services start duckdb-otlp` runs `duckdb-otlp serve` with its defaults: OTLP/HTTP on `127.0.0.1:4318`, OTLP/gRPC on `127.0.0.1:4317`, and a local DuckLake under `$XDG_DATA_HOME/duckdb-otlp` (`~/.local/share/duckdb-otlp` when unset). The service can only run those defaults, because `duckdb-otlp` has no configuration file; to change the mode, ports, bind host, or token, run `duckdb-otlp serve` yourself instead.

### Release tarballs

Each release ships a tarball per platform — `linux-amd64`, `linux-arm64`, `darwin-amd64`, `darwin-arm64` — holding a single executable named `duckdb-otlp`, plus a `SHA256SUMS` file. To do by hand what the script does:

```sh
VERSION=v0.7.2
PLATFORM=darwin-arm64    # or linux-amd64, linux-arm64, darwin-amd64
BASE=https://github.com/smithclay/duckdb-otlp/releases/download/$VERSION

curl -fsSLO "$BASE/duckdb-otlp-$VERSION-$PLATFORM.tar.gz"
curl -fsSLO "$BASE/SHA256SUMS"
shasum -a 256 -c SHA256SUMS --ignore-missing
tar -xzf "duckdb-otlp-$VERSION-$PLATFORM.tar.gz"
sudo mv duckdb-otlp /usr/local/bin/
```

On Linux use `sha256sum -c SHA256SUMS --ignore-missing` instead of `shasum`. macOS binaries are unsigned and unnotarized, so Gatekeeper will quarantine one downloaded through a browser; `curl` does not set the quarantine attribute, and `xattr -d com.apple.quarantine duckdb-otlp` clears it if you hit it.

The container ships the same binary:

```sh
docker run --rm -p 4318:4318 ghcr.io/smithclay/duckdb-otlp:latest
```

## Commands

| Command | What it does |
|---------|--------------|
| `duckdb-otlp [serve]` | Start the OTLP listeners. The default when no subcommand is given. |
| `duckdb-otlp validate` | Resolve the configuration, print the effective settings and generated SQL, exit. |
| `duckdb-otlp doctor` | Check every configured listener and report each one. Exit 0 means healthy. |
| `duckdb-otlp version` | Print the version. |
| `duckdb-otlp help [COMMAND]` | Show help for the tool or one command. |

### Reading files and querying data

`duckdb-otlp` only receives. To read OTLP/OTAP files, or to query what it stored, use the `otlp` extension from the `duckdb` CLI:

```sh
duckdb -c "INSTALL otlp FROM community; LOAD otlp;
           COPY (FROM read_otlp_traces('traces.pb')) TO 'traces.parquet'"
```

While the server runs it holds the catalog's write lock, so query it over [Quack](#serve) (`--quack PORT`) instead.

Mistyping a command is an error rather than a silent fall-through to `serve`:

```
$ duckdb-otlp valdate
ERROR: Unknown command "valdate".
Did you mean `duckdb-otlp validate`?
```

## Exit codes and output streams

| Code | Meaning |
|------|---------|
| `0` | Success. |
| `1` | The work failed — a listener is down, the configuration is incomplete, startup failed. |
| `2` | The command line was wrong — unknown command or flag, a missing or extra argument. |

The split matters to anything that checks the result rather than reading it: `2` says fix the invocation, `1` says the invocation was fine and the work was not.

The banner and reports go to stdout; warnings and errors go to stderr. The `serve` banner is written unbuffered, so a redirected or captured stdout (`docker logs`, a systemd journal, a CI log) shows it as the server starts rather than when it stops.

## Configuration precedence

Command-line flag, then environment variable, then built-in default. Every flag that maps onto a setting overrides the matching variable, so `--http 4318` wins over `DUCKDB_OTLP_HTTP_PORT=9999`. A flag is layered over the environment rather than written into it, so its value — a `--token` above all — stays inside the configuration and is never published to the rest of the process.

Each command accepts only the flags that apply to it. `doctor` takes every `serve` flag except `--init-sql` and `--dry-run`, and `--json` belongs to `doctor` alone. Naming a flag outside its command is an error that says where the flag does belong:

```
$ duckdb-otlp doctor --init-sql init.sql
ERROR: `doctor` does not accept "--init-sql"; it is a flag of: serve, validate.
```

`duckdb-otlp help COMMAND` lists what a command takes.

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
| `--init-sql PATH` | `DUCKDB_OTLP_INIT_SQL` | *(none)* |
| `--secret-dir DIR` | `DUCKDB_OTLP_SECRET_DIR` | `$HOME/.duckdb/stored_secrets` |
| `--token TOKEN` | `DUCKDB_OTLP_TOKEN` | *(none)* |
| `--no-auth` | `DUCKDB_OTLP_DISABLE_AUTH` | `0` |
| `--quack PORT` | `DUCKDB_QUACK_PORT` | off (`0` disables) |
| `--quack-token TOKEN` | `DUCKDB_QUACK_TOKEN` | *(none)* |
| — | `DUCKDB_QUACK_SSL_CERT_FILE`, `DUCKDB_QUACK_SSL_KEY_FILE` | self-signed (generated under `<data dir>/.duckdb/extension_data/quack`, so it survives a redeploy with the data) |
| `--startup-timeout SECS` | `DUCKDB_OTLP_STARTUP_TIMEOUT` | `60` |

Passing a token as a flag makes it visible in the process list; prefer `DUCKDB_OTLP_TOKEN` outside of local use.

The [`otlp_serve` tuning options](../serve/) are daemon settings too, named `DUCKDB_OTLP_` plus the option in upper case: `DUCKDB_OTLP_HTTP_THREADS`, `DUCKDB_OTLP_MAX_BODY_BYTES`, `DUCKDB_OTLP_MAX_BUFFERED_BYTES`, `DUCKDB_OTLP_SEAL_TARGET_BYTES`, `DUCKDB_OTLP_SEAL_MAX_AGE_MS`, `DUCKDB_OTLP_MAINTENANCE_INTERVAL_MS`, `DUCKDB_OTLP_PROMOTE_RESOURCE_ATTRIBUTES` and `DUCKDB_OTLP_PROMOTE_SCOPE_ATTRIBUTES`. An unset one keeps the extension's default.

### Destinations (`--mode`)

A mode is a preset: a SQL file ([`src/server/modes/`](https://github.com/smithclay/duckdb-otlp/tree/main/src/server/modes)) compiled into the binary, which installs extensions, creates secrets and attaches the catalog. `duckdb-otlp validate --mode MODE` prints it expanded. Each one is a point in a grid of three independent choices — the catalog kind, where that catalog lives, and where the data files go.

| Mode | Catalog | Catalog store | Data files | Required settings |
|------|---------|---------------|------------|-------------------|
| `local-ducklake` *(default)* | DuckLake | local DuckDB file | local directory | *(none)* |
| `parquet` | *(none)* | — | local directory or `s3://` | `AWS_REGION` for S3 |
| `aws-ducklake` | DuckLake | local DuckDB file | `s3://` | `DUCKLAKE_DATA_PATH`, `AWS_REGION` |
| `gcp-ducklake` | DuckLake | Postgres | `gcss://` | `DUCKLAKE_DATA_PATH`, `PGHOST`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` |
| `r2-local-ducklake` | DuckLake | local DuckDB file | R2 | `R2_BUCKET`, `CLOUDFLARE_ACCOUNT_ID` |
| `r2-neon-ducklake` | DuckLake | Postgres | R2 | the two above, plus `PGHOST`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` |
| `r2-data-catalog` | Iceberg REST | Cloudflare | R2 (catalog-managed) | `R2_BUCKET`, `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN` |
| `s3-tables` | Iceberg REST | AWS | S3 (catalog-managed) | `S3_TABLES_BUCKET_ARN`, `AWS_REGION` |
| `none` | *(whatever `--init-sql` attaches)* | — | — | *(none)* |

DuckLake presets set these catalog-wide options after attaching the lake. `duckdb-otlp validate` prints the statements. To set the options individually, use `--init-sql`, which runs after the preset.

| Setting | Default | DuckLake options |
|---------|---------|------------------|
| `DUCKDB_OTLP_TARGET_FILE_SIZE` | `256MB` | `target_file_size` for compaction. Include a size unit, such as `128MiB`. |
| `DUCKDB_OTLP_MAINTENANCE_RETENTION` | `900s` (15 minutes) | `expire_older_than` and `delete_older_than` for snapshot and file cleanup. Supply a DuckDB interval, such as `2 hours`. |

Missing settings are reported together, in one run:

```console
$ duckdb-otlp validate --mode r2-data-catalog
ERROR: Missing 3 required settings for DUCKDB_MODE=r2-data-catalog:
  CLOUDFLARE_API_TOKEN
  CLOUDFLARE_ACCOUNT_ID  (or set CLOUDFLARE_WAREHOUSE)
  R2_BUCKET  (or set CLOUDFLARE_WAREHOUSE)
```

#### Storage settings

One name per concept, across every mode that uses it.

| Setting | Used by | Notes |
|---------|---------|-------|
| `DUCKLAKE_DATA_PATH` | every DuckLake mode | where Parquet data files go |
| `DUCKLAKE_CATALOG_PATH` | DuckLake modes with a local catalog | a `postgres:…` value moves the catalog to Postgres |
| `PARQUET_EXPORT_PATH` | `parquet` | local path or `s3://` (default `<data-dir>/parquet`); an `s3://` path loads the S3 filesystem and AWS credential chain |
| `S3_BUCKET`, `S3_PREFIX` | `parquet` | composed into `s3://bucket/prefix` when `PARQUET_EXPORT_PATH` is unset |
| `S3_ENDPOINT`, `S3_URL_STYLE` | `parquet` | for S3-compatible storage |
| `S3_TABLES_BUCKET_ARN` | `s3-tables` | the table bucket's ARN |
| `AWS_REGION`, `AWS_PROFILE` | every AWS mode | standard AWS variables; `AWS_DEFAULT_REGION` also accepted |
| `R2_BUCKET`, `R2_PREFIX` | every R2 mode | composed into `s3://bucket/prefix` |
| `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` | every R2 mode | optional — see below |
| `R2_ENDPOINT` | every R2 mode | defaults to `<account>.r2.cloudflarestorage.com` |
| `CLOUDFLARE_ACCOUNT_ID` | every R2 mode | derives the endpoint, catalog URI, and warehouse |
| `CLOUDFLARE_API_TOKEN` | `r2-data-catalog` | R2 Data Catalog read/write token |
| `CLOUDFLARE_CATALOG_URI`, `CLOUDFLARE_WAREHOUSE` | `r2-data-catalog` | both derived from the account and bucket; set only to override |
| `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `PGSSLMODE` | Postgres-catalog modes | the standard libpq names |
| `DUCKDB_OTLP_GCS_EXTENSION_PATH` | `gcp-ducklake` | a local `gcs.duckdb_extension` to `LOAD` instead of the community build; turns on `allow_unsigned_extensions`. The image sets it to its bundled build |
| `DUCKLAKE_DATA_INLINING_ROW_LIMIT` | every DuckLake mode | DuckLake's per-insert inlining limit; `0` writes every seal straight to Parquet |

Values are not validated beyond being present: a malformed one fails when the SQL runs. Credential values are never interpolated into the generated SQL. They are bound as session variables and referenced with `getvariable()`, so `validate` can print the full boot SQL without disclosing them.

#### Credentials from the DuckDB secret store

Storage credentials do not have to be environment variables. If no key pair is set, the mode emits no `CREATE SECRET` at all and DuckDB resolves access through its own persistent secret store (or, for AWS and GCP, the provider credential chain). Create the secret once:

```bash
duckdb -c "
  INSTALL httpfs; LOAD httpfs;
  CREATE PERSISTENT SECRET r2_storage (
    TYPE s3, KEY_ID '...', SECRET '...',
    REGION 'auto', ENDPOINT '<account>.r2.cloudflarestorage.com', URL_STYLE 'path');"
```

It is written to `$HOME/.duckdb/stored_secrets` with `0600` permissions and loaded automatically by every later run. `--secret-dir` moves that directory, for deployments that mount secrets read-only somewhere else.

The startup banner always names the source it resolved, so the two cases are never confused:

```text
Credentials: R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY
Credentials: DuckDB secret store (no R2 key pair in the environment)
```

Setting only one half of a key pair is an error, not a silent fall-through to the store.

#### Reading back the `parquet` dataset

`parquet` mode has no catalog: each seal writes straight to `<root>/<table>/year=/month=/day=/`. Read it with `read_parquet`, passing `hive_partitioning = false` (the partition keys live only in the path):

```sql
FROM read_parquet('<root>/otlp_logs/**/*.parquet', hive_partitioning = false, union_by_name = true);
```

With Quack enabled, the server also defines an `otlp_<signal>` view over each signal once files exist.

#### `--mode none`

`none` attaches nothing and installs nothing. The destination is entirely whatever `--init-sql` sets up, and `--catalog` names it:

```bash
duckdb-otlp serve --mode none --init-sql ./lake.sql --catalog lake
```

```sql
-- lake.sql: S3 data files with a Postgres catalog, which no preset covers
INSTALL ducklake; INSTALL postgres; INSTALL httpfs;
LOAD ducklake; LOAD postgres; LOAD httpfs;
ATTACH 'ducklake:postgres:dbname=lake host=db.internal' AS lake
  (DATA_PATH 's3://telemetry/lake');
```

With no `--catalog`, data lands in the control database — a working single-file server with no lakehouse at all.

### Custom DuckDB setup (`--init-sql`)

`--init-sql PATH` (or `DUCKDB_OTLP_INIT_SQL`) runs a SQL script **after** the mode has attached its catalog and **before** ingest starts. It is the escape hatch for DuckDB configuration the modes do not model: extra `ATTACH`es, `SET` statements, additional secrets, or views over the ingest tables.

```sql
-- /etc/duckdb-otlp/init.sql
SET memory_limit = '8GB';
CREATE OR REPLACE VIEW recent_errors AS
  SELECT * FROM otlp_logs WHERE severity_number >= 17;
```

```bash
duckdb-otlp serve --init-sql /etc/duckdb-otlp/init.sql
```

Behavior worth knowing:

- **`serve` and `validate` take it.** `doctor` deliberately does **not** — the container `HEALTHCHECK` runs it on every probe, and a liveness check must not execute operator SQL.
- **`validate` prints the script in place** without running it, so `duckdb-otlp validate --init-sql ...` shows exactly what startup would execute.
- **Failures are fatal.** An unreadable path or a failing statement stops startup rather than silently serving without the configuration you asked for. An empty script is a no-op.
- The script runs before the mode's catalog becomes the instance default, so it can `ATTACH` a catalog that `--catalog` then names.

#### Adding a second destination

A server has exactly **one** ingest target — `otlp_serve` rejects `parquet_export_path` combined with a catalog, and the commit path writes either Parquet or a catalog, never both. To land data in two places, start a second server on its own port from the init script:

```sql
-- alongside the mode's DuckLake catalog on 4318, a plain Parquet drop on 4319
SELECT 1 FROM otlp_serve('otlp:0.0.0.0:4319',
                         parquet_export_path := '/data/parquet',
                         token := getvariable('duckdb_otlp_effective_token'));
```

This is **fan-out by port, not a tee**: a batch lands in whichever port received it, so the exporter chooses the destination. There is no supported way to mirror one stream into two targets — one target has one writer by design, which is what keeps a single sealer per catalog.

Servers started this way are stopped and committed on shutdown along with the daemon's own, because shutdown runs [`otlp_stop()`](../serve/#otlp_stop) with no argument.

### Selecting listeners

Transports are selected by which ports you set. A **non-zero** port narrows the set to what you named; `0` switches one transport off without un-defaulting the others:

```sh
duckdb-otlp --grpc 4317               # gRPC only
duckdb-otlp --grpc 0                  # HTTP only (gRPC off, HTTP still on its default)
duckdb-otlp --host 0.0.0.0 --token "$TOKEN"
```

An IPv6 literal works either bracketed or bare, and appears bracketed in the listener URI:

```sh
duckdb-otlp --host ::1                      # OTLP http: otlp:[::1]:4318
duckdb-otlp --host :: --token "$TOKEN"      # every interface, IPv6
```

Every listener, Quack included, binds the one `--host`. The earlier per-transport settings (`OTEL_HTTP_ADDR`, `OTEL_GRPC_ADDR`, `DUCKDB_OTLP_TRANSPORTS`, `DUCKDB_OTLP_LISTEN_URI`, `DUCKDB_QUACK_ENABLED`, `DUCKDB_QUACK_ADDR`) are rejected at startup with the flag that replaces them.

OTAP/Arrow is a different protocol served by `otap_serve`, so it cannot be combined with standard OTLP listeners in one process:

```sh
duckdb-otlp --otap 4317 --http 0 --grpc 0
```

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
| `OTEL_EXPORTER_OTLP_HEADERS` | An `Authorization=Bearer <token>` entry supplies the expected token. Other entries are ignored. |
| `OTEL_EXPORTER_OTLP_TIMEOUT`, `OTEL_EXPORTER_OTLP_COMPRESSION` | Accepted and ignored (client-side only; compressed bodies are always accepted). |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_PROTOCOL` | **Not read at all.** |
| `OTEL_EXPORTER_OTLP_{TRACES,LOGS,METRICS}_ENDPOINT` | **Rejected at startup.** |
| `OTEL_EXPORTER_OTLP_{CERTIFICATE,CLIENT_KEY,CLIENT_CERTIFICATE}` | **Rejected at startup.** |

`OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_EXPORTER_OTLP_PROTOCOL` are ignored deliberately. They are usually already set in a developer's shell for their application's exporter; reading them as *bind* settings would silently move or narrow the listeners. Use `--host` and `--http`/`--grpc`.

The TLS and per-signal-endpoint variables are rejected rather than ignored for the same reason in reverse: this server does not terminate TLS and serves every signal on one listener, so accepting those settings silently would leave you believing something untrue about your deployment. Put a TLS-terminating proxy in front instead.

## `doctor`

```sh
duckdb-otlp doctor [serve flags]
```

Checks that the configured listeners are up, one line per check, exiting 0 only when every one passed. It takes the same flags as `serve`, so it probes exactly what those settings would bind.

```
$ duckdb-otlp doctor
ok    OTLP http  127.0.0.1:4318
FAIL  OTLP grpc  127.0.0.1:4317  (no response)
```

Every check runs even after one fails, so a partial answer is visible rather than just the first problem. `--json` prints one object instead, for a monitor, a script or an agent that should not be parsing prose:

```
$ duckdb-otlp doctor --json
{"healthy":false,"checks":[{"check":"OTLP http","endpoint":"127.0.0.1:4318","ok":true},
                           {"check":"OTLP grpc","endpoint":"127.0.0.1:4317","ok":false}]}
``` The container image's `HEALTHCHECK` is this command, and Docker keeps its output in the container's health log. `duckdb-otlp healthcheck` is still accepted as the same command, so existing probes keep working.

## Docker

The image runs the same binary, with container-shaped defaults set in the image rather than in the code:

```
DUCKDB_OTLP_DATA_DIR=/data
DUCKDB_OTLP_HOST=0.0.0.0
DUCKDB_OTLP_HTTP_PORT=4318
```

Setting the HTTP port selects HTTP alone; add `DUCKDB_OTLP_GRPC_PORT=4317` for gRPC and `DUCKDB_QUACK_PORT=9494` (with `DUCKDB_QUACK_TOKEN`) for Quack.

Because the image binds `0.0.0.0`, it requires a token:

```sh
docker run --rm -p 4318:4318 \
  -e DUCKDB_MODE=local-ducklake \
  -e DUCKDB_OTLP_TOKEN=replace-with-a-private-token \
  -v otlp-data:/data ghcr.io/smithclay/duckdb-otlp:latest
```

`duckdb-otlp-server` remains available inside the image as an alias for `duckdb-otlp`.
