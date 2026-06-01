---
title: "How to Run the Docker Server Image"
---

The Docker image starts the existing DuckDB `otlp_serve` server and initializes the selected catalog/storage backend from environment variables. It does not require users to run DuckDB setup SQL by hand.

> The native extension currently serves OTLP/HTTP on `4318`; it does not implement OTLP/gRPC.

## Local DuckLake quickstart

Create a local env file from the documented template:

```bash
cp .env.example .env
```

Use the local DuckLake mode:

```env
DUCKDB_MODE=local-ducklake
DUCKDB_DATABASE=/data/duckdb-otlp-control.duckdb
DUCKDB_SCHEMA=main
DUCKDB_OTLP_TOKEN=dev-otlp-token-123456

DUCKLAKE_NAME=otel
DUCKLAKE_CATALOG_PATH=/data/ducklake/catalog.duckdb
DUCKLAKE_DATA_PATH=/data/ducklake/storage
```

Start the container:

```bash
docker run --rm \
  --env-file .env \
  -p 4318:4318 \
  -v "$(pwd)/data:/data" \
  ghcr.io/smithclay/duckdb-otlp:latest
```

`ducklake-local` is accepted as an alias for `local-ducklake`.

## Cloudflare quickstart

Use this mode when you already have a Cloudflare R2 bucket with R2 Data Catalog enabled. Save this as `cloudflare.env`:

```env
DUCKDB_MODE=cloudflare
DUCKDB_DATABASE=/data/duckdb-otlp-control.duckdb
DUCKDB_OTLP_TOKEN=dev-otlp-token-123456

CLOUDFLARE_ACCOUNT_ID=...
CLOUDFLARE_R2_BUCKET=...
CLOUDFLARE_ACCESS_KEY_ID=...
CLOUDFLARE_SECRET_ACCESS_KEY=...
CLOUDFLARE_CATALOG_URI=https://catalog.cloudflarestorage.com/<account-id>/<bucket>
CLOUDFLARE_CATALOG_TOKEN=...
```

Start the container:

```bash
docker run --rm \
  --env-file cloudflare.env \
  -p 4318:4318 \
  ghcr.io/smithclay/duckdb-otlp:latest
```

`cloudflare` is an alias for `r2-data-catalog`. Secrets are passed into DuckDB with `getenv(...)` and are not printed by the entrypoint.

## Supported modes

The image supports these mode names:

| Mode | Catalog | Storage |
| --- | --- | --- |
| `local-ducklake` | Local DuckLake metadata file | Local filesystem |
| `r2-data-catalog` | Cloudflare R2 Data Catalog Iceberg REST catalog | Cloudflare R2 bucket |
| `s3-tables` | Amazon S3 Tables Iceberg REST catalog | S3 Tables table bucket |
| `r2-local-ducklake` | Local DuckLake metadata file | Cloudflare R2 bucket |
| `r2-neon-ducklake` | Neon PostgreSQL DuckLake metadata catalog | Cloudflare R2 bucket |

Aliases:

| Alias | Mode |
| --- | --- |
| `ducklake-local` | `local-ducklake` |
| `cloudflare` | `r2-data-catalog` |
| `s3tables` | `s3-tables` |

Invalid modes fail before DuckDB starts and print the supported list.

## Configuration reference

Common variables:

| Variable | Default | Description |
| --- | --- | --- |
| `DUCKDB_MODE` | required | Mode-based startup. |
| `DUCKDB_DATABASE` | `/data/duckdb-otlp-control.duckdb` | DuckDB database opened by the server process. Do not set this to the same catalog name as the mode catalog. |
| `DUCKDB_SCHEMA` | mode-specific | Schema for OTLP tables. |
| `DUCKDB_OTLP_TOKEN` | `dev-otlp-token-123456` | Bearer token for OTLP/HTTP POSTs. Must be at least 16 characters. |
| `OTEL_HTTP_ADDR` | `0.0.0.0:4318` | HTTP bind address used to build the `otlp:` listen URI. |
| `DRY_RUN` | `0` | Set to `1` to validate config, print planned SQL, and exit without starting the server. |

Local DuckLake:

| Variable | Default | Description |
| --- | --- | --- |
| `DUCKLAKE_NAME` | `otel` | DuckDB catalog name for `local-ducklake`. |
| `DUCKLAKE_CATALOG_PATH` | `/data/ducklake/catalog.duckdb` | DuckLake metadata catalog path. |
| `DUCKLAKE_DATA_PATH` | `/data/ducklake/storage` | DuckLake data file path. |

Cloudflare R2 Data Catalog:

| Variable | Required | Description |
| --- | --- | --- |
| `CLOUDFLARE_ACCOUNT_ID` | yes | Cloudflare account ID. |
| `CLOUDFLARE_R2_BUCKET` | yes | R2 bucket with Data Catalog enabled. Also accepts `R2_BUCKET_NAME` or `R2_BUCKET`. |
| `CLOUDFLARE_R2_ENDPOINT` | no | Optional R2 S3-compatible endpoint override. Defaults to `<account-id>.r2.cloudflarestorage.com`. |
| `CLOUDFLARE_ACCESS_KEY_ID` | yes | R2 S3 access key. Also accepts benchmark aliases such as `R2_ACCESS_KEY_ID`. |
| `CLOUDFLARE_SECRET_ACCESS_KEY` | yes | R2 S3 secret key. Also accepts benchmark aliases such as `R2_SECRET_ACCESS_KEY`. |
| `CLOUDFLARE_CATALOG_URI` | yes | R2 Data Catalog URI from Wrangler. |
| `CLOUDFLARE_CATALOG_TOKEN` | yes | R2 Data Catalog API token. Also accepts `CLOUDFLARE_API_TOKEN`. |
| `CLOUDFLARE_WAREHOUSE` | no | Optional Iceberg warehouse override. Defaults to `<account-id>_<bucket>`. |

Amazon S3 Tables:

| Variable | Required | Description |
| --- | --- | --- |
| `S3_TABLES_BUCKET_ARN` | yes | S3 Tables bucket ARN. Also accepts `S3_TABLES_TABLE_BUCKET_ARN` or `TABLE_BUCKET_ARN`. |
| `AWS_REGION` | derived from ARN when possible | AWS region. Also accepts `AWS_DEFAULT_REGION`. |
| `AWS_PROFILE` | optional | AWS profile for local Docker runs when `~/.aws` is mounted into the container. The benchmark runner exports temporary credentials from this profile for the container. |

Cloudflare R2 DuckLake modes:

| Mode | Required variables |
| --- | --- |
| `r2-local-ducklake` | R2 bucket, R2 access key/secret, optional `CLOUDFLARE_R2_PREFIX`, optional `DUCKLAKE_CATALOG_PATH`. |
| `r2-neon-ducklake` | R2 settings plus `NEON_PGHOST`, `NEON_PGPORT`, `NEON_PGDATABASE`, `NEON_PGUSER`, `NEON_PGPASSWORD`, `NEON_PGSSLMODE`. |

## AWS profile credentials

For local Docker runs, pass an AWS profile and mount the AWS config directory:

```bash
docker run --rm \
  --env-file s3tables-profile.env \
  -e AWS_PROFILE=cli-dev \
  -v "$HOME/.aws:/root/.aws:ro" \
  -p 4318:4318 \
  ghcr.io/smithclay/duckdb-otlp:latest
```

The mode uses DuckDB's AWS `credential_chain` provider. With `AWS_PROFILE` set, it generates a profile-based secret; without it, it uses the environment credential chain.

## Dry run

Validate configuration and inspect generated SQL without starting the server:

```bash
docker run --rm --env-file .env -e DRY_RUN=1 ghcr.io/smithclay/duckdb-otlp:latest
```

The output lists the selected mode, extensions, and generated initialization SQL. Credential values are referenced through environment variables and are not printed.

## Adding new modes

Mode implementations live under `docker/duckdb-otlp-server/modes/`. Add a new `<mode>.sh` that defines:

```sh
MODE_EXTENSIONS="extension names for dry-run output"

mode_defaults() {
  # Set CATALOG, SCHEMA, and mode-specific defaults.
}

mode_validate() {
  # Validate only variables required by this mode.
}

mode_emit_sql() {
  # Print deterministic DuckDB initialization SQL.
}
```

Then add the mode name to `SUPPORTED_MODES` in `docker/duckdb-otlp-server/entrypoint.sh` and document it in `.env.example`. Keep secrets in environment variables and reference them from SQL with `getenv('NAME')`.

## Troubleshooting

- Invalid mode: the entrypoint fails before starting DuckDB and prints the supported `DUCKDB_MODE` values.
- Missing mode: set `DUCKDB_MODE`; the image no longer accepts ad hoc setup SQL at startup.
- Missing credentials: only variables required by the selected mode are validated.
- Catalog name conflict: do not use a `DUCKLAKE_NAME` or `DUCKDB_CATALOG` equal to the basename of `DUCKDB_DATABASE`; DuckDB already uses that name for the opened database.
- Permission errors: for local DuckLake, ensure the mounted `/data` directory is writable by the container.
- Volume surprises: without `-v "$(pwd)/data:/data"`, local DuckLake data is stored inside the container filesystem and disappears with `--rm`.
- Extension install failures: the image pre-installs common extensions at build time, but startup still runs `INSTALL` defensively. Check network/proxy settings if building the image yourself.
- No gRPC listener: use OTLP/HTTP endpoints on port `4318`.

## See also

- [How to stream to DuckLake](../stream-to-ducklake/)
- [How to stream to Amazon S3 Tables](../stream-to-s3-tables/)
- [How to stream to Cloudflare R2 Data Catalog](../stream-to-r2-data-catalog/)
- [How to benchmark disposable catalog ingest](../benchmark-disposable-catalogs/)
