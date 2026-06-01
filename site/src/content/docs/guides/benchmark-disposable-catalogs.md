---
title: "How to Benchmark Disposable Catalog Ingest"
---

Use `scripts/benchmark_catalog_ingest.py` to compare live OTLP/HTTP ingest across disposable catalog and storage combinations. The runner is inspired by the OpenTelemetry Collector [Log10kDPS load-test benchmark](https://open-telemetry.github.io/opentelemetry-collector-contrib/benchmarks/loadtests/): it sends log records at a target records-per-second rate, samples container CPU and memory, reconciles accepted rows with committed table rows, and writes one consolidated report.

The benchmark starts the published Docker image locally:

```text
ghcr.io/smithclay/duckdb-otlp:latest
```

Override the image when testing a local build:

```bash
uv run python scripts/benchmark_catalog_ingest.py \
  --image duckdb-otlp:local
```

For each scenario it creates temporary provider resources, starts `duckdb-otlp`, sends OTLP/HTTP logs to `/v1/logs`, flushes the server, records table counts and storage object counts where the provider exposes them, then drops tables and deletes the temporary resources. Pass `--keep` to leave resources behind for debugging.

> This is a local benchmark harness, not a hosted benchmark service. Run it from a machine with Docker, `uv`, and the relevant provider CLIs configured.

## Scenarios

The runner supports:

| Scenario | Catalog | Storage |
| --- | --- | --- |
| `local-ducklake` | Local DuckLake metadata file in the container | Local container filesystem |
| `r2-data-catalog` | Cloudflare R2 Data Catalog Iceberg REST catalog | Cloudflare R2 bucket |
| `s3-tables` | Amazon S3 Tables Iceberg REST catalog | S3 Tables table bucket |
| `r2-neon-ducklake` | Neon PostgreSQL DuckLake metadata catalog | Cloudflare R2 bucket |
| `r2-local-ducklake` | Local DuckLake metadata file in the container | Cloudflare R2 bucket |

## Configure credentials

Put provider configuration in `.env` or export it before running the script. Do not commit `.env`.

```bash
# Cloudflare R2 and R2 Data Catalog
CLOUDFLARE_API_TOKEN=...
CLOUDFLARE_ACCOUNT_ID=...
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
# Also accepted:
# CLOUDFLARE_S3_ACCESS_KEY_ID=...
# CLOUDFLARE_S3_SECRET_ACCESS_KEY=...

# Amazon S3 Tables
AWS_PROFILE=cli-dev
AWS_REGION=us-west-2

# Neon-backed DuckLake metadata
NEON_PROJECT_ID=...
# Optional: use an existing disposable database URL instead of creating a branch
NEON_DATABASE_URL=postgresql://...
```

`CLOUDFLARE_ACCOUNT_ID` is optional when `wrangler whoami` can infer a single account. The Cloudflare token must be able to create/delete R2 buckets, enable/disable R2 Data Catalog, and list/delete R2 objects. The R2 access key pair is passed into DuckDB as an S3-compatible secret for DuckLake data files.

The runner defaults to `AWS_PROFILE=cli-dev` when no AWS profile is set. `AWS_REGION` is optional when the AWS CLI profile has a configured region. The runner creates and deletes the CloudFormation stack for S3 Tables from the host, then exports temporary AWS credentials from the active profile and passes them into the container so DuckDB can sign S3 Tables requests.

For Neon, set `NEON_DATABASE_URL` if you already have a disposable PostgreSQL database. Otherwise the runner tries to create a temporary branch with `neonctl` using `NEON_PROJECT_ID`, then deletes that branch during cleanup.

## Run all scenarios

```bash
uv run python scripts/benchmark_catalog_ingest.py \
  --duration 300 \
  --rate 10000 \
  --batch-size 1000
```

Run one scenario while iterating:

```bash
uv run python scripts/benchmark_catalog_ingest.py \
  --scenario r2-local-ducklake \
  --duration 60 \
  --rate 10000
```

Use `--keep` when a run fails and you want to inspect the container, catalog, or bucket:

```bash
uv run python scripts/benchmark_catalog_ingest.py \
  --scenario r2-neon-ducklake \
  --duration 60 \
  --keep
```

With `--keep`, clean up manually after debugging. Without `--keep`, the script attempts to drop the six OTLP tables, stop and remove the Docker container, empty temporary R2 buckets, delete those buckets, delete the S3 Tables CloudFormation stack, and delete any Neon branch it created.

## Read the report

Each run writes:

```text
output/catalog-benchmarks/<run-id>/results.json
output/catalog-benchmarks/<run-id>/report.md
```

The Markdown report includes:

- accepted log records per second
- dropped records, computed as attempted minus accepted
- average and maximum Docker CPU percentage
- average and maximum Docker memory in MiB
- `otlp_flush` and `otlp_server_list` counters
- committed row counts for all six OTLP tables
- local filesystem file counts for `local-ducklake`
- R2 object counts and Parquet file counts where applicable
- S3 Tables table counts where object-level files are not directly enumerable

## Validate setup without running load

Use `--dry-run` to render scenario SQL and planned resource names without starting the Docker container or sending OTLP data:

```bash
uv run python scripts/benchmark_catalog_ingest.py \
  --scenario s3-tables \
  --dry-run
```

The dry-run output is written under `output/catalog-benchmarks/<run-id>/`.

## See also

- [Stream to DuckLake](../stream-to-ducklake/)
- [Stream to Amazon S3 Tables](../stream-to-s3-tables/)
- [Stream to Cloudflare R2 Data Catalog](../stream-to-r2-data-catalog/)
- [Live Ingest Reference](../../reference/serve/)
