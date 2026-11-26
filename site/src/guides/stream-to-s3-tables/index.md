# How to Stream OTLP to Amazon S3 Tables

Use the `duckdb-otlp` Docker image in `s3-tables` mode to stream OTLP/HTTP exports into an Iceberg catalog hosted by **Amazon S3 Tables**.

The container initializes DuckDB, loads the required extensions, attaches the S3 Tables table bucket, starts the ingest server, and commits accepted rows in batches.

Choose **Amazon S3 Tables** when you want AWS-managed table buckets and an Iceberg REST catalog endpoint. To write partitioned Parquet files to a regular `s3://` bucket, use [Stream to Parquet](../stream-to-parquet/).

Live ingestion uses OTLP/HTTP on port `4318`. WASM builds do not include the ingest server.

## Create S3 Tables resources

Choose a region and an AWS CLI profile that can create CloudFormation and S3 Tables resources:

```bash
export AWS_PROFILE=cli-dev
export AWS_REGION=us-west-2
export STACK_NAME=duckdb-otlp-s3tables

export AWS_ACCOUNT_ID="$(
  aws sts get-caller-identity \
    --profile "$AWS_PROFILE" \
    --query Account \
    --output text
)"

export TABLE_BUCKET_NAME="duckdb-otlp-s3tables-${AWS_ACCOUNT_ID}-${AWS_REGION}"
```

If `aws sts get-caller-identity` fails because your profile expired, refresh that profile first with your normal AWS auth flow, for example `aws sso login --profile "$AWS_PROFILE"` for SSO profiles.

Save this CloudFormation template as `s3tables-otlp.yaml`:

```yaml
AWSTemplateFormatVersion: '2010-09-09'
Description: Amazon S3 Tables resources for duckdb-otlp.

Parameters:
  TableBucketName:
    Type: String
    Description: Name of the Amazon S3 Tables table bucket.
    MinLength: 3
    MaxLength: 63
  NamespaceName:
    Type: String
    Description: Single-level namespace for duckdb-otlp tables.
    Default: otlp

Resources:
  OtlpTableBucket:
    Type: AWS::S3Tables::TableBucket
    Properties:
      TableBucketName: !Ref TableBucketName
      Tags:
        - Key: project
          Value: duckdb-otlp

  OtlpNamespace:
    Type: AWS::S3Tables::Namespace
    Properties:
      TableBucketARN: !GetAtt OtlpTableBucket.TableBucketARN
      Namespace: !Ref NamespaceName

Outputs:
  TableBucketName:
    Value: !Ref TableBucketName
  TableBucketArn:
    Value: !GetAtt OtlpTableBucket.TableBucketARN
  NamespaceName:
    Value: !Ref NamespaceName
```

Deploy it:

```bash
aws cloudformation deploy \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --stack-name "$STACK_NAME" \
  --template-file s3tables-otlp.yaml \
  --parameter-overrides \
    TableBucketName="$TABLE_BUCKET_NAME" \
    NamespaceName=otlp
```

Read the table bucket ARN. DuckDB attaches this ARN; DuckDB cannot attach an `s3://` path for S3 Tables.

```bash
export TABLE_BUCKET_ARN="$(
  aws cloudformation describe-stacks \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='TableBucketArn'].OutputValue | [0]" \
    --output text
)"
```

## Configure

Create `s3tables.env`:

```ini
DUCKDB_MODE=s3-tables
DUCKDB_OTLP_TOKEN=dev-token-123456

DUCKDB_CATALOG=s3tables
DUCKDB_SCHEMA=otlp

DUCKDB_QUACK_PORT=9494
DUCKDB_QUACK_TOKEN=dev-quack-token-123456

AWS_REGION=us-west-2
AWS_PROFILE=cli-dev
S3_TABLES_BUCKET_ARN=<table-bucket-arn>
```

Replace `<table-bucket-arn>` with the `TABLE_BUCKET_ARN` value from CloudFormation.

## Start the server

Prefer to run manually in the DuckDB shell? See [Run manually](#run-manually).

Mount your AWS config read-only so DuckDB can use the configured profile:

```bash
docker run --rm --name duckdb-otlp \
  --env-file s3tables.env \
  -p 4318:4318 \
  -p 9494:9494 \
  -v "$HOME/.aws:/root/.aws:ro" \
  ghcr.io/smithclay/duckdb-otlp:latest
```

The container creates these Iceberg tables in the S3 Tables namespace if they do not exist:

- `s3tables.otlp.otlp_logs`
- `s3tables.otlp.otlp_traces`
- `s3tables.otlp.otlp_metrics_gauge`
- `s3tables.otlp.otlp_metrics_sum`
- `s3tables.otlp.otlp_metrics_histogram`
- `s3tables.otlp.otlp_metrics_exp_histogram`

## POST a log record

In another terminal:

```bash
curl -sS http://localhost:4318/v1/logs \
  -H 'Authorization: Bearer dev-token-123456' \
  -H 'Content-Type: application/json' \
  -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"s3-tables-demo"}},{"key":"deployment.environment","value":{"stringValue":"docs"}}]},"scopeLogs":[{"scope":{"name":"duckdb-otlp-guide"},"logRecords":[{"timeUnixNano":"1704067200000000000","observedTimeUnixNano":"1704067200123456789","severityNumber":9,"severityText":"INFO","body":{"stringValue":"hello from Amazon S3 Tables"},"attributes":[{"key":"guide","value":{"stringValue":"stream-to-s3-tables"}}]}]}]}]}'
```

**Response:**

```json
{"status":"buffered","rows":1,"batches":1}
```

Rows are accepted before they are durable. They commit automatically in the
background, on graceful shutdown, or immediately after an explicit flush.

## Query committed rows

Flush and query through Quack from a host DuckDB process:

The server image is distroless and has no shell or DuckDB CLI, so do not use
`docker exec ... sh -c` for inspection SQL. The examples in this guide enable
Quack, publish port `9494`, and use DuckDB 2.0's `CONNECT` statement for
ordinary server-side SQL. Replace `<fingerprint>` with the TLS certificate
fingerprint the container prints at startup (see
[Query with Quack](../query-with-quack/)).

```bash
duckdb <<'SQL'
INSTALL quack;
LOAD quack;

CREATE SECRET duckdb_otlp_quack (
  TYPE quack,
  SCOPE 'quack:localhost:9494',
  TOKEN 'dev-quack-token-123456'
);

ATTACH 'quack:localhost:9494' AS otel (TYPE quack, SSL_FINGERPRINT '<fingerprint>');
CONNECT otel;

SELECT * FROM otlp_flush();

SELECT service_name, severity_text, body
FROM s3tables.otlp.otlp_logs
WHERE service_name = 's3-tables-demo'
ORDER BY time_unix_nano DESC
LIMIT 5;

DISCONNECT;
SQL
```

## Stop cleanly

If you plan to delete the S3 Tables resources immediately, skip this
step and use [Clean up](#clean-up) instead.

```bash
docker stop duckdb-otlp
```

The image runs `otlp_stop()` during shutdown,
so remaining buffered rows are committed before the process exits.

## Clean up

Drop the Iceberg tables before deleting the CloudFormation stack; S3 Tables table buckets cannot be removed while tables remain:

```bash
duckdb <<'SQL'
INSTALL quack;
LOAD quack;

CREATE SECRET duckdb_otlp_quack (
  TYPE quack,
  SCOPE 'quack:localhost:9494',
  TOKEN 'dev-quack-token-123456'
);

ATTACH 'quack:localhost:9494' AS otel (TYPE quack, SSL_FINGERPRINT '<fingerprint>'); -- printed by the container at startup
CONNECT otel;

SELECT status FROM otlp_stop('otlp:0.0.0.0:4318');
DROP TABLE IF EXISTS s3tables.otlp.otlp_logs;
DROP TABLE IF EXISTS s3tables.otlp.otlp_traces;
DROP TABLE IF EXISTS s3tables.otlp.otlp_metrics_gauge;
DROP TABLE IF EXISTS s3tables.otlp.otlp_metrics_sum;
DROP TABLE IF EXISTS s3tables.otlp.otlp_metrics_histogram;
DROP TABLE IF EXISTS s3tables.otlp.otlp_metrics_exp_histogram;
DETACH s3tables;

DISCONNECT;
SQL

docker stop duckdb-otlp
```

Then delete the S3 Tables table bucket and namespace stack:

```bash
aws cloudformation delete-stack \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --stack-name "$STACK_NAME"

aws cloudformation wait stack-delete-complete \
  --profile "$AWS_PROFILE" \
  --region "$AWS_REGION" \
  --stack-name "$STACK_NAME"
```

## Run manually

To run this configuration in a DuckDB 2.0+ shell instead of the daemon, execute the SQL below. Replace bracketed values with the corresponding values from this guide. Keep the shell open while clients send telemetry.

```bash
duckdb duckdb-otlp-control.duckdb
```

```sql
-- The daemon embeds otlp statically; the shell loads the extension explicitly.
INSTALL otlp FROM community;
LOAD otlp;

-- Configure the AWS credential chain and attach the S3 Tables catalog.
INSTALL iceberg;
INSTALL aws;
INSTALL httpfs;
LOAD iceberg;
LOAD aws;
LOAD httpfs;
CREATE OR REPLACE SECRET s3_tables_secret (
  TYPE s3,
  PROVIDER credential_chain,
  CHAIN config,
  PROFILE 'cli-dev',
  REGION 'us-west-2'
);
ATTACH '<table-bucket-arn>' AS s3tables (
  TYPE iceberg,
  ENDPOINT_TYPE s3_tables
);

-- Create the target schema before starting ingestion.
CREATE SCHEMA IF NOT EXISTS s3tables.otlp;

-- Match the daemon's default-catalog setting for unqualified queries.
USE s3tables;

-- Start OTLP/HTTP. Seal cadence, file sizes, and buffer limits use defaults;
-- override only as needed — see the Live Ingest Reference:
-- https://smithclay.github.io/duckdb-otlp/reference/serve/
SELECT listen_url, catalog_name, schema_name
FROM otlp_serve(
  'otlp:0.0.0.0:4318',
  catalog := 's3tables',
  schema := 'otlp',
  token := 'dev-token-123456',
  allow_other_hostname := true
);

-- The guide's daemon configuration also enables Quack.
INSTALL quack;
LOAD quack;
SELECT listen_uri
FROM quack_serve(
  'quack:0.0.0.0:9494',
  token := 'dev-quack-token-123456',
  allow_other_hostname := true
);
```

Before closing DuckDB, stop both listeners cleanly:

```sql
-- Stop Quack, then commit buffered telemetry and stop OTLP.
CALL quack_stop('quack:0.0.0.0:9494');
SELECT status, dropped_rows
FROM otlp_stop('otlp:0.0.0.0:4318');
```

## Troubleshooting

- If the container cannot find AWS credentials at startup, confirm `AWS_PROFILE` in `s3tables.env` matches a profile in the mounted `$HOME/.aws` directory.
- If attachment fails, confirm `S3_TABLES_BUCKET_ARN` is the table bucket ARN from CloudFormation. DuckDB cannot attach an `s3://` path for S3 Tables.
- If no rows appear after a `202` response, run the flush command before querying.
- If stack deletion fails, rerun [Clean up](#clean-up) while the container is still running, then delete the CloudFormation stack again.
- S3 Tables stores live ingest timestamp columns with microsecond precision because the Iceberg catalog does not accept DuckDB `TIMESTAMP_NS` columns.
- If DuckDB reports unsupported catalog checkpointing, no action is required; ingest, flush, and stop durability behavior stays the same.

## See also

- [DuckDB Amazon S3 Tables docs](https://duckdb.org/docs/current/core_extensions/iceberg/amazon_s3_tables.html)
- [DuckDB Iceberg REST catalog docs](https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_rest_catalogs)
- [DuckDB + S3 Tables walkthrough by Thomas Krizsan](https://blog.pesky.moe/posts/2026-04-01-duckdb-s3/)
- [Live Ingest Reference](../../reference/serve/)
