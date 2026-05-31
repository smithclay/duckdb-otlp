---
title: "How to Stream OTLP to Amazon S3 Tables"
---

Use `otlp_serve(..., catalog := 's3tables')` to stream OTLP/HTTP exports into an Iceberg catalog hosted by **Amazon S3 Tables**.

This guide is specifically for **Amazon S3 Tables**, the managed AWS service with table buckets and an Iceberg REST catalog endpoint. It is not the same as writing Iceberg metadata and Parquet files to an ordinary `s3://` bucket.

> Requires the native extension. Live ingestion is HTTP-only and is not available in WASM builds.

## Create the S3 Tables resources

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

Read the table bucket ARN. This ARN is what DuckDB attaches; it is not an `s3://` path.

```bash
export TABLE_BUCKET_ARN="$(
  aws cloudformation describe-stacks \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='TableBucketArn'].OutputValue | [0]" \
    --output text
)"

echo "$TABLE_BUCKET_ARN"
```

## Start DuckDB with AWS credentials

DuckDB signs S3 Tables requests with AWS credentials. The most reliable local-dev path is to export your AWS CLI profile into the current shell and let DuckDB read the `env` credential chain:

```bash
eval "$(aws configure export-credentials --profile "$AWS_PROFILE" --format env)"
duckdb s3tables-otlp.duckdb
```

The `eval` command puts temporary AWS credentials in this shell's environment for DuckDB. Do not paste the exported values into logs or source files.

## Attach Amazon S3 Tables as an Iceberg catalog

In DuckDB, install and load the extensions, create an AWS signing secret, and attach the S3 Tables table bucket as an Iceberg catalog:

```sql
INSTALL otlp FROM community;
LOAD otlp;

INSTALL iceberg;
INSTALL aws;
INSTALL httpfs;

LOAD iceberg;
LOAD aws;
LOAD httpfs;

CREATE OR REPLACE SECRET s3_tables_secret (
  TYPE s3,
  PROVIDER credential_chain,
  CHAIN env,
  REGION 'us-west-2'
);

ATTACH '<table-bucket-arn>' AS s3tables (
  TYPE iceberg,
  ENDPOINT_TYPE s3_tables
);

CREATE SCHEMA IF NOT EXISTS s3tables.otlp;
```

Replace `<table-bucket-arn>` with the `TABLE_BUCKET_ARN` value from CloudFormation, for example:

```sql
ATTACH 'arn:aws:s3tables:us-west-2:<aws-account-id>:bucket/duckdb-otlp-s3tables-<aws-account-id>-us-west-2'
AS s3tables (
  TYPE iceberg,
  ENDPOINT_TYPE s3_tables
);
```

The important pieces are:

- `TYPE iceberg` because S3 Tables exposes an Iceberg REST catalog.
- `ENDPOINT_TYPE s3_tables` because this is the Amazon S3 Tables service.
- The attach string is the **S3 Tables table bucket ARN**, not a plain S3 bucket URL.

## Start the ingest server

Start the OTLP/HTTP server and target the attached S3 Tables catalog:

```sql
SELECT listen_url, catalog_name, schema_name
FROM otlp_serve(
  'otlp:localhost:4318',
  catalog := 's3tables',
  schema := 'otlp',
  token := 'dev-token-123456'
);
```

`otlp_serve` creates these Iceberg tables in the S3 Tables namespace if they do not already exist:

- `s3tables.otlp.otlp_logs`
- `s3tables.otlp.otlp_traces`
- `s3tables.otlp.otlp_metrics_gauge`
- `s3tables.otlp.otlp_metrics_sum`
- `s3tables.otlp.otlp_metrics_histogram`
- `s3tables.otlp.otlp_metrics_exp_histogram`

Leave this DuckDB session running while clients send OTLP/HTTP requests.

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

Rows are accepted before they are durable. They commit automatically in the background, on graceful `otlp_stop`, or immediately on optional `otlp_flush`.

## Query committed rows

For a deterministic check, force a synchronous commit:

```sql
SELECT * FROM otlp_flush('otlp:localhost:4318');
```

Then query the Iceberg table hosted by Amazon S3 Tables:

```sql
SELECT
  service_name,
  severity_text,
  body,
  resource_attributes,
  log_attributes
FROM s3tables.otlp.otlp_logs
WHERE service_name = 's3-tables-demo'
ORDER BY timestamp DESC
LIMIT 5;
```

To inspect buffer and commit counters:

```sql
SELECT
  catalog_name,
  schema_name,
  total_rows,
  buffered_rows,
  last_seal_age_ms AS last_commit_age_ms,
  seals_total AS commits_total
FROM otlp_server_list();
```

## Stop cleanly

```sql
SELECT status FROM otlp_stop('otlp:localhost:4318');
```

`otlp_stop` commits remaining buffered rows before returning. A plain database or connection close stops the server but does not commit buffered rows, so stop the server before closing DuckDB.

## Clean up

Drop the Iceberg tables before deleting the CloudFormation stack; S3 Tables table buckets cannot be removed while tables remain:

```sql
DROP TABLE IF EXISTS s3tables.otlp.otlp_logs;
DROP TABLE IF EXISTS s3tables.otlp.otlp_traces;
DROP TABLE IF EXISTS s3tables.otlp.otlp_metrics_gauge;
DROP TABLE IF EXISTS s3tables.otlp.otlp_metrics_sum;
DROP TABLE IF EXISTS s3tables.otlp.otlp_metrics_histogram;
DROP TABLE IF EXISTS s3tables.otlp.otlp_metrics_exp_histogram;
DETACH s3tables;
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

## Notes

- Use `ENDPOINT_TYPE s3_tables` for Amazon S3 Tables. For an ordinary S3 bucket containing Iceberg files, use a different Iceberg catalog setup.
- DuckDB uses a `TYPE s3` secret here for AWS request signing. The attached catalog is still an Iceberg catalog.
- S3 Tables accepts Iceberg `TIMESTAMP` columns, not DuckDB's `TIMESTAMP_NS` type. Live ingest tables therefore store OTLP timestamp columns as DuckDB `TIMESTAMP` with microsecond precision.
- OTLP count and flag fields use signed SQL integer types so the created tables stay compatible with Iceberg catalogs that do not accept DuckDB unsigned integer types.
- Use `create_tables := false` only when all six target tables already exist with the exact DuckDB-visible columns and types expected by `duckdb-otlp`.
- DuckDB's Iceberg REST catalog docs do not currently document a useful `CHECKPOINT` maintenance path such as manifest rewrite or snapshot expiration. `duckdb-otlp` only probes the generic DuckDB `CHECKPOINT <catalog>` hook on its internal maintenance cadence; if the catalog reports checkpointing as unsupported, automatic maintenance is disabled for that server and normal ingest/flush/stop durability behavior is unchanged.

## See also

- [DuckDB Amazon S3 Tables docs](https://duckdb.org/docs/current/core_extensions/iceberg/amazon_s3_tables.html)
- [DuckDB Iceberg REST catalog docs](https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_rest_catalogs)
- [DuckDB + S3 Tables walkthrough by Thomas Krizsan](https://blog.pesky.moe/posts/2026-04-01-duckdb-s3/)
- [Live Ingest Reference](../../reference/serve/)
- [Live Ingest Quickstart](../../quickstart/serve/)
