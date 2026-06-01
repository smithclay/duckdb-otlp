---
title: "How to Stream OTLP to Amazon S3 Tables"
---

Run the `duckdb-otlp` Docker image in `s3-tables` mode to stream OTLP/HTTP exports into an Iceberg catalog hosted by **Amazon S3 Tables**. The container initializes DuckDB, loads the required extensions, attaches the S3 Tables table bucket, starts the ingest server, and commits accepted rows in batches.

This guide is specifically for **Amazon S3 Tables**, the managed AWS service with table buckets and an Iceberg REST catalog endpoint. It is not the same as writing Iceberg metadata and Parquet files to an ordinary `s3://` bucket.

> Live ingestion is OTLP/HTTP on port `4318`. The ingest server is not available in WASM builds.

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
```

## Configure

Create `s3tables.env`:

```ini
DUCKDB_MODE=s3-tables
DUCKDB_OTLP_TOKEN=dev-token-123456

DUCKDB_CATALOG=s3tables
DUCKDB_SCHEMA=otlp

AWS_REGION=us-west-2
AWS_PROFILE=cli-dev
S3_TABLES_BUCKET_ARN=<table-bucket-arn>
```

Replace `<table-bucket-arn>` with the `TABLE_BUCKET_ARN` value from CloudFormation.

## Start the server

Mount your AWS config read-only so DuckDB can use the configured profile:

```bash
docker run --rm --name duckdb-otlp \
  --env-file s3tables.env \
  -p 4318:4318 \
  -v "$HOME/.aws:/root/.aws:ro" \
  ghcr.io/smithclay/duckdb-otlp:latest
```

The container creates these Iceberg tables in the S3 Tables namespace if they do not already exist:

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

Rows are accepted before they are durable. They commit automatically in the background, on graceful shutdown, or immediately after an explicit flush.

## Query committed rows

Flush and query through the running container:

```bash
docker exec duckdb-otlp sh -c \
  "printf '%s\n' \
    \"SELECT * FROM otlp_flush('otlp:0.0.0.0:4318');\" \
    \"SELECT service_name, severity_text, body\" \
    \"FROM s3tables.otlp.otlp_logs\" \
    \"WHERE service_name = 's3-tables-demo'\" \
    \"ORDER BY timestamp DESC\" \
    \"LIMIT 5;\" \
    > /tmp/duckdb-otlp.sql"

docker logs --tail 80 duckdb-otlp
```

## Stop cleanly

If you plan to delete the S3 Tables resources immediately, skip this step and use [Clean up](#clean-up) instead.

```bash
docker stop duckdb-otlp
```

The image sends `otlp_stop('otlp:0.0.0.0:4318')` during shutdown, so remaining buffered rows are committed before the process exits.

## Clean up

Drop the Iceberg tables before deleting the CloudFormation stack; S3 Tables table buckets cannot be removed while tables remain:

```bash
docker exec duckdb-otlp sh -c \
  "printf '%s\n' \
    \"SELECT status FROM otlp_stop('otlp:0.0.0.0:4318');\" \
    \"DROP TABLE IF EXISTS s3tables.otlp.otlp_logs;\" \
    \"DROP TABLE IF EXISTS s3tables.otlp.otlp_traces;\" \
    \"DROP TABLE IF EXISTS s3tables.otlp.otlp_metrics_gauge;\" \
    \"DROP TABLE IF EXISTS s3tables.otlp.otlp_metrics_sum;\" \
    \"DROP TABLE IF EXISTS s3tables.otlp.otlp_metrics_histogram;\" \
    \"DROP TABLE IF EXISTS s3tables.otlp.otlp_metrics_exp_histogram;\" \
    \"DETACH s3tables;\" \
    > /tmp/duckdb-otlp.sql"

docker logs --tail 80 duckdb-otlp
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

## Notes

- Use `ENDPOINT_TYPE s3_tables` for Amazon S3 Tables. For an ordinary S3 bucket containing Iceberg files, use a different Iceberg catalog setup.
- DuckDB uses a `TYPE s3` secret for AWS request signing. The attached catalog is still an Iceberg catalog.
- S3 Tables accepts Iceberg `TIMESTAMP` columns, not DuckDB's `TIMESTAMP_NS` type. Live ingest tables therefore store OTLP timestamp columns as DuckDB `TIMESTAMP` with microsecond precision.
- OTLP count and flag fields use signed SQL integer types so the created tables stay compatible with Iceberg catalogs that do not accept DuckDB unsigned integer types.
- DuckDB's Iceberg REST catalog docs do not currently document a useful `CHECKPOINT` maintenance path such as manifest rewrite or snapshot expiration. `duckdb-otlp` only probes the generic DuckDB `CHECKPOINT <catalog>` hook on its internal maintenance cadence; if the catalog reports checkpointing as unsupported, automatic maintenance is disabled for that server and normal ingest/flush/stop durability behavior is unchanged.

## See also

- [DuckDB Amazon S3 Tables docs](https://duckdb.org/docs/current/core_extensions/iceberg/amazon_s3_tables.html)
- [DuckDB Iceberg REST catalog docs](https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_rest_catalogs)
- [DuckDB + S3 Tables walkthrough by Thomas Krizsan](https://blog.pesky.moe/posts/2026-04-01-duckdb-s3/)
- [Live Ingest Reference](../../reference/serve/)
- [Live Ingest Quickstart](../../quickstart/serve/)
