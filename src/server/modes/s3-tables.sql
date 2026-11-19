-- s3-tables: an Amazon S3 Tables bucket (managed Iceberg).
-- @set CATALOG ${DUCKDB_CATALOG:-s3tables}
-- @set SCHEMA ${DUCKDB_SCHEMA:-otlp}
-- @set CREDENTIALS AWS credential chain (environment, shared config, then instance role)
-- @if AWS_PROFILE
-- @set CREDENTIALS AWS profile "${AWS_PROFILE}"
-- @end
INSTALL iceberg;
INSTALL aws;
INSTALL httpfs;
LOAD iceberg;
LOAD aws;
LOAD httpfs;
CREATE OR REPLACE SECRET s3_tables_secret (
  TYPE s3,
  PROVIDER credential_chain,
  CHAIN 'env;config;instance',
-- @if AWS_PROFILE
  PROFILE '${AWS_PROFILE}',
-- @end
  REGION '${AWS_REGION:-${AWS_DEFAULT_REGION}}'
);
ATTACH '${S3_TABLES_BUCKET_ARN}' AS "${CATALOG}" (
  TYPE iceberg,
  ENDPOINT_TYPE s3_tables
);
