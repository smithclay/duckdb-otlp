-- parquet: no catalog. Each seal writes Parquet files under PARQUET_EXPORT_PATH, partitioned
-- <table>/year=/month=/day=. An s3:// path (or S3_BUCKET and S3_PREFIX, which compose one)
-- writes to S3 through the AWS credential chain.
-- @set CATALOG
-- @set SCHEMA ${DUCKDB_SCHEMA:-otlp}
-- @if S3_BUCKET
-- @set PARQUET_EXPORT_PATH ${PARQUET_EXPORT_PATH:-s3://${S3_BUCKET}/${S3_PREFIX:-duckdb-otlp}}
-- @end
-- @set PARQUET_EXPORT_PATH ${PARQUET_EXPORT_PATH:-${DUCKDB_OTLP_DATA_DIR}/parquet}
-- @if PARQUET_EXPORT_PATH^=s3://
-- @set CREDENTIALS AWS credential chain (environment, shared config, then instance role)
-- @if AWS_PROFILE
-- @set CREDENTIALS AWS profile "${AWS_PROFILE}"
-- @end
INSTALL aws;
INSTALL httpfs;
LOAD aws;
LOAD httpfs;
CREATE OR REPLACE SECRET plain_s3_secret (
  TYPE s3,
  PROVIDER credential_chain,
  CHAIN 'env;config;instance',
-- @if AWS_PROFILE
  PROFILE '${AWS_PROFILE}',
-- @end
-- @if S3_ENDPOINT
  ENDPOINT '${S3_ENDPOINT}',
-- @end
-- @if S3_URL_STYLE
  URL_STYLE '${S3_URL_STYLE}',
-- @end
  REGION '${AWS_REGION:-${AWS_DEFAULT_REGION}}'
);
-- @end
