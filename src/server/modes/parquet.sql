-- parquet: no catalog. Each seal writes Parquet files under PARQUET_EXPORT_PATH, partitioned
-- <table>/year=/month=/day=. S3_BUCKET (and S3_PREFIX) put them in S3 instead of on local disk.
-- @set CATALOG
-- @set SCHEMA ${DUCKDB_SCHEMA:-otlp}
-- @if S3_BUCKET
-- @set PARQUET_EXPORT_PATH ${PARQUET_EXPORT_PATH:-s3://${S3_BUCKET}/${S3_PREFIX:-duckdb-otlp}}
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
-- @set PARQUET_EXPORT_PATH ${PARQUET_EXPORT_PATH:-${DUCKDB_OTLP_DATA_DIR}/parquet}
