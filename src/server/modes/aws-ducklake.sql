-- aws-ducklake: a DuckLake catalog in a local DuckDB file, with Parquet data files in S3.
-- @set CATALOG ${DUCKDB_CATALOG:-lake}
-- @set SCHEMA ${DUCKDB_SCHEMA:-otlp}
-- @set CATALOG_PATH ${DUCKLAKE_CATALOG_PATH:-${DUCKDB_OTLP_DATA_DIR}/ducklake/catalog.duckdb}
-- @set DATA_PATH ${DUCKLAKE_DATA_PATH}
-- @set CREDENTIALS AWS credential chain (environment, shared config, then instance role)
-- @if AWS_PROFILE
-- @set CREDENTIALS AWS profile "${AWS_PROFILE}"
-- @end
INSTALL ducklake;
INSTALL aws;
INSTALL httpfs;
LOAD ducklake;
LOAD aws;
LOAD httpfs;
-- A multi-source chain: `env` alone cannot see an EC2/ECS instance role, and `instance` alone
-- cannot see the standard AWS variables.
CREATE OR REPLACE SECRET aws_ducklake_storage (
  TYPE s3,
  PROVIDER credential_chain,
  CHAIN 'env;config;instance',
-- @if AWS_PROFILE
  PROFILE '${AWS_PROFILE}',
-- @end
  REGION '${AWS_REGION:-${AWS_DEFAULT_REGION}}'
);
ATTACH 'ducklake:${CATALOG_PATH}' AS "${CATALOG}" (
  DATA_PATH '${DATA_PATH}'
-- @if DUCKLAKE_DATA_INLINING_ROW_LIMIT
  , DATA_INLINING_ROW_LIMIT ${DUCKLAKE_DATA_INLINING_ROW_LIMIT}
-- @end
);
CALL "${CATALOG}".set_option('target_file_size', '${DUCKDB_OTLP_TARGET_FILE_SIZE:-268435456} bytes');
CALL "${CATALOG}".set_option('expire_older_than', '${DUCKDB_OTLP_MAINTENANCE_RETENTION_MS:-900000} milliseconds');
CALL "${CATALOG}".set_option('delete_older_than', '${DUCKDB_OTLP_MAINTENANCE_RETENTION_MS:-900000} milliseconds');
