-- r2-local-ducklake: a DuckLake catalog in a local DuckDB file, with Parquet data files in
-- Cloudflare R2. Without an R2 key pair, DuckDB resolves R2 access from its own secret store.
-- @set CATALOG ${DUCKDB_CATALOG:-lake}
-- @set SCHEMA ${DUCKDB_SCHEMA:-otlp}
-- @set CATALOG_PATH ${DUCKLAKE_CATALOG_PATH:-${DUCKDB_OTLP_DATA_DIR}/ducklake/catalog.duckdb}
-- DuckLake treats DATA_PATH as a directory prefix, hence the trailing slash.
-- @set DATA_PATH ${DUCKLAKE_DATA_PATH:-s3://${R2_BUCKET}/${R2_PREFIX:-duckdb-otlp}/}
-- @set CREDENTIALS DuckDB secret store (no R2 key pair in the environment)
INSTALL ducklake;
INSTALL httpfs;
LOAD ducklake;
LOAD httpfs;
-- @if R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY
-- @set CREDENTIALS R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY
CREATE OR REPLACE SECRET r2_storage (
  TYPE s3,
  KEY_ID getenv('R2_ACCESS_KEY_ID'),
  SECRET getenv('R2_SECRET_ACCESS_KEY'),
  REGION 'auto',
  ENDPOINT '${R2_ENDPOINT:-${CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com}',
  URL_STYLE 'path'
);
-- @end
ATTACH 'ducklake:${CATALOG_PATH}' AS "${CATALOG}" (
  DATA_PATH '${DATA_PATH}'
-- @if DUCKLAKE_DATA_INLINING_ROW_LIMIT
  , DATA_INLINING_ROW_LIMIT ${DUCKLAKE_DATA_INLINING_ROW_LIMIT}
-- @end
);
CALL "${CATALOG}".set_option('target_file_size', '${DUCKDB_OTLP_TARGET_FILE_SIZE:-256MB}');
CALL "${CATALOG}".set_option('expire_older_than', '${DUCKDB_OTLP_MAINTENANCE_RETENTION:-900s}');
CALL "${CATALOG}".set_option('delete_older_than', '${DUCKDB_OTLP_MAINTENANCE_RETENTION:-900s}');
