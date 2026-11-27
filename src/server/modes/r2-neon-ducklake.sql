-- r2-neon-ducklake: a DuckLake catalog in Postgres (Neon, or any libpq PG* connection), with
-- Parquet data files in Cloudflare R2.
-- @set CATALOG ${DUCKDB_CATALOG:-lake}
-- @set SCHEMA ${DUCKDB_SCHEMA:-otlp}
-- DuckLake treats DATA_PATH as a directory prefix, hence the trailing slash.
-- @set DATA_PATH ${DUCKLAKE_DATA_PATH:-s3://${R2_BUCKET}/${R2_PREFIX:-duckdb-otlp}/}
-- @set CREDENTIALS DuckDB secret store (no R2 key pair in the environment)
INSTALL ducklake;
INSTALL postgres;
INSTALL httpfs;
LOAD ducklake;
LOAD postgres;
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
CREATE OR REPLACE SECRET postgres_secret (
  TYPE postgres,
  HOST getenv('PGHOST'),
  PORT '${PGPORT:-5432}',
  DATABASE getenv('PGDATABASE'),
  USER getenv('PGUSER'),
  PASSWORD getenv('PGPASSWORD'),
  SSLMODE '${PGSSLMODE:-require}'
);
CREATE OR REPLACE SECRET ducklake_secret (
  TYPE ducklake,
  METADATA_PATH '',
  DATA_PATH '${DATA_PATH}',
  METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_secret'}
);
ATTACH 'ducklake:ducklake_secret' AS "${CATALOG}"
-- @if DUCKLAKE_DATA_INLINING_ROW_LIMIT
  (DATA_INLINING_ROW_LIMIT ${DUCKLAKE_DATA_INLINING_ROW_LIMIT})
-- @end
;
CALL "${CATALOG}".set_option('target_file_size', '${DUCKDB_OTLP_TARGET_FILE_SIZE:-268435456} bytes');
CALL "${CATALOG}".set_option('expire_older_than', '${DUCKDB_OTLP_MAINTENANCE_RETENTION_MS:-900000} milliseconds');
CALL "${CATALOG}".set_option('delete_older_than', '${DUCKDB_OTLP_MAINTENANCE_RETENTION_MS:-900000} milliseconds');
