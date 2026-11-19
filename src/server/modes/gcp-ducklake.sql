-- gcp-ducklake: a DuckLake catalog in Postgres (Cloud SQL, or any libpq PG* connection), with
-- Parquet data files in Google Cloud Storage. DUCKLAKE_DATA_PATH must be gcss://bucket/prefix:
-- the gcss scheme selects the native GCS filesystem even when httpfs is also loaded.
-- @set CATALOG ${DUCKDB_CATALOG:-lake}
-- @set SCHEMA ${DUCKDB_SCHEMA:-otlp}
-- @set DATA_PATH ${DUCKLAKE_DATA_PATH}
-- @set CREDENTIALS Google application default credentials
INSTALL ducklake;
INSTALL postgres;
LOAD ducklake;
LOAD postgres;
-- @if DUCKDB_OTLP_GCS_EXTENSION_PATH
-- A local gcs build: the community one prints a failed upload finalize instead of raising it,
-- so DuckLake would commit data files that were never written. It is unsigned.
-- @set ALLOW_UNSIGNED_EXTENSIONS true
LOAD '${DUCKDB_OTLP_GCS_EXTENSION_PATH}';
-- @else
INSTALL gcs FROM community;
LOAD gcs;
-- @end
CREATE OR REPLACE SECRET gcp_storage (
  TYPE gcp,
  PROVIDER credential_chain
);
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
