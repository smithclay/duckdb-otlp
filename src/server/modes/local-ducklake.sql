-- local-ducklake (the default): a DuckLake catalog in a local DuckDB file, with Parquet data
-- files on local disk.
--
-- Every mode is a file like this one, compiled into the daemon. `duckdb-otlp validate` prints
-- what it expands to. The syntax:
--   ${NAME}            the setting's value (a flag or environment variable); required
--   ${NAME:-default}   the value, or `default` when it is unset (defaults may nest)
--   getenv('NAME')     a secret, read at run time so its value never appears in printed SQL;
--                      required
--   -- @set NAME value assign a variable; CATALOG, SCHEMA, DATA_PATH, CATALOG_PATH,
--                      PARQUET_EXPORT_PATH and CREDENTIALS are read back by the daemon
--   -- @if A B ... / -- @end
--                      keep the lines between only when one of A, B... is set
--                      (`A^=prefix`: when A starts with prefix)
-- Other comment lines are dropped. Every missing setting is reported in one error.
--
-- `--mode none --init-sql FILE` runs a file of your own instead.

-- @set CATALOG ${DUCKDB_CATALOG:-otel}
-- @set SCHEMA ${DUCKDB_SCHEMA:-main}
-- @set CATALOG_PATH ${DUCKLAKE_CATALOG_PATH:-${DUCKDB_OTLP_DATA_DIR}/ducklake/catalog.duckdb}
-- @set DATA_PATH ${DUCKLAKE_DATA_PATH:-${DUCKDB_OTLP_DATA_DIR}/ducklake/storage}
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:${CATALOG_PATH}' AS "${CATALOG}" (
  DATA_PATH '${DATA_PATH}'
-- @if DUCKLAKE_DATA_INLINING_ROW_LIMIT
  , DATA_INLINING_ROW_LIMIT ${DUCKLAKE_DATA_INLINING_ROW_LIMIT}
-- @end
);
CALL "${CATALOG}".set_option('target_file_size', '${DUCKDB_OTLP_TARGET_FILE_SIZE:-268435456} bytes');
CALL "${CATALOG}".set_option('expire_older_than', '${DUCKDB_OTLP_MAINTENANCE_RETENTION_MS:-900000} milliseconds');
CALL "${CATALOG}".set_option('delete_older_than', '${DUCKDB_OTLP_MAINTENANCE_RETENTION_MS:-900000} milliseconds');
