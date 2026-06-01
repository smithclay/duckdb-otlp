MODE_EXTENSIONS="ducklake otlp"

mode_defaults() {
    CATALOG="$(catalog_default "${DUCKLAKE_NAME:-otel}")"
    SCHEMA="$(schema_default main)"
    DUCKLAKE_CATALOG_PATH="${DUCKLAKE_CATALOG_PATH:-${DATA_DIR}/ducklake/catalog.duckdb}"
    DUCKLAKE_DATA_PATH="${DUCKLAKE_DATA_PATH:-${DATA_DIR}/ducklake/storage}"
}

mode_validate() {
    catalog_dir="$(dirname "$DUCKLAKE_CATALOG_PATH")"
    mkdir -p "$catalog_dir" "$DUCKLAKE_DATA_PATH"
}

mode_emit_sql() {
    catalog_ident="$(quote_ident "$CATALOG")"
    catalog_path_sql="$(sql_quote "ducklake:${DUCKLAKE_CATALOG_PATH}")"
    data_path_sql="$(sql_quote "$DUCKLAKE_DATA_PATH")"
    cat <<SQL
INSTALL ducklake;
LOAD ducklake;
LOAD otlp;
ATTACH ${catalog_path_sql} AS ${catalog_ident} (
  DATA_PATH ${data_path_sql}
);
SQL
}
