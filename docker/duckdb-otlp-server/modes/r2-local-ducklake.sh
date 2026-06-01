MODE_EXTENSIONS="ducklake httpfs otlp"

mode_defaults() {
    CATALOG="$(catalog_default "${DUCKLAKE_NAME:-lake}")"
    SCHEMA="$(schema_default otlp)"
    DUCKLAKE_CATALOG_PATH="${DUCKLAKE_CATALOG_PATH:-${DATA_DIR}/ducklake/catalog.duckdb}"
    R2_ACCESS_KEY_VAR="$(first_set_var CLOUDFLARE_ACCESS_KEY_ID R2_ACCESS_KEY_ID CLOUDFLARE_S3_ACCESS_KEY_ID CLOUDFLARE_R2_ACCESS_KEY_ID CLOUDFLARE_S3_KEY_ID || true)"
    R2_SECRET_KEY_VAR="$(first_set_var CLOUDFLARE_SECRET_ACCESS_KEY R2_SECRET_ACCESS_KEY CLOUDFLARE_S3_SECRET_ACCESS_KEY CLOUDFLARE_R2_SECRET_ACCESS_KEY CLOUDFLARE_S3_SECRET_KEY || true)"
    R2_ENDPOINT_HOST="$(r2_endpoint_default)"
    DUCKLAKE_DATA_PATH="${DUCKLAKE_DATA_PATH:-$(r2_data_path)}"
}

mode_validate() {
    require_any_var "R2 access key" CLOUDFLARE_ACCESS_KEY_ID R2_ACCESS_KEY_ID CLOUDFLARE_S3_ACCESS_KEY_ID CLOUDFLARE_R2_ACCESS_KEY_ID CLOUDFLARE_S3_KEY_ID
    require_any_var "R2 secret key" CLOUDFLARE_SECRET_ACCESS_KEY R2_SECRET_ACCESS_KEY CLOUDFLARE_S3_SECRET_ACCESS_KEY CLOUDFLARE_R2_SECRET_ACCESS_KEY CLOUDFLARE_S3_SECRET_KEY
    require_any_var "R2 bucket" CLOUDFLARE_R2_BUCKET R2_BUCKET_NAME R2_BUCKET
    catalog_dir="$(dirname "$DUCKLAKE_CATALOG_PATH")"
    mkdir -p "$catalog_dir"
}

mode_emit_sql() {
    catalog_ident="$(quote_ident "$CATALOG")"
    catalog_path_sql="$(sql_quote "ducklake:${DUCKLAKE_CATALOG_PATH}")"
    data_path_sql="$(sql_quote "$DUCKLAKE_DATA_PATH")"
    endpoint_sql="$(sql_quote "$R2_ENDPOINT_HOST")"
    cat <<SQL
INSTALL ducklake;
INSTALL httpfs;
LOAD ducklake;
LOAD httpfs;
LOAD otlp;
CREATE OR REPLACE SECRET r2_storage (
  TYPE s3,
  KEY_ID getenv('${R2_ACCESS_KEY_VAR}'),
  SECRET getenv('${R2_SECRET_KEY_VAR}'),
  REGION 'auto',
  ENDPOINT ${endpoint_sql},
  URL_STYLE 'path'
);
ATTACH ${catalog_path_sql} AS ${catalog_ident} (
  DATA_PATH ${data_path_sql}
);
SQL
}
