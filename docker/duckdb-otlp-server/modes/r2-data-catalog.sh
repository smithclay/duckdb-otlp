MODE_EXTENSIONS="iceberg httpfs otlp"

mode_defaults() {
    CATALOG="$(catalog_default "${CLOUDFLARE_CATALOG_NAME:-r2catalog}")"
    SCHEMA="$(schema_default otlp)"
    CLOUDFLARE_CATALOG_TOKEN_VAR="$(first_set_var CLOUDFLARE_CATALOG_TOKEN CLOUDFLARE_API_TOKEN || true)"
    CLOUDFLARE_ACCESS_KEY_VAR="$(first_set_var CLOUDFLARE_ACCESS_KEY_ID R2_ACCESS_KEY_ID CLOUDFLARE_S3_ACCESS_KEY_ID CLOUDFLARE_R2_ACCESS_KEY_ID CLOUDFLARE_S3_KEY_ID || true)"
    CLOUDFLARE_SECRET_KEY_VAR="$(first_set_var CLOUDFLARE_SECRET_ACCESS_KEY R2_SECRET_ACCESS_KEY CLOUDFLARE_S3_SECRET_ACCESS_KEY CLOUDFLARE_R2_SECRET_ACCESS_KEY CLOUDFLARE_S3_SECRET_KEY || true)"
    CLOUDFLARE_R2_ENDPOINT_HOST="$(r2_endpoint_default)"
    CLOUDFLARE_WAREHOUSE="${CLOUDFLARE_WAREHOUSE:-${R2_WAREHOUSE:-}}"
    if [ -z "$CLOUDFLARE_WAREHOUSE" ] && [ -n "${CLOUDFLARE_ACCOUNT_ID:-}" ] && [ -n "${CLOUDFLARE_R2_BUCKET:-${R2_BUCKET_NAME:-${R2_BUCKET:-}}}" ]; then
        CLOUDFLARE_WAREHOUSE="${CLOUDFLARE_ACCOUNT_ID}_$(r2_bucket_value)"
    fi
}

mode_validate() {
    require_any_var "Cloudflare catalog token" CLOUDFLARE_CATALOG_TOKEN CLOUDFLARE_API_TOKEN
    require_any_var "Cloudflare R2 access key" CLOUDFLARE_ACCESS_KEY_ID R2_ACCESS_KEY_ID CLOUDFLARE_S3_ACCESS_KEY_ID CLOUDFLARE_R2_ACCESS_KEY_ID CLOUDFLARE_S3_KEY_ID
    require_any_var "Cloudflare R2 secret key" CLOUDFLARE_SECRET_ACCESS_KEY R2_SECRET_ACCESS_KEY CLOUDFLARE_S3_SECRET_ACCESS_KEY CLOUDFLARE_R2_SECRET_ACCESS_KEY CLOUDFLARE_S3_SECRET_KEY
    require_any_var "Cloudflare R2 bucket" CLOUDFLARE_R2_BUCKET R2_BUCKET_NAME R2_BUCKET
    require_var CLOUDFLARE_ACCOUNT_ID
    require_var CLOUDFLARE_CATALOG_URI
    if [ -z "$CLOUDFLARE_WAREHOUSE" ]; then
        fail "Missing Cloudflare warehouse. Set CLOUDFLARE_WAREHOUSE or provide CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_R2_BUCKET"
    fi
}

mode_emit_sql() {
    catalog_ident="$(quote_ident "$CATALOG")"
    warehouse_sql="$(sql_quote "$CLOUDFLARE_WAREHOUSE")"
    endpoint_sql="$(sql_quote "$CLOUDFLARE_CATALOG_URI")"
    r2_endpoint_sql="$(sql_quote "$CLOUDFLARE_R2_ENDPOINT_HOST")"
    cat <<SQL
INSTALL iceberg;
INSTALL httpfs;
LOAD iceberg;
LOAD httpfs;
LOAD otlp;
CREATE OR REPLACE SECRET cloudflare_r2_secret (
  TYPE s3,
  KEY_ID getenv('${CLOUDFLARE_ACCESS_KEY_VAR}'),
  SECRET getenv('${CLOUDFLARE_SECRET_KEY_VAR}'),
  REGION 'auto',
  ENDPOINT ${r2_endpoint_sql},
  URL_STYLE 'path'
);
CREATE OR REPLACE SECRET cloudflare_catalog_secret (
  TYPE ICEBERG,
  TOKEN getenv('${CLOUDFLARE_CATALOG_TOKEN_VAR}')
);
ATTACH ${warehouse_sql} AS ${catalog_ident} (
  TYPE ICEBERG,
  ENDPOINT ${endpoint_sql},
  SECRET cloudflare_catalog_secret
);
SQL
}
