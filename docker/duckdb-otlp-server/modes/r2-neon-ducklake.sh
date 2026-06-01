MODE_EXTENSIONS="ducklake postgres httpfs otlp"

mode_defaults() {
    CATALOG="$(catalog_default "${DUCKLAKE_NAME:-lake}")"
    SCHEMA="$(schema_default otlp)"
    R2_ACCESS_KEY_VAR="$(first_set_var CLOUDFLARE_ACCESS_KEY_ID R2_ACCESS_KEY_ID CLOUDFLARE_S3_ACCESS_KEY_ID CLOUDFLARE_R2_ACCESS_KEY_ID CLOUDFLARE_S3_KEY_ID || true)"
    R2_SECRET_KEY_VAR="$(first_set_var CLOUDFLARE_SECRET_ACCESS_KEY R2_SECRET_ACCESS_KEY CLOUDFLARE_S3_SECRET_ACCESS_KEY CLOUDFLARE_R2_SECRET_ACCESS_KEY CLOUDFLARE_S3_SECRET_KEY || true)"
    R2_ENDPOINT_HOST="$(r2_endpoint_default)"
    DUCKLAKE_DATA_PATH="${DUCKLAKE_DATA_PATH:-$(r2_data_path)}"
    NEON_PGPORT="${NEON_PGPORT:-5432}"
    NEON_PGSSLMODE="${NEON_PGSSLMODE:-require}"
}

mode_validate() {
    require_any_var "R2 access key" CLOUDFLARE_ACCESS_KEY_ID R2_ACCESS_KEY_ID CLOUDFLARE_S3_ACCESS_KEY_ID CLOUDFLARE_R2_ACCESS_KEY_ID CLOUDFLARE_S3_KEY_ID
    require_any_var "R2 secret key" CLOUDFLARE_SECRET_ACCESS_KEY R2_SECRET_ACCESS_KEY CLOUDFLARE_S3_SECRET_ACCESS_KEY CLOUDFLARE_R2_SECRET_ACCESS_KEY CLOUDFLARE_S3_SECRET_KEY
    require_any_var "R2 bucket" CLOUDFLARE_R2_BUCKET R2_BUCKET_NAME R2_BUCKET
    require_var NEON_PGHOST
    require_var NEON_PGDATABASE
    require_var NEON_PGUSER
    require_var NEON_PGPASSWORD
}

mode_emit_sql() {
    catalog_ident="$(quote_ident "$CATALOG")"
    data_path_sql="$(sql_quote "$DUCKLAKE_DATA_PATH")"
    endpoint_sql="$(sql_quote "$R2_ENDPOINT_HOST")"
    cat <<SQL
INSTALL ducklake;
INSTALL postgres;
INSTALL httpfs;
LOAD ducklake;
LOAD postgres;
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
CREATE OR REPLACE SECRET postgres_secret (
  TYPE postgres,
  HOST getenv('NEON_PGHOST'),
  PORT getenv('NEON_PGPORT'),
  DATABASE getenv('NEON_PGDATABASE'),
  USER getenv('NEON_PGUSER'),
  PASSWORD getenv('NEON_PGPASSWORD'),
  SSLMODE getenv('NEON_PGSSLMODE')
);
CREATE OR REPLACE SECRET ducklake_secret (
  TYPE ducklake,
  METADATA_PATH '',
  DATA_PATH ${data_path_sql},
  METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_secret'}
);
ATTACH 'ducklake:ducklake_secret' AS ${catalog_ident};
SQL
}
