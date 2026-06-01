MODE_EXTENSIONS="iceberg aws httpfs otlp"

mode_defaults() {
    CATALOG="$(catalog_default "${S3_TABLES_CATALOG_NAME:-s3tables}")"
    SCHEMA="$(schema_default otlp)"
    AWS_REGION_VALUE="${AWS_REGION:-${AWS_DEFAULT_REGION:-}}"
    AWS_PROFILE_VALUE="${AWS_PROFILE:-${AWS_DEFAULT_PROFILE:-}}"
    S3_TABLES_BUCKET_ARN="${S3_TABLES_BUCKET_ARN:-${S3_TABLES_TABLE_BUCKET_ARN:-${TABLE_BUCKET_ARN:-}}}"
    if [ -z "$AWS_REGION_VALUE" ] && [ -n "$S3_TABLES_BUCKET_ARN" ]; then
        AWS_REGION_VALUE="$(printf '%s' "$S3_TABLES_BUCKET_ARN" | sed -n 's/^arn:[^:]*:s3tables:\([^:]*\):.*/\1/p')"
    fi
}

mode_validate() {
    if [ -z "$S3_TABLES_BUCKET_ARN" ]; then
        fail "Missing S3 Tables bucket ARN. Set S3_TABLES_BUCKET_ARN, S3_TABLES_TABLE_BUCKET_ARN, or TABLE_BUCKET_ARN"
    fi
    if [ -z "$AWS_REGION_VALUE" ]; then
        fail "Missing AWS region for DUCKDB_MODE=${MODE}. Set AWS_REGION/AWS_DEFAULT_REGION or use an S3 Tables ARN that includes a region"
    fi
}

mode_emit_sql() {
    catalog_ident="$(quote_ident "$CATALOG")"
    region_sql="$(sql_quote "$AWS_REGION_VALUE")"
    bucket_arn_sql="$(sql_quote "$S3_TABLES_BUCKET_ARN")"
    cat <<SQL
INSTALL iceberg;
INSTALL aws;
INSTALL httpfs;
LOAD iceberg;
LOAD aws;
LOAD httpfs;
LOAD otlp;
CREATE OR REPLACE SECRET s3_tables_secret (
  TYPE s3,
  PROVIDER credential_chain,
SQL
    if [ -n "$AWS_PROFILE_VALUE" ]; then
        profile_sql="$(sql_quote "$AWS_PROFILE_VALUE")"
        cat <<SQL
  CHAIN config,
  PROFILE ${profile_sql},
SQL
    else
        cat <<SQL
  CHAIN env,
SQL
    fi
    cat <<SQL
  REGION ${region_sql}
);
ATTACH ${bucket_arn_sql} AS ${catalog_ident} (
  TYPE iceberg,
  ENDPOINT_TYPE s3_tables
);
SQL
}
