-- r2-data-catalog: Cloudflare R2 Data Catalog (a managed Iceberg REST catalog) over R2. The
-- warehouse and catalog URI default to what wrangler prints when you enable the catalog.
-- @set CATALOG ${DUCKDB_CATALOG:-r2catalog}
-- @set SCHEMA ${DUCKDB_SCHEMA:-otlp}
-- @set CREDENTIALS DuckDB secret store (no R2 key pair in the environment)
INSTALL iceberg;
INSTALL httpfs;
LOAD iceberg;
LOAD httpfs;
-- @if R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY
-- @set CREDENTIALS R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY
CREATE OR REPLACE SECRET cloudflare_r2_secret (
  TYPE s3,
  KEY_ID getenv('R2_ACCESS_KEY_ID'),
  SECRET getenv('R2_SECRET_ACCESS_KEY'),
  REGION 'auto',
  ENDPOINT '${R2_ENDPOINT:-${CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com}',
  URL_STYLE 'path'
);
-- @end
CREATE OR REPLACE SECRET cloudflare_catalog_secret (
  TYPE ICEBERG,
  TOKEN getenv('CLOUDFLARE_API_TOKEN')
);
ATTACH '${CLOUDFLARE_WAREHOUSE:-${CLOUDFLARE_ACCOUNT_ID}_${R2_BUCKET}}' AS "${CATALOG}" (
  TYPE ICEBERG,
  ENDPOINT '${CLOUDFLARE_CATALOG_URI:-https://catalog.cloudflarestorage.com/${CLOUDFLARE_ACCOUNT_ID}/${R2_BUCKET}}',
  SECRET cloudflare_catalog_secret
);
