#!/bin/sh
set -eu

DATA_DIR="${DUCKDB_OTLP_DATA_DIR:-/data}"
DATABASE="${DUCKDB_OTLP_DATABASE:-:memory:}"
LISTEN_URI="${DUCKDB_OTLP_LISTEN_URI:-otlp:0.0.0.0:4318}"
TOKEN="${DUCKDB_OTLP_TOKEN:-dev-otlp-token-123456}"
SCHEMA="${DUCKDB_OTLP_SCHEMA:-main}"
CATALOG="${DUCKDB_OTLP_CATALOG:-lake}"
CATALOG_TYPE="${DUCKDB_OTLP_CATALOG_TYPE:-ducklake}"
CONTROL_FIFO="${DUCKDB_OTLP_CONTROL_FIFO:-/tmp/duckdb-otlp.sql}"
SETUP_SQL_FILE="${DUCKDB_OTLP_SETUP_SQL_FILE:-}"
SETUP_SQL="${DUCKDB_OTLP_SETUP_SQL:-}"
BOOT_SQL="/tmp/duckdb-otlp-boot.sql"

sql_escape() {
    printf "%s" "$1" | sed "s/'/''/g"
}

DATA_DIR_SQL=$(sql_escape "$DATA_DIR")
LISTEN_URI_SQL=$(sql_escape "$LISTEN_URI")
TOKEN_SQL=$(sql_escape "$TOKEN")
SCHEMA_SQL=$(sql_escape "$SCHEMA")
CATALOG_SQL=$(sql_escape "$CATALOG")

mkdir -p "$DATA_DIR/files"
rm -f "$CONTROL_FIFO"
mkfifo "$CONTROL_FIFO"
rm -f "$BOOT_SQL"
printf ".bail on\n" > "$BOOT_SQL"

duckdb -unsigned "$DATABASE" < "$CONTROL_FIFO" &
duckdb_pid=$!

exec 3> "$CONTROL_FIFO"

write_otlp_serve_sql() {
    if [ -n "$CATALOG" ]; then
        cat >> "$BOOT_SQL" <<SQL
CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${SCHEMA};
SELECT listen_url, auth_token, catalog_name, schema_name
FROM otlp_serve(
    '${LISTEN_URI_SQL}',
    catalog := '${CATALOG_SQL}',
    schema := '${SCHEMA_SQL}',
    token := '${TOKEN_SQL}',
    allow_other_hostname := true
);
SQL
    else
        cat >> "$BOOT_SQL" <<SQL
SELECT listen_url, auth_token, catalog_name, schema_name
FROM otlp_serve(
    '${LISTEN_URI_SQL}',
    schema := '${SCHEMA_SQL}',
    token := '${TOKEN_SQL}',
    allow_other_hostname := true
);
SQL
    fi
}

case "$CATALOG_TYPE" in
    ducklake|local-ducklake)
        cat >> "$BOOT_SQL" <<SQL
INSTALL ducklake;
LOAD ducklake;
LOAD otlp;
ATTACH 'ducklake:${DATA_DIR_SQL}/metadata.ducklake' AS ${CATALOG} (DATA_PATH '${DATA_DIR_SQL}/files/');
SQL
        ;;
    motherduck)
        MOTHERDUCK_ATTACH="${DUCKDB_OTLP_MOTHERDUCK_ATTACH:-md:test-ducklake}"
        case "$MOTHERDUCK_ATTACH" in
            md:*|motherduck:*) ;;
            *)
                echo "DUCKDB_OTLP_MOTHERDUCK_ATTACH must be a MotherDuck attach string such as md:test-ducklake" >&2
                exit 1
                ;;
        esac
        MOTHERDUCK_ATTACH_SQL=$(sql_escape "$MOTHERDUCK_ATTACH")
        cat >> "$BOOT_SQL" <<SQL
INSTALL md;
LOAD md;
LOAD otlp;
ATTACH '${MOTHERDUCK_ATTACH_SQL}' AS ${CATALOG};
SQL
        ;;
    custom)
        if [ -n "$SETUP_SQL_FILE" ]; then
            cat "$SETUP_SQL_FILE" >> "$BOOT_SQL"
        elif [ -n "$SETUP_SQL" ]; then
            printf "%s\n" "$SETUP_SQL" >> "$BOOT_SQL"
        else
            echo "DUCKDB_OTLP_CATALOG_TYPE=custom requires DUCKDB_OTLP_SETUP_SQL or DUCKDB_OTLP_SETUP_SQL_FILE" >&2
            exit 1
        fi
        ;;
    none|default)
        cat >> "$BOOT_SQL" <<SQL
LOAD otlp;
SQL
        CATALOG=""
        ;;
    *)
        echo "Unsupported DUCKDB_OTLP_CATALOG_TYPE=${CATALOG_TYPE}" >&2
        exit 1
        ;;
esac

write_otlp_serve_sql
cat "$BOOT_SQL" >&3

echo "duckdb-otlp listening on ${LISTEN_URI}"
echo "Catalog type: ${CATALOG_TYPE}"
echo "Catalog: ${CATALOG:-default}"
if [ "$CATALOG_TYPE" = "ducklake" ] || [ "$CATALOG_TYPE" = "local-ducklake" ]; then
    echo "DuckLake metadata: ${DATA_DIR}/metadata.ducklake"
    echo "DuckLake data path: ${DATA_DIR}/files"
fi
echo "Control FIFO: ${CONTROL_FIFO}"

stop_server() {
    echo "Stopping duckdb-otlp..."
    printf "SELECT status FROM otlp_stop('%s');\n.quit\n" "$LISTEN_URI_SQL" >&3 || true
    wait "$duckdb_pid" || true
}

trap stop_server INT TERM
wait "$duckdb_pid"
