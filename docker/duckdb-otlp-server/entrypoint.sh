#!/bin/sh
set -eu

DATA_DIR="${DUCKDB_OTLP_DATA_DIR:-/data}"
DATABASE="${DUCKDB_OTLP_DATABASE:-:memory:}"
LISTEN_URI="${DUCKDB_OTLP_LISTEN_URI:-otlp:0.0.0.0:4318}"
TOKEN="${DUCKDB_OTLP_TOKEN:-dev-otlp-token-123456}"
SCHEMA="${DUCKDB_OTLP_SCHEMA:-main}"
CONTROL_FIFO="${DUCKDB_OTLP_CONTROL_FIFO:-/tmp/duckdb-otlp.sql}"

sql_escape() {
    printf "%s" "$1" | sed "s/'/''/g"
}

DATA_DIR_SQL=$(sql_escape "$DATA_DIR")
LISTEN_URI_SQL=$(sql_escape "$LISTEN_URI")
TOKEN_SQL=$(sql_escape "$TOKEN")
SCHEMA_SQL=$(sql_escape "$SCHEMA")

mkdir -p "$DATA_DIR/files"
rm -f "$CONTROL_FIFO"
mkfifo "$CONTROL_FIFO"

duckdb -unsigned "$DATABASE" < "$CONTROL_FIFO" &
duckdb_pid=$!

exec 3> "$CONTROL_FIFO"

cat >&3 <<SQL
INSTALL ducklake;
LOAD ducklake;
LOAD otlp;
ATTACH 'ducklake:${DATA_DIR_SQL}/metadata.ducklake' AS lake (DATA_PATH '${DATA_DIR_SQL}/files/');
SELECT listen_url, auth_token, catalog_name
FROM otlp_serve(
    '${LISTEN_URI_SQL}',
    catalog := 'lake',
    schema := '${SCHEMA_SQL}',
    token := '${TOKEN_SQL}',
    allow_other_hostname := true
);
SQL

echo "duckdb-otlp listening on ${LISTEN_URI}"
echo "DuckLake metadata: ${DATA_DIR}/metadata.ducklake"
echo "DuckLake data path: ${DATA_DIR}/files"
echo "Control FIFO: ${CONTROL_FIFO}"

stop_server() {
    echo "Stopping duckdb-otlp..."
    printf "SELECT status FROM otlp_stop('%s');\n.quit\n" "$LISTEN_URI_SQL" >&3 || true
    wait "$duckdb_pid" || true
}

trap stop_server INT TERM
wait "$duckdb_pid"
