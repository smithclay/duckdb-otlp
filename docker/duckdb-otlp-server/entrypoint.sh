#!/bin/sh
set -eu

SCRIPT_HOME="${DUCKDB_OTLP_SCRIPT_HOME:-/usr/local/share/duckdb-otlp-server}"
MODE_DIR="${DUCKDB_OTLP_MODE_DIR:-${SCRIPT_HOME}/modes}"

CONTROL_FIFO="${DUCKDB_OTLP_CONTROL_FIFO:-/tmp/duckdb-otlp.sql}"
BOOT_SQL="${DUCKDB_OTLP_BOOT_SQL:-/tmp/duckdb-otlp-boot.sql}"
DATA_DIR="${DUCKDB_OTLP_DATA_DIR:-/data}"
DRY_RUN="${DRY_RUN:-0}"

DEFAULT_TOKEN="dev-otlp-token-123456"
SUPPORTED_MODES="local-ducklake r2-data-catalog s3-tables r2-neon-ducklake r2-local-ducklake"

usage() {
    cat <<'EOF'
Usage:

docker run --rm \
  --env-file .env \
  -p 4317:4317 \
  -p 4318:4318 \
  -v "$(pwd)/data:/data" \
  IMAGE_NAME

Required for mode-based startup:

DUCKDB_MODE=local-ducklake|r2-data-catalog|s3-tables|r2-neon-ducklake|r2-local-ducklake

Aliases:

  ducklake-local -> local-ducklake
  cloudflare     -> r2-data-catalog
  s3tables       -> s3-tables

Useful common settings:

  DUCKDB_DATABASE=/data/duckdb-otlp-control.duckdb
  OTEL_HTTP_ADDR=0.0.0.0:4318
  DUCKDB_OTLP_TOKEN=change-me-at-least-16-chars
  DRY_RUN=1

If DUCKDB_MODE is unset, the image keeps the legacy DUCKDB_OTLP_CATALOG_TYPE
behavior for backwards compatibility.
EOF
}

case "${1:-}" in
    help|--help|-h)
        usage
        exit 0
        ;;
    "")
        ;;
    *)
        exec "$@"
        ;;
esac

log() {
    printf '%s\n' "$*"
}

error() {
    printf 'ERROR: %s\n' "$*" >&2
}

fail() {
    error "$1"
    printf '\n' >&2
    usage >&2
    exit 1
}

is_truthy() {
    case "${1:-}" in
        1|true|TRUE|yes|YES|on|ON) return 0 ;;
        *) return 1 ;;
    esac
}

sql_escape() {
    printf '%s' "$1" | sed "s/'/''/g"
}

sql_quote() {
    printf "'%s'" "$(sql_escape "$1")"
}

quote_ident() {
    printf '"%s"' "$(printf '%s' "$1" | sed 's/"/""/g')"
}

get_var() {
    eval "printf '%s' \"\${$1:-}\""
}

first_set_var() {
    for name in "$@"; do
        if [ -n "$(get_var "$name")" ]; then
            printf '%s' "$name"
            return 0
        fi
    done
    return 1
}

require_var() {
    name="$1"
    if [ -z "$(get_var "$name")" ]; then
        fail "Missing required environment variable ${name} for DUCKDB_MODE=${MODE}"
    fi
}

require_any_var() {
    label="$1"
    shift
    if first_set_var "$@" >/dev/null 2>&1; then
        return 0
    fi
    fail "Missing required environment variable for ${label}. Set one of: $*"
}

host_from_addr() {
    value="$1"
    case "$value" in
        \[*\]:*) printf '%s' "$value" | sed 's/^\[\(.*\)\]:[^:]*$/\1/' ;;
        *:*) printf '%s' "$value" | sed 's/:[^:]*$//' ;;
        *) printf '%s' "$value" ;;
    esac
}

port_from_addr() {
    value="$1"
    case "$value" in
        *:*) printf '%s' "$value" | sed 's/^.*://' ;;
        *) printf '%s' "$value" ;;
    esac
}

endpoint_host() {
    value="$1"
    value="${value#http://}"
    value="${value#https://}"
    value="${value%%/*}"
    printf '%s' "$value"
}

database_catalog_name() {
    db="$1"
    case "$db" in
        :memory:|"") return 1 ;;
    esac
    name="$(basename "$db")"
    case "$name" in
        *.duckdb) name="${name%.duckdb}" ;;
        *.db) name="${name%.db}" ;;
    esac
    printf '%s' "$name"
}

validate_catalog_does_not_shadow_database() {
    db_catalog="$(database_catalog_name "$DATABASE" || true)"
    if [ -n "$db_catalog" ] && [ "$CATALOG" = "$db_catalog" ]; then
        fail "DUCKDB catalog name conflict: DUCKDB_DATABASE=${DATABASE} creates catalog \"${db_catalog}\", so the mode catalog cannot also be \"${CATALOG}\". Change DUCKLAKE_NAME, DUCKDB_CATALOG, or DUCKDB_DATABASE."
    fi
}

r2_endpoint_default() {
    if [ -n "${CLOUDFLARE_R2_ENDPOINT:-}" ]; then
        endpoint_host "$CLOUDFLARE_R2_ENDPOINT"
    elif [ -n "${CLOUDFLARE_S3_API_HOST:-}" ]; then
        endpoint_host "$CLOUDFLARE_S3_API_HOST"
    elif [ -n "${R2_ENDPOINT:-}" ]; then
        endpoint_host "$R2_ENDPOINT"
    else
        require_var CLOUDFLARE_ACCOUNT_ID
        printf '%s.r2.cloudflarestorage.com' "$CLOUDFLARE_ACCOUNT_ID"
    fi
}

r2_bucket_value() {
    var="$(first_set_var CLOUDFLARE_R2_BUCKET R2_BUCKET_NAME R2_BUCKET || true)"
    if [ -z "$var" ]; then
        fail "Missing required R2 bucket variable. Set CLOUDFLARE_R2_BUCKET, R2_BUCKET_NAME, or R2_BUCKET"
    fi
    get_var "$var"
}

r2_prefix_value() {
    prefix="${CLOUDFLARE_R2_PREFIX:-${R2_PREFIX:-duckdb-otlp/}}"
    prefix="${prefix#/}"
    printf '%s' "$prefix"
}

r2_data_path() {
    bucket="$(r2_bucket_value)"
    prefix="$(r2_prefix_value)"
    if [ -n "$prefix" ]; then
        printf 's3://%s/%s' "$bucket" "$prefix"
    else
        printf 's3://%s/' "$bucket"
    fi
}

catalog_default() {
    default_value="$1"
    printf '%s' "${DUCKDB_CATALOG:-${DUCKDB_OTLP_CATALOG:-$default_value}}"
}

schema_default() {
    default_value="$1"
    printf '%s' "${DUCKDB_SCHEMA:-${DUCKDB_OTLP_SCHEMA:-$default_value}}"
}

emit_server_sql() {
    catalog_ident="$(quote_ident "$CATALOG")"
    schema_ident="$(quote_ident "$SCHEMA")"
    listen_sql="$(sql_quote "$LISTEN_URI")"
    catalog_sql="$(sql_quote "$CATALOG")"
    schema_sql="$(sql_quote "$SCHEMA")"
    cat <<SQL
CREATE SCHEMA IF NOT EXISTS ${catalog_ident}.${schema_ident};
SELECT listen_url, auth_token, catalog_name, schema_name
FROM otlp_serve(
    ${listen_sql},
    catalog := ${catalog_sql},
    schema := ${schema_sql},
    token := getenv('DUCKDB_OTLP_EFFECTIVE_TOKEN'),
    allow_other_hostname := true
);
SQL
}

emit_legacy_server_sql() {
    listen_sql="$(sql_quote "$LEGACY_LISTEN_URI")"
    schema_sql="$(sql_quote "$LEGACY_SCHEMA")"
    if [ -n "$LEGACY_CATALOG" ]; then
        catalog_sql="$(sql_quote "$LEGACY_CATALOG")"
        catalog_ident="$(quote_ident "$LEGACY_CATALOG")"
        schema_ident="$(quote_ident "$LEGACY_SCHEMA")"
        cat <<SQL
CREATE SCHEMA IF NOT EXISTS ${catalog_ident}.${schema_ident};
SELECT listen_url, auth_token, catalog_name, schema_name
FROM otlp_serve(
    ${listen_sql},
    catalog := ${catalog_sql},
    schema := ${schema_sql},
    token := getenv('DUCKDB_OTLP_EFFECTIVE_TOKEN'),
    allow_other_hostname := true
);
SQL
    else
        cat <<SQL
SELECT listen_url, auth_token, catalog_name, schema_name
FROM otlp_serve(
    ${listen_sql},
    schema := ${schema_sql},
    token := getenv('DUCKDB_OTLP_EFFECTIVE_TOKEN'),
    allow_other_hostname := true
);
SQL
    fi
}

legacy_emit_sql() {
    catalog_ident="$(quote_ident "$LEGACY_CATALOG")"
    case "$LEGACY_CATALOG_TYPE" in
        ducklake|local-ducklake)
            cat <<SQL
INSTALL ducklake;
LOAD ducklake;
LOAD otlp;
ATTACH 'ducklake:${DATA_DIR}/metadata.ducklake' AS ${catalog_ident} (DATA_PATH '${DATA_DIR}/files/');
SQL
            ;;
        motherduck)
            motherduck_attach="${DUCKDB_OTLP_MOTHERDUCK_ATTACH:-md:test-ducklake}"
            case "$motherduck_attach" in
                md:*|motherduck:*) ;;
                *) fail "DUCKDB_OTLP_MOTHERDUCK_ATTACH must be a MotherDuck attach string such as md:test-ducklake" ;;
            esac
            motherduck_attach_sql="$(sql_quote "$motherduck_attach")"
            cat <<SQL
INSTALL md;
LOAD md;
LOAD otlp;
ATTACH ${motherduck_attach_sql} AS ${catalog_ident};
SQL
            ;;
        custom)
            if [ -n "$LEGACY_SETUP_SQL_FILE" ]; then
                cat "$LEGACY_SETUP_SQL_FILE"
            elif [ -n "$LEGACY_SETUP_SQL" ]; then
                printf '%s\n' "$LEGACY_SETUP_SQL"
            else
                fail "DUCKDB_OTLP_CATALOG_TYPE=custom requires DUCKDB_OTLP_SETUP_SQL or DUCKDB_OTLP_SETUP_SQL_FILE"
            fi
            ;;
        none|default)
            cat <<'SQL'
LOAD otlp;
SQL
            LEGACY_CATALOG=""
            ;;
        *)
            fail "Unsupported DUCKDB_OTLP_CATALOG_TYPE=${LEGACY_CATALOG_TYPE}"
            ;;
    esac
    emit_legacy_server_sql
}

print_extensions() {
    if [ -z "${MODE_EXTENSIONS:-}" ]; then
        return
    fi
    log "Extensions:"
    for extension in $MODE_EXTENSIONS; do
        log "  ${extension}"
    done
    log ""
}

write_boot_sql() {
    rm -f "$BOOT_SQL"
    printf '.bail on\n' > "$BOOT_SQL"
    if [ "$MODE" = "legacy" ]; then
        legacy_emit_sql >> "$BOOT_SQL"
    else
        mode_emit_sql >> "$BOOT_SQL"
        emit_server_sql >> "$BOOT_SQL"
    fi
}

wait_for_duckdb_startup() {
    tries=0
    while [ "$tries" -lt 20 ]; do
        if ! kill -0 "$duckdb_pid" 2>/dev/null; then
            wait "$duckdb_pid" || true
            return 1
        fi
        tries=$((tries + 1))
        sleep 0.25
    done
    return 0
}

run_duckdb() {
    rm -f "$CONTROL_FIFO"
    mkfifo "$CONTROL_FIFO"

    duckdb -unsigned "$DATABASE" < "$CONTROL_FIFO" &
    duckdb_pid=$!

    exec 3> "$CONTROL_FIFO"
    cat "$BOOT_SQL" >&3

    if ! wait_for_duckdb_startup; then
        error "DuckDB initialization failed"
        exit 1
    fi

    log "DuckDB initialization complete"
    log "Starting server..."

    stop_server() {
        log "Stopping duckdb-otlp..."
        printf "SELECT status FROM otlp_stop('%s');\n.quit\n" "$(sql_escape "$LISTEN_URI")" >&3 || true
        wait "$duckdb_pid" || true
    }

    trap stop_server INT TERM
    wait "$duckdb_pid"
}

normalize_mode() {
    case "$1" in
        ducklake-local) printf '%s' "local-ducklake" ;;
        cloudflare) printf '%s' "r2-data-catalog" ;;
        s3tables) printf '%s' "s3-tables" ;;
        *) printf '%s' "$1" ;;
    esac
}

raw_mode="${DUCKDB_MODE:-}"

if [ -z "$raw_mode" ]; then
    MODE="legacy"
    LEGACY_CATALOG_TYPE="${DUCKDB_OTLP_CATALOG_TYPE:-ducklake}"
    LEGACY_CATALOG="${DUCKDB_OTLP_CATALOG:-lake}"
    LEGACY_SCHEMA="${DUCKDB_OTLP_SCHEMA:-main}"
    LEGACY_LISTEN_URI="${DUCKDB_OTLP_LISTEN_URI:-otlp:0.0.0.0:4318}"
    LEGACY_SETUP_SQL_FILE="${DUCKDB_OTLP_SETUP_SQL_FILE:-}"
    LEGACY_SETUP_SQL="${DUCKDB_OTLP_SETUP_SQL:-}"
    DATABASE="${DUCKDB_OTLP_DATABASE:-:memory:}"
    LISTEN_URI="$LEGACY_LISTEN_URI"
    TOKEN="${DUCKDB_OTLP_TOKEN:-$DEFAULT_TOKEN}"
    DISPLAY_MODE="legacy:${LEGACY_CATALOG_TYPE}"
    MODE_EXTENSIONS=""
else
    MODE="$(normalize_mode "$raw_mode")"
    mode_script="${MODE_DIR}/${MODE}.sh"
    if [ ! -f "$mode_script" ]; then
        error "Unsupported DUCKDB_MODE \"${raw_mode}\""
        printf '\nSupported modes:\n' >&2
        for supported in $SUPPORTED_MODES; do
            printf '  - %s\n' "$supported" >&2
        done
        exit 1
    fi

    DATABASE="${DUCKDB_DATABASE:-${DUCKDB_OTLP_DATABASE:-/data/duckdb-otlp-control.duckdb}}"
    OTEL_HTTP_ADDR="${OTEL_HTTP_ADDR:-0.0.0.0:4318}"
    OTEL_GRPC_ADDR="${OTEL_GRPC_ADDR:-0.0.0.0:4317}"
    LISTEN_URI="${DUCKDB_OTLP_LISTEN_URI:-otlp:${OTEL_HTTP_ADDR}}"
    TOKEN="${OTEL_AUTH_TOKEN:-${DUCKDB_OTLP_TOKEN:-$DEFAULT_TOKEN}}"
    DISPLAY_MODE="$MODE"

    # shellcheck source=/dev/null
    . "$mode_script"
    mode_defaults
    mode_validate
    validate_catalog_does_not_shadow_database
fi

mkdir -p "$DATA_DIR"
export DUCKDB_OTLP_EFFECTIVE_TOKEN="$TOKEN"

write_boot_sql

if [ "$MODE" != "legacy" ]; then
    log "Starting DuckDB OTEL Server"
    log ""
    log "Mode: ${DISPLAY_MODE}"
    log "Database: ${DATABASE}"
    log ""
    log "OTLP HTTP: ${OTEL_HTTP_ADDR}"
    log "OTLP gRPC: ${OTEL_GRPC_ADDR} (not served; duckdb-otlp currently supports OTLP/HTTP)"
    log ""
    print_extensions
else
    log "Starting DuckDB OTEL Server"
    log ""
    log "Mode: ${DISPLAY_MODE}"
    log "Database: ${DATABASE}"
    log "OTLP HTTP: $(host_from_addr "${LISTEN_URI#otlp:}"):$(port_from_addr "${LISTEN_URI#otlp:}")"
    log ""
fi

if is_truthy "$DRY_RUN"; then
    log "DRY_RUN=1; planned initialization only."
    log ""
    log "Generated initialization SQL:"
    if [ "$MODE" = "legacy" ] && [ "${LEGACY_CATALOG_TYPE:-}" = "custom" ]; then
        log "(omitted for legacy custom SQL to avoid printing user-provided secrets)"
    else
        cat "$BOOT_SQL"
    fi
    exit 0
fi

run_duckdb
