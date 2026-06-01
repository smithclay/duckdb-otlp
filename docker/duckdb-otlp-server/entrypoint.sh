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
  -p 4318:4318 \
  -v "$(pwd)/data:/data" \
  IMAGE_NAME

Required:

DUCKDB_MODE=local-ducklake|r2-data-catalog|s3-tables|r2-neon-ducklake|r2-local-ducklake

Aliases:

  ducklake-local -> local-ducklake
  cloudflare     -> r2-data-catalog
  s3tables       -> s3-tables

Useful common settings:

  DUCKDB_DATABASE=/data/duckdb-otlp-control.duckdb
  OTEL_HTTP_ADDR=0.0.0.0:4318
  DUCKDB_OTLP_TOKEN=change-me-at-least-16-chars
  DUCKDB_QUACK_ENABLED=0
  DUCKDB_QUACK_ADDR=0.0.0.0:9494
  DUCKDB_QUACK_TOKEN=required-when-quack-enabled
  DRY_RUN=1
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
    printf '%s' "${DUCKDB_CATALOG:-$default_value}"
}

schema_default() {
    default_value="$1"
    printf '%s' "${DUCKDB_SCHEMA:-$default_value}"
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

emit_quack_sql() {
    if ! is_truthy "$QUACK_ENABLED"; then
        return
    fi

    quack_listen_sql="$(sql_quote "$QUACK_LISTEN_URI")"
    cat <<SQL
LOAD quack;
SELECT listen_uri, auth_token
FROM quack_serve(
    ${quack_listen_sql},
    token := getenv('DUCKDB_QUACK_EFFECTIVE_TOKEN'),
    allow_other_hostname := true
);
SQL
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
    mode_emit_sql >> "$BOOT_SQL"
    emit_server_sql >> "$BOOT_SQL"
    emit_quack_sql >> "$BOOT_SQL"
}

http_probe() {
    curl -fsS --max-time 2 "$1" >/dev/null 2>&1
}

# Wait until the server is actually accepting requests, not merely until the
# DuckDB process is alive. A slow remote ATTACH (R2/Iceberg/Neon) can keep the
# process up long before otlp_serve binds, and a hang would never bind at all,
# so probe /healthz (and the Quack endpoint when enabled) for real readiness.
# Boot SQL runs with `.bail on`, so a failed statement exits the process and is
# caught by the kill -0 check below.
wait_for_duckdb_startup() {
    otlp_port="${OTEL_HTTP_ADDR##*:}"
    quack_port="${QUACK_HTTP_ADDR##*:}"
    otlp_health_url="http://127.0.0.1:${otlp_port:-4318}/healthz"
    quack_health_url="http://127.0.0.1:${quack_port:-9494}/"
    deadline_tries=$((STARTUP_TIMEOUT_SECS * 4))
    tries=0
    while [ "$tries" -lt "$deadline_tries" ]; do
        if ! kill -0 "$duckdb_pid" 2>/dev/null; then
            wait "$duckdb_pid" || true
            return 1
        fi
        if http_probe "$otlp_health_url"; then
            if ! is_truthy "$QUACK_ENABLED" || http_probe "$quack_health_url"; then
                return 0
            fi
        fi
        tries=$((tries + 1))
        sleep 0.25
    done
    error "Timed out after ${STARTUP_TIMEOUT_SECS}s waiting for DuckDB to accept requests"
    return 1
}

run_duckdb() {
    rm -f "$CONTROL_FIFO"
    mkfifo "$CONTROL_FIFO"

    duckdb -unsigned -init "$BOOT_SQL" "$DATABASE" < "$CONTROL_FIFO" &
    duckdb_pid=$!

    exec 3> "$CONTROL_FIFO"

    if ! wait_for_duckdb_startup; then
        error "DuckDB initialization failed"
        exit 1
    fi

    log "DuckDB initialization complete"
    log "Starting server..."

    stop_server() {
        log "Stopping duckdb-otlp..."
        if is_truthy "$QUACK_ENABLED"; then
            printf "CALL quack_stop('%s');\n" "$(sql_escape "$QUACK_LISTEN_URI")" >&3 || true
        fi
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

if [ -z "${DUCKDB_MODE:-}" ]; then
    fail "Missing required environment variable DUCKDB_MODE"
fi

MODE="$(normalize_mode "$DUCKDB_MODE")"
mode_script="${MODE_DIR}/${MODE}.sh"
if [ ! -f "$mode_script" ]; then
    error "Unsupported DUCKDB_MODE \"${DUCKDB_MODE}\""
    printf '\nSupported modes:\n' >&2
    for supported in $SUPPORTED_MODES; do
        printf '  - %s\n' "$supported" >&2
    done
    exit 1
fi

DATABASE="${DUCKDB_DATABASE:-/data/duckdb-otlp-control.duckdb}"
OTEL_HTTP_ADDR="${OTEL_HTTP_ADDR:-0.0.0.0:4318}"
STARTUP_TIMEOUT_SECS="${DUCKDB_OTLP_STARTUP_TIMEOUT:-60}"
LISTEN_URI="${DUCKDB_OTLP_LISTEN_URI:-otlp:${OTEL_HTTP_ADDR}}"
TOKEN="${OTEL_AUTH_TOKEN:-${DUCKDB_OTLP_TOKEN:-$DEFAULT_TOKEN}}"
QUACK_ENABLED="${DUCKDB_QUACK_ENABLED:-${QUACK_ENABLED:-0}}"
QUACK_HTTP_ADDR="${DUCKDB_QUACK_ADDR:-${QUACK_HTTP_ADDR:-0.0.0.0:9494}}"
QUACK_LISTEN_URI="${DUCKDB_QUACK_LISTEN_URI:-quack:${QUACK_HTTP_ADDR}}"
QUACK_TOKEN_VAR="$(first_set_var DUCKDB_QUACK_TOKEN QUACK_AUTH_TOKEN || true)"
if is_truthy "$QUACK_ENABLED" && [ -z "$QUACK_TOKEN_VAR" ]; then
    fail "DUCKDB_QUACK_ENABLED=1 requires a dedicated Quack token. Set DUCKDB_QUACK_TOKEN or QUACK_AUTH_TOKEN. The Quack endpoint grants full SQL read/write to every attached catalog, so it does not reuse the OTLP ingest token."
fi
if [ -n "$QUACK_TOKEN_VAR" ]; then
    QUACK_TOKEN="$(get_var "$QUACK_TOKEN_VAR")"
else
    QUACK_TOKEN=""
fi

# shellcheck source=/dev/null
. "$mode_script"
mode_defaults
mode_validate
validate_catalog_does_not_shadow_database

mkdir -p "$DATA_DIR"
export DUCKDB_OTLP_EFFECTIVE_TOKEN="$TOKEN"
export DUCKDB_QUACK_EFFECTIVE_TOKEN="$QUACK_TOKEN"

if is_truthy "$QUACK_ENABLED"; then
    MODE_EXTENSIONS="${MODE_EXTENSIONS:-} quack"
fi

write_boot_sql

log "Starting DuckDB OTEL Server"
log ""
log "Mode: ${MODE}"
log "Database: ${DATABASE}"
log ""
log "OTLP HTTP: ${OTEL_HTTP_ADDR}"
if is_truthy "$QUACK_ENABLED"; then
    log "Quack: ${QUACK_LISTEN_URI}"
    log ""
    log "WARNING: Quack grants full SQL read/write access to every attached"
    log "         catalog over an unencrypted connection, guarded only by the"
    log "         Quack token. Keep ${QUACK_LISTEN_URI} on a trusted network and"
    log "         terminate TLS at a proxy before exposing it publicly."
else
    log "Quack: disabled"
fi
log ""
print_extensions

if is_truthy "$DRY_RUN"; then
    log "DRY_RUN=1; planned initialization only."
    log ""
    log "Generated initialization SQL:"
    cat "$BOOT_SQL"
    exit 0
fi

run_duckdb
