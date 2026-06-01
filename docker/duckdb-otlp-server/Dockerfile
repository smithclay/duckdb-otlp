FROM debian:bookworm-slim

ARG DUCKDB_VERSION=v1.5.3
ARG TARGETARCH

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        unzip \
        zlib1g \
    && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    case "${TARGETARCH:-amd64}" in \
        amd64) duckdb_arch="amd64" ;; \
        arm64) duckdb_arch="arm64" ;; \
        *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL \
        "https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/duckdb_cli-linux-${duckdb_arch}.zip" \
        -o /tmp/duckdb.zip; \
    unzip /tmp/duckdb.zip -d /usr/local/bin; \
    chmod +x /usr/local/bin/duckdb; \
    rm -f /tmp/duckdb.zip

RUN duckdb -unsigned -c "INSTALL ducklake;" \
    && duckdb -unsigned -c "INSTALL iceberg;" \
    && duckdb -unsigned -c "INSTALL httpfs;" \
    && duckdb -unsigned -c "INSTALL aws;" \
    && duckdb -unsigned -c "INSTALL postgres;" \
    && duckdb -unsigned -c "INSTALL quack;" \
    && duckdb -unsigned -c "INSTALL otlp FROM 'https://smithclay.github.io/duckdb-otlp';"

COPY docker/duckdb-otlp-server/entrypoint.sh /usr/local/bin/duckdb-otlp-server
COPY docker/duckdb-otlp-server/modes /usr/local/share/duckdb-otlp-server/modes

RUN chmod +x /usr/local/bin/duckdb-otlp-server \
    && mkdir -p /data

LABEL org.opencontainers.image.title="duckdb-otlp server" \
      org.opencontainers.image.description="DuckDB OTLP/HTTP ingest server with mode-based DuckDB catalog initialization" \
      org.opencontainers.image.source="https://github.com/smithclay/duckdb-otlp"

EXPOSE 4318 9494
VOLUME ["/data"]
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD sh -c 'otlp_port="${OTEL_HTTP_ADDR##*:}"; \
        curl -fsS "http://127.0.0.1:${otlp_port:-4318}/healthz" >/dev/null || exit 1; \
        case "${DUCKDB_QUACK_ENABLED:-${QUACK_ENABLED:-0}}" in \
          1|true|TRUE|yes|YES|on|ON) \
            quack_addr="${DUCKDB_QUACK_ADDR:-${QUACK_HTTP_ADDR:-0.0.0.0:9494}}"; \
            quack_port="${quack_addr##*:}"; \
            curl -fsS "http://127.0.0.1:${quack_port:-9494}/" >/dev/null || exit 1 ;; \
        esac'

ENTRYPOINT ["duckdb-otlp-server"]
