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
    && duckdb -unsigned -c "INSTALL otlp FROM 'https://smithclay.github.io/duckdb-otlp';"

COPY docker/duckdb-otlp-server/entrypoint.sh /usr/local/bin/duckdb-otlp-server

RUN chmod +x /usr/local/bin/duckdb-otlp-server \
    && mkdir -p /data

LABEL org.opencontainers.image.title="duckdb-otlp server" \
      org.opencontainers.image.description="DuckDB OTLP/HTTP ingest server writing to a local DuckLake volume" \
      org.opencontainers.image.source="https://github.com/smithclay/duckdb-otlp"

EXPOSE 4318
VOLUME ["/data"]
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD curl -fsS http://127.0.0.1:4318/healthz >/dev/null || exit 1

ENTRYPOINT ["duckdb-otlp-server"]
