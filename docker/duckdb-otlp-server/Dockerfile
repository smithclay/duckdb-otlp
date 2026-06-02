# Single source of truth for the duckdb-otlp server image. Stages:
#   builder         - compiles the static daemon from source (Rust + DuckDB).
#   daemon-export   - exports just the binary (CI uploads this as an artifact).
#   deps            - throwaway: primes the DuckDB extension cache and stages the one
#                     shared lib distroless lacks (libz). The DuckDB CLI lives ONLY here.
#   runtime-base    - distroless final minus the daemon binary (extensions, libz, health).
#   runtime-source  - runtime-base + binary from `builder` (local build-from-source).
#   runtime-prebuilt- runtime-base + binary from `docker-bin/` (CI, no recompile).
#
# `docker build` with no --target builds `runtime-source`. CI builds `daemon-export` once
# per arch, then packages with `--target runtime-prebuilt`, so packaging never compiles
# DuckDB (runtime-prebuilt does not depend on `builder`).
#
# The final image is gcr.io/distroless/cc-debian12 (nonroot): glibc + NSS (DNS) + libstdc++
# + openssl + CA certs, ~48MB, no shell, no package manager, no bundled DuckDB CLI. The
# daemon and all six runtime extensions link only glibc/libstdc++/libgcc/libz, so this is
# all they need. See AGENTS.md.

FROM rust:1-bookworm AS builder

ARG DUCKDB_VERSION=v1.5.3

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        cmake \
        git \
        ninja-build \
        pkg-config \
        python3 \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . /src

RUN GEN=ninja OVERRIDE_GIT_DESCRIBE=${DUCKDB_VERSION} make server-release

FROM scratch AS daemon-export
COPY --from=builder /src/build/release/extension/otlp/duckdb_otlp_server /duckdb-otlp-server

FROM debian:bookworm-slim AS deps

ARG DUCKDB_VERSION=v1.5.3
ARG TARGETARCH
# Runtime user in the distroless nonroot image.
ARG RUN_UID=65532
ARG RUN_GID=65532
ENV HOME=/duckdb-home

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl unzip zlib1g \
    && rm -rf /var/lib/apt/lists/*

# DuckDB CLI is used only to prime the extension cache; it is NOT copied into the final image.
RUN set -eux; \
    case "${TARGETARCH:-amd64}" in \
        amd64) duckdb_arch="amd64" ;; \
        arm64) duckdb_arch="arm64" ;; \
        *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL \
        "https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/duckdb_cli-linux-${duckdb_arch}.zip" \
        -o /tmp/duckdb.zip; \
    unzip /tmp/duckdb.zip -d /tmp/duckdb-cli; \
    rm -f /tmp/duckdb.zip

# Prime every mode's extensions into HOME=/duckdb-home so the daemon's startup INSTALL/LOAD
# is offline. The daemon (otlp) is statically embedded and is intentionally not installed.
RUN set -eux; mkdir -p "${HOME}"; \
    for ext in ducklake iceberg httpfs aws postgres quack; do \
        /tmp/duckdb-cli/duckdb -unsigned -c "INSTALL ${ext};"; \
    done; \
    chown -R "${RUN_UID}:${RUN_GID}" "${HOME}"

# Stage libz from the apt-installed zlib1g (the only shared lib the daemon needs that
# distroless/cc lacks) and a runtime-user-owned empty data dir (so fresh named volumes
# inherit good ownership).
RUN set -eux; \
    mkdir -p /staging/lib; \
    libz="$(find /usr/lib /lib -name 'libz.so.1' 2>/dev/null | head -n1)"; \
    test -n "${libz}"; \
    cp -L "${libz}" /staging/lib/libz.so.1; \
    install -d -o "${RUN_UID}" -g "${RUN_GID}" /staging/data

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime-base

# The one shared lib distroless/cc lacks. LD_LIBRARY_PATH makes it resolvable regardless of
# the multiarch triplet layout.
COPY --from=deps /staging/lib/libz.so.1 /opt/duckdb-otlp/lib/libz.so.1
ENV LD_LIBRARY_PATH=/opt/duckdb-otlp/lib
# Pre-primed extension cache and a writable data dir, both owned by the nonroot user.
COPY --from=deps --chown=65532:65532 /duckdb-home /duckdb-home
COPY --from=deps --chown=65532:65532 /staging/data /data
ENV HOME=/duckdb-home

LABEL org.opencontainers.image.title="duckdb-otlp server" \
      org.opencontainers.image.description="DuckDB OTLP/HTTP ingest server with mode-based DuckDB catalog initialization" \
      org.opencontainers.image.source="https://github.com/smithclay/duckdb-otlp"

EXPOSE 4318 9494
VOLUME ["/data"]
# The daemon health-checks itself (distroless has no shell/curl): the `healthcheck`
# subcommand probes /readyz (and Quack when enabled) over loopback. /readyz only reports
# that the listener is bound; the daemon logs a WARNING if seals stop committing.
HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD ["/usr/local/bin/duckdb-otlp-server", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/duckdb-otlp-server"]

FROM runtime-base AS runtime-prebuilt
ARG TARGETARCH
COPY docker-bin/${TARGETARCH}/duckdb-otlp-server /usr/local/bin/duckdb-otlp-server

FROM runtime-base AS runtime-source
COPY --from=builder /src/build/release/extension/otlp/duckdb_otlp_server /usr/local/bin/duckdb-otlp-server
