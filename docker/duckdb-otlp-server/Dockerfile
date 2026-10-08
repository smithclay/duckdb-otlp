# Single source of truth for the duckdb-otlp server image. Stages:
#   builder         - compiles the static daemon from source (Rust + DuckDB).
#   daemon-export   - exports just the binary (CI uploads this as an artifact).
#   deps            - throwaway: primes the DuckDB extension cache. The DuckDB CLI lives
#                     ONLY here.
#   zlib            - throwaway: stages Wolfi's libz, the one shared lib the base lacks.
#   runtime-base    - distroless final minus the daemon binary (extensions, libz, health).
#   runtime-source  - runtime-base + binary from `builder` (local build-from-source).
#   runtime-prebuilt- runtime-base + binary from `docker-bin/` (CI, no recompile).
#
# `docker build` with no --target builds `runtime-source`. CI builds `daemon-export` once
# per arch, then packages with `--target runtime-prebuilt`, so packaging never compiles
# DuckDB (runtime-prebuilt does not depend on `builder`).
#
# The final image is cgr.dev/chainguard/glibc-dynamic (Wolfi, uid 65532 nonroot): glibc +
# libstdc++ + libgcc + CA certs, no shell, no package manager, no bundled DuckDB CLI. The
# daemon and the runtime extensions link only glibc/libstdc++/libgcc/libz (the extensions
# carry their own TLS statically), so this is all they need. It replaced
# gcr.io/distroless/cc-debian12, whose CVE reports were dominated by Debian 12's libssl3 —
# a library nothing in this image loads. See AGENTS.md.

FROM rust:1-bookworm AS builder

ARG DUCKDB_VERSION=v1.5.6

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
COPY --from=builder /src/build/release/extension/otlp/duckdb-otlp /duckdb-otlp-server

FROM debian:bookworm-slim AS deps

ARG DUCKDB_VERSION=v1.5.6
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
    /tmp/duckdb-cli/duckdb -c "INSTALL gcs FROM community; LOAD gcs;"; \
    chown -R "${RUN_UID}:${RUN_GID}" "${HOME}"

# A runtime-user-owned empty data dir (so fresh named volumes inherit good ownership).
RUN install -d -o "${RUN_UID}" -g "${RUN_GID}" /staging/data

# Compatibility alias: the binary is `duckdb-otlp` (it is a general CLI now), but the
# historical entry point was `duckdb-otlp-server`. Distroless has no shell, so the symlink
# is created here and copied in; COPY preserves it rather than dereferencing it.
RUN mkdir -p /staging/bin && ln -s duckdb-otlp /staging/bin/duckdb-otlp-server

# The GCS extension's bundled libcurl looks for a Red Hat-style CA path during
# ADC token refresh. Link it to the runtime base's maintained trust store.
# Copy the directory below to preserve the symlink rather than dereferencing it.
RUN mkdir -p /staging/certs \
    && ln -s /etc/ssl/certs/ca-certificates.crt /staging/certs/ca-bundle.crt

# libz is the one shared lib the daemon links that glibc-dynamic lacks. Take it from Wolfi,
# the same distribution as the runtime base, so it tracks the same security updates.
FROM cgr.dev/chainguard/wolfi-base:latest@sha256:05d24163df148be377275af8374c16523a1dc7e19bf4f1c689784791553c5e45 AS zlib
RUN apk add --no-cache zlib && mkdir -p /staging && cp -L /usr/lib/libz.so.1 /staging/libz.so.1

# Pinned by digest: Chainguard's free tier only publishes `latest`, and Renovate bumps the
# digest so the image picks up rebuilt (CVE-fixed) bases without floating silently.
FROM cgr.dev/chainguard/glibc-dynamic:latest@sha256:2a3f1ec8a825dfbb1211b5835a9b8b3fb8d76c7bd22a189b1f84157ed2028293 AS runtime-base

COPY --from=deps /staging/certs/ /etc/pki/tls/certs/

# Wolfi's library directory is /usr/lib, already on the loader's default search path.
COPY --from=zlib /staging/libz.so.1 /usr/lib/libz.so.1
# Pre-primed extension cache and a writable data dir, both owned by the nonroot user.
COPY --from=deps --chown=65532:65532 /duckdb-home /duckdb-home
COPY --from=deps --chown=65532:65532 /staging/data /data
ENV HOME=/duckdb-home
# Copy the directory, not the file: a direct COPY of a symlink dereferences it (the same
# reason the CA bundle above is copied as a directory), and this link's target only lands in
# the leaf stage below.
COPY --from=deps /staging/bin/ /usr/local/bin/

# Container-shaped defaults live HERE, not in the C++ defaults, which are laptop-shaped
# (loopback bind, XDG data dir, both transports). Keeping the split explicit means there is
# no "am I in a container" detection at runtime, and these four lines are the complete
# description of how the image differs from running the CLI directly.
ENV DUCKDB_OTLP_DATA_DIR=/data \
    DUCKDB_OTLP_HOST=0.0.0.0 \
    DUCKDB_OTLP_TRANSPORTS=http \
    DUCKDB_QUACK_ADDR=0.0.0.0:9494

LABEL org.opencontainers.image.title="duckdb-otlp server" \
      org.opencontainers.image.description="DuckDB OTLP HTTP/gRPC ingest server with mode-based DuckDB catalog initialization" \
      org.opencontainers.image.source="https://github.com/smithclay/duckdb-otlp"

EXPOSE 4317 4318 9494
VOLUME ["/data"]
# The daemon health-checks itself (the base has no shell/curl): the `doctor`
# subcommand probes every enabled listener: HTTP /readyz, gRPC TCP connect, and
# Quack when enabled, printing a line per check that Docker keeps in the container's
# health log. `healthcheck` remains an accepted spelling for existing probes.
# The daemon also logs a WARNING if seals stop committing.
# timeout must exceed the worst case: `doctor` now probes EVERY listener rather than
# returning at the first failure, and an unresponsive (as opposed to closed) port costs 2s
# each, so up to four checks can take ~8s. At the old 3s Docker killed the probe before it
# printed, losing the per-check report that is the reason for not short-circuiting.
HEALTHCHECK --interval=15s --timeout=10s --start-period=15s --retries=3 \
    CMD ["/usr/local/bin/duckdb-otlp", "doctor"]

ENTRYPOINT ["/usr/local/bin/duckdb-otlp"]

FROM runtime-base AS runtime-prebuilt
ARG TARGETARCH
COPY docker-bin/${TARGETARCH}/duckdb-otlp-server /usr/local/bin/duckdb-otlp

FROM runtime-base AS runtime-source
COPY --from=builder /src/build/release/extension/otlp/duckdb-otlp /usr/local/bin/duckdb-otlp
