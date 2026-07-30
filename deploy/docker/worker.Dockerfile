# Talon worker image.
#
# Multi-stage: a Rust build stage compiles the release binary, and a slim Debian
# runtime stage ships only the binary. Runs as a non-root user; configuration
# and the object-store credentials come from TALON_WORKER_* environment
# variables at runtime (the Azure SAS token is env-only and never baked in).
#
# Build from the repository root:
#   docker build -f deploy/docker/worker.Dockerfile -t talon-worker .

# ---- build stage ---------------------------------------------------------
FROM rust:1.96.1-bookworm AS build

# protoc compiles the WAL record schema (proto/wal.proto). ADR 0003 §9.4
# requires a specified on-disk format rather than a derived Rust encoding, so
# the build now depends on it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN cargo build --release -p talon-worker \
    && strip target/release/talon-worker

# ---- runtime stage -------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --user-group --home-dir /nonexistent \
       --no-create-home talon \
    && mkdir -p /var/cache/talon \
    && chown 10001:10001 /var/cache/talon

COPY --from=build /src/target/release/talon-worker /usr/local/bin/talon-worker

USER 10001:10001

# Data plane and admin (metrics/health/status). Bind 0.0.0.0 in a container.
ENV TALON_WORKER_LISTEN=0.0.0.0:7001 \
    TALON_WORKER_ADMIN_LISTEN=0.0.0.0:8001 \
    TALON_WORKER_CACHE_DIRS=/var/cache/talon
EXPOSE 7001 8001
VOLUME ["/var/cache/talon"]

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8001/healthz || exit 1

ENTRYPOINT ["talon-worker"]
