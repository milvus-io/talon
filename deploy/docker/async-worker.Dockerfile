# Talon async worker image.
#
# The extent-cache worker: caches exact byte ranges rather than 256MB blocks,
# so a selective columnar read fetches only what it asked for. See ADR 0005.
#
# Multi-stage: a Rust build stage compiles the release binary, and a slim Debian
# runtime stage ships only the binary. Runs as a non-root user; configuration
# and the object-store credentials come from TALON_ASYNC_WORKER_* environment
# variables at runtime (every secret is env-only and never baked in).
#
# Build from the repository root:
#   docker build -f deploy/docker/async-worker.Dockerfile -t talon-async-worker .

# ---- build stage ---------------------------------------------------------
FROM rust:1.96.1-bookworm AS build

WORKDIR /src
COPY . .

RUN cargo build --release -p talon-async-worker \
    && strip target/release/talon-async-worker

# ---- runtime stage -------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --user-group --home-dir /nonexistent \
       --no-create-home talon \
    && mkdir -p /var/cache/talon-extents \
    && chown 10001:10001 /var/cache/talon-extents

COPY --from=build /src/target/release/talon-async-worker \
     /usr/local/bin/talon-async-worker

USER 10001:10001

# Ports are deliberately offset from the block worker's 7001/8001 so both can
# run on one host during a migration.
ENV TALON_ASYNC_WORKER_LISTEN=0.0.0.0:7101 \
    TALON_ASYNC_WORKER_ADMIN_LISTEN=0.0.0.0:8101 \
    TALON_ASYNC_WORKER_CACHE_DIR=/var/cache/talon-extents
EXPOSE 7101 8101

# Declared for the I/O path, not for persistence: run descriptors live only in
# memory, so this directory is wiped at every start (ADR 0005 section 7).
VOLUME ["/var/cache/talon-extents"]

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8101/healthz || exit 1

ENTRYPOINT ["talon-async-worker"]
