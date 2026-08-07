# Talon worker image.
#
# Multi-stage: cargo-chef stages cache the dependency build as its own image
# layer (keyed by the workspace manifests + lockfile, so source-only changes
# skip recompiling every dependency), a Rust build stage compiles the release
# binary, and a slim Debian runtime stage ships only the binary. Runs as a
# non-root user; configuration and the object-store credentials come from
# TALON_WORKER_* environment variables at runtime (the Azure SAS token is
# env-only and never baked in).
#
# Build from the repository root:
#   docker build -f deploy/docker/worker.Dockerfile -t talon-worker .

# ---- chef stage ----------------------------------------------------------
FROM rust:1.96.1-bookworm AS chef

RUN cargo install cargo-chef --locked --version 0.1.77
WORKDIR /src

# ---- planner stage -------------------------------------------------------
# `prepare` distills the workspace manifests into a recipe. It only parses
# Cargo metadata, so it needs no system build dependencies, and its output
# only changes when a manifest, the lockfile, or the toolchain pin changes.
FROM chef AS planner

COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---- build stage ---------------------------------------------------------
FROM chef AS build

# protoc compiles the WAL record schema (proto/wal.proto). ADR 0003 §9.4
# requires a specified on-disk format rather than a derived Rust encoding, so
# the build depends on it. The worker's own build script is masked during the
# cook, so protoc is only exercised by the final build — installing it before
# the cook just keeps this apt layer stable.
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Dependencies only: workspace build scripts are masked during the cook, and
# COPY --from is content-addressed, so this expensive layer survives any
# source-only change. --locked on the cook makes a stale lockfile fail here
# in seconds instead of after the multi-minute dependency build.
COPY --from=planner /src/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json \
    --package talon-worker

COPY . .

# --locked: the image must build exactly the dependency versions CI tested.
RUN cargo build --release --locked -p talon-worker \
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
