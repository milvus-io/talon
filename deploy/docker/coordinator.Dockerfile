# Talon coordinator image.
#
# Multi-stage: cargo-chef stages cache the dependency build as its own image
# layer (keyed by the workspace manifests + lockfile, so source-only changes
# skip recompiling every dependency), a Rust build stage compiles the release
# binary with both production state-store backends (etcd + Kubernetes), and a
# slim Debian runtime stage ships only the binary. The container runs as a
# non-root user and bakes in no configuration or secrets — everything comes
# from TALON_COORDINATOR_* environment variables or a mounted --config file at
# runtime.
#
# Build from the repository root:
#   docker build -f deploy/docker/coordinator.Dockerfile -t talon-coordinator .

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

# protoc is required to compile the etcd client's generated protobuf code — a
# third-party build script, which cargo-chef does not mask, so it already runs
# while cooking dependencies below.
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Dependencies only: workspace build scripts are masked during the cook, and
# COPY --from is content-addressed, so this expensive layer survives any
# source-only change. --locked on the cook makes a stale lockfile fail here
# in seconds instead of after the multi-minute dependency build.
COPY --from=planner /src/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json \
    --package talon-coordinator --features etcd,kubernetes

COPY . .

# Both production backends compiled in; rustls (not OpenSSL) keeps the runtime
# free of a system TLS dependency. --locked: the image must build exactly the
# dependency versions CI tested.
RUN cargo build --release --locked -p talon-coordinator --features etcd,kubernetes \
    && strip target/release/talon-coordinator

# ---- runtime stage -------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# ca-certificates for TLS to etcd / the Kubernetes API; curl for HEALTHCHECK.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --user-group --home-dir /nonexistent \
       --no-create-home talon

COPY --from=build /src/target/release/talon-coordinator /usr/local/bin/talon-coordinator

USER 10001:10001

# Control plane and admin (metrics/health/API/UI). Bind 0.0.0.0 in a container
# so the ports are reachable; the loopback defaults are for local dev only.
ENV TALON_COORDINATOR_LISTEN=0.0.0.0:7000 \
    TALON_COORDINATOR_ADMIN_LISTEN=0.0.0.0:8000
EXPOSE 7000 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8000/healthz || exit 1

ENTRYPOINT ["talon-coordinator"]
