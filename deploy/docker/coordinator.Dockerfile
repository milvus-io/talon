# Talon coordinator image.
#
# Multi-stage: a Rust build stage compiles the release binary with both
# production state-store backends (etcd + Kubernetes), and a slim Debian runtime
# stage ships only the binary. The container runs as a non-root user and bakes
# in no configuration or secrets — everything comes from TALON_COORDINATOR_*
# environment variables or a mounted --config file at runtime.
#
# Build from the repository root:
#   docker build -f deploy/docker/coordinator.Dockerfile -t talon-coordinator .

# ---- build stage ---------------------------------------------------------
FROM rust:1.96.1-bookworm AS build

# protoc is required to compile the etcd client's generated protobuf code.
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# Both production backends compiled in; rustls (not OpenSSL) keeps the runtime
# free of a system TLS dependency.
RUN cargo build --release -p talon-coordinator --features etcd,kubernetes \
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
