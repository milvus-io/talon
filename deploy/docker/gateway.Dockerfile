# Talon object-store gateway image.
#
# The runtime is intentionally suitable for loopback sidecars only until #446
# installs provider authentication, authorization, and TLS.

FROM rust:1.96.1-bookworm AS chef

RUN cargo install cargo-chef --locked --version 0.1.77
WORKDIR /src

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
COPY --from=planner /src/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json \
    --package talon-gateway
COPY . .
RUN cargo build --release --locked -p talon-gateway --bin talon-gateway \
    && strip target/release/talon-gateway

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --user-group --home-dir /nonexistent \
       --no-create-home talon
COPY --from=build /src/target/release/talon-gateway /usr/local/bin/talon-gateway

USER 10001:10001
ENV TALON_GATEWAY_BIND=127.0.0.1:8080 \
    TALON_GATEWAY_MODE=development \
    TALON_GATEWAY_ROUTE=cache \
    RUST_LOG=info
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["talon-gateway"]
