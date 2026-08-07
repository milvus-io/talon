# Talon FUSE client image.
#
# Multi-stage: cargo-chef stages cache the dependency build as its own image
# layer (keyed by the workspace manifests + lockfile, so source-only changes
# skip recompiling every dependency), a Rust build stage compiles the release
# binary with the `mount` feature (kernel FUSE adapter), and a slim Debian
# runtime stage ships the binary plus libfuse3.
#
# Mounting talks to the kernel, so a container running this image needs
# `/dev/fuse` and the SYS_ADMIN capability, e.g.:
#   docker run --rm --device /dev/fuse --cap-add SYS_ADMIN \
#     -e TALON_FUSE_COORDINATOR=coordinator:7000 \
#     -e TALON_FUSE_NAMESPACE_PREFIX=az/container talon-fuse \
#     --mountpoint /mnt/talon
#
# Build from the repository root:
#   docker build -f deploy/docker/fuse.Dockerfile -t talon-fuse .

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

# No system build dependencies: fuser is pinned with default-features = false
# (crates/talon-fuse/Cargo.toml), which drops its libfuse/pkg-config link path
# — the crate talks to /dev/fuse directly. Only the runtime stage needs the
# fuse3 package (for the fusermount3 helper).

# Dependencies only: workspace build scripts are masked during the cook, and
# COPY --from is content-addressed, so this expensive layer survives any
# source-only change. --locked on the cook makes a stale lockfile fail here
# in seconds instead of after the multi-minute dependency build.
COPY --from=planner /src/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json \
    --package talon-fuse --features mount

COPY . .

# --locked: the image must build exactly the dependency versions CI tested.
RUN cargo build --release --locked -p talon-fuse --features mount \
    && strip target/release/talon-fuse

# ---- runtime stage -------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# fuse3 provides the runtime library and the fusermount3 helper.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates fuse3 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --user-group --home-dir /nonexistent \
       --no-create-home talon

COPY --from=build /src/target/release/talon-fuse /usr/local/bin/talon-fuse

# The mount syscall requires elevated privileges; run this image with
# --cap-add SYS_ADMIN --device /dev/fuse. It does not open network ports.
ENTRYPOINT ["talon-fuse"]
