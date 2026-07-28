# Talon FUSE client image.
#
# Multi-stage: a Rust build stage compiles the release binary with the `mount`
# feature (kernel FUSE adapter), and a slim Debian runtime stage ships the
# binary plus libfuse3.
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

# ---- build stage ---------------------------------------------------------
FROM rust:1.96.1-bookworm AS build

# fuser links libfuse via pkg-config; the -dev package provides the headers.
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libfuse3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN cargo build --release -p talon-fuse --features mount \
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
