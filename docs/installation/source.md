# Install from source

Building from source is the hardcore path — aimed at contributors and anyone who
wants to run unreleased code. If you just want to run Talon, [Docker](./docker.md)
or [Kubernetes](./kubernetes.md) are quicker.

## Prerequisites

- **Rust** — the toolchain is pinned in `rust-toolchain.toml`; `rustup` selects
  it automatically, so no manual version juggling.
- **A Linux host** — required for the FUSE client. The coordinator and worker
  run anywhere Rust does.
- **`protoc`** — the Protocol Buffers compiler, needed to build the coordinator
  with the `etcd`/`kubernetes` backends.

## Build

```sh
git clone https://github.com/milvus-io/talon.git
cd talon
cargo build --workspace
```

The first build compiles all crates and may take a few minutes; later builds are
incremental.

## Run a single-node cluster

Start a coordinator and a worker with the development **memory** backend:

```sh
# Coordinator: control plane on :7000, admin API + UI on :8000.
cargo run -p talon-coordinator -- \
  --listen 127.0.0.1:7000 --admin-listen 127.0.0.1:8000 \
  --cluster-id demo --node-id coord-0

# Worker (in another shell): registers with the coordinator above.
TALON_WORKER_AZURE_ACCOUNT=demo \
TALON_WORKER_AZURE_SAS="sv=demo&sig=demo" \
TALON_WORKER_CACHE_DIRS=/tmp/talon-cache \
cargo run -p talon-worker -- \
  --listen 127.0.0.1:7001 --admin-listen 127.0.0.1:8001 \
  --coordinator 127.0.0.1:7000 --cluster-id demo --node-id worker-0
```

Then open **http://127.0.0.1:8000/ui** or check `curl -s http://127.0.0.1:8000/readyz`.

The [getting started tutorial](../tutorials/getting-started.md) walks through the
API, the management console, and the FUSE client step by step.

## FUSE client

Mounting talks to the kernel, so `talon-fuse` is compiled behind a `mount`
feature and needs `/dev/fuse` (Linux with FUSE and `libfuse3-dev` installed):

```sh
cargo run -p talon-fuse --features mount -- \
  --mountpoint /tmp/talon-mnt --coordinator 127.0.0.1:7000
```

## Developing

See [CONTRIBUTING.md](https://github.com/milvus-io/talon/blob/main/CONTRIBUTING.md)
for the full workflow — running the test suite, linting, the benchmark harness,
and submitting changes. Run `just` to list common development tasks.
