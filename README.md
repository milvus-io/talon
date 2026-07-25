# Talon

**A distributed object-store cache written in Rust.**

Talon sits between compute clients and a durable blob store (S3, GCS, Azure
Blob), caching large immutable objects on local NVMe across a fleet of worker
nodes and serving them through a read-only FUSE filesystem — high sequential
read throughput, horizontal cache scale-out, and POSIX access for unmodified
applications.

[![CI](https://github.com/milvus-io/talon/actions/workflows/ci.yml/badge.svg)](https://github.com/milvus-io/talon/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

## Quick start

Requirements: the Rust toolchain (pinned in `rust-toolchain.toml`; `rustup`
picks it up automatically) and a Linux host for the FUSE client.

```sh
git clone https://github.com/milvus-io/talon.git
cd talon
cargo build --workspace
```

Run a single-node cluster with the development **memory** backend — one
coordinator and one worker:

```sh
# Coordinator: control plane on :7000, admin API + management UI on :8000.
cargo run -p talon-coordinator -- \
  --listen 127.0.0.1:7000 --admin-listen 127.0.0.1:8000 \
  --cluster-id demo --node-id coord-0

# Worker (in another shell): registers with the coordinator above.
cargo run -p talon-worker -- \
  --listen 127.0.0.1:7001 --admin-listen 127.0.0.1:8001 \
  --coordinator 127.0.0.1:7000 --cluster-id demo --node-id worker-0
```

Check the cluster is healthy:

```sh
curl -s http://127.0.0.1:8000/readyz           # {"ready":true}
curl -s http://127.0.0.1:8000/api/v1/cluster    # cluster summary JSON
```

### Management console

Every coordinator serves a built-in web console at **http://127.0.0.1:8000/ui**
— no external assets, no separate deploy. It shows live cluster health, traffic
trends, per-worker capacity and hotspots, an active-active coordinator topology
panel, and a searchable fleet table.

![Talon management console — cluster overview](docs/assets/ui/overview.png)

## Documentation

Start with the section that matches what you're doing:

- **Using Talon** — the [Getting started tutorial](docs/tutorials/getting-started.md)
  builds the workspace, runs a cluster, and opens the management console;
  [DESIGN.md](DESIGN.md) explains what each component does.
- **Operating Talon** — [Operator runbook](docs/operations/runbook.md) (HA,
  etcd/Kubernetes backends, configuration, upgrades, alerts) and
  [security hardening](docs/operations/security.md).
- **Understanding Talon** — [DESIGN.md](DESIGN.md) (v1 architecture and the
  decisions behind it) and the [architecture decision records](docs/adr/).
- **Contributing** — [CONTRIBUTING.md](CONTRIBUTING.md) (build, test, submit
  changes) and [BENCHMARKS.md](BENCHMARKS.md) (the microbenchmark harness).

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) to get
started. Run `just` to list common development tasks.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
