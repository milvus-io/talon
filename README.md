# Talon

**An object-store cache whose metadata does not grow with your data — so there
is no scaling ceiling and no single point of failure.**

Most caching filesystems put a metadata database in front of your object store,
with a row per file. That database decides how many files you can have, and it
decides what happens when it goes down.

Talon's rule is the opposite: **if a fact can be rebuilt by listing the object
store, it is not stored anywhere.** The namespace, file sizes, mtimes, and
directory structure are all derived from the object store's own key listing.
An ordinary file costs **zero** metadata records. Three things follow:

- **S3 sets your scale limit, not us.** No per-object rows to shard, no inode
  ceiling, no rebalance when the namespace grows.
- **There is little state to protect.** Coordinators are stateless and run
  active-active; restart one and there is nothing to recover. Lose a worker and
  you lose cache, not data — the next read is a miss, not an outage.
- **A plain S3 client can read anything Talon writes.** No proprietary on-disk
  format, no lock-in, no migration to get back out.

Advanced features that genuinely cannot derive their state — hard links, POSIX
locking, write-back — are moving to an **optional**, deliberately sparse
metadata store ([ADR 0003](docs/adr/0003-optional-metadata-store.md), proposed).
Even then the rule holds: a singly-linked, unlocked file still costs zero
records, so the store stays bounded by the features you actually use rather
than by how much data you keep.

Reads never copy: `sendfile` from NVMe to socket, `splice` from socket to NVMe
on fill, driven by io_uring. At 1024 concurrent connections that measures **26%
more throughput and 17× lower p50 latency — on comparable CPU and 19% less
memory** — than the same server on Tokio. Per-core throughput stays flat from 1
to 16 rings, so 8 cores serve **108K rps** and adding cores adds throughput
with nothing to tune
([measurements](docs/explanation/data-plane-runtime.md)).

Read it through a FUSE mount, or use the [Python](docs/clients/python.md) /
[Java](docs/clients/java.md) SDKs when a mount isn't the right fit.

**POSIX behaviour is measured, not asserted.** Against a real kernel mount,
Talon passes **99.2% of pjdfstest** (8,731 of 8,798 assertions across 238 test
files). Reproduce it in one command:

```sh
sudo TALON_REQUIRE_FUSE=1 TALON_RUN_PJDFSTEST=1 \
  cargo test -p talon-fuse --features mount --test mount_e2e \
  mount_pjdfstest_compatibility_suite -- --ignored --nocapture
```

The remaining 0.8% is one gap, not sixty-seven: **hard links to object-backed
files are refused with `EPERM`**. A hard link would need a copy per path, and
copies can diverge with nothing to reconcile them ([#363](https://github.com/milvus-io/talon/issues/363)),
so Talon refuses rather than approximating. The fix is inode indirection
([ADR 0003 §5](docs/adr/0003-optional-metadata-store.md)); until it lands, 51 of
those 67 failures are that refusal and its cascade. **POSIX locking is likewise
refused rather than faked** — `getlk`/`setlk` return `EOPNOTSUPP` instead of
falling back to kernel-local locks that would look cluster-wide and not be.

[![CI](https://github.com/milvus-io/talon/actions/workflows/ci.yml/badge.svg)](https://github.com/milvus-io/talon/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

## Quick start

The fastest way to run Talon is with Docker — one command starts a coordinator,
a worker, and the management UI:

```sh
docker compose up
```

Then open the management console at **http://127.0.0.1:8000/ui**, or check health:

```sh
curl -s http://127.0.0.1:8000/readyz           # {"ready":true}
curl -s http://127.0.0.1:8000/api/v1/cluster    # cluster summary JSON
```

This runs a single-node cluster with the development **memory** backend. For the
active-active HA topology (etcd, three coordinators), use `docker compose
--profile ha up`. Full details: [install with Docker](docs/installation/docker.md).

### Kubernetes

For production, deploy with the Helm chart — active-active coordinators, scalable
workers, and a choice of state backend:

```sh
helm install talon deploy/helm/talon -n talon --create-namespace
```

See [install with Kubernetes](docs/installation/kubernetes.md).

### From source

Building from source (Rust toolchain) is the contributor path — see
[installing from source](docs/installation/source.md).

### Management console

Every coordinator serves a built-in web console at **http://127.0.0.1:8000/ui**
— no external assets, no separate deploy. It shows live cluster health, traffic
trends, per-worker capacity and hotspots, an active-active coordinator topology
panel, and a searchable fleet table.

![Talon management console — cluster overview](docs/assets/ui/overview.png)

## Documentation

Start with the section that matches what you're doing:

- **Installing Talon** — [Docker](docs/installation/docker.md) (fastest),
  [Kubernetes](docs/installation/kubernetes.md) (production), or
  [from source](docs/installation/source.md) (contributors).
- **Deciding if Talon fits** — [Use cases](docs/use-cases/overview.md): model
  training, checkpointing, notebooks and data sharing, cross-cloud reads, and
  analytics — including where it does *not* help.
- **Reading from Talon in code** — [Client SDKs](docs/clients/overview.md):
  a [Python](docs/clients/python.md) wheel and a native-free
  [Java](docs/clients/java.md) jar, for when a FUSE mount is not the right fit.
- **Using Talon** — the [Getting started tutorial](docs/tutorials/getting-started.md)
  builds the workspace, runs a cluster, and opens the management console;
  [DESIGN.md](DESIGN.md) explains what each component does, and
  [Data-plane runtime](docs/explanation/data-plane-runtime.md) covers the
  zero-copy path and the io_uring measurements.
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
