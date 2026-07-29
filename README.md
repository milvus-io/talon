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

<!-- TODO(posix): POSIX compatibility is deliberately NOT claimed here yet.
     Measured 2026-07-29 against a real kernel FUSE mount (pjdfstest, 238
     files / 8798 assertions): roughly 2,540 assertions fail, i.e. ~71% pass.
     Failures are dominated by two chmod/permission files (1284/2353 and
     1106/2099); the rest are unlink/11 (90/270), rmdir/11 (15/47),
     truncate/05, utimensat/07, rename/21+23, symlink/05+06.
     The chmod concentration suggests a single root cause (mount options /
     mode propagation) rather than 2,390 independent bugs — worth diagnosing
     before publishing any number, since one fix may move the rate a lot.
     Reproduce:
       sudo TALON_REQUIRE_FUSE=1 TALON_RUN_PJDFSTEST=1 \
         cargo test -p talon-fuse --features mount --test mount_e2e \
         mount_pjdfstest_compatibility_suite -- --ignored --nocapture
     When publishing: state the real rate and name the gaps. Hard links are
     known-broken (ADR 0003 / #363 / #359); POSIX locking is unimplemented. -->

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
