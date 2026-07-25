# Install with Docker

Docker is the fastest way to get a Talon cluster running — one command starts a
coordinator, a worker, and the management UI. No Rust toolchain required.

## Prerequisites

- Docker Engine 24+ with the Compose plugin (`docker compose version`).

## Quick start

From a checkout of the repository:

```sh
docker compose up
```

This starts a single-node cluster with the development **memory** backend:

- **coordinator** — control plane on `:7000`, admin API + management UI on `:8000`
- **worker** — registers with the coordinator (placeholder object-store
  credentials so it starts; reading real objects needs real credentials)

Open the management console at **http://127.0.0.1:8000/ui**, or check health:

```sh
curl -s http://127.0.0.1:8000/readyz            # {"ready":true}
curl -s http://127.0.0.1:8000/api/v1/cluster     # cluster summary JSON
```

Stop with `Ctrl-C`, or `docker compose down` to remove the containers.

## Images

The Compose file pulls the published images from GitHub Container Registry:

- `ghcr.io/milvus-io/talon-coordinator`
- `ghcr.io/milvus-io/talon-worker`
- `ghcr.io/milvus-io/talon-fuse`

Each is tagged with `latest`, the release version (`X.Y.Z`), and the commit SHA.
To build them locally from source instead of pulling:

```sh
docker compose build       # or: docker compose up --build
```

## HA profile (etcd, three coordinators)

To exercise the active-active topology shown in the management console, start the
`ha` profile — an etcd backend with three coordinators and a worker:

```sh
docker compose --profile ha up
```

The UI is published from one coordinator at **http://127.0.0.1:8000/ui**; the
HA topology panel lists all three.

## Real object-store credentials

The default worker ships with placeholder Azure credentials so the control
plane, API, and UI work for exploration. To read real objects, supply genuine
credentials via environment variables (never bake them into an image):

```sh
TALON_WORKER_AZURE_ACCOUNT=<account> \
TALON_WORKER_AZURE_SAS='<sas-token>' \
docker compose up
```

The SAS token is read only from the environment, never from a committed file.
See [security hardening](../operations/security.md) for handling secrets.

## Running a single image

Each image runs standalone. For example, a coordinator with the memory backend:

```sh
docker run --rm -p 8000:8000 -p 7000:7000 \
  ghcr.io/milvus-io/talon-coordinator:latest \
  --cluster-id demo --node-id coord-0
```

The FUSE client image mounts through the kernel and therefore needs `/dev/fuse`
and the `SYS_ADMIN` capability:

```sh
docker run --rm --device /dev/fuse --cap-add SYS_ADMIN \
  ghcr.io/milvus-io/talon-fuse:latest \
  --mountpoint /mnt/talon --coordinator coordinator:7000
```

## Next steps

- **Production on Kubernetes** — [install with Kubernetes](./kubernetes.md).
- **Understand the cluster** — the [getting started tutorial](../tutorials/getting-started.md)
  walks through the API, UI, and FUSE client in detail.
- **Simulate object-store latency** — the [latency lab](../testing/latency-lab.md)
  runs a local emulator behind a latency proxy to see the cache mask backend
  latency.
- **Build from source** — [installing from source](./source.md) (contributors).
