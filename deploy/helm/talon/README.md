# Talon Helm chart

Deploys the Talon distributed object-store cache: active-active coordinators and
horizontally scalable cache workers. This chart parameterizes the raw manifests
under `deploy/kubernetes/`.

## Install

```sh
helm install talon deploy/helm/talon -n talon --create-namespace
```

The default backend is `kubernetes` (Lease-based HA, no external datastore). The
chart never contains credentials — create the required Secrets out-of-band.

### Cluster types

**One release is one cluster, and a cluster runs one placement ring**
([ADR 0006](../../../docs/adr/0006-one-ring-per-cluster.md)).
`coordinator.clusterType` picks it:

| `coordinator.clusterType` | Worker | Caches | Good for |
|---|---|---|---|
| `block` (default) | `talon-worker` | fixed 256 MiB blocks | full-partition scans, sequential reads, writes, listings |
| `async` | `talon-async-worker` | the exact byte ranges asked for | Parquet/Lance footers, column-chunk projection, point lookups |

The enabled worker must match the type, and the chart fails the render
otherwise — a coordinator refuses to register a worker of the other kind, so
the alternative is a pool of healthy-looking pods that are turned away at every
heartbeat.

Serving both access patterns means **two releases**, each with its own
coordinator address:

```sh
helm install talon       deploy/helm/talon -n talon --create-namespace
helm install talon-async deploy/helm/talon -n talon-async --create-namespace \
  --set coordinator.clusterType=async \
  --set worker.enabled=false --set asyncWorker.enabled=true
```

An async cluster is read-only and cannot list: writes and directory listings —
and therefore FUSE `readdir` — need the block cluster.

### Backends

| `coordinator.backend` | HA | Extra setup |
|-----------------------|----|-------------|
| `memory` | no (single replica) | none — dev/demo only |
| `kubernetes` (default) | yes | none; the chart adds a namespaced Lease Role/RoleBinding |
| `etcd` | yes | create the etcd Secret first (see `deploy/kubernetes/etcd-secret.example.yaml`) |

```sh
# etcd backend
kubectl create secret generic talon-etcd -n talon \
  --from-literal=endpoints=https://etcd-0:2379 \
  --from-literal=username=talon --from-literal=password="$PW" \
  --from-file=ca.crt --from-file=client.crt --from-file=client.key
helm install talon deploy/helm/talon -n talon --set coordinator.backend=etcd
```

### Workers

Workers are enabled by default and need object-store credentials from a Secret:

```sh
kubectl create secret generic talon-worker-backend -n talon \
  --from-literal=azure-account="$ACCOUNT" --from-literal=azure-sas="$SAS"
```

Set `worker.persistence.enabled=true` to back the cache with a PVC (node-local
SSD) instead of an `emptyDir`. Disable workers entirely with
`worker.enabled=false`.

Async workers read the same Secret shape under `TALON_ASYNC_WORKER_*` names, so
one Secret serves either cluster type. Their
`asyncWorker.persistence.enabled=true` matters more than the block worker's:
with an `emptyDir` the NVMe checkpoint has nothing to recover from on restart.

## Key values

| Key | Default | Purpose |
|-----|---------|---------|
| `coordinator.backend` | `kubernetes` | State backend: `memory`/`kubernetes`/`etcd` |
| `coordinator.clusterType` | `block` | Placement ring: `block`/`async` |
| `coordinator.replicas` | `3` | Coordinator replicas (forced to 1 for `memory`) |
| `worker.enabled` | `true` | Deploy cache workers |
| `worker.replicas` | `3` | Worker replicas |
| `worker.topologySpreadWhenUnsatisfiable` | `ScheduleAnyway` | Worker hostname spread policy |
| `worker.blockSizeBytes` | `268435456` | Cache block size in bytes |
| `worker.capacityBytes` | `8589934592` | Per-worker cache capacity |
| `worker.l1CapacityBytes` | `0` | L1 DRAM capacity; zero disables L1 |
| `worker.l1PageSizeBytes` | `262144` | Fixed L1 DRAM page size |
| `asyncWorker.enabled` | `false` | Deploy async workers (requires `clusterType=async`) |
| `asyncWorker.replicas` | `3` | Async worker replicas |
| `asyncWorker.capacityBytes` | `8589934592` | Per-worker NVMe capacity |
| `asyncWorker.l1CapacityBytes` | `0` | L1 DRAM capacity; zero disables L1 |
| `asyncWorker.checkpointIntervalBytes` | `268435456` | Bytes between NVMe checkpoints; zero disables warm restart |
| `image.registry` / `image.tag` | `ghcr.io/milvus-io` / chart appVersion | Image source |
| `serviceMonitor.enabled` | `false` | Prometheus Operator ServiceMonitor |

See `values.yaml` for the full list.

## Validation

`helm lint` and `helm template` (all three backends) run in CI. The chart
rejects invalid configurations at render time — an unknown backend, the
`memory` backend with more than one replica, an unknown cluster type, or a
worker whose kind contradicts `coordinator.clusterType`.
