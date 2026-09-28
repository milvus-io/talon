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

Workers use a StatefulSet with retained PVCs by default. Set
`worker.persistence.storageClass` for your local/persistent storage class.
Disabling persistence is for disposable development. Disable workers entirely
with `worker.enabled=false`.

Chart 0.2 changes workload kind and requires explicit migration from the old
Deployment and ephemeral claims. Follow the [rolling-upgrade runbook](../../../docs/how-to/rolling-upgrade.md)
before upgrading an existing release or activating retained membership.
Use `worker.rollingUpdate.partition` to advance one canary ordinal at a time.

## Key values

| Key | Default | Purpose |
|-----|---------|---------|
| `coordinator.backend` | `kubernetes` | State backend: `memory`/`kubernetes`/`etcd` |
| `coordinator.replicas` | `3` | Coordinator replicas (forced to 1 for `memory`) |
| `worker.enabled` | `true` | Deploy cache workers |
| `worker.replicas` | `3` | Worker replicas |
| `worker.topologySpreadWhenUnsatisfiable` | `ScheduleAnyway` | Worker hostname spread policy |
| `worker.blockSizeBytes` | `268435456` | Cache block size in bytes |
| `worker.capacityBytes` | `8589934592` | Per-worker cache capacity |
| `worker.l1CapacityBytes` | `0` | L1 DRAM capacity; zero disables L1 |
| `worker.l1PageSizeBytes` | `262144` | Fixed L1 DRAM page size |
| `image.registry` / `image.tag` | `ghcr.io/milvus-io` / chart appVersion | Image source |
| `serviceMonitor.enabled` | `false` | Prometheus Operator ServiceMonitor |

See `values.yaml` for the full list.

## Validation

`helm lint` and `helm template` (all three backends) run in CI. The chart
rejects invalid configurations at render time — an unknown backend, or the
`memory` backend with more than one replica.
