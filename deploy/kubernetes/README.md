# Talon HA deployment (Kubernetes)

Production manifests for running Talon coordinators **active-active** — three
stateless replicas behind one Service, with a user-selected shared-state
backend. Pick **exactly one** coordinator Deployment (Kubernetes Lease *or*
external etcd); deploying both is a misconfiguration.

## Files

| File | Purpose |
|------|---------|
| `service.yaml` | ClusterIP Service + PodDisruptionBudget (`minAvailable: 2`). Apply for either backend. |
| `coordinator-kubernetes.yaml` | Coordinator Deployment using the Kubernetes Lease backend (no external creds). |
| `coordinator-etcd.yaml` | Coordinator Deployment using an external etcd backend (Secret-mounted creds/TLS). |
| `worker.yaml` | Worker Deployment (cache nodes) + ServiceAccount. Shared by both coordinator backends. **Block clusters only.** |
| `async-worker.yaml` | Async-worker Deployment (extent cache) + ServiceAccount. **Async clusters only.** |
| `worker-secret.example.yaml` | Template Secret for the worker's object-store (Azure account/SAS) credentials. |
| `rbac.yaml` | Least-privilege namespaced Lease RBAC. **Kubernetes backend only.** |
| `etcd-secret.example.yaml` | Template Secret for etcd endpoints/credentials/TLS and the optional management token. **etcd backend only.** |
| `servicemonitor.yaml` | Prometheus Operator ServiceMonitor (or use the pod scrape annotations). |

## Quick start — Kubernetes Lease backend

```sh
kubectl create namespace talon
kubectl apply -n talon -f rbac.yaml -f service.yaml -f coordinator-kubernetes.yaml
```

The pods authenticate to the API server with their mounted ServiceAccount token
and write one Lease per node. No external datastore is required.

## Workers

Cache nodes register with the coordinator Service and serve object ranges. Apply
`worker.yaml` alongside whichever coordinator you chose, after creating the
object-store Secret (never commit it) — see `worker-secret.example.yaml`:

```sh
kubectl create secret generic talon-worker-backend -n talon \
  --from-literal=azure-account="$ACCOUNT" --from-literal=azure-sas="$SAS"
kubectl apply -n talon -f worker.yaml
```

The workers are horizontally scalable (`kubectl scale deployment/talon-worker
--replicas=N`) and independent of the coordinator's HA backend. The cache is an
`emptyDir`; swap in a PVC for a node-local SSD in production.

## Cluster types: one ring per cluster

A cluster runs **one** placement ring and admits **one** worker role
([ADR 0006](../../docs/adr/0006-one-ring-per-cluster.md)); a worker of the other
kind is refused at registration. Both coordinator manifests state this
explicitly as `TALON_COORDINATOR_CLUSTER_TYPE=block`.

| Cluster type | Worker manifest | Caches |
|---|---|---|
| `block` | `worker.yaml` | fixed 256 MiB blocks; also serves writes and listings |
| `async` | `async-worker.yaml` | the exact byte ranges asked for; reads and stats only |

Applying `async-worker.yaml` next to a `block` coordinator produces pods that
come up healthy and are turned away at every heartbeat, which looks like a
cache that never warms. Serving both means **two namespaces**, each with its
own coordinator and Service:

```sh
kubectl create namespace talon-async
kubectl apply -n talon-async -f rbac.yaml -f service.yaml \
  -f coordinator-kubernetes.yaml -f async-worker.yaml
kubectl set env -n talon-async deployment/talon-coordinator \
  TALON_COORDINATOR_CLUSTER_TYPE=async
```

The async worker reads the same object-store Secret shape under
`TALON_ASYNC_WORKER_*` names, so `worker-secret.example.yaml` covers it too.

## Quick start — external etcd backend

```sh
kubectl create namespace talon
# Create the real Secret (never commit it) — see etcd-secret.example.yaml:
kubectl create secret generic talon-etcd -n talon \
  --from-literal=endpoints=https://etcd-0:2379,https://etcd-1:2379 \
  --from-literal=username=talon --from-literal=password="$PW" \
  --from-file=ca.crt --from-file=client.crt --from-file=client.key
kubectl apply -n talon -f service.yaml -f coordinator-etcd.yaml
```

## HA properties

- **Replicas & quorum**: 3 replicas; `RollingUpdate` with `maxUnavailable: 0`,
  `maxSurge: 1`; a PDB keeping `minAvailable: 2`; topology spread + pod
  anti-affinity across `kubernetes.io/hostname`.
- **Probes**: startup (slow cold starts), liveness (process up), readiness
  (shared-state reachable — a backend outage pulls the pod from the Service and
  fails closed without killing it).
- **Graceful termination**: `terminationGracePeriodSeconds: 30`; SIGINT triggers
  the coordinator's lease release + drain so peers see it leave promptly.
- **Security**: `runAsNonRoot`, seccomp `RuntimeDefault`; management auth token
  and etcd credentials come from Secrets and are never logged.

## Backend selection

Each Deployment pins `TALON_COORDINATOR_STATE_BACKEND` (`kubernetes` or `etcd`)
and `TALON_COORDINATOR_HA_ENABLED=true`. The memory backend fails validation
when HA is requested, so it can never be used for a multi-replica deployment.

## Metrics

Both Deployments carry `prometheus.io/scrape` pod annotations (port 8000,
`/metrics`). With the Prometheus Operator, apply `servicemonitor.yaml` instead.
The recording rules, alerts, and Grafana dashboard live in
`../observability/`.

## CI validation

The `talon-observability` crate embeds these manifests and validates them in the
standard `cargo test` job (no cluster needed): every document parses, both
coordinator Deployments run ≥3 replicas with all three probes and a quorum-safe
rollout, exactly one backend is selected per Deployment, the RBAC is a
namespaced Lease-only Role (no ClusterRole, no secrets), etcd credentials are
`secretKeyRef`s (never inline), and the example Secret contains only
placeholders. Both worker Deployments are validated too — block and async
alike: each runs non-root with all three probes, dials the coordinator Service,
and takes its object-store credentials from a Secret, never inline. Two further
checks cover the split itself: the coordinator manifests must name their
cluster type rather than lean on the default, and the two worker Deployments
must carry distinct names so applying both into one namespace adds a second
Deployment (which the coordinator then refuses) instead of silently
overwriting the first.
