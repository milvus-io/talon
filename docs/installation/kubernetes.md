# Install with Kubernetes

For production, deploy Talon on Kubernetes with the Helm chart. It runs
active-active coordinators behind a Service, horizontally scalable cache
workers, and your choice of shared-state backend.

## Prerequisites

- A Kubernetes cluster and `kubectl` context.
- Helm 3 (`helm version`).

## Quick start (Helm)

```sh
helm install talon deploy/helm/talon -n talon --create-namespace
```

The default backend is **kubernetes** (a Lease-based HA backend needing no
external datastore; the chart adds a namespaced Lease Role/RoleBinding).

Reach the management API/UI by port-forwarding the coordinator Service:

```sh
kubectl -n talon port-forward svc/talon-coordinator 8000:8000
# then open http://127.0.0.1:8000/ui
```

## Choosing a state backend

| `coordinator.backend` | HA | Extra setup |
|-----------------------|----|-------------|
| `memory` | no (single replica) | none — dev/demo only |
| `kubernetes` (default) | yes | none; chart adds Lease RBAC |
| `etcd` | yes | create the etcd Secret first |

For the external etcd backend, create the credential Secret before installing
(never commit it):

```sh
kubectl create secret generic talon-etcd -n talon \
  --from-literal=endpoints=https://etcd-0:2379 \
  --from-literal=username=talon --from-literal=password="$PW" \
  --from-file=ca.crt --from-file=client.crt --from-file=client.key

helm install talon deploy/helm/talon -n talon --create-namespace \
  --set coordinator.backend=etcd
```

## Workers

Workers are enabled by default and need object-store credentials from a Secret:

```sh
kubectl create secret generic talon-worker-backend -n talon \
  --from-literal=azure-account="$ACCOUNT" --from-literal=azure-sas="$SAS"
```

Scale workers with `--set worker.replicas=N`, and back their cache with a
node-local PVC instead of an `emptyDir` via `--set worker.persistence.enabled=true`.
Disable workers with `--set worker.enabled=false`.

See the [chart README](https://github.com/milvus-io/talon/tree/main/deploy/helm/talon)
for the full list of values.

## Raw manifests (without Helm)

If you don't use Helm, apply the raw manifests under `deploy/kubernetes/`
directly. Pick exactly one coordinator backend:

```sh
kubectl create namespace talon
# Kubernetes Lease backend:
kubectl apply -n talon -f deploy/kubernetes/rbac.yaml \
  -f deploy/kubernetes/service.yaml \
  -f deploy/kubernetes/coordinator-kubernetes.yaml
# Workers (after creating the talon-worker-backend Secret):
kubectl apply -n talon -f deploy/kubernetes/worker.yaml
```

The [`deploy/kubernetes/README.md`](https://github.com/milvus-io/talon/tree/main/deploy/kubernetes)
documents the etcd variant, RBAC, and HA properties.

## Next steps

- **Operate it** — HA, backends, upgrades, and alerts in the
  [operator runbook](../operations/runbook.md).
- **Secure it** — authentication, TLS, and secret handling in
  [security hardening](../operations/security.md).
