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
placeholders.
