# Talon operator runbook

Operating the Talon management plane: coordinators running active-active behind
one Service, with a strongly consistent shared-state backend (Kubernetes or
etcd). This document covers deployment, configuration, upgrades, and
alert-driven troubleshooting.

See also: [security.md](security.md) (auth/TLS/hardening),
[`deploy/kubernetes/`](../../deploy/kubernetes/) (manifests), and
[`deploy/observability/`](../../deploy/observability/) (Prometheus rules +
Grafana).

---

## 1. Quick starts

### Development (memory backend)

Single process, no shared state. **Not for HA** — the memory backend fails
validation when `ha_enabled` or `coordinator_replicas > 1`.

```sh
cargo run -p talon-coordinator -- --admin-listen 127.0.0.1:8000
# API:  http://127.0.0.1:8000/api/v1/cluster
# UI:   http://127.0.0.1:8000/ui
```

### Kubernetes HA (Lease backend)

Three coordinators using Kubernetes `Lease` objects as shared state; no external
datastore.

```sh
kubectl create namespace talon
kubectl apply -n talon \
  -f deploy/kubernetes/rbac.yaml \
  -f deploy/kubernetes/service.yaml \
  -f deploy/kubernetes/coordinator-kubernetes.yaml
```

### External etcd HA

Three coordinators backed by an existing etcd cluster. Create the credentials
Secret out-of-band (see `deploy/kubernetes/etcd-secret.example.yaml`), then:

```sh
kubectl apply -n talon \
  -f deploy/kubernetes/service.yaml \
  -f deploy/kubernetes/coordinator-etcd.yaml
```

---

## 2. Backend decision table

| Aspect | Memory | Kubernetes Lease | External etcd |
|--------|--------|------------------|---------------|
| Use case | dev/test only | Kubernetes-native clusters | non-K8s or shared etcd fleets |
| Consistency | single process | linearizable (API server) | linearizable (etcd) |
| HA supported | no | yes | yes |
| Operational owner | — | your Kubernetes control plane | your etcd operators |
| Credentials | none | pod ServiceAccount token | Secret (user/pass and/or mTLS) |
| Liveness authority | in-process | Lease `renewTime`+TTL | etcd lease TTL |
| Failure mode | process death = total loss | API-server outage → coordinators fail closed | etcd outage → coordinators fail closed |
| Migration | → K8s/etcd by redeploying with a new backend | ↔ etcd requires a drain + redeploy (records are rebuildable) | ↔ K8s likewise |

Records in the shared store are **ephemeral and rebuildable** from live process
heartbeats, so switching backends is a redeploy, not a data migration: drain the
old deployment, apply the new backend's Deployment, and workers re-register
within one heartbeat interval.

---

## 3. Configuration reference

All values are settable by CLI flag, `TALON_COORDINATOR_*` env var, or TOML
file (precedence: flag > env > file > default). Backend-specific blocks
(`[etcd]`, `[kubernetes]`) come from the TOML file or the env vars listed under
"Backend-specific configuration" below, not from CLI flags.

| Setting | Env var | Default | Units / notes |
|---------|---------|---------|---------------|
| Control listen | `TALON_COORDINATOR_LISTEN` | `127.0.0.1:7000` | worker/client control plane |
| Admin listen | `TALON_COORDINATOR_ADMIN_LISTEN` | `127.0.0.1:8000` | metrics/health/API/UI |
| Cluster id | `TALON_COORDINATOR_CLUSTER_ID` | `default` | logical cluster |
| Node id | `TALON_COORDINATOR_NODE_ID` | control address | stable per replica |
| Backend | `TALON_COORDINATOR_STATE_BACKEND` | `memory` | `memory` \| `etcd` \| `kubernetes` |
| HA enabled | `TALON_COORDINATOR_HA_ENABLED` | `false` | rejects memory backend when true |
| Replicas | `TALON_COORDINATOR_REPLICAS` | `1` | expected coordinator count |
| Heartbeat interval | `TALON_COORDINATOR_HEARTBEAT_INTERVAL_MS` | `5000` | ms |
| Unhealthy after | `TALON_COORDINATOR_UNHEALTHY_AFTER_MS` | `15000` | ms; must be > heartbeat |
| Lease TTL | `TALON_COORDINATOR_LEASE_TTL_MS` | `30000` | ms; must be > unhealthy_after |
| Request timeout | `TALON_COORDINATOR_REQUEST_TIMEOUT_MS` | `3000` | ms; per backend op |
| Auth token | `TALON_COORDINATOR_AUTH_TOKEN` | unset | ≥16 chars enables API/UI auth |
| Trust proxy | `TALON_COORDINATOR_TRUST_FORWARDED` | `false` | honor `X-Forwarded-For` |

**Validation** requires `heartbeat_interval < unhealthy_after < lease_ttl` and a
non-zero request timeout; the memory backend is rejected under HA. Secrets
(auth token, etcd password/keys) are never logged, returned by the API, or
exported in metrics.

### Backend-specific configuration

Selecting `etcd` or `kubernetes` requires a matching configuration block. The
block is provided as a TOML table under `--config`, or, for the fields below, by
environment variable (env wins over the file). Binaries must be built with the
matching feature (`--features etcd` / `--features kubernetes`); selecting a
backend whose feature is absent fails fast at startup with an actionable error.

**etcd** — TOML `[etcd]` table or env:

| Setting | Env var | Notes |
|---------|---------|-------|
| Endpoints | `TALON_COORDINATOR_ETCD_ENDPOINTS` | comma-separated `host:port` list |
| Username | `TALON_COORDINATOR_ETCD_USERNAME` | optional; requires password |
| Password | `TALON_COORDINATOR_ETCD_PASSWORD` | optional; keep in a Secret, not the file |
| CA cert path | `TALON_COORDINATOR_ETCD_CA_CERT_PATH` | PEM; enables TLS |
| Client cert path | `TALON_COORDINATOR_ETCD_CLIENT_CERT_PATH` | PEM; mutual TLS |
| Client key path | `TALON_COORDINATOR_ETCD_CLIENT_KEY_PATH` | PEM; mutual TLS |
| Prefix | (TOML `prefix` only) | keyspace prefix; default `/talon` |

```toml
state_backend = "etcd"
ha_enabled = true
coordinator_replicas = 3
[etcd]
endpoints = ["https://etcd-0:2379", "https://etcd-1:2379"]
prefix = "/talon"
[etcd.tls]
ca_cert_path = "/etc/talon/etcd/ca.crt"
client_cert_path = "/etc/talon/etcd/client.crt"
client_key_path = "/etc/talon/etcd/client.key"
```

**Kubernetes** — TOML `[kubernetes]` table or env:

| Setting | Env var | Notes |
|---------|---------|-------|
| Namespace | `TALON_COORDINATOR_K8S_NAMESPACE` | namespace holding Talon Lease objects |
| Cluster id | (TOML `cluster_id` only) | defaults to the coordinator `cluster_id` |
| Context | (TOML `context` only) | kubeconfig context; unset uses in-cluster config |

```toml
state_backend = "kubernetes"
cluster_id = "talon"
ha_enabled = true
coordinator_replicas = 3
[kubernetes]
namespace = "talon"
```

The shipped Deployments in `deploy/kubernetes/` set these env vars from a
Secret; see §1.

### Derived deadlines

- **Worker/coordinator removal**: a crashed node's record disappears at most
  `lease_ttl` (default 30s) after its last accepted heartbeat.
- **Unhealthy marking**: a node shows `unhealthy` after `unhealthy_after`
  (default 15s) of silence, before removal.
- **Failover interruption bound**: a client's request that hits a failing
  coordinator retries another via the load-balanced Service; placement stays
  correct because every coordinator derives the same deterministic version from
  the same membership.

---

## 4. Upgrades, rollback, and protocol compatibility

- **Rolling upgrade**: the Deployments use `maxUnavailable: 0`, `maxSurge: 1`,
  and a PDB (`minAvailable: 2`), so quorum is preserved. SIGINT triggers each
  coordinator's graceful lease release, so peers see it leave promptly instead
  of waiting out the TTL.
- **Placement version compatibility**: the placement version is an opaque
  equality token (a content hash of the healthy worker set). During a mixed
  version window a client sees at worst one extra placement refresh, never a
  stale pin — the transition is wire-compatible (ADR 0001 §7).
- **Node status schema**: the heartbeat carries a `schema_version`; the store
  rejects records from an unsupported schema. Roll forward one minor version at
  a time.
- **Rollback**: redeploy the prior image. Because shared state is rebuildable,
  no state restore is required; records from the newer version expire and are
  replaced by the rolled-back processes' heartbeats within one lease TTL.

---

## 5. Alert runbooks

Each section matches a `runbook_url` anchor from
`deploy/observability/prometheus/talon.alerts.yml`.

### coordinator-quorum-lost

**Alert:** `TalonCoordinatorQuorumLost` (critical) — zero coordinators report
ready for >1m. The control plane is down cluster-wide.

1. Check pods: `kubectl get pods -n talon -l app.kubernetes.io/component=coordinator`.
2. If all pods are `CrashLoopBackOff` or `Pending`, inspect
   `kubectl describe`/`logs` — common causes are an unreachable state backend
   (see *state-store-errors*) or bad config (validation failure at startup).
3. If pods are `Running` but not ready, the backend is unreachable: readiness
   reflects shared-state health and the coordinators are failing closed. Fix the
   backend, do not delete pods.
4. Recovery: readiness returns within one `request_timeout` of backend recovery.

### coordinator-scrape-missing

**Alert:** `TalonCoordinatorScrapeMissing` (warning) — Prometheus cannot scrape a
coordinator for >2m (`up == 0`). This is scrape absence, **not** stale data.

1. Confirm the pod is alive: `kubectl get pod -n talon <name>`.
2. Check the admin port and NetworkPolicy between Prometheus and port 8000.
3. If the pod is gone, the Deployment will reschedule it; verify capacity.

### worker-scrape-missing

**Alert:** `TalonWorkerScrapeMissing` (warning) — a worker endpoint is
unscrapeable for >2m. Its cached blocks are unreachable via that endpoint.

1. Check the worker process/pod and its network path to Prometheus.
2. If the worker is truly down, its coordinator record expires at `lease_ttl`
   and placement rebalances automatically.

### state-store-errors

**Alert:** `TalonStateStoreErrors` (critical) — coordinators are failing shared
state-store operations. New authoritative reads fail closed.

1. **etcd**: check etcd health/quorum, TLS/cert expiry, and auth. Verify the
   `talon-etcd` Secret endpoints/credentials.
2. **Kubernetes**: check API-server availability and that the Lease RBAC is
   applied (`kubectl auth can-i --as=system:serviceaccount:talon:talon-coordinator update leases -n talon`).
3. Coordinators recover automatically once the backend is healthy; readiness and
   membership resume on the next successful snapshot.

### cluster-view-stale

**Alert:** `TalonClusterViewStale` (warning) — the freshest snapshot across ready
coordinators is >60s old while scrapes succeed. A watch may be wedged or the
backend severely lagged.

1. Distinguish from *coordinator-scrape-missing*: here the process is scraped
   fine but its data is old.
2. Check backend latency (etcd `wal_fsync`/compaction, API-server latency).
3. If a single coordinator is stale, restarting it forces a fresh relist;
   prefer fixing the backend if all coordinators are stale.

### worker-capacity-pressure

**Alert:** `TalonWorkerCapacityPressure` (warning) — a worker is >90% of
configured cache capacity for >10m. Eviction pressure is high; hit rate may
degrade.

1. Confirm on the fleet/node view (`/ui#/nodes`) or `GET /api/v1/nodes`.
2. Add worker capacity or scale out workers to spread the working set.
3. Sustained pressure across the fleet indicates the working set exceeds total
   cache; plan capacity.

### backend-fetch-errors

**Alert:** `TalonBackendFetchErrors` (warning) — >5% of worker object-store
fetches fail for >5m. This erodes the cache.

1. Check the object store (S3/GCS/Azure) health, throttling, and credentials.
2. Verify ranged-GET support and `Content-Range` validation (mis-configured
   proxies can return wrong bytes).
3. Correlate with a specific backend/bucket via worker logs.

### cache-hit-rate-low

**Alert:** `TalonCacheHitRateLow` (warning) — cluster hit rate <50% for >15m
under real traffic.

1. Expected transiently after a cold start or a fleet rebalance; confirm it is
   sustained.
2. Otherwise investigate capacity/eviction churn (see *worker-capacity-pressure*)
   or a workload whose working set exceeds cache.

### worker-high-error-ratio

**Alert:** `TalonWorkerHighErrorRatio` (warning) — a worker returns errors for
>5% of requests for >5m.

1. Inspect the worker's logs and its node detail (`/api/v1/nodes/{id}`).
2. Correlate with *backend-fetch-errors* (upstream) vs. local faults (disk,
   memory pressure).
3. If a single worker is faulty, drain/restart it; placement rebalances.

---

## 6. Backup and restore

The shared store holds only ephemeral, rebuildable node records — **there is no
Talon state to back up**. The durable source of truth is the object store; the
worker caches are reconstructable. Back up etcd per your etcd operations for its
*own* recovery, but a full Talon management-plane loss recovers by redeploying:
coordinators and workers re-register within one heartbeat interval.

---

## 7. Health, readiness, and metrics endpoints

| Endpoint | Auth | Meaning |
|----------|------|---------|
| `GET /healthz` | public | process liveness (200 unless shutting down) |
| `GET /readyz` | public | shared-state reachable (503 fails closed) |
| `GET /metrics` | public | Prometheus exposition |
| `GET /api/v1/*` | protected | versioned management API (see #82) |
| `/ui` | protected | management console |

"Protected" requires the bearer token when `TALON_COORDINATOR_AUTH_TOKEN` is set;
see [security.md](security.md).
