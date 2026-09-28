# Rolling upgrade with retained Worker membership

Persistent membership retains logical ownership while a Worker is offline. Reads to that
owner return `Unavailable` or `Timeout`; the application decides whether to use
origin fallback. Version conflicts, rate limits, protocol errors and origin
errors are not availability failures. Failed retained reads are not resent by
the SDK. All binaries and clients must implement the same schema-6 contract;
there is no membership-mode switch or compatibility fallback.

## Prepare storage and migrate existing deployments

Use a durable etcd or Kubernetes state backend. `memory` is for development and
cannot retain members across Coordinator process loss. Keep at least two ready
Coordinators, sharing the same cluster ID, backend namespace and credentials.
Back up the backend registry and inventory every Worker ID, zone and cache path,
including offline Workers.

Chart 0.2 changes Workers from a Deployment to a StatefulSet with one retained
PVC per ordinal. This is an explicit workload migration, not a safe in-place
`helm upgrade` from 0.1: Kubernetes treats the two kinds as separate workloads,
and Helm removal of the old workload can delete its generic ephemeral PVCs.
Before upgrading the chart, stop/fence the old Worker for each directory, protect
its volume from deletion, and explicitly bind the retained PV to the new
StatefulSet claim (`cache-<release>-worker-<ordinal>`). Verify the actual cache
bytes and ownership at the new mount before starting the replacement. Rehearse
the storage-specific PV/PVC rebinding procedure; never rely on Pod names to move
cache contents or let both workload kinds open the same disk.

For local disks, adapt `deploy/kubernetes/worker-local-pv.example.yaml`: use a
Retain reclaim policy, WaitForFirstConsumer, and PV node affinity. Supply one
volume per slot, with capacity at least the configured cache capacity. Raw
Worker manifests expect the `talon-local` storage class. Helm uses the default
persistent storage class unless `worker.persistence.storageClass` is supplied.
`worker.persistence.enabled=false` is only for disposable development.

A nonempty legacy directory requires its **exact old Worker ID** on the first
new startup (`--node-id` or `TALON_WORKER_NODE_ID`). Supply this per directory;
if old IDs differed, migrate each ordinal with its own configuration. The
process imports that ID into the synced `worker_identity` file while holding
`.worker.lock`. For an empty directory it generates a new ID. Later explicit
IDs must match; cluster ID, block size and page size must also match. Never
regenerate/delete the identity file to bypass a mismatch. Certificates on the
mTLS control channel must name this persisted Worker ID.

Keep PVCs on StatefulSet deletion and scale-down. Losing a disk is a separate
member replacement: stop/fence its process and explicitly retire the member;
a new empty directory receives a new ID and changes topology.

## Prepare a compatible deployment

1. Verify that all Coordinators, Workers and SDKs implement the same control
   schema and persistent membership contract. The superseded lease-only protocol
   is incompatible; it requires a coordinated replacement before serving traffic.
2. Keep at least two Coordinators with `maxUnavailable: 0`, `maxSurge: 1`,
   readiness stabilization and spare capacity. Their readiness gate loads shared
   membership and binds the listeners before serving.
3. Inventory `GET /api/v1/worker-membership` and `GET /api/v1/worker-discovery`
   through authenticated administration connections. Check every registered ID
   and zone against the disk inventory and resolve instance conflicts.
4. Membership edits use `PUT /api/v1/worker-membership` with the unchanged opaque
   `expected_registry_revision` and the complete `registry` object. Preserve all
   existing member records. HTTP 204 means success; HTTP 409 requires reloading
   and reviewing the registry before constructing a new update. This API imports,
   retires or explicitly reactivates members; it does not activate a mode.

## Replace one Worker at a time

For three Workers, start a canary with `worker.rollingUpdate.partition=2` and
the new image. Verify ordinal 2, then lower the partition to 1 and finally 0.
StatefulSet OrderedReady, `minReadySeconds: 15`, and retained per-ordinal PVCs
provide sequencing. The headless Worker Service governs the StatefulSet;
clients still use discovered individual Worker addresses.

SIGTERM first closes request admission (including persistent sockets), removes
readiness and withdraws this instance. Accepted requests and background disk
mutations drain, access metadata is checkpointed, and the directory lock is
released on exit. The runtime budget is 20 seconds; manifests allow 45 seconds
for termination. A deadline overrun terminates the whole process while the lock
is still held, followed by normal crash recovery. Unacknowledged business writes
may have uncertain outcomes and must not be blindly retried.

At each ordinal check:

- `worker_id` and `topology_token` stay unchanged, while `instance_id` and
  `state_token` change. Offline/conflicting owners remain in the placement table.
- Readiness recovers only after the directory lock, disk recovery, listener and
  sole-instance registration succeed. Persistent clients recover on subsequent
  requests; discovery freshness is at most 500 ms, with refresh attempts limited
  to one per client per 100 ms.
- Previously cached, valid bytes remain hits. Compare origin GET counts before
  and after restart; separate TTI expiry, eviction and corruption from identity
  changes. Check unavailable/timeout rates and caller fallback independently.

Use `/api/v1/worker-discovery` for retained instance state; `/api/v1/nodes` remains
the legacy leased-node view and is not the retained Worker inventory. Metrics
`talon_coordinator_worker_members` and
`talon_coordinator_worker_member_states{state="serving|offline|conflict"}` reflect
successful discovery observations. On backend failure they retain last-good
counts; discovery fails closed. Worker drain logs report elapsed time and forced
termination. Inspect recovered page/block metrics and backend errors too.

## Retire, reactivate and roll back

Stop/fence a Worker and wait for withdrawal or lease expiry. Use the same
revision-checked PUT to set its `retired` field to `true`; retain the record as a
tombstone. Heartbeats cannot reactivate it. Explicitly clearing `retired` is an
operator action and changes topology again. Scaling down the StatefulSet alone
only makes members offline and preserves their ownership.

Rollback requires a binary that supports the same persistent membership and
control protocol. Keep identity files, registry records, PVCs and backend state.
Downgrading to the superseded lease-only implementation is unsupported; there is
no mode toggle that makes its ownership behavior compatible.

## Reproduce process acceptance

Build the Worker, Coordinator with `etcd`, and Rust SDK example
`rolling_upgrade_probe`. Run `scripts/rolling_upgrade_e2e.py --etcd <binary>
--work-dir <new-artifact-directory> --page-size 4096 --backend-contract` inside
the prescribed development environment. Repeat with `--page-size 0` for whole
blocks. It starts isolated services, keeps an SDK client alive through restart,
asserts no origin refetch, exercises SIGTERM/SIGKILL, address changes, conflicting
identities, Coordinator replacement, backend outage, retirement and reactivation.
All logs and `report.json` are retained. `--runtime uring` requires real native
io_uring and rejects a Tokio fallback; it is a separate acceptance run. Helm
rendering and this process harness do not claim deployed Kubernetes validation.
