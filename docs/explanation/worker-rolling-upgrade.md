# Talon Rolling Upgrade Design: Dynamic Membership and Stable Placement During Restarts

- Status: Implementation in progress; deploy only after all required layers pass acceptance.
- Date: 2026-09-28.
- Source baseline: `c4796fe0f47afae2bebd48168f87b5a25c0bc428`.
- Audience: Maintainers of the Talon Coordinator, Worker, client SDKs, and deployments.

## 1. Goals and Core Decisions

Talon allows an individual Worker to become temporarily unavailable during an upgrade while retaining its logical identity, block ownership, and local disk cache. The replacement process opens the same directory on the same machine, recovers the cache, and resumes service. When a request encounters an unavailable Worker, Talon returns a recognizable error, leaving the caller to decide whether to access origin storage or use another fallback.

Membership supports dynamic scaling: adding a member or explicitly removing one changes placement. Temporary outages, process restarts, and address changes affect only instance state, preserving logical block ownership. This design decouples membership from liveness; it avoids the term "static topology," which could imply that scaling is prohibited.

Core decisions:

1. Persist `worker_id` in the data directory. An exclusive directory lock determines which process may use and modify the cache.
2. The Coordinator maintains persistent member records separately from expiring process state. Heartbeat expiration does not remove a member.
3. Clients first compute the block owner from logical membership, then resolve that member to a serving instance.
4. Return `Unavailable` when a Worker is unavailable. Do not hide downtime by waiting for recovery, choosing another logical Worker, or automatically accessing origin storage in the SDK.
5. The first version restarts Workers sequentially. It requires neither overlapping old and new processes nor an interprocess handoff RPC.
6. Coordinators continue to use rolling replacement across multiple instances. Member records must survive Coordinator restarts.

A graceful upgrade here guarantees stable placement and cache identity, recognizable errors, and the ability to upgrade one Worker at a time. It does not guarantee that every Talon request succeeds during the upgrade. The integrating application must validate end-to-end availability and object-version correctness during fallback.

## 2. Scope and Assumptions

### 2.1 In Scope

- Worker identity initialization, restarts using the same directory, and disk cache recovery.
- Member registration, explicit removal, instance heartbeats, offline state, and client discovery.
- Fast failure on the read path and structured error propagation through the Rust, C, Python, and Java SDKs.
- Coordinator and Worker shutdown draining, plus a reference Kubernetes deployment.
- A single membership contract and acceptance tests before the first deployment.

### 2.2 Out of Scope

- Sharing memory between old and new Workers, passing listening sockets, migrating existing TCP connections, or inheriting the L1 cache.
- Multiple writers serving from one directory, cache migration across machines, or distributed disk locks.
- Adding read replicas, automatic origin fallback, or automatic block ownership migration within Talon.
- Applying this read-error policy to automatic retries of Put/Delete or other operations with potentially uncertain outcomes.
- Changing normal read-through behavior: an online Worker continues to access origin storage on a cache miss according to existing API semantics.

The old and new processes must access the same actual filesystem directory, use compatible cache formats and block/page configurations, and safely complete or fail outstanding requests. Disk cache reuse covers only data that remains valid after recovery and has not been evicted by TTI or capacity policies. Downtime continues to count toward the existing page idle TTI.

## 3. Current Implementation and Gaps

The following facts describe the source baseline, not an implementation of this proposal.

| Path | Existing capability | Required change |
| --- | --- | --- |
| [Worker startup and shutdown](../../crates/talon-worker/src/main.rs) | Holds the root directory lock; scans whole-block and paged caches to rebuild indexes; handles SIGINT/SIGTERM, stops ring accepts, and shuts down page maintenance | Persist directory identity; fully drain foreground and background tasks; advertise availability only after recovery |
| [CacheRootLock](../../crates/talon-worker/src/page_access_store.rs) | Uses nonblocking exclusive `flock` on `.worker.lock`; some background disk operations retain lock ownership | Make lock lifetime cover every cache mutation path |
| [Shared state contract](../../crates/talon-coordinator/src/state_store/mod.rs) | Stores leased node state; snapshots contain unexpired nodes | Add a member registry independent of lease expiration; separate process and member records |
| [Coordinator membership synchronization](../../crates/talon-coordinator/src/observability/state.rs) | Includes only healthy, ready Workers in placement membership | Separate all logical members from serving instances |
| [Maglev](../../crates/talon-core/src/placement.rs) | Builds deterministic placement from Worker IDs; the current membership token also includes addresses | Preserve the placement algorithm; address and liveness updates must not change logical ownership |
| [Client membership cache](../../crates/talon-cache-client/src/membership_cache.rs), [read path](../../crates/talon-cache-client/src/block_reader.rs) | Caches members and addresses; may try candidate Workers and refresh/retry after failure | Return availability errors directly and refresh discovery for subsequent requests |
| [Error protocol](../../crates/talon-transport/src/data.rs), [read error classification](../../crates/talon-cache-client/src/range_stream.rs) | Already provides `Unavailable`, `Timeout`, and other categories, plus `fallback_eligible()` | Carry the same classification through public read APIs and language bindings without parsing error strings |
| [Coordinator main loop](../../crates/talon-coordinator/src/main.rs) | The SIGINT path marks shutdown and removes its own lease | Handle SIGTERM and wait for in-flight control requests |
| [Worker Helm template](../../deploy/helm/talon/templates/worker.yaml) | Deployment, probes, and a 30-second termination grace period; Pod name as ID and Pod IP as address | Use reusable local volumes, derive identity from the directory, and support sequential restarts |

The current Helm `persistence.enabled` setting uses a generic ephemeral PVC. This is different from a StatefulSet's persistent PVC binding and does not guarantee cache reuse across Pod replacement.

## 4. Members, Processes, and Local Directories

### 4.1 Data Directory Identity

Each cache root persists a `worker_identity` file containing at least a format version, `cluster_id`, and a randomly generated `worker_id`. This identity is not a lease, is not deleted when the process exits, and does not contain a transient Pod IP.

Initialization proceeds in this order: acquire the exclusive directory lock, read or create the identity file, validate cluster ownership and configuration, then recover the cache. Persist a new identity using a temporary file, file synchronization, atomic publication, and parent-directory synchronization. Registration with the Coordinator must wait until persistence succeeds.

A corrupt identity file, cluster mismatch, or conflict between the configured ID and the file must fail startup with a clear diagnostic. Never silently generate a replacement ID. During initial migration, an existing deployment may explicitly import its old `node_id` while holding the lock to preserve placement. The file becomes authoritative after import. Generate a new ID by default only for a new member with an empty directory.

The first version applies to the first cache root currently used by the implementation. Future support for multiple independent disks must explicitly define the relationship between a disk set and Worker identity, rather than independently generating different identities for the same Worker in each configured directory.

### 4.2 Local Exclusive Lock

Reuse `.worker.lock`. The lock file persists, but the kernel maintains lock ownership. A file containing `writable=true` must not replace a filesystem lock. Do not delete or replace the lock file while a process is running.

Only the lock holder may recover indexes, accept data requests, fill the cache from origin storage, evict data, run GC, or write checkpoints. The lock must remain held until the last asynchronous or blocking task that could access or modify the cache finishes. An early return from the main task must not release it prematurely.

A process that cannot acquire the lock must neither scan and install indexes from a changing cache nor serve reads. The first version retains the simple startup-failure behavior and lets deployment tooling retry; it does not require a fully initialized standby Worker waiting for the lock.

The kernel releases the lock on process exit, including abnormal exit. Restart still requires crash recovery: acquiring the lock does not prove that disk state is complete. Copying a directory to another machine and concurrently serving under the same ID is unsupported because a local lock does not provide exclusion across machines.

### 4.3 Two Record Types in the Coordinator

The fields below define the design contract. Implementation PRs will assign concrete type names and wire identifiers.

| Record | Main fields | Lifetime |
| --- | --- | --- |
| `WorkerMember` | Cluster, Worker ID, stable placement attributes such as zone, and member state | Persistent; independent of heartbeat TTL |
| `WorkerInstance` | Worker ID, instance ID, address, `writable`, local recovery readiness, heartbeat sequence, and expiration information | Leased; keyed by `(worker_id, instance_id)` |

`instance_id` may reuse the process-incarnation concept and changes on every startup. It distinguishes records and supports conditional updates; UUID ordering or machine timestamps must not decide cache ownership. Holding the lock determines local authority. Registration uses the existing cluster authentication boundary, and a reported claim cannot replace actual lock checks in the Worker.

Multiple instance records for the same Worker ID may briefly coexist, but the logical member appears only once. The Coordinator selects instances whose records are unexpired and report both lock ownership and local recovery readiness:

- Exactly one candidate: publish its address as available for service.
- No candidates: publish unavailable status and retain the member.
- Multiple candidates: publish an identity conflict and unavailable status. Do not arbitrarily select the most recently received record.

A delayed heartbeat or shutdown notification from an old instance affects only its own record; it cannot overwrite or delete the new instance. Requests may fail temporarily while an old record remains unexpired. Do not add an interprocess handoff protocol solely to eliminate this window. The new instance becomes externally ready only after the Coordinator confirms it as the sole serving instance. Local recovery readiness reported in heartbeats is independent of external readiness, avoiding circular dependency.

## 5. Dynamic Membership and Topology Updates

### 5.1 Persistent Registration and Temporary Outages

Member registration is idempotent. The first registration of a new directory creates a member; a restart using an existing directory reuses that member and updates only instance records. Only initial registration and explicit administrative changes to member attributes or membership change placement inputs.

Heartbeat expiration, readiness becoming false, connection failure, and Pod IP changes do not remove members. Even an extended outage does not automatically change topology. Operators use alerts to decide whether to repair or permanently remove a member, preventing a slow upgrade recovery from becoming implicit scale-down.

Permanent removal requires a separate administrative operation, not lease deletion. Confirm that the Worker has stopped serving before removal. Persist a retired marker so that delayed heartbeats or automatic process restarts cannot rejoin the member. Re-enabling the same ID requires explicitly clearing its retired state. Audit member additions, removals, and changes to placement attributes such as zone.

### 5.2 State Backends

- etcd: store member records in a separate key space without leases; retain leases for instance state.
- Kubernetes: use independent persistent resources for members, such as one ConfigMap per member, and separate Leases for instances. Member resources must not have a Pod owner reference; deleting a Pod must not cascade to member deletion. Add the required RBAC permissions.
- Memory: support development and contract tests only; do not claim membership survives Coordinator restarts.

All HA Coordinators share the same member registry. The backend must provide a member list from a consistent resource version. Join instance state by ID; uncertainty may yield unknown/unavailable status but must not remove placement inputs. Treat Kubernetes resourceVersion and the existing `StoreRevision` as opaque values. Do not parse them as integers or present versions from two resource lists as one atomic transaction version.

### 5.3 Client Placement Table

Keep the existing Maglev ranking algorithm and change its input to registered logical members that are not retired.

```text
Object version + block -> Maglev(logical members) -> worker_id
                                                         |
                                    Current instance state -> endpoint or Unavailable
```

The block routing cache stores logical Worker IDs. Resolve addresses through a separate instance view. Taking a member offline or updating its address must not change its Maglev ownership or filter it out of the placement table.

With zone affinity enabled, derive the same-zone subset from logical membership rather than online instances. If all same-zone Workers are temporarily offline, return unavailable instead of automatically switching to a cross-zone topology; the caller chooses fallback. Only when explicit removal leaves the same-zone membership empty may the existing configuration determine whether to use global membership.

Separate the topology content token from the instance-state content token. The topology token covers logical attributes that affect placement; the state token covers instances, addresses, and availability. Both are equality tokens, not fencing counters. The current membership token includes addresses and cannot directly support a claim that address changes leave the topology version unchanged. Heartbeat renewal updates expiration only and should not invalidate per-block caches or rebuild the Maglev table.

## 6. Client Discovery and Error Contract

### 6.1 Return Failures and Leave Fallback to the Caller

Each block request selects one logical owner and one serving instance. Return immediately when the owner is known to be offline. After a network failure, perform necessary resource cleanup and return without waiting for restart, trying another logical Worker, or refreshing and resending within the same request.

The current client's candidate iteration, refresh-and-retry behavior, and redial after a reused connection fails must not hide failures. There is no separate legacy membership or read policy. Normal connection reuse and concurrency for requests that have not failed remain unchanged.

"Return immediately" means adding no upgrade-specific wait after detecting an error. Unresponsive connections still require the existing connection/request deadlines to detect a timeout. Check caller cancellation and the overall deadline first; do not classify explicit cancellation or local argument errors as Worker unavailability.

### 6.2 Reuse Existing Error Categories

Worker unavailability maps to the existing `DataErrorCode::Unavailable` / `CacheReadError::Unavailable`. Do not introduce a synonymous `WorkerUnavailable` wire enum. Public SDKs must expose stable classification without string parsing. Diagnostic context includes the target Worker ID, instance ID, endpoint, and original cause when known; do not invent target information when it is unavailable.

| Scenario | Returned category | Reroute this request within Talon? |
| --- | --- | --- |
| Known member with no serving instance, recovery or draining in progress, or conflicting instance identities | `Unavailable` | No |
| Connection refused, connection reset, or unexpected EOF before a complete response | `Unavailable` | No |
| Connection/read exceeds the configured network deadline | `Timeout` | No |
| Origin object missing, version mismatch, or permission/argument error | Preserve the existing domain error category | No; do not disguise it as unavailability |
| Corrupt frame, inconsistent length, or incompatible protocol | `Protocol` / existing protocol category | No |
| Origin storage error or tenant rate limiting during normal Worker service | `Origin` / `RateLimited` | Unchanged by this design |

An exited process cannot send an error frame, so the SDK synthesizes the appropriate category. A running process that has stopped admitting requests may return the existing typed `Unavailable` response.

Rust public errors expose a consistent classification API. C appends unavailable/timeout categories to the existing status enum while preserving all existing values and structure layouts. Python and Java expose recognizable exception categories or structured codes. All bindings convert along the same error-cause chain without guessing from strings. Each SDK implementation PR must finalize and test its public symbols.

The existing `fallback_eligible()` is advisory classification; it does not execute fallback. Callers may apply their own policy to `Unavailable` / `Timeout`, while preserving the original object version and read range. Origin fallback must not hide errors such as version mismatch or rate limiting.

### 6.3 Partial Failure and Buffer Lifetime

If any block in a multi-block read fails, do not report the incomplete overall read as successful. Bytes already written into a caller-owned buffer are not a complete result when the operation fails. Before invoking the failure callback, ensure that all remaining operations have stopped accessing that buffer. Returning errors promptly must preserve existing cancellation and ownership contracts.

A streaming read may deliver successful chunks followed by an error; previously delivered data cannot be retracted. The caller may resume at the correct offset only when the object version and completed offset are known, or restart the entire logical read. An HTTP/gateway stream that has already sent successful headers cannot replace them with an error status; terminate the stream according to the existing interface and let the caller handle the failure.

### 6.4 Recovery for Subsequent Requests

Separate failure delivery from membership refresh. The current request fails directly and may trigger a coalesced background instance-view refresh for subsequent requests. Refresh has a concurrency bound, minimum interval, and deadline; do not create a background task for every failed request.

While requests are active, the SDK refreshes instance views at the configured interval, including instances locally marked offline. Idle clients need not poll continuously, but must revalidate expired state when activity resumes. Retaining an offline snapshot must never permanently disable rediscovery.

State caches have finite validity. The Coordinator returns remaining session validity or equivalent freshness information, which the client converts to a local monotonic-clock deadline. Expired or unconfirmed state must not authorize arbitrary use of an old process, although the logical membership snapshot may be retained. Remove broken connections from the pool when a cached address fails. On observing a new incarnation, discard connections to the old instance even if the IP is unchanged.

The Coordinator proxy paths for `StatObject` / `ListObjects` must also avoid offline instances. These operations do not have block-owner semantics and may choose healthy Workers under their existing proxy policy; metadata calls need not use the same instance as block reads. Metadata failures must expose recognizable categories too. Otherwise, a read without `known_stat` loses the fallback signal before reaching the block request.

## 7. Worker Lifecycle

### 7.1 Startup

1. Open the actual cache root, acquire the exclusive lock, and read or initialize the identity.
2. Generate the process incarnation and initialize control/management paths. Data service remains unavailable.
3. Validate formats and configuration, recover whole-block and paged indexes plus required TTI/cleanup state, and initialize the data listener. Return `Unavailable` for data requests until authorized to serve.
4. Register the persistent member and report this instance's address, lock ownership, and local recovery state. Report local readiness only after recovery and data-listener initialization complete. Registration must respect retired state.
5. Once the Coordinator accepts this instance as the sole serving target, admit business requests and set readiness to true. Requests arriving early during state propagation may receive `Unavailable`.

If the data directory is unreadable or its format is incompatible, fail startup rather than claim successful cache reuse. Skip individual corrupt or uncommitted cache units according to the existing recovery policy, recording recovery counts and failure reasons. Subsequent normal requests may refill those units.

### 7.2 Normal Shutdown

1. SIGTERM, SIGINT, or a management operation starts idempotent draining. Stop admitting requests before lowering readiness.
2. Make a best-effort report to the Coordinator that this instance is no longer serving. Reporting failure must neither block local draining nor delete the persistent member.
3. Stop accepting new connections and reject new business requests at every request boundary on existing connections. Stopping accepts alone does not drain persistent connections.
4. Complete admitted requests, origin cache fills, and associated disk commits. Stop launching GC/eviction work and wait for existing background operations.
5. Complete required checkpoints, stop state reporting, close storage and connections, and finally release the directory lock and exit.

Both the Tokio and Monoio/io_uring data paths must track in-flight tasks and provide the same semantics. Returning from the main accept future must not destroy a runtime that is still processing requests.

Draining has an overall timeout. Once it expires, remaining reads may fail and the process may terminate, but the lock must not be released while background threads can still modify the cache. Abnormal termination relies on existing crash recovery. Any unacknowledged business writes retain their original write contract and must not be replayed automatically under the read-unavailability policy.

### 7.3 State Transitions

| Phase | Directory lock | Logical member | Serving? |
| --- | --- | --- | --- |
| Initial setup / restart recovery | Held | Added or retained | No |
| Ready | Held | Retained | Yes |
| Draining | Held until tasks finish | Retained | No; new requests fail |
| Offline | Not held by a running instance | Retained | No |
| Ready again | Held by the new process | Same member | Yes |
| Retired | Operator stops the instance first | Excluded from placement; retirement marker retained | No |

## 8. Coordinator Rolling Upgrade

### 8.1 Upgrade Model and Preconditions

Coordinators are active-active: every serving instance can handle registration, heartbeats, and membership queries through the same shared etcd/Kubernetes backend. There is no leader transfer and no direct state handoff between old and new processes. A replacement reconstructs its local view from shared state. Worker identities, instance records, and caches belong to neither the old Coordinator nor its replacement.

For a concrete example, start with two ready instances, `C1` and `C2`, behind a stable Coordinator Service. Replace `C1` with `C3`, validate the result, then replace `C2` with `C4`. These labels denote distinct processes, not identities that must be transferred.

Before starting:

- Use a shared persistent backend; the memory backend does not provide these guarantees.
- Confirm that both versions implement the same persistent membership, shared-record, and client protocol contracts described in Section 10.
- Verify backend health and enough Coordinator capacity to serve traffic while one old instance drains. The reference deployment uses at least two replicas and retains at least one serving instance throughout replacement.
- Use `maxUnavailable: 0` and `maxSurge: 1`, as in the existing HA Helm strategy, with sufficient capacity to schedule the additional Pod. Set an appropriate readiness stabilization interval and termination grace budget.
- Record the logical membership, topology token, sampled block owners, and Worker session freshness. Avoid concurrent membership changes so upgrade-induced changes can be distinguished from intentional scaling.

### 8.2 Step 1: Start the Replacement Without Routing Traffic to It

Start `C3` with the new image, the same cluster ID, shared backend, authentication configuration, and membership contract. Give it its own Coordinator process identity and lease. Keep readiness false while it initializes control listeners, backend access, and local state.

`C1` and `C2` continue serving during this stage. If `C3` cannot connect to the backend, load the schema, or initialize its listeners, it remains unready and the rollout stops making progress. Do not stop an old instance merely because the replacement process has started.

### 8.3 Step 2: Load Shared State and Pass the Readiness Gate

Before `C3` becomes ready, it must:

1. Load a complete persistent member list, including offline members and retired markers.
2. Load current Worker instance state, retaining the distinction between membership and liveness. An unavailable instance must not disappear from logical placement.
3. Initialize the ongoing synchronization mechanism. Updates that occur during initial loading must be incorporated through a valid watch continuation or a subsequent reconciliation; initialization must not leave a permanent gap.
4. Build its local membership/placement view using the same membership contract and placement configuration as existing Coordinators.
5. Confirm that its control listeners can serve requests, backend access is healthy, and its own Coordinator registration has succeeded.

Only then set readiness to true. Never expose a partially loaded member list or use an empty topology as a startup placeholder. An actually empty cluster is valid only after a successful complete load establishes that it is empty. Member and instance snapshots need not form a cross-resource transaction: uncertainty about an instance may produce unavailable status, but cannot remove a member from placement.

At a quiescent membership revision, `C3` must return the same topology token and sampled block owners as `C1`/`C2`. Instance status may differ briefly within the configured synchronization/freshness bounds. Verify a membership query and heartbeat acceptance against the replacement; process liveness alone is insufficient.

### 8.4 Step 3: Add the Replacement to Service

Once `C3` passes readiness, it becomes eligible behind the stable Coordinator Service. Allow the configured stabilization interval and confirm that it handles control traffic successfully before retiring `C1`.

New connections may reach `C2` or `C3`; existing connections to `C1` remain attached to `C1` until closed. Service endpoint changes do not migrate TCP connections. Workers and clients continue using the same Coordinator Service address and do not need to learn `C3`'s Pod IP.

No Worker re-registration campaign, cache scan, identity change, or topology rebuild is required solely because a Coordinator was replaced. Normal heartbeats and discovery requests may reach any ready Coordinator, which reads and updates the same shared records.

### 8.5 Step 4: Drain and Stop the Old Instance

When `C1` receives SIGTERM, SIGINT, or an explicit drain request, execute the following idempotent sequence:

1. Atomically enter draining state and stop admitting new control operations, including operations arriving on existing connections. Mark readiness false and publish its own non-serving state on a best-effort basis.
2. Stop accepting new connections. During endpoint propagation, late requests must receive a recognizable unavailable response where possible; otherwise the connection closes and the caller observes a transport error. Do not rely on endpoint removal alone to enforce admission.
3. Finish already admitted registrations, heartbeats, membership queries, and metadata proxy operations within their deadlines. Preserve their existing completion semantics; in particular, do not acknowledge a state update before the required backend write succeeds.
4. Close idle control connections and close active connections after their admitted work finishes. Include both public and mTLS listeners and any associated background work in the drain accounting.
5. Stop and join the Coordinator's own heartbeat task before deleting its lease, so a late self-heartbeat cannot recreate the record. Stop remaining synchronization tasks after request handlers no longer need them.
6. Remove only `C1`'s Coordinator record, conditional on its process incarnation, then close resources and exit. Never delete Worker members, Worker instance records, or shared backend resources. If self-record deletion fails, let its lease expire rather than block shutdown indefinitely.

The whole sequence has a bounded shutdown deadline within the Pod termination grace period, including any `preStop` time and an exit margin. If the deadline expires, terminate remaining work and report forced shutdown. A response lost during shutdown may leave an operation's outcome uncertain; do not claim it was rolled back or blindly replay non-idempotent management operations.

`C2` and `C3` keep serving while `C1` drains. Stopping `C1` must not revoke Worker leases merely because their latest heartbeat was handled by `C1`.

### 8.6 Step 5: Handle Connection Changes Without Changing Worker Placement

Workers whose control connections close reconnect through the stable Service. Their next heartbeat carries the same Worker ID, Worker incarnation, and normal heartbeat sequence; reconnecting to another Coordinator does not create a new Worker instance. Heartbeat retries retain the existing idempotency and ordering rules.

Clients discard failed Coordinator connections and use the stable Service for subsequent discovery. Any retry of a control operation follows that operation's existing idempotency and deadline rules. This does not authorize resending a failed block read: Section 6's immediate-failure policy still applies to the Worker data path.

Clients with an unexpired Worker instance view can continue direct Worker requests while Coordinator connections change. If a request requires fresh discovery and no Coordinator can provide it, return a recognizable error instead of waiting for the rollout or publishing an empty topology. Keep the logical member snapshot even when instance freshness expires.

Keep heartbeat reconnect and backend recovery within the configured Worker session validity budget. If a session does expire, that Worker may temporarily become unavailable to clients, but its membership and block ownership remain unchanged. Coordinator HA reduces control-plane interruptions; it does not guarantee that every in-flight control request succeeds.

### 8.7 Step 6: Validate and Continue the Rollout

After `C1` exits, verify that:

- `C2` and `C3` are ready and handling registration, heartbeats, and membership queries.
- The logical member set, topology token, and sampled block owners are unchanged in the absence of explicit membership operations.
- Worker IDs and incarnations remain unchanged, and heartbeats continue without an upgrade-induced expiration spike.
- No empty or partially loaded topology was published; backend errors, control-request failures, and reconnect latency remain within the operational budget.
- Direct Worker reads still succeed and no Worker restart or cache recovery was triggered by the Coordinator replacement.

Then replace `C2` with `C4` using the same sequence. The Deployment's readiness/availability checks gate ordinary replacement. Additional gates based on metrics or topology comparison require rollout tooling or operator-controlled batches; `maxSurge` alone does not implement them.

If a replacement fails before readiness, retain the old serving instances and fix or roll back the new image. If it fails after an old instance has exited, stop further replacements, keep healthy instances serving, and restore capacity with a compatible image. If the shared backend becomes unhealthy, pause the rollout; creating more Coordinator processes does not repair shared-state availability. None of these recovery actions should remove Worker members or change placement. Protocol compatibility requirements are described in Section 10.

## 9. Kubernetes Deployment and Upgrade Procedure

The initial reference deployment uses a Worker StatefulSet with persistent PVCs and Local PVs, while Coordinators remain a Deployment. Configure PVC retention and use Local PV node affinity to bind each volume to its machine. The directory is authoritative for Worker identity; a Pod-name environment variable must no longer override it.

A normal StatefulSet rolling update stops the old Pod before creating its replacement with the same ordinal, matching this design's allowance for temporary unavailability. It neither migrates existing connections nor starts an overlapping replacement for the same ordinal to wait on the lock. No controller for overlapping processes is required.

Configure the deployment with:

- startupProbe: allow enough time for cache recovery so that a long scan is not mistaken for a deadlock.
- readinessProbe: pass only when the lock is held, recovery is complete, the data listener is available, and instance registration is accepted; fail during draining.
- livenessProbe: check process health without treating temporary inability to obtain serving status as process failure.
- SIGTERM draining and, if needed, a `preStop` hook invoking the same idempotent drain operation. Both share the Pod termination grace budget.
- Sequential updates, an appropriate `minReadySeconds`, and partition-based canary rollout. Check Coordinator-visible state and actual reads after each instance recovers.

Talon clients continue to connect directly to Worker data endpoints. Kubernetes readiness does not replace Coordinator state publication. The first version adds no per-Worker Service routing layer.

RWO permits access by multiple Pods on the same node and is not a substitute for a file lock. PDBs constrain voluntary disruptions such as evictions; they do not control StatefulSet/Deployment rolling-update concurrency.

Upgrade procedure:

1. Confirm that callers recognize availability errors and have configured fallback. Validate cache-format compatibility and the image rollback path.
2. Check membership, directory identities, PVCs, and node bindings. Ensure that no membership scaling or permanent removal is in progress.
3. Roll Coordinators first if needed to deploy the required protocol and membership capabilities.
4. Upgrade one Worker and verify that its member record and block ownership remain unchanged while offline.
5. After the new instance recovers the same directory and ID, verify that it is the sole serving instance, inspect cache hits, and check caller fallback metrics.
6. Proceed to the next Worker only after acceptance checks pass. Stop the rollout if recovery fails or origin load exceeds its budget.

## 10. One Membership Contract Before Deployment

Talon has not been deployed in production. This work replaces the old lease-only membership semantics directly; it does not introduce a Legacy/Retained mode, a mode configuration field, an activation API, or runtime protocol fallback. All Workers use instance heartbeats, and all Coordinators derive logical ownership from the persistent member registry. Temporary instance loss changes availability, not membership.

The single `MembershipQuery` / `MembershipList` exchange carries that persistent view. `NodeStatusHeartbeat` carries local instance readiness and `NodeStatusAck` separately reports acceptance and service admission. Standalone registration messages, legacy heartbeats, versioned query alternatives and protocol fallback have been removed. The first status heartbeat registers the logical Worker; subsequent reports renew its instance lease. SDK layers must consume the instance-aware discovery directly so an offline owner is distinguished from a serving endpoint. They must not probe a legacy mode, switch read policies, or fall back to lease-only placement. Rust/native and Java wire consumers use this schema directly; subsequent SDK layers still implement the full availability and immediate-failure policy before deployment.

The protocol carries persistent members, instance states and freshness, without a membership-mode field or capability-switch response. Keep the transport schema explicit so incompatible messages fail clearly. A future supported rolling upgrade requires both releases to implement the same membership and read-error contract; supporting a downgrade to the superseded lease-only implementation is outside this pre-deployment change.

Ordinary heartbeats from an already admitted, unchanged instance update a bounded in-memory entry without backend I/O. New incarnations and changes to address, readiness, health, or deployment metadata still require authoritative admission and local routing publication before acknowledgement. Queueing is included in the admission request timeout. Duplicate sequences do not renew receipt time. A separate publication task runs at the Coordinator heartbeat interval: it reads the registry once and publishes only the latest queued report per instance when the last confirmed report is at least one third of the lease TTL old. At most 16 instance publications run concurrently; slow publication does not hold the discovery/admission lock. The requested remaining lease duration is measured from heartbeat receipt, not publication, and expired, retired, or rezoned queued reports are discarded.

Discovery reconciliation also warms the instance cache from shared snapshots, so a Worker routed to another Coordinator can use its fast path. A newly observed higher shared sequence advances the confirmed-report timestamp; re-reading the same sequence does not. A cached heartbeat grant requires a recent discovery observation and a locally published or newly observed shared report younger than half the lease TTL. In-memory heartbeats alone cannot extend either deadline. If the publication task cannot keep up, admission falls back to authoritative confirmation rather than extending the cache indefinitely; this fast path is not a guarantee of zero backend work under every load or timing configuration.

`MembershipQuery` reads the installed observation in memory. Its maximum cache age is `unhealthy_after_ms` in the server configuration; a stalled refresh or backend failure cannot keep discovery usable indefinitely. Receiving a heartbeat, serving a query, and a successful health probe do not refresh that age. Other Coordinators learn changes through their independent periodic reconciliation, not direct peer messages or a watch. Cache views can temporarily differ across Coordinators; monotonic client observations are a separate protocol concern.

Management APIs continue to use their leased node-status projection for node details, aggregate metrics and a genuine backend snapshot revision. Periodic coalesced publication maintains that projection alongside instance leases, so recent metrics may lag and buffered reports can be lost on a Coordinator crash. Member retirement and zone changes remain explicit revision-checked administrative operations; they are not mode activation. This layer changes neither the wire layout nor client-side revision ordering.

## 11. Failure Handling and Observability

| Failure | Behavior |
| --- | --- |
| Worker receives SIGKILL | Kernel releases the lock; membership persists; requests return unavailability/timeout; the replacement performs crash recovery |
| Heartbeats stop while the process remains alive | Session expiration makes the instance unavailable without changing topology; reporting recovery restores availability |
| Stale cached address or pooled connection | Fail the current request and discard the broken connection; coalesce refreshes for subsequent requests |
| New instance starts before the old writable record expires | A temporary conflict may be reported; do not arbitrarily select a writer; recover after the old record is withdrawn or expires |
| Temporary Coordinator/state-backend outage | Retain logical views; expired instance state cannot claim availability indefinitely; fail requests that require unavailable fresh state |
| Disk loss or incompatible format | Do not claim that the original cache was recovered; operators repair it or explicitly rebuild/remove the member |
| Permanent Worker failure | Continue reporting offline status until an administrator explicitly removes the member; only then change placement |

Record member and online-instance counts, offline duration, identity conflicts, recovered block/page/byte counts, drain duration, forced terminations, availability/timeout errors, discovery refresh outcomes, and instance recovery time. Prefer management interfaces and logs for high-cardinality Worker/instance details; avoid unbounded request-metric labels.

Distinguish topology changes from instance-state changes so operators can confirm that a restart did not remove a member. The integrating application measures fallback success, origin traffic, and end-to-end latency. Talon error counts alone cannot establish fallback success.

## 12. Validation and Acceptance

The table below is the acceptance contract. Per-layer executed checks and validation gaps are recorded in the corresponding PR; unexecuted integration scenarios are not implied to pass. Future builds, tests, and benchmarks must use the repository's required development container and `wt-build`, with all outputs, caches, and test data under `/data/yuruiz`.

| Scenario | Required result |
| --- | --- |
| Interrupted initialization and duplicate startup | A partially written identity is never accepted; at most one process modifies a directory; conflicts do not generate a new ID |
| Normal restart and downtime exceeding heartbeat TTL | Persistent membership and topology token remain unchanged; compare owners for a fixed block sample, not just member counts |
| Address/instance changes with unchanged zone | Ownership stays stable and routing moves to the new address; a restart at the same address does not reuse old-instance connections |
| Addition, explicit removal, and delayed heartbeats after retirement | Topology follows membership operations; ordinary heartbeats cannot undo retirement |
| Multiple Coordinators with etcd and Kubernetes backends | Membership survives Coordinator restarts; offline is distinct from removal; identical member snapshots produce identical placement |
| Cache recovery after normal and abnormal shutdown | Valid whole-block/page data remains a cache hit; backend request counts confirm identity changes did not trigger refetches; count TTI expiration and corruption separately |
| Offline Worker, connection refusal, EOF, and timeout | Every public SDK exposes stable categories; reads perform no hidden retries, cross-Worker attempts, or origin fallback |
| Object-version change, origin errors, rate limiting, and malformed frames | Preserve the original error rather than misclassify it as Worker unavailability eligible for fallback |
| Multi-block reads, streaming, and caller-owned buffers | Failure is not reported as complete success; no buffer access after the callback; delivered prefixes and retry ranges remain well-defined |
| Tokio and io_uring draining, saturated connections, and background writes | Persistent connections admit no new work; the lock remains held until tasks finish; recovery succeeds after forced termination |
| Coordinator rolling replacement using SIGTERM | New instances load membership before readiness; old instances drain; Worker members remain present |
| One-at-a-time Kubernetes updates | Same PVC, machine, and ID; caller fallback during downtime; original cache hits after recovery before the next update |
| SDK/Coordinator protocol agreement | Matching clients consume instance-aware discovery directly; incompatible messages fail explicitly without a second membership policy |

During fault injection, measure Talon failure latency, caller fallback success, cache hit rate after recovery, and origin traffic. Acceptance does not require zero Talon errors: controlled availability errors are expected. Unrecognized errors, data from the wrong object version, unintended topology changes, and unintended cache loss are failures.

## 13. Implementation Sequence

Implementation uses a linear PR stack, with implementation, regression tests, and relevant documentation in each layer. Deploy the stack only after its server and client layers implement the single membership contract.

| Layer | Independently deliverable scope | Validation focus |
| --- | --- | --- |
| 1 | Directory identity initialization, old-ID import, and existing lock contract | Normal/crash restart, concurrent initialization, configuration conflicts |
| 2 | Persistent member registry, CAS and retirement in memory, etcd and Kubernetes | Backend contracts, concurrent updates, tombstones and capacity bounds |
| 3 | Versioned instance discovery, sole-instance readiness and membership administration | Protocol compatibility, offline/conflicting instances and revision propagation |
| 4 | Rust client and CLI placement by logical membership, bounded refresh and immediate read failure | Stable ownership, no hidden rerouting or resend, discovery freshness and buffer lifetime |
| 5 | Rust/C/Python error classification and typed metadata failures | Public error categories, ABI compatibility and preservation of domain/protocol errors |
| 6 | Java retained discovery and typed read failures | Wire conformance, no resend, offline ownership and incarnation recovery |
| 7 | Bounded Coordinator/Worker draining and complete task lifetimes | Both data runtimes, SIGTERM, stale state and directory lock lifetime |
| 8 | Sequential upgrades with persistent local volumes, migration/rollback procedures and integration validation | Actual downtime, original cache recovery, caller fallback and deployment contracts |

Each layer includes its implementation, regression tests and relevant documentation. Message definitions without a working consumer and tests are not a complete feature.

## 14. References and Scope of Reuse

- [Alluxio AI-3.6: Worker Management and Consistent Hashing](https://documentation.alluxio.io/ee-ai-en/ai-3.6/overview/worker-management-and-consistent-hashing): informs retained registration, separation of offline state from member removal, identity files, and explicit removal. Its static ring still supports adding and removing members; Talon keeps its own Maglev algorithm. This is a reference from enterprise documentation, not a verified equivalent open-source implementation.
- [Alluxio AI-3.6: I/O Resiliency](https://documentation.alluxio.io/ee-ai-en/ai-3.6/overview/io-resiliency): illustrates that stable identity and I/O fault tolerance are separate concerns. This Talon design leaves fallback to callers instead of adopting its automatic fallback chain.
- [Kubernetes StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/), [Local Volumes](https://kubernetes.io/docs/concepts/storage/volumes/#local), and [Pod termination](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination-flow): provide sequential replacement, volume reuse, and termination budgets. They do not replace filesystem locks or Talon instance discovery.
- [ADR 0001](../adr/0001-management-plane-ha.md), [ADR 0006](../adr/0006-zone-aware-cache-reads.md), and [Page idle TTI](page-tti.md): existing membership, zone, and cache-lifecycle contracts that must be updated alongside implementation.
