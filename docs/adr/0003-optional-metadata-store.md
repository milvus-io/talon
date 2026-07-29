# ADR 0003: Optional Metadata Store and Capability Negotiation

- Status: Proposed
- Date: 2026-07-28
- Last revised: 2026-07-29 (§3, §4, §5, §6, §7, §9 — hard-link promotion uses
  a crash-safe TMS transition and reconciles through a background scrubber plus
  `talon fsck`; distributed lock errors distinguish unsupported capability,
  unavailable TMS, contention, and deadlock; clients proxy TMS operations
  through coordinators; write-back uses fixed write shards, effective
  assignments in TMS, replica-local dirty manifests, fencing terms, quorum
  recovery, owner-routed reads, and a fixed default of 4,096 shards with offline
  resharding; `fsync` reaches the origin, identity-changing namespace operations
  use a flush barrier, recovery and routing deadlines are fixed, origin conflict
  policy is configurable, and the local WAL and dirty-capacity defaults are
  specified)
- Tracking issue: #274 (roadmap items 4 and 7)
- Motivating defects: #363 (hard links cannot be made write-atomic), #359
- Prerequisite for: write-back (ADR 0002 §2), POSIX locking
- Amends: ADR 0001 context, §1, and §9, and ADR 0002 §8 (conditionally — see
  §8); qualifies ADR 0002's estimate of the remaining write-back work
- Does not supersede: ADR 0002; §9 constrains a future superseding write-back
  ADR but does not enable write-back

## Context

Three separate features have converged on the same missing primitive.

**Hard links.** `link()` is implemented as a backend copy: one inode maps to N
independent objects, and every write fans out across them (`mount.rs:350`).
Object stores have no cross-key atomic write, so the copies can diverge and
nothing reconciles them (#363, #359). `st_nlink` already reports a link count
(`ops.rs:698`, counted from the in-memory dentry index) that the backend has no
way to honour.

**Write-back.** ADR 0002 §2 defers write-back behind five entry conditions, the
first being replication before acknowledgement. Something must record which
objects are un-flushed, which worker owns each, and which replicas hold the
bytes. ADR 0002 explicitly rejected coordinator-owned dirty state "in this
iteration" because it "turns the coordinator into a durable metadata owner and
pulls in consensus, which is the ADR 0001 trigger."

**POSIX locking.** Not implemented. `fcntl` locks and `O_EXCL` semantics across
clients require single-writer ownership that survives a client crash.

The shared requirement is not "somewhere to put data." It is **durable,
strongly consistent ownership that outlives a single process and is visible to
every client**. ADR 0001 §1 excluded leader election and consensus on exactly
this basis, and anticipated this moment:

> If Talon later gains durable write-back metadata or a singleton workflow, that
> feature requires a separate ADR.

This is the ADR for the shared metadata primitive and the write-back
prerequisites that depend on it. It does not enable write-back; ADR 0002 still
requires a later ADR that changes the shipped acknowledgement contract.

### What must not be lost

Talon's namespace is currently derived, not stored. The mount rebuilds the tree
from a backend listing (`main.rs:84`); directories are trailing-slash marker
blobs; everything else lives in in-memory `HashMap`s. `ListObjects` is defined
in the wire protocol but has **zero server-side implementations**. The object
store's key list *is* the source of truth.

That property — **path == data** — is why a plain S3 client can read what Talon
wrote, why a coordinator is disposable, and why there is no metadata to recover
after a restart. Making every ordinary file depend on stored namespace metadata
would convert Talon from a cache in front of an object store into an
object-store-backed filesystem (the JuiceFS model). This ADR permits narrow,
explicit exceptions only for files actively using a TMS-backed feature; it does
not make stored metadata the default namespace.

## Decision

### 1. Introduce an optional Metadata Store (TMS)

A cluster may be configured with a **Metadata Store**: a strongly consistent,
highly available store holding ownership facts that cannot be derived from the
object store.

TMS is **optional**. A cluster without one is a complete, supported Talon
deployment offering exactly today's feature set. TMS is not a performance tier
or a recommended default; it is the price of admission for a specific group of
features, and clusters that do not need those features must not pay it.

### 2. The admission rule: only non-derivable facts

> If a fact can be rebuilt by listing the object store, it does not go in TMS.

This single rule is what keeps §1's promise. It is not a guideline — it is the
boundary that prevents TMS from growing into a namespace database.

| In TMS | Not in TMS |
|---|---|
| `path -> inode` for files with more than one link | File existence (the key list is the truth) |
| Lock holder, owner identity, lease expiry | Directory structure (marker blobs) |
| Active link and namespace transition records | Permanent records for ordinary single-path files |
| Hard-link namespace mapping revision and mutation guard | Per-file records for ordinary single-path files |
| Effective write-shard owner, fencing term, state, and acting replica set (§9) | File-to-write-shard mappings (derived from the object identity, §9) |
| Inode reference counts | File size, mtime, ETag (`head` is the truth) |
| | Per-object dirty inventory and generations (replica-local, §9) |
| | Desired shard placement (derived from the worker set hash, §9) |

The right column stays derivable **whether or not TMS is configured**. The same
is not true of the left column. An ordinary single-path namespace remains
listable and readable without TMS, but multiply-linked path mappings,
write-shard fencing history, and active transition state are intentionally
non-rebuildable.

Permanent TMS loss therefore makes hard-linked paths unavailable and may strand
acknowledged un-flushed mutations even when their inode objects or replica
payloads still physically exist. Backup and tested restore are mandatory once a
deployment enables a TMS-backed feature. "Optional" means a cluster can choose
not to configure TMS; it does not mean TMS is disposable after the cluster has
committed non-rebuildable state to it.

One entry needs a caveat, because it is excluded on different grounds from the
rest. Everything else in the right column is derivable *from the object store*.
Dirty state is not — by definition the bytes are not there yet. It is
recoverable from the durable local manifests of the replicas that acknowledged
the write, which is a weaker property than derivability from the object store.
It is excluded from TMS by §3a's write-rate argument rather than by §2's
derivability test. §9 makes that exclusion safe by keeping the low-frequency
write-shard assignment in TMS and requiring every acknowledged replica to carry
enough local metadata to enumerate and order its dirty objects.

### 3. TMS is sparse by construction

TMS holds records only for *entities actively using a TMS-backed feature*, not
for every object:

- a file with a single link occupies zero TMS records;
- a multiply-linked file occupies one bounded inode entity plus bounded path
  index entries for its actual links;
- an unlocked file occupies zero.
- a namespace with hard links enabled occupies one mapping-revision and mutation
  guard record, independent of object count;
- an identity-changing operation occupies one transition record only while it
  is in progress;
- a cluster without write-back occupies zero write-shard records;
- a cluster with write-back occupies one bounded record per configured write
  shard, independent of object count and write rate.

This is what makes an etcd-class backend viable. Node records under ADR 0001
number in the hundreds; per-object records for a billion-object bucket would
not fit (etcd holds its keyspace in memory, with a practical ceiling in the
single-digit GB and a 1.5 MB per-value limit). Per-*linked*-file and
per-*locked*-file records are orders of magnitude smaller, and are zero for
workloads that use neither feature. Write-shard records are different but still
bounded: enabling write-back creates a fixed routing table, not a record
population that grows with the bucket.

The link count is bounded as well as each record. The initial default maximum
is 65,535 links per inode and is configurable downward per namespace. `link()`
must reject an operation that would exceed the configured limit with `EMLINK`
before creating a transition or copying data. An implementation may use smaller
transaction batches internally, but it must not advertise a limit that its TMS
backend cannot update and validate atomically.

A deployment that exceeds an etcd-class backend's capacity is a signal that the
workload wants a filesystem, not a cache. TMS does not attempt to serve it.

### 3a. Sparsity is a claim about record count; write-back needs a claim about write rate

§3's argument is a *capacity* argument, and it holds for links and locks. It
does **not** transfer to write-back state, and the difference is what §9's
design has to be built around.

Link and lock records are user-triggered, rare, and long-lived. Dirty-object
records would be none of those:

| | Links / locks | Dirty objects |
|---|---|---|
| Triggered by | An explicit, uncommon syscall | **Every write**, implicitly |
| Lifetime | Indefinite | Seconds: create, flush, delete |
| Steady-state count | Small, stable | Proportional to **write throughput** |
| Write pattern | Occasional | Sustained create/delete churn |

The population of dirty records is not a function of how many objects exist. It
is `write_rate × flush_latency`. A checkpointing workload writing 1,000 objects
per second with a two-second flush lag holds only ~2,000 records — a trivial
*capacity* figure that conceals 1,000 creates plus 1,000 deletes per second
landing on a consensus group.

**etcd's binding constraint is write throughput, not key count.** Every write is
a Raft round trip plus an fsync; sustained write rates are practically in the
low tens of thousands per second cluster-wide, and latency degrades well before
that ceiling. So a naive "one TMS record per dirty object" design puts a
consensus write on the critical path of every single write — in a feature whose
entire purpose is to make writes fast. It would plausibly cost more than
write-back gains, while adding a hard availability dependency to the write path
that §6 explicitly refuses for reads.

The 256 MiB default block size does not rescue this. Dirty records are
per-object rather than per-block (`write_cache.rs` already tracks at object
granularity), which is a constant factor; the proportionality to write rate is
unchanged.

The conclusion is not that write-back is infeasible, but that **dirty state must
not be modelled as one TMS record per dirty object**. §9 states the alternative.

### 4. Optionality is capability negotiation, never silent degradation

This is the load-bearing clause.

An operation that requires TMS, in a cluster without TMS, **fails with a
distinct errno**. It must never fall back to an approximation:

| Operation or condition | Result |
|---|---|
| `link()` without TMS | `EPERM` |
| `link()` capability configured but TMS unavailable | `EAGAIN` |
| `fcntl`/`flock` without the TMS locking capability | `EOPNOTSUPP` |
| TMS locking configured but unavailable | `ENOLCK` |
| Non-blocking lock conflicts with another holder | `EAGAIN` |
| Lock acquisition would create a detected deadlock | `EDEADLK` |
| Write-back without the required TMS capability | Not offered; write-through per ADR 0002 §1 |

`EOPNOTSUPP` means that this cluster does not implement distributed locking.
`ENOLCK` is reserved for the distinct case where the cluster offers locking but
cannot currently reach or use the lock service. Talon never turns either case
into a successful mount-local lock.

The FUSE implementation must explicitly implement the `getlk`, `setlk`, and
`flock` callbacks and forward them to the distributed lock path. It must not
omit those callbacks and rely on a kernel or libfuse fallback, because such a
fallback can make locks appear to work while only coordinating processes on one
mount.

Silent degradation is precisely the defect in #363: `link()` currently appears
to succeed, produces copies that can diverge, cannot self-repair, and does not
report which copies went stale. A feature that looks supported and is subtly
wrong is worse than one that plainly refuses, because applications build on the
former.

Consequently a client must be able to **discover** cluster capabilities before
relying on them. Capabilities are reported by the coordinator with their own
capability revision and exposed through the management API so an operator can
see what a cluster offers without attempting an operation. The capability
revision and §9's write-routing revision are not ADR 0001's
membership-derived `PlacementVersion`.

Backend capability is a property of the *cluster*, not a client mount.
Activation may be namespace-specific: for example, a cluster can support
write-back while enabling it for only selected prefixes. Two mounts of the same
cluster and namespace must observe the same semantics; a mount flag cannot
silently opt into a weaker contract. These are product-level facts and must be
documented as such, not buried in a client config reference.

### 5. Hard links use TMS indirection; without TMS they are refused

With TMS, a file that acquires a second link moves to inode-addressed storage
and its visible paths become references:

```
data object (one copy, inode-addressed):
  s3/bucket/.__talon_internal/inodes/<ns>/<ino>

visible paths (references, resolved through TMS):
  s3/bucket/data/a.bin  --+
  s3/bucket/data/b.bin  --+--> <ino>
```

After the second-link promotion, adding or removing another link is an O(1)
metadata operation with no object copy. A write touches exactly one object, and
divergence becomes structurally impossible rather than merely unlikely.
Reference counts and the `path -> inode` map live in TMS.

Indirection applies **only to multiply-linked files**. A file with one name
remains a plain object at its visible path, directly readable by any S3 client.
When a link count returns to one, the object moves back. This confines the cost
of indirection to files that actually use the feature and preserves path == data
everywhere else.

Note the internal-object prefix and the `<ns>/<ino>` addressing scheme already
exist, used by unlink-while-open (`ops.rs:2349`). That mechanism is *not* a
precedent for links: its mapping only has to survive inside one process for one
open handle's lifetime, so in-memory state suffices. Hard links must hold across
processes, clients, and restarts. The addressing convention is reusable; the
persistence model is not.

Because linked paths have no visible-path objects, namespace enumeration in a
hard-link-enabled namespace merges the ordinary object-store listing with the
TMS path index and hides the internal inode prefix. Coordinators expose a
paged, revisioned mapping snapshot; FUSE clients cache that sparse snapshot and
refresh it on revision changes or stale-mapping errors. A directory listing
must fail with `EAGAIN` rather than return a silently incomplete result when TMS
is unavailable and the client has no current snapshot. Plain object-store
clients intentionally do not see TMS-backed linked paths; this is the narrow
exception to path == data accepted by this ADR.

#### Crash-safe promotion to inode-addressed storage

Creating the second link promotes a plain path-addressed object into
inode-addressed storage. The copy, TMS mutation, and old-object deletion cannot
be one cross-system transaction, so the transition is an explicit durable state
machine.

The coordinator first creates a fenced TMS operation record:

```text
LinkTransition {
  operation_id
  state: PREPARING
  source_path
  new_path
  source_version
  expected_mapping_revision
  inode
  owner_session
  operation_worker
  operation_worker_incarnation
}
```

While `PREPARING` exists, reads of the source may continue from the original
object, the new path remains absent, and new writes or namespace mutations
against either path return a retryable error. The coordinator selects a healthy
operation worker and records its worker ID and incarnation in the transition.
That worker, using its existing object-store credentials, conditionally copies
`source_path@source_version` to the unique inode object and verifies its length
and checksum. The coordinator never reads or forwards the payload.

That write exclusion requires an explicit distributed fence; checking TMS only
inside `link()` is insufficient because a client may hold a stale negative
mapping. A namespace with the hard-link capability therefore has one
monotonically increasing `MappingRevision` and a TMS-backed mutation guard:

- FUSE clients cache sparse path mappings plus the namespace revision obtained
  from a coordinator. Every read, write, and namespace mutation in that
  namespace carries that revision.
- Workers watch TMS and validate the revision against local state before
  resolving a path or accepting a mutation. This is a local hot-path check, not
  a TMS round trip per operation.
- Creating a transition advances the revision. Before copying data, the
  coordinator waits until every healthy mutation-serving worker has observed
  the fence, or its previous guard has expired and been fenced. A worker that
  cannot keep its guard current stops accepting mutations for that namespace.
- Stale clients receive `STALE_MAPPING`, refresh through any coordinator, and
  retry. A current negative mapping proves that an ordinary unlinked object
  still uses its visible path.

This namespace-wide revision may cause unrelated writers to refresh after the
rare link transition, but it avoids a per-object TMS record and closes the race
where an old client writes the visible-path object after promotion.

After the copy is complete, one TMS compare-and-swap verifies the operation ID,
owner session, source version, and mapping revision, then atomically:

```text
source_path -> inode
new_path    -> inode
inode.link_count = 2
remove LinkTransition
```

That TMS transaction is the linearization point of `link()`. Before it,
`source_path` is authoritative and `new_path` does not exist. After it, the
inode object is authoritative for both paths. `link()` returns success only
after this transaction commits. Deleting the old path-addressed object is
post-commit cleanup and uses the captured source version as a precondition.

Recovery is unambiguous:

| Crash point | Authoritative data | Recovery |
|---|---|---|
| Before inode copy | Original path object | Abort the transition |
| After copy, before TMS commit | Original path object | Resume commit or delete the orphan inode object |
| After TMS commit, before old-object deletion | Inode object | Delete the obsolete path object |
| After deletion | Inode object | No action |

Every write and namespace mutation carries the mapping revision it resolved.
Workers reject an old revision with `STALE_MAPPING`; a request that encounters
`PREPARING` receives `EAGAIN`. The client refreshes the mapping and retries, so
an open handle created before promotion continues against the inode object
rather than writing a new generation to the obsolete visible-path object.

Adding a third or later link needs no object copy: one TMS transaction adds the
new path index, increments the bounded link count, and advances the mapping
revision. Removing a link while at least two paths remain is the inverse
transaction.

Removing one of the final two links demotes the remaining path back to ordinary
path-addressed storage. It uses the same fenced transition pattern:

1. block writes and namespace mutations for both mapped paths;
2. have an operation worker conditionally copy the inode object to the remaining
   visible path and verify its version, length, and checksum;
3. atomically remove both path indexes and the inode record from TMS, recording
   the new mapping revision; this transaction is the linearization point of
   `unlink()`;
4. delete the inode object as post-commit cleanup.

Before the TMS commit, both paths still resolve through TMS and the inode object
is authoritative, so a partial visible-path copy is ignored. After the commit,
the removed path is absent and the remaining plain object is authoritative. A
crash before commit resumes or removes the unused path copy; a crash after
commit only needs to delete the obsolete inode object. This is the required
inverse of promotion, not an optional space optimization.

#### Reconciliation between TMS and inode objects

The transition protocol prevents a crash from making an ambiguous state, but it
does not eliminate abandoned copies, external deletion, storage corruption, or
operator mistakes. Talon therefore provides both a low-frequency background
scrubber and an operator-facing `talon fsck` command.

One active coordinator acquires a TMS scrub lease for the namespace so
active-active coordinators do not schedule the same repair concurrently. This
is a resumable leased task, not an elected coordinator role: any coordinator
may acquire the lease after expiry and continue from durable TMS state. The
holder takes a linearizable TMS snapshot and assigns object-store inspection
and server-side copy/delete work to a healthy worker with backend credentials.
The worker lists the internal inode prefix and returns bounded metadata and
verification results; payload bytes and backend credentials never pass through
the coordinator. Before any TMS mutation, the coordinator rechecks the relevant
records. Destructive cleanup also requires a configurable grace period and
proof that no live `LinkTransition` refers to the object.

Repair policy is deliberately asymmetric:

| Condition | Automatic action |
|---|---|
| Inode object has no TMS reference | Move to quarantine; delete only after a second unreferenced check and the grace period |
| Stored link count differs from the number of path references | Recompute from the TMS path map and repair with compare-and-swap |
| TMS path references a missing inode object | Mark the inode `CORRUPT`, fail access, and alert; never fabricate data or delete the references automatically |
| Transition-owned inode object remains after owner loss | Resume or abort according to the durable transition state |

`talon fsck` uses the same comparison engine and supports dry-run, quarantine
inspection, explicit repair, and an audit record of every destructive action.
The background scrubber only applies repairs whose safety can be proved from
two observations and the absence of a live transition. Missing authoritative
data always requires operator action or restoration from an external backup.

Without TMS, `link()` returns `EPERM`. **This is the only part of this ADR that
should be implemented before TMS exists**, because it is a correctness fix for
shipped behaviour (#363) and does not depend on anything here.

### 6. TMS is not an availability dependency for ordinary reads

TMS unavailability degrades TMS-backed features; it must not affect the
ordinary read-through path.

- Reads, cache hits and misses, placement lookups, and write-through writes in a
  namespace without TMS-backed namespace features are unaffected. None consults
  TMS.
- Operations requiring TMS fail closed. Capability absence and backend
  unavailability are separate protocol states and produce the operation-specific
  errno defined in §4, plus distinct logs and metrics so operators can separate
  configuration from an outage.
- A held lock whose lease cannot be refreshed expires. It follows ADR 0001
  §3's failure principle, not its node-record lease: the holder must assume loss
  on refresh failure.

This is consistent with roadmap item 1 (fail-open): a cache failure must not
become a query failure for an ordinary non-TMS-backed read-through object.
Hard-linked files and write-back namespaces are explicit exceptions because
their current path or bytes may exist only in TMS-backed state or on the acting
replicas.

A namespace that enables hard links adds one narrower availability dependency:
workers must keep §5's mutation guard current to accept writes and namespace
mutations. They do not contact TMS per operation, but they fail such mutations
with `EAGAIN` after the guard expires during a TMS outage. Ordinary reads of
unlinked path-addressed objects remain available.

This section concerns TMS failure only. Failure of `ClusterStateStore` retains
ADR 0001 §8's fail-closed behavior for new authoritative membership and
placement reads. A coordinator must not initiate a write-shard handoff or
recovery from a stale last-good membership snapshot.

**Write-back namespaces are the deliberate exception.** Before a dirty object
is flushed, the object store contains an older version. A read that bypasses
the write owner and falls back to the object store can therefore violate
read-your-writes. For a namespace that explicitly enables write-back:

- clients cache the effective write-shard routing table obtained from a
  coordinator and send reads to the shard owner without consulting TMS per
  operation;
- the owner serves a replica-local dirty version when present, otherwise it
  uses the ordinary cache/origin read path;
- a stale route is rejected by owner/term validation and causes the client to
  refresh that shard;
- a shard in `RECOVERING` or `INCOMPLETE` does not fail open to the origin.

This is a property of the explicitly enabled write-back namespace, not a change
to Talon's default read-through mode.

### 7. TMS reuses the `ClusterStateStore` contract shape, as a separate store

TMS is a distinct store with its own trait, not new record types inside
`ClusterStateStore`. Cluster membership is ephemeral, bounded, and rebuildable
from live processes (ADR 0001 §2); TMS records are durable and *not*
rebuildable. Mixing them in one abstraction would make ADR 0001 §2's
"bounded, rebuildable" invariant untrue by construction.

The contract shape is reused because it already fits: linearizable snapshot,
watch-from-revision, lease-scoped ownership, compare-and-swap, a backend-neutral
error model with explicit `Compacted`/`WatchLagged`/`Timeout`/`Unavailable`
variants, and pluggable backends with a memory implementation for tests.

Both may target the same physical etcd cluster under different prefixes. They
remain separate abstractions with separate invariants.

Capabilities are advertised per TMS backend, not inferred from the existence of
a generic compare-and-swap call:

- distributed locks require server-observed expiring sessions and atomic
  compare-and-swap;
- hard links require atomic transactions across transition, inode, and path
  index records;
- write-back additionally requires persistent fencing terms, atomic shard-state
  transitions, and server-observed session expiry.

A backend that cannot satisfy one of these contracts must not advertise that
capability. In particular, the Kubernetes Lease representation from ADR 0001 is
not a multi-record transaction service and cannot provide hard links or
write-back by itself. The first hard-link and write-back TMS implementation
therefore targets etcd; a lease-only backend may still provide locking if it
meets the locking contract.

FUSE clients never receive TMS credentials or connect to TMS directly.
Hard-link and lock operations are proxied through any active coordinator:

```text
FUSE client -> any coordinator -> TMS
```

The coordinator validates and forwards the operation but does not own the
resulting metadata. Each hard-link state transition is durable in TMS and
advances through compare-and-swap transactions. A lock is owned by a renewable
client session identified by a session ID and an authenticated token accepted
by every coordinator, not by the coordinator process that happened to proxy
acquisition.

The client may renew or release the session through any coordinator, so a
coordinator failure does not transfer or drop its locks. If the client stops
renewing, TMS expiry releases them. If renewal fails, the client must assume the
session and all of its locks are lost. This keeps coordinators stateless while
confining TMS credentials and network access to the management tier.

Coordinators arbitrate and record transitions but do not receive object-store
credentials solely for TMS features and do not proxy payload bytes. A worker
already authorized for the namespace performs conditional copies, deletes,
flushes, quarantine moves, and checksum verification under a fenced operation
token. If that worker fails, any coordinator may fence it and assign another
worker to resume the durable transition.

The same reasoning is what forbids folding write-back's dirty inventory into
TMS. §7 separates two stores because membership is *ephemeral and rebuildable*
while TMS records are *durable and not rebuildable* — different invariants, so
different abstractions. Dirty inventory differs from TMS records along a
different axis (high-frequency and short-lived versus low-frequency and
long-lived, §3a), and the conclusion is the same: it does not belong in TMS.
§9 keeps it replica-local rather than inventing a third store, because the
workers' NVMe already provides durability and crash recovery for exactly this
data.

### 8. ADR 0001 context and §1 are amended, conditionally

ADR 0001 excluded leader election, conditioned on Talon having no durable
non-rebuildable metadata. TMS introduces exactly that, so the condition no
longer holds for clusters that enable it.

The amendment is narrow:

- **Coordinators remain stateless and active-active, always.** TMS is
  authoritative; coordinators cache but never own. Deleting a coordinator still
  requires no state recovery.
- **Coordinators remain outside the data path.** They may create, fence, and
  commit durable transition records, but operation workers move object bytes.
  This preserves ADR 0001's prohibition on routing data-plane bytes through the
  management tier.
- **Single-writer ownership is delegated to TMS**, via lease-scoped
  compare-and-swap, not to an elected coordinator. There is still no leader.
- **`ClusterStateStore` remains bounded and rebuildable.** TMS is a separate
  consistency domain, so ADR 0001 §2's record and backend contract is unchanged.
- **The object store remains the data store, but not the sole metadata source.**
  Multiply-linked path mappings, locks, and transition state are durable in TMS.
  This conditionally amends ADR 0001's context without moving payload ownership
  into coordinators.
- **ADR 0001's `PlacementVersion` remains membership-derived.** TMS-confirmed
  write routing has a separate revision because effective ownership may lag the
  desired HRW result during handoff or recovery.
- **ADR 0001's node and cluster resources remain read-only.** TMS-backed
  operator actions add authenticated mutating resources under a separate
  namespace in the management API. They transact against TMS and command fenced
  workers; they do not make `ClusterStateStore` node records mutable or expose
  direct TMS credentials to the UI.
- **Every TMS mutation is authenticated and authorized.** Operator actions,
  including conflict resolution, quarantine deletion, recovery retry, and
  resharding, also emit durable audit records. Client-originated link and lock
  requests carry the authenticated namespace identity but never TMS
  credentials.
- ADR 0001's rejection of *custom Raft inside Talon* stands. TMS uses an
  existing consensus system; Talon does not operate one. A backend may support
  links and locks without supporting write-back. The write-back capability
  additionally requires atomic compare-and-swap, a persistent fencing term, and
  server-side lease/session expiry. The current Kubernetes Lease backend, whose
  expiry is judged from client wall clocks and which lacks multi-record
  transactions, does not meet the hard-link or write-back contracts. It may
  advertise locking only after it meets §7's server-observed session contract.
  The first hard-link and write-back implementations therefore target etcd.
- For links and locks, object data remains in the object store and ADR 0001's
  data-durability statement remains true. A future ADR that actually enables
  write-back must explicitly amend that statement for the interval in which
  acknowledged bytes exist only on the acting worker replicas.
- For clusters without TMS, ADR 0001 is unchanged in full.

ADR 0002 §8 remains correct for the shipped write-through configuration, but
its blanket conclusion that ADR 0001 is unamended no longer applies once a
cluster enables TMS-backed links or locks. This conditional amendment changes
no write acknowledgement point: ADR 0002 §1 remains authoritative until a
future write-back ADR explicitly supersedes it.

### 9. Relationship to ADR 0002: fixed write shards, not dirty-object records

TMS is a **prerequisite** for write-back, not a parallel feature. The naive
design — one TMS record per un-flushed object — puts a consensus write and
delete on the critical path of every object write. The design instead separates
the low-frequency fact that needs global agreement from the high-frequency
facts that belong on the data replicas:

```text
TMS (consensus, low frequency):
  fixed write-shard configuration
  effective owner, fencing term, state, and acting replica set per shard

Worker NVMe (replica-local, high frequency):
  dirty object payloads
  ordered mutation records
  flush and retirement records
```

The result is a fixed-size TMS routing table whose write rate is proportional
to worker membership changes and shard transitions, not object traffic.

#### 9.1. A file maps to one fixed write shard

Write-back introduces a placement key that is deliberately different from the
version-bearing `BlockId` used by the read cache. The canonical write identity
is:

```text
(namespace_id, backend, bucket, object_path)
```

It excludes object version, ETag, block offset, block size, and page index. The
write shard is a pure, deterministic function:

```text
write_shard = H(
  scheme_version || namespace_salt || backend || bucket || object_path
) mod shard_count
```

`shard_count` is a power of two and is fixed when write-back is enabled for the
namespace. The hash algorithm, encoding, salt, shard count, and scheme version
form durable cluster configuration. They must use an explicitly specified,
cross-version-stable hash rather than Rust's `DefaultHasher`. Changing any of
them is a resharding operation, not a normal configuration update.

The initial implementation uses **4,096 write shards per namespace by
default**. An explicit override must be a power of two in the inclusive range
256 through 16,384. The count is selected when write-back is first enabled and
does not automatically follow the current worker count. Operators should choose
enough shards for the largest planned worker fleet; the default provides 64
primary shards per worker at 64 workers before replication is considered.

The initial implementation does not support online resharding. It rejects an
in-place change to the shard count, hash, salt, or encoding while write-back is
active. Resharding requires this offline write-back procedure:

1. stop admitting new write-back mutations while allowing reads to continue;
2. flush every committed dirty mutation and require every shard to be `ACTIVE`
   with no pending handoff or recovery;
3. disable the old write-back routing generation;
4. atomically install a new routing generation containing the new hash
   configuration and a fresh shard descriptor table;
5. re-enable write-back only after coordinators and workers have observed the
   new generation;
6. reject requests carrying the retired generation with `STALE_ROUTING`, so
   clients refresh rather than write through an obsolete mapping.

Because no dirty mutation crosses the generation boundary, this procedure does
not need to move replica-local dirty payloads between old and new shards. Old
descriptors may be deleted only after the routing-cache grace period and an
audit confirms that no old-generation request is still being accepted.

A write shard is an ownership and recovery group, not a physical split of a
file. Every block and every generation of one file maps to the same shard and
therefore to the same primary owner:

```text
object path
    |
    | stable hash of object identity
    v
write shard
    |
    | effective assignment from TMS
    v
one primary worker
```

The existing read-cache placement remains free to distribute immutable,
versioned blocks by `BlockId`. Write-back ownership must not reuse that
placement key.

#### 9.2. HRW computes a desired worker ranking

For each write shard, Highest Random Weight (HRW, or rendezvous hashing) gives
every eligible worker a deterministic score:

```text
score = H(placement_scheme || shard_id || worker_id)
```

Workers are ranked by descending score. The first eligible worker is the
desired primary; the following workers are desired replicas. Replica selection
must also enforce configured failure-domain separation such as host and zone.
Weighted HRW may be added for workers with materially different capacities, but
rapidly changing load measurements must not be hash inputs because they would
continually move ownership.

HRW produces a **desired assignment**, not an immediately effective one. A
worker joining or leaving changes the desired ranking, but no client may act on
that new ranking until the affected shard has completed handoff or recovery and
TMS has committed the new effective assignment.

All active-active coordinators can compute the same desired assignment from the
same healthy worker set. A coordinator reconciliation loop proposes shard
transitions through compare-and-swap; TMS resolves races and records the one
effective result. Coordinators remain stateless and never forward object bytes.

#### 9.3. TMS stores the effective assignment and fencing term

Each configured write shard has a bounded durable descriptor:

```text
WriteShardDescriptor {
  shard_id
  term
  state: ACTIVE | DRAINING | RECOVERING | INCOMPLETE
  owner
  owner_incarnation
  previous_acting_set
  acting_set
  replication_factor
  write_quorum
}
```

`term` is a monotonically increasing fencing number. It must survive owner
lease expiry; an ephemeral owner/session key may disappear on expiry, but the
last committed term and replica history must remain. Acquiring or transferring
a shard atomically increments the term and binds the new owner to its process
incarnation.

Every write, replica operation, recovery request, and owner-routed read carries
`(shard_id, term)`. Workers persist the highest term observed for each shard and
reject operations from lower terms. Expiry of the TMS owner session, or a fresh
ADR 0001 snapshot that marks the worker unhealthy, permits a coordinator to
propose reassignment. Neither event grants ownership by itself: the TMS
compare-and-swap must increment the fencing term. ADR 0001's 30-second
membership-record lease is separate and may remain present for diagnosis while
the higher TMS term prevents a paused or partitioned old owner from mutating
data.

Clients do not contact TMS per operation. A client obtains the effective shard
routing table from a coordinator, caches it, computes `object -> shard`
locally, looks up `shard -> owner` locally, and connects directly to that
worker. `NOT_OWNER`, `STALE_TERM`, `RECOVERING`, or connection failure causes a
refresh of the affected shard. A full refresh is only needed at startup,
revision loss, or large routing changes.

The response carries a `WriteRoutingVersion` derived from the TMS routing
generation and effective descriptor revision. It is distinct from ADR 0001's
membership-derived `PlacementVersion`: membership changes immediately alter
the desired HRW ranking, while effective write ownership changes only after a
safe handoff or recovery commits. Clients and coordinators must never substitute
one token for the other.

Write-shard routing entries have a 30-second default TTL. `NOT_OWNER`,
`STALE_TERM`, `STALE_ROUTING`, a routing-revision mismatch, or connection
failure invalidates the affected entry immediately; the client does not wait
for TTL expiry. The stale-route protocol errors are internal redirect signals
and are not returned to the application when refresh and retry succeed.

#### 9.4. Dirty inventory is durable on every acknowledged replica

Dirty inventory is not owner-only. Every replica whose acknowledgement counts
toward durability must persist both the payload and enough metadata to enumerate
and order it during takeover:

```text
DirtyMutation {
  object
  shard_id
  mutation_id: (term, sequence)
  client_request_id
  length
  checksum
  base_origin_version
  flush_attempts
  last_error_class
  state: PREPARED | COMMITTED | FLUSHED | FAILED | CONFLICT | EXPORTED | ABANDONED
}
```

`sequence` is monotonically increasing within one shard term. Mutation IDs are
ordered lexicographically, so `(13, 1)` is newer than every mutation from term
12, regardless of the old owner's process-local counter. A worker restart uses
a new incarnation and reacquires ownership under a new term rather than trying
to make a process-local sequence globally meaningful.

The current `WriteCache` sidecar and rename ordering are useful local
prototypes, but the replicated implementation uses the following durable
format:

- payloads are immutable files, written to a temporary name, checksummed with
  BLAKE3, `fsync`ed, renamed into place, and followed by a directory `fsync`;
- metadata is in one append-only WAL per configured durable cache directory,
  not one WAL per write shard;
- WAL segments are 256 MiB. Records use a fixed little-endian envelope with
  magic, format version, record type, lengths, and CRC32C, followed by a
  length-delimited Protobuf body. Rust `serde`/`bincode` and JSON are not durable
  on-disk formats for this protocol;
- the WAL uses 32 KiB physical blocks with full/first/middle/last fragments, so
  recovery can identify a torn tail and resynchronize after a damaged block;
- the `PREPARED` record contains the payload file identity, mutation ID, client
  request ID, object identity, origin base version, length, and checksum. The
  payload is durable before `PREPARED` can become durable;
- `COMMITTED`, `FLUSHED`, `FAILED`, `CONFLICT`, `EXPORTED`, and `ABANDONED` are
  separate records. No object payload bytes are copied into the WAL.

One WAL writer per cache directory performs group commit. It issues an `fsync`
when either 2 milliseconds have elapsed since the first unflushed record or
1 MiB of WAL records are waiting. A caller is acknowledged only by the
replicas whose required record is included in the completed `fsync`; the timer
is a maximum batching delay, not an early-acknowledgement path.

Compaction writes a checksummed snapshot of the live per-shard mutation index
at a precise WAL position. The worker `fsync`s the temporary snapshot, renames
it, `fsync`s the directory, appends and `fsync`s a `CHECKPOINT` record naming
that snapshot, and only then deletes covered WAL segments and `fsync`s the
directory again. A crash before `CHECKPOINT` leaves an ignorable snapshot; a
crash after it can replay the snapshot plus later WAL. Compaction runs when WAL
metadata reaches 2 GiB or more than 50 percent of its records are obsolete.

Recovery loads the latest valid checkpoint and replays later WAL records.
Payload deletion is legal only after a terminal retirement record is durable on
`W` replicas and no live checkpoint references the file. A corrupt local WAL or
payload is repaired from the recovery quorum; if the quorum cannot provide the
record and bytes, the shard becomes `INCOMPLETE` rather than skipping the
damage.

#### 9.5. Write acknowledgement requires replica durability

The initial write-back contract uses three replicas with acknowledgement after
two durable copies (`R = 3`, `W = 2`) in distinct configured failure domains.
Future configurations may vary `R`, but `W` remains at least two and the
recovery quorum rules below must continue to hold.

For one object write:

1. The client computes the write shard and sends the request to the cached
   effective owner with the cached term.
2. The owner validates that its local lease guard and term are current.
3. The owner assigns the next `(term, sequence)` mutation ID.
4. The owner streams the payload and `PREPARED` manifest record to the acting
   replicas.
5. Each replica fsyncs the complete payload and prepare record before
   acknowledging.
6. After `W` prepare acknowledgements, the owner replicates a `COMMITTED`
   record for that mutation to `W` replicas.
7. The owner reports success only after the commit record is durable on `W`
   replicas.
8. The owner queues the mutation for asynchronous flush to the object store.

Only the primary accepts client writes. Replicas store full durable copies but
do not independently order client mutations. The primary serializes mutations
of the same object and permits at most one origin upload for that object at a
time, so late completion of an older upload cannot move the origin backwards.
This orders whole-object generations only; it does not by itself implement
cross-client `O_APPEND`, byte-range merge, or atomicity beyond one assembled
object. ADR 0002 §3's exclusions remain unless the future enabling ADR
explicitly replaces them.
Recovery ignores `PREPARED` mutations that have no durable commit record. A
crash after commit but before the client receives the reply remains the usual
ambiguous-completion case: retry must use a client request ID so the recovered
owner can return the existing result instead of creating a second mutation.
Client request IDs are 128-bit random values. Their result records survive
checkpoint compaction until at least 24 hours after the mutation reaches a
terminal state; the retention period is configurable upward, not downward.

Pending full-object generations may be coalesced after a newer generation is
`COMMITTED` on `W` replicas. An in-flight origin upload, an `fsync` waiter, or a
generation still needed for request deduplication keeps its referenced payload
alive. Coalescing may remove redundant uploads; it must not remove the newest
committed state or make an acknowledged request unrecognizable.

#### 9.6. `fsync` reaches the origin; path changes use a flush barrier

ADR 0002's whole-object buffering remains in force. A FUSE `write()` may only
modify the client-side assembled object and carries no durability or
cross-client visibility promise by itself. The assembled object is sent at the
`flush`/`fsync`/close acknowledgement point and remains subject to
`DEFAULT_MAX_OBJECT_BYTES`; write-back does not solve the separate streaming
and sparse-large-write problem tracked by #347.

For a future write-back mode, the normal FUSE `flush`/close path returns after
the assembled mutation is `COMMITTED` on `W` workers. It reports a prior write
or replication error but does not claim that the object store is current.

`fsync()` and `fdatasync()` provide the explicit origin boundary. At entry they
capture the highest committed mutation for the file and wait until that
mutation, or a later full-object mutation that contains it, has been
conditionally written to the object store and its `FLUSHED` record is durable
on `W` replicas. Concurrent writes after the captured mutation are not part of
that call. An origin conflict or unrecoverable dirty state makes `fsync` fail
with `EIO`; a transient recovery or TMS outage follows the bounded `EAGAIN`
rule in §9.8.

After a successful `fsync`, a later `flush` or close with no intervening write
reuses the same committed mutation and must not create another generation or
perform a second origin PUT.

An operation that changes the stable object identity cannot leave dirty bytes
under the old identity. File and directory rename, hard-link promotion, and
unlink therefore use a flush barrier in a write-back namespace:

1. a coordinator creates one durable `NamespaceTransition` in TMS containing
   the operation ID, source and target path or prefix, mapping revisions, and
   state;
2. overlapping identity-changing transitions for the namespace are serialized,
   and the affected shard owners stop admitting new writes after their current
   committed sequence; reads continue from the owner;
3. every affected dirty mutation is flushed to the origin and reaches
   `FLUSHED` on `W` replicas. A directory transition queries all write-shard
   owners for dirty objects under the affected prefixes;
4. only then does a fenced operation worker execute the object-store copy/delete
   portion of the existing crash-safe namespace transition, including the
   hard-link state machine in §5; the coordinator commits only the TMS state and
   new mapping revision;
5. workers release the barrier and clients refresh routing for the new object
   identity.

Writes that reach a barrier wait within the normal operation deadline and then
receive `EAGAIN`. If the flush cannot complete, the path change does not commit.
This makes a dirty rename or unlink slower than a clean one, and may make a
directory rename expensive, but it prevents the latest bytes from remaining
owned by a shard computed from a name that no longer exists.

#### 9.7. Reads in a write-back namespace go through the shard owner

The absence of a per-object dirty record in TMS means a client cannot know
whether the origin is current. Consequently every read in a write-back-enabled
namespace first reaches the effective shard owner:

```text
client read
    |
    v
shard owner
    |
    +-- latest committed dirty mutation exists --> serve replica-local data
    |
    +-- no dirty mutation ----------------------> ordinary cache/origin path
```

This preserves read-your-writes for reads that reach Talon's data path without
putting TMS on every read. ADR 0002 §4's kernel page-cache caveat still applies:
owner routing cannot invalidate a stale page that the kernel serves without
calling Talon. A future enabling write-back ADR must define the invalidation or
cache-lifetime mechanism before claiming cross-client read-your-writes for the
mounted filesystem.

A write-back namespace also cannot retain unconditional fail-open semantics: a
shard whose owner is unavailable or whose history is not yet recovered must not
silently serve the older origin version.

#### 9.8. Worker changes use handoff or quorum recovery

A worker joining changes the desired HRW assignment for some shards. The
coordinator moves those shards gradually, with bounded concurrency, rather than
making the new HRW result effective immediately. A planned handoff is:

1. mark the old shard `DRAINING`;
2. keep the old owner serving while the new acting set receives its dirty
   manifest and payloads;
3. briefly stop new writes and copy the final mutations;
4. atomically increment the term and commit the new owner/acting set in TMS;
5. activate the new owner and retire the old assignment.

A planned worker shutdown uses the same drain procedure before the process
exits.

An abrupt owner failure uses recovery:

1. compare-and-swap the shard to a new term in `RECOVERING`, preserving the
   previous acting set;
2. send `FenceAndSnapshot(new_term)` to the previous acting set;
3. each responding replica atomically persists the higher term, stops accepting
   the old term, and returns its dirty manifest through that fence point;
4. collect at least `Q = R - W + 1` responses from the previous acting set;
5. discard uncommitted prepares, then merge manifests by object and choose the
   greatest committed `(term, sequence)`;
6. verify payload checksums and repair the selected mutations to the new acting
   set until at least `W` durable copies exist;
7. rebuild the dirty read index and flush queue;
8. commit the shard as `ACTIVE`.

`W + Q > R` guarantees that the recovery responses intersect every write that
could have been acknowledged. With `R = 3` and `W = 2`, recovery therefore
requires two responses from the previous acting set. If that quorum or a
required payload cannot be obtained, the shard remains `INCOMPLETE`; writes are
refused and reads do not fall back to the origin.

An old worker that returns after reassignment carries a lower term. The current
replicas reject its writes, and stale clients are redirected by
`NOT_OWNER`/`STALE_TERM`.

Recovery uses these default deadlines:

- the existing membership threshold declares a worker unhealthy after 15
  seconds without an accepted heartbeat;
- after a shard enters `RECOVERING`, fencing and collection of `Q` manifests
  must complete within 10 seconds;
- repair must show durable progress at least once every 30 seconds;
- the shard must return to `ACTIVE` within 120 seconds of entering
  `RECOVERING`.

The 15-second unhealthy threshold may trigger a proposal before ADR 0001's
30-second membership lease expires; the persistent TMS term, not deletion of the
membership record, fences the old owner. If a coordinator cannot obtain a fresh
`ClusterStateStore` snapshot under ADR 0001 §8, it starts no new recovery.

Missing the manifest deadline, lacking the quorum or a required payload, making
no progress for 30 seconds, or exceeding 120 seconds moves the shard to
`INCOMPLETE`. New evidence, such as a previous replica returning, may start a
new fenced recovery attempt under a higher term; it does not silently make the
old attempt active.

While a shard is `RECOVERING`, workers return a retryable protocol response with
a bounded retry delay. The FUSE client retries with backoff for at most 30
seconds per filesystem operation, then returns `EAGAIN`. An `INCOMPLETE` shard
returns `EIO` because retry alone cannot prove that the acknowledged data is
available. TMS unavailability without evidence of missing or corrupt data also
returns `EAGAIN`, never a false `EIO`.

#### 9.9. Flush, retirement, external writers, and capacity

Origin flush preserves per-object mutation order. The flusher records the
origin version returned by a successful PUT and replicates a `FLUSHED` record
to `W` replicas before payload deletion. Otherwise a primary crash after the
origin PUT but before replica cleanup could resurrect an already-flushed
mutation during recovery.

Before retiring the payload, the owner also installs a clean-version fence
containing that returned origin version. An owner-routed read carrying an older
versioned `BlockId` receives `STALE_VERSION` and refreshes instead of serving an
older immutable cache block. The fence survives recovery and remains at least
through the maximum placement and write-routing cache TTL. This preserves ADR
0002 §3's monotonic-read rule for reads that reach Talon; ADR 0002 §4's kernel
page-cache caveat remains separate.

Transient origin failures use a bounded retry ladder: five total attempts,
exponential backoff starting at 200 milliseconds, and a 30-second delay cap.
Attempt count and classified error are replicated mutation state, so owner
failover or process restart cannot reset the budget and create an accidental
retry-forever loop. A new owner recomputes the bounded delay from the persisted
attempt count instead of trusting another worker's wall clock. Exhausting the
budget records `FAILED` on `W` replicas and parks the payload. Reads continue to
return the committed Talon version, while management state, metrics, and alerts
require an operator to retry, export, or abandon it. An explicit retry starts a
new bounded attempt budget and is audited.

A write-back namespace must have a single write path. Bucket policy or
equivalent credentials should prevent applications from bypassing Talon and
writing the same prefix directly. The flusher still uses an origin
precondition, based on the recorded `base_origin_version`, to detect accidental
external modification rather than silently overwrite it.

Origin conflict policy is configured per namespace:

| Policy | Behaviour |
|---|---|
| `manual` (default) | Park the mutation in `CONFLICT`; preserve read-your-writes and require an operator decision |
| `talon_wins` | Read the current origin version and retry one conditional PUT against that exact version; another concurrent change returns to `CONFLICT` |
| `export_then_manual` | Copy the Talon payload to the protected conflict prefix, verify it, and record `EXPORTED` on `W` replicas, but keep serving the Talon version until an operator resolves the conflict |

There is no automatic `origin_wins` mode, exported or otherwise: automatically
making the acknowledged Talon version disappear from its path would contradict
ADR 0002 §3's rejection of configurable read-your-writes. Changing policy
affects new conflicts only; already parked conflicts require an explicit
management operation. The management API and `talon` CLI expose inspect, retry
as Talon-wins, export, and abandon/accept-origin. The last operation requires
the exact mutation ID plus a data-loss confirmation flag. Every automatic and
manual resolution is audited with namespace, object identity, mutation ID,
origin version, policy, actor, and result.

Dirty capacity includes payload files, live WAL/checkpoint metadata, pending
and in-flight uploads, and parked `FAILED` or `CONFLICT` mutations. Dirty data
is pinned and read-cache data is evicted first.

The default reserved recovery bandwidth is 64 MiB/s per worker. The default
physical dirty capacity of a worker is the smaller of 25 percent of its
configured durable cache capacity and 60 seconds of that recovery bandwidth:

```text
worker_dirty_capacity = min(
  durable_cache_capacity * 25%,
  reserved_recovery_bandwidth * 60 seconds
)

per_shard_dirty_limit = worker_dirty_capacity / 4
```

A separately configured write-back volume replaces `durable_cache_capacity` in
the same formula unless explicitly overridden. Bounding the whole worker, not
only each shard, ensures that one failed worker's complete dirty population fits
within the 60-second transfer budget. This leaves a two-times margin inside the
120-second recovery deadline for verification, an additional replica copy, and
control work. The per-shard limit prevents one shard from consuming more than a
quarter of that budget. The default cluster-wide logical dirty limit is the sum
of live worker dirty capacities divided by `R`, so the configured replication
factor fits physically even after balancing.

At 80 percent of a per-shard, per-worker, or cluster limit, admission is
throttled. At 90 percent, new mutations are rejected with `ENOSPC`; the
remaining capacity is reserved for WAL growth, handoff, recovery, conflict
export, and mutations already in progress. Before writing `PREPARED`, the owner
must obtain reservations from enough replicas to reach `W`. A single mutation
that cannot fit under the applicable limits is rejected before any success can
be reported. Operators may override these defaults, but validation rejects a
per-shard limit whose configured recovery-bandwidth budget cannot meet the
120-second target.

#### 9.10. Fault tests and observability are part of the contract

The implementation is not complete when the happy-path protocol works. Before
write-back can be enabled, automated tests must use real process termination or
network isolation at each durable boundary:

- owner death while streaming a payload, after one prepare, after `W` prepares,
  after `W` commits but before the client reply, and after the reply;
- owner death during origin PUT, after origin success but before `FLUSHED`
  replication, and during payload retirement;
- transient origin failure across owner restart, retry-budget exhaustion into
  `FAILED`, and explicit retry without resetting history silently;
- owner/TMS partition followed by reassignment, then resumption of the stale
  owner and attempted writes under the old term;
- stale client routing before, during, and after planned handoff;
- abrupt owner loss with a recoverable quorum and with fewer than `Q`
  previous replicas reachable;
- replica corruption, missing payload with present manifest, and manifest
  compaction interrupted at every ordering step;
- per-shard and per-worker dirty-capacity exhaustion;
- TMS unavailability during normal operation, drain, and recovery;
- external origin modification causing a conditional PUT conflict;
- crash during every flush-barrier phase for file rename, directory rename,
  hard-link promotion, and unlink;
- `fsync` racing a newer write, origin conflict, owner failure, and recovery;
- WAL torn writes, corrupted physical blocks, segment rollover, and crashes
  before and after each checkpoint/rename/directory-`fsync` boundary;
- all three origin conflict policies and every explicit resolution command.

Required metrics and management state include shard counts by state, ownership
term changes, stale-term rejections, handoff/recovery duration and bytes, dirty
and failed bytes, prepared/committed/flushed mutation counts, incomplete
shards, origin conflicts, and throttled or rejected writes. Object paths must
not become unbounded metric labels. Per-shard descriptors, object identities,
and conflict records are paged from TMS-backed management resources; they are
not copied into ADR 0001's bounded 16 KiB node-status records.

#### 9.11. This still does not enable write-back

This section resolves the ownership granularity, stable hash input, generation
ordering, client routing, replica discovery, and takeover shape that the
previous version left open. It also makes the cost explicit: a write-back
namespace has a primary-routed read path and cannot always fail open to the
origin.

ADR 0002 §2 remains in force. TMS and the shard protocol satisfy only part of
its entry conditions. Write-back still requires bounded production
implementation, failure-injection evidence, observability, operator procedures,
and its own ADR superseding ADR 0002 before any configuration can make this
path reachable.

This qualifies ADR 0002's consequence that future write-back is merely a
"wiring and replication problem." The unwired sidecar proves useful local
mechanisms, but the replicated WAL, quorum recovery, owner-routed read path,
fencing, conflict handling, and namespace barriers specified here are
substantive new implementation and operational work.

Until that superseding ADR is accepted, ADR 0002's shipped contract is
unchanged: `flush`/`fsync`/close acknowledge only after the origin, RF=1 remains
valid, the write-back WAL stays unwired, and ordinary reads may fail open to the
origin. The enabling ADR must explicitly supersede:

- ADR 0002 §1's origin-only acknowledgement point with §9.5 and §9.6's
  replicated write-back point;
- ADR 0002 §3's origin-defined concurrent-writer behavior with owner
  serialization and conditional origin flush;
- ADR 0002 §4's unresolved kernel page-cache boundary with a tested
  invalidation or bounded-cache mechanism;
- ADR 0002 §6's unconditional fail-open rule for write-back namespaces; and
- ADR 0002 §8's conclusion that ADR 0001 is unamended.

## Consequences

### Positive

- Hard links become correct rather than approximately correct, or are honestly
  refused. Either outcome is better than shipping silent divergence.
- Write-back's hardest prerequisite gains a design, without write-back becoming
  reachable. Ownership leases keep consensus off the per-write path, so the
  prerequisite does not silently cost more than the feature is worth (§3a, §9).
- POSIX locking becomes expressible for the first time.
- Clusters that need none of this are unaffected: no new dependency, no new
  failure domain, no new operational burden.
- The path == data property is preserved for every file that does not use a
  TMS-backed feature — which, for the target workloads, is essentially all of
  them.

### Costs and risks

- **A second consistency domain.** Object store and TMS can disagree: an inode
  record whose data object is gone, or an orphaned data object with no
  referrer. The §5 scrubber, quarantine, and `talon fsck` workflow become
  required operational machinery.
- **Two POSIX dialects.** Clusters with and without TMS behave differently.
  §4 makes this explicit rather than silent, but it is still a documentation and
  support burden.
- **A new operational dependency** for clusters that enable it: capacity,
  backup, credential rotation, upgrade ordering against coordinators.
- **The admission rule will be under constant pressure.** Every future feature
  will have a reason its metadata belongs in TMS. §2 is only as strong as the
  willingness to enforce it; each addition should require an ADR amendment.
- **Link/unlink becomes multi-step** (TMS update + object move) and is not
  atomic across the two. The durable transition and flush-barrier protocols
  make crashes recoverable at the cost of extra storage operations and
  temporarily blocked writes.
- **Write-back changes the read path for an enabled namespace.** Reads first
  reach the shard owner because the origin may be behind. A recovering or
  incomplete shard cannot fail open to the origin.
- **A fixed shard table must be operated.** Worker joins and planned removals
  trigger bounded background handoff; worker failure triggers quorum recovery.
  Moving too many shards at once can consume network and NVMe bandwidth needed
  by foreground traffic.
- **Write-back recovery becomes a quorum query, not a lookup.** Taking over a
  shard requires fencing and interrogating enough of its previous acting set to
  intersect every acknowledged write. If the quorum cannot be reached, the
  shard remains unavailable rather than guessing from the origin.
- **Every durable replica carries metadata as well as bytes.** The local
  manifest/WAL, compaction, checksums, failed-flush state, and capacity limits
  become production storage machinery rather than an optional queue.
- **Direct origin writers are incompatible with an unconditional write-back
  promise.** A write-back prefix needs a single Talon write path plus
  conditional origin PUTs and an operator-visible conflict state.
- **`fsync` pays origin latency.** Applications that call it after every write
  intentionally give up most write-back latency benefit in exchange for an
  explicit guarantee that the object store is current.
- **Path-changing operations may be slow.** Rename, hard-link promotion, and
  unlink must flush dirty state first; directory rename may inspect all write
  shards for affected dirty objects.

## Validation gates

No architecture-level open question remains for TMS ownership, write-shard
routing, replica durability, recovery, or origin flushing. Its status remains
Proposed because the selected defaults and failure behaviour require evidence
before they can become a production contract:

- benchmark the 4,096-shard routing table, 2-millisecond WAL group commit,
  checkpoint thresholds, and dirty-capacity formula on the target NVMe and
  worker sizes;
- demonstrate that the configured per-shard limit can be reconstructed within
  the 120-second deadline at the reserved recovery bandwidth while foreground
  traffic continues;
- pass §9.10's process-kill, network-partition, corruption, compaction,
  namespace-transition, and conflict-policy matrix on real filesystems and
  object-store backends;
- verify upgrade and downgrade rejection for every durable hash, routing, WAL,
  checkpoint, and Protobuf schema version;
- ship the management API, CLI, metrics, alerts, and runbook before any
  configuration can enable write-back.

Changing a measured tuning default within the bounds defined here does not
require a new architecture. Weakening durability, changing error semantics, or
adding an automatic data-discard path does require an ADR amendment.

This ADR makes distributed locking possible but does not replace a
feature-specific locking ADR. Before POSIX locks can ship, that ADR must define
the bounded byte-range representation, blocking and fairness rules, waiter
recovery, and cross-file deadlock detection. Those details do not change the
TMS credential, session, capability, or failure decisions made here.

ADR 0002 §4's kernel page-cache coherence problem also remains a separate
prerequisite for a filesystem-wide cross-client read-your-writes claim. The
owner-routed protocol in §9.7 solves stale origin reads only after a read reaches
Talon.

## Rejected alternatives

### Make hard links correct without a metadata store

Impossible, not merely hard. Correct links require one set of bytes reachable
under two names; without a persistent indirection layer the only representation
is N copies, and no object store offers a cross-key atomic write to keep them
equal. Every mitigation shrinks the divergence window rather than closing it.
See #363.

### Put link/lock/dirty records in `ClusterStateStore`

Rejected per §7. ADR 0001 §2 defines that store's contents as bounded,
ephemeral, and rebuildable from live processes. Durable non-rebuildable records
would falsify that invariant, and the two stores have genuinely different
retention, capacity, and recovery semantics.

### Store metadata as objects in the object store

Rejected. It reintroduces the problem it is meant to solve: a create becomes two
non-atomic PUTs (data plus reference), moving the cross-key consistency hazard
from the rare hard-link path onto the common path. It also offers no compare-and-swap
primitive suitable for lock ownership.

### Make TMS mandatory

Rejected. It would add a consensus-system dependency to every deployment,
including the read-only cache workloads that are Talon's primary use case and
need none of it. The optionality is not a compromise — it is what keeps the
simple deployment simple.

### Client-local metadata

Rejected. Invisible to other mounts. Two clients on one bucket would diverge
immediately, which is worse than the copy semantics being replaced.

### One TMS record per dirty object

Rejected per §3a and §9. It is the obvious modelling of write-back state and it
does not survive a throughput calculation: the record population scales with
`write_rate × flush_latency`, so every write becomes a consensus write plus a
consensus delete. etcd's binding constraint is write throughput rather than key
count, so this saturates the metadata store in a feature whose purpose is faster
writes, and makes TMS an availability dependency of the write path — the same
coupling §6 refuses for ordinary reads.

Recording fixed, long-lived write-shard assignments instead, and keeping the
dirty inventory on every acknowledging replica's already-durable local storage,
preserves the property that matters (no two workers can successfully mutate the
same shard term) at a consensus write rate proportional to membership change
rather than to traffic.

### A third store tuned for high-frequency dirty records

Rejected. Having established that dirty inventory does not fit TMS, the tempting
next step is a store that does — something log-structured or sharded, tuned for
churn. It is unnecessary: the data is already durable, checksummed, and
crash-recoverable on the acting replicas' NVMe, and the only consumer outside
the active owner is a recovering successor. Adding a third distributed store to
serve one cold path would be a large operational cost for data that already has
a bounded replica set.

### Reuse versioned `BlockId` placement for write ownership

Rejected. `BlockId` includes version and block offset, so a new object version
or another block of the same file may rank a different worker. That is useful
for distributing immutable read-cache blocks and wrong for write ownership.
Write placement hashes only the stable object identity so every block and
generation of a file has one primary owner.

### Make a new HRW result effective immediately

Rejected. A worker join or removal changes the desired HRW ranking before dirty
state has moved or been recovered. Treating that calculation as immediately
effective can produce two primaries or route writes to a worker without the
acknowledged history. HRW proposes; handoff/recovery runs; TMS commits the
effective owner and higher term.

### Let clients derive the effective owner directly from HRW

Rejected. A client can derive the file's stable shard, but it cannot know
whether a desired ownership move is still draining, recovering, incomplete, or
already active. Clients therefore cache the TMS-confirmed routing table exposed
by coordinators and refresh stale shard entries on explicit worker errors.

### Defer the `link()` fix until TMS ships

Rejected. #363 is shipping behaviour today. Returning `EPERM` is correct
independent of TMS and should not wait for it.

## References

- ADR 0001: Active-Active Management Plane and Shared Cluster State
- ADR 0002: Write-Cache Durability and Consistency Contract
- David Thaler and Chinya Ravishankar, "Using Name-Based Mappings to Increase
  Hit Rates" (Highest Random Weight / rendezvous hashing)
- Mike Burrows, "The Chubby Lock Service for Loosely-Coupled Distributed
  Systems" (leases and fencing sequencers)
- Sage Weil et al., "CRUSH: Controlled, Scalable, Decentralized Placement of
  Replicated Data" (stable placement groups and failure-domain-aware replicas)
- Diego Ongaro et al., "Fast Crash Recovery in RAMCloud" (replica-local
  recovery metadata and distributed reconstruction)
- LevelDB log format (32 KiB checksummed physical blocks and fragmented logical
  records)
- SQLite write-ahead log documentation (checkpoint ordering and reader/writer
  separation)
- Hard links are backend copies: milvus-io/talon#363
- Partial hard-link writeback divergence: milvus-io/talon#359
- POSIX link semantics: milvus-io/talon#323
- Roadmap: milvus-io/talon#274
