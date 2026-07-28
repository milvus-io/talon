# ADR 0003: Optional Metadata Store and Capability Negotiation

- Status: Proposed
- Date: 2026-07-28
- Tracking issue: #274 (roadmap items 4 and 7)
- Motivating defects: #363 (hard links cannot be made write-atomic), #359
- Prerequisite for: write-back (ADR 0002 §2), POSIX locking
- Amends: ADR 0001 §1 (conditionally — see §8)

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

This is that ADR.

### What must not be lost

Talon's namespace is currently derived, not stored. The mount rebuilds the tree
from a backend listing (`main.rs:84`); directories are trailing-slash marker
blobs; everything else lives in in-memory `HashMap`s. `ListObjects` is defined
in the wire protocol but has **zero server-side implementations**. The object
store's key list *is* the source of truth.

That property — **path == data** — is why a plain S3 client can read what Talon
wrote, why a coordinator is disposable, and why there is no metadata to recover
after a restart. Any metadata store that dilutes it converts Talon from a cache
in front of an object store into an object-store-backed filesystem (the JuiceFS
model). That is a legitimate architecture and it is not this one.

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
| Dirty-object owner and replica set | File size, mtime, ETag (`head` is the truth) |
| Inode reference counts | Placement (derived from the worker set hash) |

The right column stays derivable **whether or not TMS is configured**. A cluster
that enables TMS and then loses it permanently still has an intact, readable
namespace; it loses hard links, locks, and any un-flushed writes, not its data.

### 3. TMS is sparse by construction

TMS holds one record per *entity actively using a TMS-backed feature*, not one
record per object:

- a file with a single link occupies zero TMS records;
- a file with three links occupies one;
- an object with no un-flushed writes occupies zero;
- an unlocked file occupies zero.

This is what makes an etcd-class backend viable. Node records under ADR 0001
number in the hundreds; per-object records for a billion-object bucket would
not fit (etcd holds its keyspace in memory, with a practical ceiling in the
single-digit GB and a 1.5 MB per-value limit). Per-*linked*-file and
per-*dirty*-object records are orders of magnitude smaller, and are zero for
workloads that use neither feature.

A deployment that exceeds an etcd-class backend's capacity is a signal that the
workload wants a filesystem, not a cache. TMS does not attempt to serve it.

### 4. Optionality is capability negotiation, never silent degradation

This is the load-bearing clause.

An operation that requires TMS, in a cluster without TMS, **fails with a
distinct errno**. It must never fall back to an approximation:

| Operation | Without TMS |
|---|---|
| `link()` | `EPERM` |
| `fcntl` lock | `ENOLCK` |
| Write-back | Not offered; write-through per ADR 0002 §1 |

Silent degradation is precisely the defect in #363: `link()` currently appears
to succeed, produces copies that can diverge, cannot self-repair, and does not
report which copies went stale. A feature that looks supported and is subtly
wrong is worse than one that plainly refuses, because applications build on the
former.

Consequently a client must be able to **discover** cluster capabilities before
relying on them. Capabilities are reported by the coordinator, cached with the
placement epoch, and exposed through the management API so an operator can see
what a cluster offers without attempting an operation.

Note that this makes capability a property of the *cluster*, not the mount: two
Talon clusters may legitimately offer different POSIX semantics. That is a
product-level fact and must be documented as such, not buried in a config
reference.

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

`link()` becomes O(1) with no byte movement, a write touches exactly one
object, and divergence becomes structurally impossible rather than merely
unlikely. Reference counts and the `path -> inode` map live in TMS.

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

Without TMS, `link()` returns `EPERM`. **This is the only part of this ADR that
should be implemented before TMS exists**, because it is a correctness fix for
shipped behaviour (#363) and does not depend on anything here.

### 6. TMS is not an availability dependency for reads

TMS unavailability degrades TMS-backed features; it must not affect the read
path.

- Reads, cache hits and misses, placement lookups, and write-through writes are
  unaffected. None consults TMS.
- Operations requiring TMS fail closed, with the same errno as an unconfigured
  cluster plus a distinct log and metric so "not configured" and "configured but
  unreachable" are separable in an incident.
- A held lock whose lease cannot be refreshed expires. Lease semantics are the
  same as ADR 0001 §3: the holder must assume loss on refresh failure.

This is consistent with roadmap item 1 (fail-open): a cache failure must not
become a query failure. A metadata failure must not either.

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

### 8. ADR 0001 §1 is amended, conditionally

ADR 0001 excluded leader election, conditioned on Talon having no durable
non-rebuildable metadata. TMS introduces exactly that, so the condition no
longer holds for clusters that enable it.

The amendment is narrow:

- **Coordinators remain stateless and active-active, always.** TMS is
  authoritative; coordinators cache but never own. Deleting a coordinator still
  requires no state recovery.
- **Single-writer ownership is delegated to TMS**, via lease-scoped
  compare-and-swap, not to an elected coordinator. There is still no leader.
- ADR 0001's rejection of *custom Raft inside Talon* stands. TMS uses an
  existing consensus system (etcd, or the Kubernetes API); Talon does not
  operate one.
- For clusters without TMS, ADR 0001 is unchanged in full.

### 9. Relationship to ADR 0002

TMS is a **prerequisite** for write-back, not a parallel feature.

ADR 0002 §2's first entry condition — replication before acknowledgement —
requires durable knowledge of which workers hold which un-flushed bytes. That is
a TMS record by the §2 admission rule: it cannot be derived from the object
store, because by definition the data is not there yet.

ADR 0002 §2 remains in force. TMS satisfies one of five entry conditions.
Write-back still requires the other four and its own superseding ADR. Nothing in
this ADR makes write-back reachable.

## Consequences

### Positive

- Hard links become correct rather than approximately correct, or are honestly
  refused. Either outcome is better than shipping silent divergence.
- Write-back's hardest prerequisite gains a design, without write-back becoming
  reachable.
- POSIX locking becomes expressible for the first time.
- Clusters that need none of this are unaffected: no new dependency, no new
  failure domain, no new operational burden.
- The path == data property is preserved for every file that does not use a
  TMS-backed feature — which, for the target workloads, is essentially all of
  them.

### Costs and risks

- **A second consistency domain.** Object store and TMS can disagree: an inode
  record whose data object is gone, or an orphaned data object with no
  referrer. Reconciliation (an fsck-equivalent) is required and is not
  designed here.
- **Two POSIX dialects.** Clusters with and without TMS behave differently.
  §4 makes this explicit rather than silent, but it is still a documentation and
  support burden.
- **A new operational dependency** for clusters that enable it: capacity,
  backup, credential rotation, upgrade ordering against coordinators.
- **The admission rule will be under constant pressure.** Every future feature
  will have a reason its metadata belongs in TMS. §2 is only as strong as the
  willingness to enforce it; each addition should require an ADR amendment.
- **Link/unlink becomes multi-step** (TMS update + object move) and is not
  atomic across the two. Crash mid-transition must be recoverable, which needs
  the same staging/marker discipline `write_cache.rs` already uses locally.

## Open questions

These are unresolved and block moving this ADR to Accepted.

1. **Do FUSE clients connect to TMS directly, or through the coordinator?**
   Direct access means distributing TMS credentials to every client and widening
   the network exposure. Proxying through the coordinator keeps the credential
   boundary where it is but puts the coordinator on the path of every lock
   acquisition — bounded by §8's "cache but never own", though the latency and
   failure-coupling consequences need to be worked out.

2. **How is the link indirection transition made crash-safe?** Moving a file
   into inode-addressed storage is a copy, a TMS update, and a delete. A crash
   between any two leaves an inconsistent pair. A commit-marker scheme
   analogous to `write_cache.rs`'s sidecar is the obvious candidate, but the
   ordering has not been designed.

3. **What reconciles the two domains?** Orphaned inode objects and dangling
   references need a detection and repair path. Offline tool, background
   scrubber, or lease-driven cleanup — undecided.

4. **Is `ENOLCK` right for locks without TMS?** It is the honest errno, but many
   applications treat lock failure as fatal where they would tolerate advisory
   locking being a no-op. This needs a survey of the target workloads before
   being fixed.

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

### Defer the `link()` fix until TMS ships

Rejected. #363 is shipping behaviour today. Returning `EPERM` is correct
independent of TMS and should not wait for it.

## References

- ADR 0001: Active-Active Management Plane and Shared Cluster State
- ADR 0002: Write-Cache Durability and Consistency Contract
- Hard links are backend copies: milvus-io/talon#363
- Partial hard-link writeback divergence: milvus-io/talon#359
- POSIX link semantics: milvus-io/talon#323
- Roadmap: milvus-io/talon#274
