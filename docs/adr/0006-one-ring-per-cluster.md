# ADR 0006: One Placement Ring Per Cluster

- Status: Proposed
- Date: 2026-08-07
- Supersedes: ADR 0005 section 6, which put both rings behind one coordinator
- Relates to: #455 client-side placement

## Context

ADR 0005 §6 gave the async worker its own rendezvous ring and ran it
**alongside** the block ring behind a single coordinator, with the client
naming a ring per request. That shape was chosen to keep the async worker
deployable without a second cluster. Building it out showed the cost is paid
somewhere worse.

**Failures are silent.** An async lookup against a cluster with no async
workers answers with an empty owner list. ADR 0005 argued this is better than
substituting a block worker, and it is — but it is indistinguishable from "the
pool has not finished starting", which is a normal transient state. The
operator's actual mistake, pointing a worker at the wrong cluster, produces no
error at any layer: the worker registers happily, is filtered out of every
lookup, and the only symptom is a cache that never warms.

**The epoch is shared.** Both rings resolved against one `Membership`, so the
epoch — a content hash of the whole node set — moved whenever *either* pool
changed. Scaling the async pool invalidated the cached placement of every
block client, for a change that could not have moved a single block. The
alternative, an epoch per ring, is a second number for clients to track and a
new way for the two to disagree.

**The separation is a filter, not a boundary.** Role filtering at lookup time
is the only thing keeping a block off an async worker. Nothing establishes that
a cluster *is* one kind or the other, so nothing can refuse a node that
contradicts it — there is no fact for the refusal to appeal to.

None of these are bugs in the implementation of §6; they are consequences of a
cluster having no opinion about which ring it runs.

## Decision

**A cluster runs exactly one ring, named at startup and fixed for its life.**

`ClusterType { Block, Async }` in `talon-core`. A coordinator resolves it from
`--cluster-type` / `TALON_COORDINATOR_CLUSTER_TYPE`, defaulting to `Block`, and
three things follow from that one value:

| | Block cluster | Async cluster |
|---|---|---|
| Ring | `RendezvousPlacement`, hashing the whole `BlockId` | `ObjectPlacement`, hashing the object identity alone |
| Worker role admitted | `NodeRole::Worker` | `NodeRole::AsyncWorker` |
| Registry | its own `Membership`, its own epoch | its own `Membership`, its own epoch |

`ClusterPlacement` is an enum over the two strategies, built from the type.
Both are unit structs, so the indirection is free at runtime and makes "a
cluster has one ring" a fact the type system holds rather than a field someone
could set twice.

**A worker of the wrong kind is refused at registration**, with a reason, and
never enters the registry. Not filtered at lookup — refused, so the operator
learns at the first heartbeat instead of from a cache-hit-rate graph a day
later. Coordinators are admitted by either type: the type constrains the worker
pool, not the control plane.

**The client is statically configured.** `talon-client --cluster-type` /
`TALON_CLUSTER_TYPE` says which kind of cluster `--coordinator` points at.
There is no discovery message. What the flag selects is no longer *which pool
answers* — the cluster has one — but **how placement resolves**, and that
genuinely differs: a block cluster's ring has a client-side Maglev equivalent
and needs no round trip (#455), while the object ring has none and must be
asked.

### The wire loses a message

`Ring` and `RingPlacementLookup` are **deleted**, not deprecated. Nothing is in
production, `RingPlacementLookup` was the only schema-5 message, and
`CONTROL_SCHEMA_VERSION` therefore falls back to **4** with no version gap left
behind. One `PlacementLookup` remains, with its schema-1 encoding intact, now
answered by whichever ring the coordinator runs.

This is the part of ADR 0005 §6 that aged worst. Its compatibility argument —
`PlacementLookup` keeps meaning the block ring, `RingPlacementLookup { ring:
Block }` must place identically, the schema-5 floor fences the encoding rather
than the ring — was three paragraphs of careful reasoning about a distinction
that does not need to exist. Moving the ring to the cluster deletes the problem
instead of solving it.

The Java client, pinned at schema 2, is unaffected either way: it only ever
receives `PlacementResponse`.

## Consequences

**Two deployments where there was one.** An async cluster is a separate Helm
release with its own coordinator, not a second worker block in an existing one.
The chart fails the render when the enabled worker contradicts
`coordinator.clusterType`, so the mistake is caught at `helm template` rather
than at first heartbeat. Two clusters sharing one etcd still need distinct
`cluster_id`s.

**No cross-pool fallback, and one visible gap.** An async worker serves reads
and stats but not listings (ADR 0005 §8), and an async cluster has no block
worker behind it. `ListObjects` therefore returns an explicit error naming the
limitation. An empty listing would be worse: a client caches it as an empty
bucket.

**Independent epochs.** Churn in one cluster is invisible to the other's
clients. This is the property the shared registry could not offer, and it is
what makes scaling the async pool cheap.

**A third ring stays cheap.** `ClusterType` is a value, and nothing here
assumes exactly two. Adding one costs a variant and a `ClusterPlacement` arm,
not a message variant and a dispatch arm at every call site — which was the
right instinct in ADR 0005 §6, just applied to the wrong noun.

**What is given up.** A single coordinator address can no longer serve both
kinds of read. A caller that genuinely needs both — block-shaped scans and
selective columnar reads over the same data — now points at two coordinators.
That is a real cost, and the honest trade for it is that such a caller was
previously relying on a routing decision no layer was checking.
