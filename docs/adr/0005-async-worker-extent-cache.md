# ADR 0005: An Async Worker with an Extent Cache

- Status: Proposed
- Date: 2026-08-05
- Relates to: #363 analytics over-fetch, `docs/use-cases/analytics.md` block granularity
- Supersedes: the paged-block plan in `DESIGN.md` section 3, for the workloads
  this worker targets

## Context

A Talon block is 256MB and is materialized whole. A read touching any byte of a
block fetches all of it. For a sequential scan that is exactly right and the
cost amortizes on first touch. For a query engine reading a Parquet or Lance
footer and then cherry-picking column chunks it is not: a few kilobytes of
useful data costs a 256MB transfer, and the next column chunk costs another one.

`DESIGN.md` section 3 anticipated this and specified **paged blocks** — a block
materialized page by page against a present bitmap. Every primitive that design
calls for is already in the tree and unit-tested (`PresentBitmap`,
`BlockForm::Paged`, `touched_pages`, `LoadKey::Page`, `CacheUnit::Page`, the
four-way `Presence` enum), and not one has a non-test caller.

Attempting to wire them up surfaced the real problem: **the block is the wrong
unit for this workload, and paged blocks keep it anyway.** A paged block is
still a 256MB extent in the index, still carries a bitmap sized to the block,
still accounts capacity per block, and still forces every read to be expressed
as a page range within a block. The fixed page size reintroduces the same
over-fetch it was meant to remove, one order of magnitude smaller.

Meanwhile a cache built for exactly this access pattern already exists and is in
production use elsewhere: an async extent cache keyed on `(file, offset)` with
no block concept at all, holding whatever byte range was actually requested.
This ADR adopts that design rather than approximating it.

### The block is a placement unit, not a storage unit

The observation that makes this cheap: **Talon's data plane never mentions
blocks.**

```
data plane     RangeRequest { object: ObjectId, offset: u64, len: u64 }
control plane  PlacementLookup { block: BlockId, k: u8 }
```

A client asks a worker for a byte range of an object. Blocks appear only in
placement, where the coordinator rendezvous-hashes a `BlockId` to decide which
worker owns which slice of the keyspace.

So a worker is free to cache however it likes without any protocol change. The
block concept can stay exactly where it is — between coordinator and client —
while disappearing entirely inside this worker.

## Decision

### 1. A separate `talon-async-worker` crate, not a mode

The new worker ships as its own crate and binary, depending on `talon-core`,
`talon-transport`, and `talon-backend`, with its own runtime, cache, and serve
path. `talon-worker` is not modified.

Two cache models in one crate would mean a runtime struct that branches on form
at every step, two capacity accounting schemes sharing one eviction budget, and
a serve path that is correct for neither cleanly. Separate crates also mean the
two can be benchmarked head to head on the same workload, which is how the claim
in section 6 gets tested rather than asserted.

### 2. The cache key is `(stream_id, offset)` and holds variable-length extents

There is no block, no page, and no fixed granularity:

```
ExtentKey { stream_id: u64, offset: u64 }
```

An entry holds whatever byte range was actually fetched. A lookup hits when the
stored run covers the requested length and misses when it is shorter, in which
case the extent is refetched at the larger size and replaces the smaller one —
so for a given `(object, offset)` the cache converges on the largest range any
reader has asked for. Nothing is rounded up, so nothing is over-fetched.

This is the property paged blocks could not deliver: a 4KB footer read costs 4KB.

### 3. `stream_id` interns the `ObjectId` alone, and the version is not in the key

Carrying object identity literally in every extent key is expensive — an
`ObjectId` is a backend discriminant plus two `String`s — so it interns to one
`u64` on first sight. An extent key is then two integers. Ids are retired rather
than recycled, so a key holding a released id can never alias a live object.

The `Version` is deliberately **not** part of that identity, and this is the one
place where the async worker's coherence is weaker than the block worker's.

`talon-worker` puts the ETag in its `BlockId`, so republishing an object at the
same path yields a different key and the superseded bytes are unreachable with
no invalidation step at all. Keying on `(ObjectId, Version)` here would give the
same property. It was rejected because the price is paid on every read of every
object — a `Version` clone and hash on the interning path, plus a version string
per stream in every checkpoint (section 9) — to defend against something that
does not happen to the objects this worker caches. Analytics files are written
once and read many times; a partition is replaced by writing a new path, not by
overwriting an old one.

So immutability is a **performance assumption backed by a bounded check**, not a
correctness requirement:

- A republish reuses the stream id, so the previously cached extents stay
  reachable. Nothing about the overwrite makes them go away.
- The runtime re-HEADs an object whenever its cached version resolution passes
  the version TTL (60s by default), and when the origin rejects a ranged GET
  mid-read. Either path compares the ETag it got against the one it held, and on
  a difference purges every extent of that object before serving.
- Staleness is therefore bounded by the version TTL rather than eliminated. A
  reader inside that window sees the old bytes.

`talon_async_worker_republish_purges_total` counts the purges. It is expected to
stay at zero; a non-zero value says something is overwriting objects in place,
and that reads of those objects were served stale for up to one TTL. Deployments
that cannot accept that window should lower the version TTL, which trades HEAD
volume for a shorter window, or route the traffic to a block-worker pool, which
removes the window entirely.

Section 7's restart behaviour follows from this too: whatever an async worker
recovers across a restart is subject to the same TTL-bounded check as a live
entry, because the version cache does not survive the restart and the first read
of an object re-HEADs it.

### 4. Two tiers: a CLOCK memory tier over a region-packed NVMe tier

**L1 (DRAM)** is sharded, byte-bounded, and evicts with a CLOCK sweep over a
dense ring, scoring entries by `age / (1 + uses)` against a threshold recalibrated
from a periodic sample rather than a full scan. Concurrent loads of the same key
coalesce on a watch channel, so N readers missing together produce one origin
fetch. Each entry counts its own hits, which section 5 needs.

**L2 (NVMe)** packs extents into pre-allocated shard files divided into 64MB
regions, addressed by `ExtentRun { region, offset_in_region, size, checksum }`.
Reclamation picks the coldest regions by decayed read volume and frees them
wholesale.

Region packing rather than one file per extent, because a file per extent means
an inode and an `open` per cached range. It also *improves* zero-copy rather
than trading it away: `sendfile(region_fd, offset, len)` works on a region file
exactly as on a `.blk`, and extents that are adjacent in an object are packed
adjacently, so a multi-extent read can coalesce into one call.

The cost is that a region is the unit of reclamation, so evicting one discards
every extent packed into it regardless of individual temperature. Decay scoring
makes that choice well; the residual imprecision is accepted.

Every region carries a pin count taken for the duration of a read and released
when the reader's guard drops. Reclamation skips pinned regions, so a write
packing a new extent can never land on a region an in-flight `pread` or
`sendfile` is reading from.

### 5. NVMe admission is frequency-gated when L1 is enabled

An extent is written to NVMe only after accumulating a minimum number of L1
hits; extents below the threshold are dropped when they leave L1. A single scan
over cold data therefore cannot evict a genuinely hot working set from NVMe —
which matters precisely because this worker targets mixed selective and scanning
traffic on the same files.

When L1 is disabled there is no hit history to gate on, and extents are admitted
on first miss. Gating unconditionally would mean an L1-less deployment never
writes to NVMe at all.

### 6. A separate rendezvous ring, keyed on the object

Two disjoint rings run behind one coordinator, selected by node role. There is
no placement-class enum: the ring *is* the distinction, and a second enum
shadowing `NodeRole` one-to-one would only be a synonym to keep in sync.

| Ring | Node set | Hash key |
|---|---|---|
| `RendezvousPlacement` | `NodeRole::Worker` | the whole `BlockId` |
| `ObjectPlacement` | `NodeRole::AsyncWorker` | the object identity alone |

**What the async ring drops.** `BlockId` has four fields — object, offset, block
size, and version. The async ring hashes only the object identity (backend,
bucket, path). The key is therefore "which name is being read", with no notion
of position and no notion of which revision.

*Dropping the offset* is what buys whole-object affinity. A columnar reader
fetches a footer and then cherry-picks column chunks at unrelated offsets; on the
block ring those hash apart, so the footer fetch warms a cache nobody else reads
and every chunk pays a cold miss. On the async ring they all resolve to one
worker, and one reader's fetch warms the next reader's hit. This is the whole
reason the ring exists.

*Dropping the version* keeps the object where it already is. With the version in
the placement key, republishing under a new ETag relocates the object: the new
owner starts cold while the previous owner still holds every warm extent of the
old revision, and nothing is gained by the move — coherence is handled a layer
down, in the worker's own cache (§3), which purges an object's extents when it
observes a new ETag. Placement stays put and the purge runs wherever the object
already lives.

**Why the client picks the ring.** The coordinator could infer it — by file
extension, by read size, by a per-prefix rule — but each of those is a policy
guess made by the process with the least information. The caller knows whether
it is about to read a Parquet footer or stream a whole partition; the
coordinator sees only a byte range. Letting the client name the ring keeps the
coordinator a pure placement function and puts the choice where the knowledge
is.

**Compatibility.** The ring travels as a `Ring` enum — `Block` (the default) or
`Async` — carried on a *new* message, `RingPlacementLookup { block, ring, k }`,
at `CONTROL_SCHEMA_VERSION = 5`.

The enum is what a third ring should cost: one value, not a message variant plus
a dispatch arm at every call site. But it rides a new message rather than being
added to `PlacementLookup`, because bincode 1.3 is positional and not
self-describing. A `ring` field between `block` and `k` shifts `k` four bytes
along; an old client's frame then runs out of buffer where the new coordinator
expects a tag, and a new client's frame leaves four trailing bytes an old
coordinator never reads. `#[serde(default)]` cannot rescue it — there is no
field name on the wire to be absent. With `MIN_CONTROL_SCHEMA_VERSION = 1` still
in force, that would break the FUSE mount, the Python client, the CLI, and the
Java client, which is pinned at schema 2.

So `PlacementLookup` keeps its schema-1 encoding, byte for byte, and its
meaning: the block ring. `RingPlacementLookup { ring: Block }` is defined to be
the same request, and is required to place identically — clients migrate one at
a time, and a fleet that split across two owner sets for the same block would
double every worker's cache footprint. Both messages answer with the same
`PlacementResponse`, so a client learns one reply shape.

The schema-5 floor applies to the new message on *either* ring, including
`Block`. The fence is on the encoding, not on the ring: a v3 coordinator cannot
decode the message at all, and must fail rather than fall back to the block
lookup — on the async ring that fallback would return a block worker, which
holds no extents. The Java client only ever receives `PlacementResponse`, so it
needs no change.

`Ring` is `#[non_exhaustive]` and its variants are append-only, since the
discriminant is the wire encoding. A coordinator that receives a ring it
predates answers with no owners rather than falling through to the block pool,
for the same reason.

**Why the rings are disjoint rather than one ring with two key functions.** The
coordinator filters placement candidates by role, so an async worker either
never reaches membership at all or silently joins the block ring and receives
blocks it cannot serve — failing at read time rather than at lookup time.
Splitting by role also means an async lookup against a cluster with no async
workers answers "no owners" instead of quietly handing back a block worker.

**Reversal of the original decision.** The first revision of this ADR routed by
asking for `BlockId { object, offset: 0, .. }` as a client-side convention and
deferred the split, calling it "a new `NodeRole` variant, a schema bump, and
placement that models two pools ... deferred until there is a deployment that
wants it." Pinning the offset to zero does approximate the key this ring
settled on, but as a convention it is unenforceable — nothing stops a client
from passing its real offset, and the failure is silent cache fragmentation
rather than an error. It also leaves the version in the hash, so an object still
migrates on every republish. And it does nothing about the pool: the coordinator
filters placement candidates by role, so an async worker would still either miss
membership entirely or join the block ring. All three are now properties of the
ring instead of hopes about client behaviour.

**What the split costs.** One very large object is served by one async worker
rather than spread across the fleet. That is the right trade for
many-small-objects columnar workloads and an argument for sizing the async pool
separately, not for merging the rings.

### 7. The NVMe tier is checkpointed for warm restart

Run descriptors are what make packed extents addressable, so a shard opened
without them holds 64 MiB regions of perfectly good bytes that nothing can name.
Each shard therefore writes a checkpoint of its entry map — three files, as in
Velox's `SsdFile`, which solves this for the cache this tier is modelled on:

```text
extents_N.bin       the region file
extents_N.bin.cpt   the entry map, the stream names it refers to, region scores
extents_N.bin.log   regions reclaimed since that checkpoint
```

**The format is hand-rolled**, not `serde`: fixed little-endian records behind a
magic, an explicit version, an xxh3 digest, and an end marker. ADR 0003 §9.4
already rules out `serde`/`bincode`/JSON as durable on-disk formats, and the
reason applies here too — a derive-driven layout changes silently when a field
is added, and this file is read by a future binary that need not match the one
that wrote it. An explicit version makes that a rejection rather than a
misparse.

**The write ordering** is Velox's, with one change:

1. `fsync` the region file, so the checkpoint never names bytes that are not
   durable;
2. write, `fsync`, and **rename** the checkpoint — Velox truncates in place;
   renaming keeps the previous checkpoint valid until the new one is complete;
3. clear the eviction log, whose records the new checkpoint supersedes.

Both crash windows leave a consistent pair: the old checkpoint with a full log,
or the new checkpoint with a stale-but-harmless one, since replaying evictions of
regions the new checkpoint already excludes is idempotent.

**The eviction log is what makes a stale checkpoint safe.** Reclaiming region R
and packing new extents into it invalidates every checkpointed entry naming R,
but the checkpoint is only rewritten periodically. The log records the
reclamation and is `fsync`ed *before* the region is reused — one sync of a few
bytes per 64 MiB reclaimed. Velox leaves this gap to the extent checksum; that
only covers the checksums-on, non-zero-copy path, which is not a guarantee worth
resting correctness on for the cost of a sync this rare.

**Recovery never fails a startup.** A checkpoint that is absent, torn,
digest-mismatched, from a different format version, or written under a different
capacity or checksum mode means that shard starts cold and truncates. A cache
that refuses to start is worse than one that starts empty. Entries in
logged-evicted regions are dropped, and stream ids are rebound through a
recovery-wide table that rejects an id claimed by two objects — an id resolving
to the wrong object is a cross-object read, which is the one failure this cache
must not have.

`checkpoint_interval_bytes` (default 64 MiB, `0` disables) is byte-triggered
rather than timed, so the cost tracks how much there is to lose and an idle shard
never rewrites a checkpoint it has not invalidated. A crash discards whatever was
written since the last checkpoint; that window is the knob.

The **L1 DRAM tier is not persisted**, as in Velox. It is refilled from L2 on
demand, and writing it down would cost more than the promotion it saves.

The residual limitation is upstream of all this: admissions stage in memory and
reach disk only once 4 MiB has accumulated, with no timer and no shutdown drain.
On light selective-read traffic the disk tier stays empty and a checkpoint
records nothing, so warm restart only benefits deployments whose admission volume
regularly crosses that threshold.

### 8. The async worker is read-only

`talon-worker` accepts writes. `WorkerRuntime::write_object` PUTs to the origin,
treats that acknowledgement as the durability point, and only then commits the
bytes to the local cache; it is reachable over the wire on both transports. ADR
0002 fixed that contract as write-through only.

The async worker rejects the write op with `Error::Unsupported`. Write traffic
goes to a `talon-worker` pool.

The reason is that a write is the one operation an extent cache gains nothing
from. This cache exists to avoid over-fetching on *selective* reads; a write
arrives as a whole object body, which is the shape the block worker already
handles well. Supporting it would mean duplicating the durability sequencing —
PUT first, cache second, never cache a failed write — in a second place, for no
benefit beyond making one read-after-write a hit instead of a miss.

The cost is that an async worker is not a drop-in replacement for a block
worker, only for the read half. A deployment that writes through Talon needs
both pools, and clients must route accordingly. Since section 6 already gives
async workers their own cluster, that routing decision exists regardless.

This also means invalidation has no *write*-path caller. `invalidate_object` is
nevertheless on the serve path, called from the version-TTL refresh described in
section 3: with the version out of the cache key it is what bounds staleness
after a republish, so it is load-bearing rather than housekeeping. A future
control-plane invalidation notification would call the same entry point to
reclaim the space immediately instead of waiting for the TTL.

## Consequences

### Positive

- A selective read fetches what it asked for and nothing more. Directly
  measurable as origin bytes fetched, which is how section 1's claim is tested.
- No fixed granularity to tune, and therefore no page size to get wrong.
- The data-plane protocol, conformance vectors, and both SDK clients are
  untouched. An async worker is wire-compatible with existing clients.
- `talon-worker` is not modified, so nothing regresses for sequential workloads,
  and the two are directly comparable on the same benchmark.
- Region count rather than extent count bounds eviction cost, inode pressure,
  and open-file count.

### Costs and risks

- **A second worker implementation to maintain.** Two serve paths, two configs,
  two sets of metrics. This is the main cost and it is not small.
- **Region-granular reclamation is imprecise.** A hot extent packed into an
  otherwise cold region is discarded with it.
- **Variable-length extents fragment** where uniform pages would pack cleanly.
  An extent that grows on refetch leaves its smaller predecessor stranded until
  the region is reclaimed.
- **Object routing concentrates load.** One very large, very hot object is
  served by one worker.
- **Warm restart depends on the 4 MiB staging trigger being crossed**, per
  section 7. Light selective-read traffic never fills the disk tier, so there is
  nothing for a checkpoint to record.
- **Coherence is TTL-bounded, not absolute.** Unlike the block worker, a
  republished object can be served stale for up to one version TTL, per
  section 3. This is the one guarantee the async worker gives up.
- **Reads only.** An async worker cannot replace a block worker outright; a
  deployment that writes through Talon runs both pools, per section 8.
- **Operators must choose** which worker fits a workload; a wrong choice is a
  performance cliff rather than an error.

## Rejected alternatives

### Paged blocks in the existing worker

The `DESIGN.md` plan, and the first approach attempted here. Rejected because it
keeps the block as the unit of accounting and addressing while adding a second
granularity underneath it, and because the fixed page size reintroduces
over-fetch. It also required correcting three latent defects to become safe: the
eviction unlink path silently discards page victims, superseded reclamation
skips them, and an LRU touch on a paged block is a no-op. An extent cache that
owns its own reclamation makes those unreachable instead of fixed.

### A mode flag inside `talon-worker`

One binary, one crate, a flag selecting the cache. Rejected per section 1: the
runtime would branch on cache model throughout, and the two eviction policies
would share one capacity budget with no coherent way to divide it.

### Extending the wire protocol with a per-request access-pattern hint

Let clients declare selective versus sequential *intent* per request, and have
the coordinator or worker adapt. Still rejected, and note this is a different
thing from the ring selection in §6: the ring names **which pool** answers,
which the caller knows unambiguously, while a hint describes **how the caller
will read**, which is a prediction. A wrong ring is a routing error the client
can correct; a wrong hint silently degrades the cache. Choosing a ring already
carries the useful part of the signal, and a finer hint can be added later
without changing the storage design.

### Block-level placement for the async worker

Keeps fleet-wide spread for large objects. Rejected for this worker because it
scatters a single columnar file's many small reads across the fleet, so every
worker holds a cold partial copy of the same file — the opposite of what a
footer-then-column-chunk pattern wants. It would also put the version back in
the key, relocating an object on every republish.

### A `PlacementClass` enum carried on the lookup

An earlier revision of this change added `PlacementClass { Block, Extent }` as a
request field selecting both the pool and the key function. Dropped: the enum
mapped one-to-one onto `NodeRole`, so it was a synonym that every call site had
to thread through and keep in sync, and it pushed a `class` parameter into the
`Placement` trait that only one implementation ever branched on. Two ring types
over two role-filtered node sets say the same thing with no new vocabulary, and
the message variant selects the ring without adding a wire field.

## Implementation sequence

1. The crate, and the L1 memory tier: sharded CLOCK eviction, watch-channel load
   coalescing, per-entry hit counts, an eviction sink.
2. The L2 region tier: shard files, regions, run descriptors, pin counts, decay
   scoring, batched writes, optional checksums, and `ObjectId` interning.
3. The tiered facade: L1 over L2 over backend, with frequency-gated admission
   and the L1-disabled fallback.
4. The serve path on the existing wire protocol, the ring split, the binary,
   configuration, and observability.
5. Benchmarks on the selective-read shape, measuring origin bytes fetched rather
   than latency alone, and documentation.

Step 5's comparison is not a head-to-head against a running `talon-worker`. It
replays one Parquet-shaped trace at two granularities over the same cache and
origin, so the fetch unit is the only variable — which is the one thing the two
designs disagree about. Comparing the two binaries on wall-clock latency would
need a real object store and would mostly measure everything they have in
common.

## References

- `DESIGN.md` section 3, block materialization — the plan this supersedes
- `docs/reference/wire-protocol.md`, for the data plane's block-free shape
- `docs/use-cases/analytics.md`, on choosing between the two workers
- `docs/operations/async-worker.md`, for running one
- ADR 0002, for the write-through contract this worker inherits unchanged
