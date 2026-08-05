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

### 3. `stream_id` interns `(ObjectId, Version)` together

Talon's cache coherence rests on the origin ETag being part of the cache key, so
republishing an object at the same path yields a different key and stale bytes
are unreachable with no invalidation protocol. A key of `(object, offset)` alone
would be weaker than what Talon already promises.

Carrying the identity literally is expensive — an `ObjectId` is a backend
discriminant plus two `String`s, and a `Version` is a third — so
`(ObjectId, Version)` interns to one `u64` on first sight. A new ETag allocates
a new id, so every extent of the superseded version becomes unreachable at once.
Ids are retired rather than recycled, so a stale key can never alias a live
object. Interned entries are released when a version is superseded, so churn
does not grow the table without bound.

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

### 6. Placement routes whole objects, with no coordinator change

Clients route by asking placement for `BlockId { object, offset: 0, .. }`
regardless of the read offset, so every read of one object version resolves to
the same worker. A columnar reader making many small scattered reads across one
file talks to one worker and warms one cache, instead of scattering across the
fleet by block.

This is a client-side convention, not a protocol change: the control message,
the schema version, and the conformance vectors are all untouched, and the
coordinator's rendezvous hashing is used exactly as it already works.

The tradeoff is losing the spread that block routing gives for very large
objects — one enormous object is served by one worker rather than distributed.
That is the right trade for many-small-objects columnar workloads and the wrong
one for a few enormous ones, which is a reason to run this worker as its own
deployment rather than mixed into a block-routed fleet.

For the first implementation, async workers form their own cluster. Mixing
block-routed and object-routed workers behind one coordinator would need a new
`NodeRole` variant, a schema bump, and placement that models two pools; that is
deferred until there is a deployment that wants it.

### 7. The NVMe tier is cold after restart

Run descriptors live only in memory, so shard files are truncated on open.

`talon-worker` rebuilds its index from `.meta` sidecars and is warm immediately;
this worker is not. Persisting run descriptors means writing and fsyncing a
manifest, validating it against region contents on load, and handling a torn
manifest — meaningful machinery to add before the read path has demonstrated its
value. The consequence is bounded: a restarted worker refetches on demand, one
extent at a time rather than one 256MB block at a time. Cold, not incorrect.

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

This also means the invalidation entry points (`invalidate_superseded`,
`invalidate_object`) have no caller on the serve path. They stay because
correctness does not depend on them — a republished object interns a new
`stream_id` and cannot reach the old version's extents either way, per section 3
— but they let a future control-plane notification reclaim the space instead of
waiting for region reclamation to find it.

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
- **Cold restart** for the NVMe tier, per section 7.
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

### Extending the wire protocol with a load hint

Let clients declare selective versus sequential intent per request. Rejected as
premature: it needs a new field, a `CONTROL_SCHEMA_VERSION` bump, regenerated
conformance vectors, and matching Python and Java client changes, before any of
this is proven. Choosing a worker at deploy time carries the same information
today, and a hint can be added later without changing the storage design.

### Block-level placement for the async worker

Keeps fleet-wide spread for large objects. Rejected for this worker because it
scatters a single columnar file's many small reads across the fleet, so every
worker holds a cold partial copy of the same file — the opposite of what a
footer-then-column-chunk pattern wants.

## Implementation sequence

1. The crate, and the L1 memory tier: sharded CLOCK eviction, watch-channel load
   coalescing, per-entry hit counts, an eviction sink.
2. The L2 region tier: shard files, regions, run descriptors, pin counts, decay
   scoring, batched writes, optional checksums, and `(ObjectId, Version)`
   interning.
3. The tiered facade: L1 over L2 over backend, with frequency-gated admission
   and the L1-disabled fallback.
4. The serve path on the existing wire protocol, the binary, configuration, and
   observability.
5. Benchmarks against `talon-worker` on the selective-read shape, measuring
   origin bytes fetched rather than latency alone, and documentation.

## References

- `DESIGN.md` section 3, block materialization — the plan this supersedes
- `docs/reference/wire-protocol.md`, for the data plane's block-free shape
- `docs/use-cases/analytics.md`, current limitation: block granularity
- ADR 0002, for the write-through contract this worker inherits unchanged
