# Use cases

Talon is built for one shape of problem: **the same large, immutable objects are
read repeatedly by compute that is not co-located with the storage.** Object
stores are durable and cheap, but every read crosses a network boundary and is
billed per request. When a fleet of GPUs or query workers reads the same
dataset, the origin becomes both the latency floor and the cost centre.

Talon puts a shared NVMe cache between them. The pages below describe the
workloads this helps, what specifically Talon does for each, and — where support
is partial — what it does not do yet.

## When Talon helps

The pattern to look for is **repeated reads of immutable data across a fleet**:

- The same objects are read more than once, by more than one machine.
- Objects are large enough that transfer time dominates request overhead.
- Data is written once and not mutated in place — new versions get new keys.
- Compute is elastic, so re-reading from origin on every scale-up is wasteful.

## When it does not

Talon is a cache, not a storage system, and being explicit about the boundary
saves disappointment:

- **Small-object, high-fanout metadata access.** The design targets large
  sequential reads; a workload dominated by kilobyte objects pays cache
  bookkeeping without recovering it in transfer savings.
- **Read-modify-write on cached data.** Writes go through to the origin
  (write-through); Talon is not a write-back buffer and does not merge partial
  updates.
- **Strong cross-client consistency.** Cache coherence is by object version
  (ETag). A reader holding an older version keeps reading it until its cached
  entry is invalidated — correct for immutable data, wrong for mutable state.
- **Single-read workloads.** If each object is read exactly once, a cache adds a
  hop and stores bytes nobody asks for again.

## Use cases

- [Model training](./training.md) — feeding a GPU fleet from a shared dataset.
- [Checkpointing](./checkpointing.md) — writing and restoring model state.
- [Notebooks and data sharing](./notebooks.md) — interactive access to shared data.
- [Cross-cloud transfer](./cross-cloud.md) — reading data that lives in another cloud.
- [Analytics and shuffle](./analytics.md) — columnar scans and intermediate data.
