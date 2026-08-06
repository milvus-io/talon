# Analytics and shuffle

## The problem

Query engines read columnar data — Parquet, Lance, ORC — with an access pattern
object stores handle poorly:

- **Reads are partial and structured.** A query reads a footer to find row
  groups, then specific column chunks. Whole-file transfer is waste.
- **Many small ranged reads.** Each is a separate request with full round-trip
  latency, and per-request cost adds up faster than bytes do.
- **Hot files are read by every worker.** Dimension tables and recent partitions
  are read by the whole cluster, repeatedly.

## What Talon does

**Serves byte ranges without whole-object transfer.** `GET_RANGE` maps directly
onto `sendfile` with an offset and length, so once a block is resident, reading
a footer moves the footer — not the file, and not through userspace.

**Deduplicates the hot-file stampede.** When every query worker reads the same
dimension table, concurrent misses for a block collapse to a single origin
fetch rather than one per reader.

**Caches by object version.** Blocks are keyed by the origin's ETag, so
republishing a partition at the same key does not serve stale data to a query.

**Reachable from a JVM engine without a mount.** The
[Java client](../clients/java.md) is a native-free jar, so a query engine can
read ranges directly rather than through a privileged FUSE mount.

## Shuffle: read the caveat

Shuffle intermediates are a **partial fit**, and it is worth being direct about
why:

Talon's design centres on large, immutable, repeatedly-read objects. Shuffle
data is written once, read once or twice, and then discarded. That inverts two
of the three assumptions — reuse is low and lifetime is short — so the cache
stores bytes it will not serve again, and eviction pressure rises without a
corresponding hit-rate gain.

Where it *does* help is the case of shuffle data read by more than one consumer,
or re-read after a task retry: the second read is local, and the write-through
path means the data is already in cache when that happens. Where it does not
help is the common single-consumer case, which is better served by local
scratch.

If shuffle is the primary workload, measure hit rate before committing to it.
The per-worker metrics exposed to Prometheus make this straightforward.

## Block granularity, and the worker that avoids it

The default worker materialises blocks **whole**. A read that touches a block
fetches the entire block — 256 MiB at the default size — even if the query
wanted a few kilobytes of one column chunk. For a sequential scan that is
exactly right, and the cost is amortised on first touch. For a query engine
reading a Parquet footer and then cherry-picking column chunks it is not: a few
kilobytes of useful data costs a 256 MiB transfer, and the next column chunk
costs another one.

That shape of read is common enough in analytics to have its own worker.
**`talon-async-worker`** caches variable-length extents — the exact ranges asked
for — with no block concept at all, so a 4 KiB footer read costs 4 KiB. On a
Parquet-shaped read trace the difference is the whole point:

```
bytes actually needed by the reads:   6.06 MiB
extent granularity:                   6.06 MiB
block granularity (4 MiB blocks):   256.00 MiB
```

(That comparison uses a 4 MiB block so the benchmark runs quickly; the gap
widens with the real 256 MiB size. `cargo bench -p talon-async-worker`.)

It is an addition rather than a replacement, and the choice is per workload:

| Access pattern | Worker |
|---|---|
| Full-partition scans, sequential reads | `talon-worker` |
| Footer reads, column-chunk projection, point lookups | `talon-async-worker` |
| Writes | `talon-worker` — the async worker is read-only |

Async workers register on a **separate placement ring** keyed on the object, so
every range of one file lands on the same node and one reader's footer fetch
warms the next reader's chunk read. Clients opt in per request; the coordinator
does not guess. Two consequences to plan for: an async worker's NVMe tier is
cold after a restart, and one very large hot object is served by one node rather
than spread across the fleet.

See [the async worker guide](../operations/async-worker.md) for how to run it,
and ADR 0005 for why it is a separate worker rather than a mode.

Practical consequence for the default worker: **it suits analytics workloads
that scan more than they seek.** Full-partition scans, repeated reads of hot
dimension tables, and range reads that align with block boundaries all benefit
from it directly. Highly selective access scattered across a large file belongs
on the async worker.

## Practical notes

- **Tune block size to the access pattern**, or change worker. The 256 MiB
  default targets sequential scans. A smaller block size trades bookkeeping for
  less over-fetch and is configurable per worker; for genuinely selective reads
  the async worker removes the tradeoff instead of shifting it.
- **Eviction is byte-accounted with reader pinning.** A block being streamed by
  an in-flight `sendfile` is never evicted underneath the reader.
- **Watch hit rate, not throughput.** For analytics the interesting metric is
  whether the working set fits; capacity and hit rate are both exported to
  Prometheus.
