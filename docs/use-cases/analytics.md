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

## Block granularity: whole blocks by default, pages on request

By default a worker materialises blocks **whole**. A read that touches a block
fetches the entire block — 256 MiB at the default size — even if the query
wanted a few kilobytes of one column chunk. For a sequential scan that is
exactly right, and the cost is amortised immediately. For sparse random access
across a large file it is not: the first touch of each block pays a full block
fetch.

For that second shape, a worker can cache **per page** instead. Set
[`l2_page_size_bytes`](../reference/configuration.md#l2_page_size_bytes) to a
non-zero page size and a block is materialised page by page against a present
bitmap: a range touching absent pages fetches only those pages' byte ranges
from the origin, not the whole block. Each present page is its own file on
disk, served by its own `sendfile`, and both miss dedup and LRU accounting
descend to `(block, page)` granularity.

The setting defaults to `0`, which keeps the whole-block behaviour above. Two
constraints apply when you enable it: the page size must divide `block_size`,
and when the L1 DRAM cache is also enabled `l1_page_size_bytes` must equal
`l2_page_size_bytes` (L1 is filled one L2 page at a time, so mismatched sizes
would leave L1 entries unfillable). The worker validates both at startup.

Practical consequence: **with the default settings Talon suits analytics
workloads that scan more than they seek.** Full-partition scans, repeated reads
of hot dimension tables, and range reads that align with block boundaries all
benefit as shipped. For highly selective point lookups scattered across a large
file, enable paged caching so a lookup costs one page rather than 256 MiB.

## Practical notes

- **Tune block size to the access pattern.** The 256 MiB default targets
  sequential scans. A smaller block size trades bookkeeping for less
  over-fetch on selective workloads, and is configurable per worker.
- **Eviction is byte-accounted with reader pinning.** A block being streamed by
  an in-flight `sendfile` is never evicted underneath the reader.
- **Watch hit rate, not throughput.** For analytics the interesting metric is
  whether the working set fits; capacity and hit rate are both exported to
  Prometheus.
