# Model training

## The problem

A training job reads the same dataset every epoch, and a fleet of workers reads
overlapping shards of it. Reading from the object store each time means:

- **Latency in the critical path.** Every batch fetch is a network round trip to
  the origin, and GPUs idle while it completes.
- **Repeated egress and request cost.** The same bytes are billed on every
  epoch, and again for every worker that reads them.
- **Throughput capped by the origin**, not by local hardware — object stores
  throttle per-account and per-prefix.

The data itself is immutable: a dataset version is written once and read many
times. That is exactly what a cache is for.

## What Talon does

**Serves repeated reads from local NVMe.** The first read of a block pulls it
from the origin and commits it to a worker's cache; subsequent reads across the
fleet are served from NVMe over the data plane. Epoch two onwards does not touch
the origin.

**Keeps bytes out of userspace.** A cache hit is served with `sendfile(2)`
straight from the block file into the socket — the block is never copied into a
buffer on the way past. See [Data-plane runtime](../explanation/data-plane-runtime.md).

**Scales cache capacity with the fleet.** Blocks are placed across workers by
rendezvous hashing, so adding a worker adds capacity and moves a minimal share
of existing placements. A dataset larger than one node's NVMe is still fully
cacheable.

**Reads ahead on sequential scans.** The FUSE client detects sequential access
and prefetches subsequent blocks, so a scan is not serialised on per-block
misses.

**Requires no application change.** Training code opens files under the FUSE
mount; the framework's existing data loader works unmodified.

**Runs on the training node without competing for it.** A worker can sit on the
same host as the job: one CPU and ~7 MB of RSS serves ~49k range reads per
second, and its CPU cost is pinned rather than elastic. See
[Colocated and sidecar deployment](./colocated.md).

## Practical notes

- **Block size matters.** The 256 MiB default suits large shard files. Datasets
  made of many small files see less benefit — the cost is per-object bookkeeping
  rather than transfer.
- **Prewarm before the job starts.** The coordinator's LOAD path can pull an
  object's blocks into the cache ahead of time, so epoch one is also a hit
  rather than paying the miss penalty across the fleet.
- **Capacity planning.** Total cache should exceed the working set, or eviction
  will churn the dataset out between epochs. Per-worker capacity and usage are
  visible in the management console and in Prometheus metrics.
