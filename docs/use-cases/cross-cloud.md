# Cross-cloud and remote data

## The problem

Data and compute increasingly live in different places — a dataset in S3, GPUs
in another cloud, an on-prem cluster reading from a cloud bucket. Reading across
that boundary is the worst case for an object store:

- **WAN latency on every read.** Round trips are tens of milliseconds, not
  single digits, and they land in the critical path of every fetch.
- **Egress is billed per byte, every time.** Re-reading the same object across
  clouds is the single most expensive way to move data.
- **Bandwidth is contended and variable**, so throughput is unpredictable in a
  way local storage is not.

Each of these gets multiplied by the number of readers.

## What Talon does

**Pays the crossing once.** The first read pulls the object across the boundary
and commits it to local NVMe. Every subsequent read — by any worker in the
cluster — is local. For a dataset read by a fleet, this turns N crossings into
one.

**Speaks all three major backends.** S3, GCS, and Azure Blob are implemented
behind one `BackendStore` trait, so the same cluster can front buckets in
different clouds, and the FUSE namespace exposes them side by side under
`/s3`, `/gcs`, and `/az`.

**Supports custom endpoints.** Endpoint overrides and path-style addressing
allow S3-compatible stores and emulators, so this is not restricted to the three
public clouds.

**Isolates the slow path.** Backend fetches run on a dedicated loader pool with
bounded concurrency, never on the data-plane runtime. A burst of misses against
a slow remote origin applies backpressure instead of stalling readers that are
hitting cache.

**Prewarms deliberately.** The coordinator's LOAD path can pull a prefix into
the cache before a job starts, so the WAN crossing happens on a schedule rather
than in the critical path of the first read.

## Practical notes

- **This is a read-through cache, not a replication tool.** Talon does not sync
  buckets or maintain a mirror; it caches what is actually read. If you need a
  full copy in the second location regardless of access, use a transfer service.
- **Egress is saved on re-reads, not first reads.** The saving is proportional
  to reuse. A workload that reads each object exactly once saves nothing.
- **Latency simulation is available for testing.** The
  [latency lab](../testing/latency-lab.md) injects modelled backend latency, so
  cross-region behaviour can be exercised without a cross-region bill.
