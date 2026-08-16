# Model checkpointing

## The problem

Checkpointing is the inverse of training's read pattern, and it is bursty:

- **Writes are large and synchronised.** A job checkpoints from every rank at
  once, and the origin absorbs the whole burst while training stalls.
- **Restores are latency-critical.** After a failure, the fleet reads the same
  checkpoint simultaneously — a thundering herd against one prefix.
- **Reads are often partial.** Inspecting a checkpoint's metadata or a single
  tensor means reading a footer or a byte range, not the whole file.

## What Talon does

**Write-through to the origin, cached on write.** A `Put` streams the object to
the backing store and caches it locally in the same operation. Durability is the
object store's, unchanged — but the bytes are already resident for the read that
usually follows.

**Serves the restore stampede from cache.** When every rank reads the same
checkpoint, the first read populates the cache and the rest are NVMe hits.
Concurrent misses for the same block are deduplicated, so a stampede produces
**one** origin fetch, not one per reader.

**Range reads without whole-file transfer.** `GET_RANGE` is served with
`sendfile` at an offset, so once a block is resident, reading a checkpoint
footer transfers the footer — not the checkpoint, and not through userspace.
Note that by default the *first* touch of a block fetches the whole block from
the origin; see
[the granularity note](./analytics.md#block-granularity-whole-blocks-by-default-pages-on-request).

**Version-correct caching.** Blocks are keyed by the object's real ETag, and the
worker sends a conditional `If-Match` on the miss path. Overwriting a checkpoint
at the same key produces a new version, and readers do not get served the old
bytes.

## Practical notes

- **Write path is write-through, not write-back.** Talon does not buffer writes
  and flush later; a `Put` returns after the origin commits. This bounds the
  failure mode (no unflushed data to lose) but means Talon does not accelerate
  the write itself — it removes the *read-back* cost.
- **Checkpoint restore is the good case.** Whole-block materialisation pairs
  with readahead for a sequential restore, which is what a full checkpoint load
  does. Workloads that only ever poke at footers over-fetch on first touch.
- **Zero-copy ingest has a tradeoff.** The `splice`-based ingest path never
  brings bytes into userspace, which also means a streaming checksum cannot be
  computed on that path. The loader path, which downloads into memory, does
  checksum.
