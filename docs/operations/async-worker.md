# The async worker

A second worker type that caches **extents** — the exact byte ranges clients
asked for — instead of fixed 256 MiB blocks. Run it when your reads are
selective: Parquet, Lance, or ORC files where a query reads a footer and then
cherry-picks a few column chunks.

It is an addition, not a replacement. It serves reads only. A deployment that
writes through Talon runs both pools. The design and the tradeoffs it accepts
are in [ADR 0005](../explanation/adr-0005-async-worker-extent-cache.md); this
page is how to run it.

## When it helps, and when it does not

| Workload | Which worker |
|---|---|
| Full-partition scans, training data, checkpoint restore | `talon-worker` |
| Parquet/Lance footer reads, column-chunk projection, point lookups | `talon-async-worker` |
| Writes of any kind | `talon-worker` — the async worker refuses them |
| One very large, very hot object read by the whole fleet | `talon-worker` — the async ring pins it to one node |

The difference is over-fetch. A block worker materialises a whole block on any
touch, so a 4 KiB footer read costs a 256 MiB transfer and the next column chunk
costs another one. The async worker fetches 4 KiB.

Measured on a 16-file Parquet-shaped read trace (footer, footer metadata, six
column chunks per file), with everything held constant except the fetch unit:

```
bytes actually needed by the reads:   6.06 MiB
extent granularity:                   6.06 MiB
block granularity (4 MiB blocks):   256.00 MiB
```

That comparison uses a 4 MiB block so the benchmark runs in milliseconds; the
gap widens with the real 256 MiB block size, bounded by the object size. Run it
yourself with `cargo bench -p talon-async-worker` — the numbers print before the
latency table.

The cost is on the other side of the same coin: because placement pins a whole
object to one worker, a single enormous hot object is served by a single node
rather than spread across the fleet. Size the async pool for object count and
read concurrency, not for total bytes.

## Placement: a second ring

Async workers sit on their own rendezvous ring, disjoint from the block ring.

| Ring | Node role | Hash key |
|---|---|---|
| block | `worker` | the whole `BlockId` — object, offset, size, version |
| async | `async_worker` | the object identity alone |

Dropping the offset is the point: every range of one file resolves to the same
worker, so one reader's footer fetch warms the next reader's column-chunk read.
On the block ring those hash apart and each chunk pays a cold miss somewhere
else.

**The client chooses the ring.** The coordinator sees only a byte range and
would have to guess; the caller knows what it is about to read. With the CLI
client:

```sh
talon-client --ring async --path /s3/warehouse/sales/part-00000.parquet \
  --offset 536866816 --len 4096
```

A lookup never crosses pools. If no async worker is registered, an async lookup
returns **no owners** rather than quietly handing back a block worker that holds
no extents.

## Current limitations

This worker runs end to end and the over-fetch claim is measured, but it is not
yet something to put in front of production traffic. Four gaps, in the order
they bite.

**Only the CLI can reach the async ring.** `talon-client --ring async` is the
one thing that sends a ring-aware lookup. The FUSE mount and the Python
bindings are the same Rust code and send `PlacementLookup`, so they always
resolve to the block pool. The Java client's encoder *could* send the new
message — its schema field describes the message rather than the sender, so its
schema-2 pin is not in the way — but the message is not implemented there.

For FUSE it is not only the lookup. The read path splits a request into
block-aligned `BlockId`s *before* it resolves placement, so pointing it at the
async ring unchanged would ask an async worker for block-shaped ranges and
reintroduce the exact over-fetch this worker exists to remove. Nor has anything
decided *which* ring a mount should use: the CLI defers that to a human flag,
and a filesystem has no human per read.

**The NVMe tier has never written a byte outside tests.** Admissions are staged
and flushed once `FLUSH_BYTES` (4 MiB) has accumulated. There is no timer and no
drain on shutdown; the only other trigger is a `flush()` called from unit tests.
A deployment that admits less than 4 MiB therefore leaves L2 empty
indefinitely — and with `l1_capacity_bytes = 0`, repeated reads of one small
extent go to the origin every time. Everything downstream of that flush —
region packing, pin counts, checksum verification, region reclamation, and the
zero-copy `sendfile` response — has only ever executed under unit tests.

**No Helm template.** `deploy/helm/*/templates/` has `coordinator.yaml` and
`worker.yaml` only. A Kubernetes deployment needs a hand-written manifest.

**Never run on Linux, never against a real object store.** Development and
testing were on macOS against an in-process origin that ignores SigV4 entirely.
Real NVMe, a real S3 endpoint, and credential handling under load are all
unexercised.

## Running it

### Docker Compose

```sh
docker compose --profile extents up
curl -s localhost:8101/api/v1/cache
```

### Directly

```sh
TALON_ASYNC_WORKER_S3_SECRET_ACCESS_KEY=... \
talon-async-worker \
  --coordinator 127.0.0.1:7000 \
  --cache-dir /mnt/nvme/talon-extents \
  --capacity-bytes 1099511627776 \
  --l1-capacity-bytes 34359738368 \
  --backend s3 --s3-region us-east-1 --s3-access-key-id AKIA...
```

Ports default to 7101 (data) and 8101 (admin), deliberately offset from the
block worker's 7001/8001 so both can run on one host during a migration.

A non-AWS `--s3-endpoint` also needs `TALON_ASYNC_WORKER_S3_PATH_STYLE=true`.
Without it the virtual-hosted form builds an invalid host and the worker exits
with `backend error: builder error`. This is shared with the block worker, not
specific to this one.

Every setting is in the
[configuration reference](../reference/configuration.md) under **Async worker**;
they all use the `TALON_ASYNC_WORKER_` prefix. The ones that matter most:

| Setting | Notes |
|---|---|
| `cache_dir` | Dedicated directory. **Wiped at every start** — see below. |
| `capacity_bytes` | NVMe ceiling. Must cover at least one 64 MiB region per shard. |
| `disk_shards` | Power of two. Default 8. |
| `l1_capacity_bytes` | DRAM tier. `0` disables it and admits to NVMe on first miss. |
| `checksums_enabled` | On by default; see the note under *Checksums*. |

## Operational differences from the block worker

### The NVMe tier is cold after restart

Run descriptors live only in memory, so the cache directory is **wiped** at
startup rather than scanned. Whatever survived on disk is unaddressable, and
keeping it would consume the whole capacity budget with bytes nothing can read.

`talon-worker` rebuilds its index from `.meta` sidecars and is warm
immediately. This worker is not. The consequence is bounded — a restarted worker
refetches on demand, one extent at a time rather than one 256 MiB block at a
time — but plan rolling restarts with it in mind, and do not point `cache_dir`
at anything you want to keep.

### Writes are refused

A `Put` or `Delete` gets an error frame naming `talon-worker` as the
destination. The connection stays in sync (the body is drained before the
refusal), so a client that mistakenly writes gets a clean error rather than a
corrupted session. Route write traffic to a block-worker pool.

### Checksums

`checksums_enabled` defaults to **on**, unlike the block worker. A region is
shared by many extents, so a torn write returns another extent's bytes rather
than obvious garbage — worth a digest. Zero-copy `sendfile` responses bypass
userspace and are not covered either way. Turn it off only if you have measured
the CPU cost and accepted the risk.

### Region-granular reclamation

Extents are packed into 64 MiB regions and reclaimed a whole region at a time,
scored by decayed read volume. A hot extent packed into an otherwise cold region
is discarded with it. This is what bounds eviction cost to the region count
rather than the extent count; the residual imprecision is accepted.

## Observability

The admin surface matches the block worker's — `/metrics`, `/healthz`,
`/readyz`, `/api/v1/status` — plus one addition:

```sh
curl -s localhost:8101/api/v1/cache
```

Metrics live in their own `talon_async_worker_*` namespace. They are
deliberately **not** merged with `talon_worker_*`: summing them would average a
4 KiB fetch with a 256 MiB one and hide exactly the difference this worker
exists to create. A dashboard that wants both plots both.

**The series to watch** is the ratio:

```
talon_async_worker_origin_bytes_fetched_total
  / talon_async_worker_bytes_served_total
```

On a cold, no-repeat workload it is 1. Anything below that is what the cache
saved. If it climbs above 1, something is over-fetching and that is a bug.

Others worth alerting on:

| Metric | Meaning |
|---|---|
| `talon_async_worker_l2_short_misses_total` | Reads that found a stored extent too short and refetched. A high rate means readers disagree about range sizes. |
| `talon_async_worker_admissions_rejected_total` | Extents that never reached the DRAM hit threshold. High is normal on scan-heavy traffic — that is the frequency gate working. |
| `talon_async_worker_admissions_dropped_total` | Extents dropped because the staging buffer was full. Sustained non-zero means NVMe writes cannot keep up. |
| `talon_async_worker_l2_extents_evicted_total` | Extents lost to region reclamation. |

### Status reporting

`NodeMetricsSnapshot` — the struct behind `/api/v1/status` and the management
UI — was designed around blocks. The async worker maps each field to its nearest
true equivalent rather than reporting zeroes:

- `block_count` reports **extents**
- `page_count` is always 0 — there are no pages
- `resident_bytes` is NVMe bytes plus DRAM bytes

The role label on the same record says `async_worker`, so the substitution is
visible. In the cluster summary, async workers are counted under
`async_worker_count` and `healthy_async_worker_count`, and are excluded from the
fleet capacity and block totals.

### Readiness

`/readyz` requires the backend, the cache, and coordinator registration. The
NVMe tier is deliberately **not** a readiness input — a DRAM-only deployment is
supported, so gating on it would keep a working worker out of rotation.

Losing the coordinator reports `Degraded`, not `Unhealthy`: the worker can still
serve every read it holds, it just is not being routed any. A coordinator
restart should not make the whole pool look like it is failing.
