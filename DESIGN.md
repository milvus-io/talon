# Talon Design (v1)

Talon is a distributed object-store cache. It sits between compute clients and a
durable backing store (any mainstream blob store — S3, GCS, Azure Blob),
caching large immutable objects on
local NVMe across a fleet of worker nodes, and exposing them through a read-only
FUSE filesystem.

This document records the v1 architecture decisions. It is intentionally scoped:
several harder problems (replication, coordinator HA, write-back) are explicitly
deferred until workload data justifies them.

> **Status note.** This is a historical record of the v1 decisions, not a
> description of current behaviour. The FUSE client has since grown a write
> path — `create`, `write`, `mkdir`, `rename`, `unlink`, `symlink`, `link`,
> `setattr`, and `fsync` are all implemented (`crates/talon-fuse/src/mount.rs`)
> — so the "read-only" framing below applies to v1 only. Write-back to the
> backing store and POSIX locking remain unimplemented; see
> [ADR 0002](docs/adr/) and [ADR 0003](docs/adr/) for where that work stands.

## Goals

- Cache large, immutable objects (checkpoints, datasets) close to compute.
- High sequential read throughput with minimal copy overhead.
- Horizontal scale-out of cache capacity across worker nodes.
- Read-only POSIX access via FUSE for unmodified applications.

## Non-goals (v1)

- Write-back / write-through to the backing store.
- Multi-region / WAN operation.
- Strong-consistency metadata or a durable coordinator log.
- Replication beyond RF=1 (hot-block RF=2 is a later addition).

## Architecture

```
          +-------------------+
          |    Coordinator    |   membership, metadata, compatibility lookup
          +-------------------+
             ^   (control)   ^
             |               |
   heartbeat |               | membership snapshot
   inventory |               |
             |               |
   +---------+--+       +----+---------+
   |  Worker    |  ...  |   Client     |
   |  (NVMe)    |<------|  (talon-fuse)|   direct data-plane reads
   +------------+ data  +--------------+
        |
        v  cache miss
   +------------+
   | Blob store |   BackendStore (S3 / GCS / Azure Blob)
   +------------+
```

Three planes:

1. **Control plane** — coordinator ↔ workers ↔ clients. Placement, membership,
   epoch/version, block inventory. Low volume.
2. **Data plane** — client ↔ worker, direct. Large object/block transfer. Never
   routed through the coordinator.
3. **Backend plane** — worker → backing store on cache miss.

## 1. RPC / transport

- **Data plane:** custom TCP with framed binary messages — a small header plus
  raw bytes. Kept deliberately thin so the hot path can use `sendfile` (file →
  socket) and `splice` (socket → file). No protobuf around large payloads.
- **Control plane:** same lightweight framed protocol, serialized with
  `bincode` for v1. Separate port / connection pool from the data plane so bulk
  traffic cannot starve control messages. If the admin API grows, migrate the
  control plane to `tonic`/protobuf.
- **QUIC / RDMA:** deferred. Revisit QUIC only for WAN / lossy / multi-path;
  revisit RDMA only if TCP + NVMe is proven insufficient in a fast rack network.
- **Zero-copy:** `bytes::Bytes` for small messages and in-memory buffer sharing;
  `sendfile`/`splice` for disk-block transfer.
- **Runtime:** two-layer model (control/protocol scheduling + zero-copy
  syscalls). Layer 2 is built and runtime-neutral; the worker data plane's
  Layer 1 runs on a thread-per-core io_uring runtime by default, with a Tokio
  fallback — see *Runtime & I/O model* below.

## Runtime & I/O model

Each `coordinator` / `worker` / `client` process splits I/O into two layers: one
for control and protocol scheduling, one for bulk data movement. **Layer 2 is
built and runtime-neutral.** Layer 1 on the **worker data plane** runs on a
thread-per-core io_uring runtime by default, falling back to Tokio where
io_uring is unavailable; every other plane stays on Tokio deliberately (see
*Runtime split*).

### Layer 1 — control & protocol scheduling

The layer owns `accept`, read/write of protocol headers, small control
messages, task spawning, timers, and metrics. All connection management and
scheduling lives here. **Large object bytes never enter userspace through this
layer** — that invariant is what makes Layer 2 possible, and it holds under any
runtime.

**Worker data plane: thread-per-core io_uring (default).** `worker/main.rs`
probes io_uring at startup and, when available, serves on N rings — one per core
by default — each pinned and binding the listen address with `SO_REUSEPORT`.
When the probe fails (older kernel, restrictive seccomp, some container
runtimes) it falls back to the Tokio accept loop and logs the fallback, so the
default is safe everywhere without a version check.

**Control plane and coordinator: Tokio.** `coordinator/main.rs` runs a
multi-threaded Tokio accept loop, as do the admin/UI/metrics endpoints. These
are not the hot path and their ecosystems are Tokio-bound.

**Why io_uring on the data plane.** Benchmarked against the Tokio
implementation on a full-integration harness (real framed protocol, real
`sendfile`, real loopback TCP, connection reuse) at the connection counts a
cache fleet actually produces — see issue #273 for method and raw data:

| comparison | throughput | per-core | p50 | p99 | RSS |
|---|---|---|---|---|---|
| 1 ring vs 1 Tokio worker | +85% | +23–35% | −46% | −44% | −15% |
| 8 rings vs 4 Tokio workers | +15% | +26% | −11% | −5% | +11% |
| 16 rings vs full Tokio | +35% | +34% | −19% | −26% | −34% |

monoio has no work-stealing scheduler; it scales by running N independent
runtimes, one pinned per core (`bind_to_cpu_set`), each binding the same address
with `SO_REUSEPORT` so the kernel hash-distributes accepts. Measured scaling is
**8.05× at 8 rings** (this host has 8 physical cores; 16 rings falls off to
8.81× as the extra rings land on SMT siblings). Per-core throughput stays flat
from 1 to 16 rings — there is no cross-ring contention to amortize.

**What the migration required.** Recorded because "swap the runtime" understates
it, and the same constraints apply to any further porting:

- **`!Send` task model.** The accept loops spawn each connection with
  `tokio::spawn`, which requires `Send` futures. monoio is thread-per-core with
  `!Send` futures, so connection state may not cross threads.
- **Buffer-ownership I/O traits.** Tokio borrows buffers
  (`read_exact(&mut buf)`); monoio's `AsyncReadRent`/`AsyncWriteRent` move
  ownership in and back out, because io_uring is completion-based and the kernel
  holds the buffer for the duration. Every socket read/write site is rewritten,
  not re-imported.
- **Ecosystem lock-in.** `axum`, `reqwest`, `kube`, `etcd-client`, and `tonic`
  are Tokio-bound. Any migration yields ring + Tokio *coexistence*, not
  replacement.
- **Not a blocker:** fd ownership across the ring/blocking boundary. A
  ring-owned monoio `TcpStream` fd can be handed straight to a blocking
  `sendfile` and the ring resumes on the same stream afterwards — none of the
  `into_std` → `set_nonblocking` → `from_std` round-trip the Tokio path pays
  per transfer is needed. The io_uring implementation is *smaller*.

Ported: the `worker/main.rs` accept loop and `handle_conn` (including
`handle_put`/`handle_delete`), plus a completion-based frame reader in
`transport::uring`. `WorkerRuntime::serve` and everything below it was
unaffected — `Arc<dyn BackendStore>` is fine, since `!Send` constrains futures
crossing threads, not shared state within a ring. It does still reach Tokio
internally (`block_store` uses `spawn_blocking`, the miss path uses Tokio
timers), so ring threads enter a Tokio handle: coexistence, as predicted.

Still on Tokio: `fuse::worker_client`, the client side of the same protocol.
The wire format is plain framed TCP, so a Tokio client interoperates with a
ring server unchanged — porting it is a client-side optimization, not a
correctness requirement.

### Layer 2 — bulk data movement (Linux zero-copy syscalls) — built

Large payloads move via kernel zero-copy, not through Rust heap buffers:

- **GET / cache read:** `sendfile(block_file_fd → socket_fd)`.
- **PUT / ingest:** `splice(socket ↔ pipe ↔ file)`.

These blocking libc syscalls run on a `spawn_blocking` pool, off the reactor, so
a slow client cannot stall protocol scheduling. (Under Tokio this parks one
worker thread of many; under a single-threaded ring it would stall the ring
outright — the isolation matters more, not less, after a monoio migration.)
Default chunk size **1 MiB**, block size **64 MiB** (our v1 default is 256 MiB —
see §3 chunking; treat block size as configurable).

This layer is **runtime-neutral**: `send_file_range` and `splice_to_file` take
`&impl AsRawFd` and depend on no runtime types, so they survive a Layer 1
change untouched.

### Runtime split

Which runtime each component targets, and why. Five of the seven already match
today; only the data-plane TCP scheduling layer diverges.

| component | target | today | rationale |
|---|---|---|---|
| worker data plane | monoio + `sendfile`/`splice` | **monoio (default), Tokio fallback** ✅ | hot path; per-core efficiency and tail latency |
| client ↔ worker data plane | monoio | Tokio | same hot path, client side |
| coordinator ↔ worker control | either | Tokio | low volume; bincode over framed TCP |
| coordinator admin / UI / etcd / k8s | **Tokio** | Tokio ✅ | axum/kube/etcd-client are Tokio-bound |
| worker miss loader | Tokio or blocking pool, **off-ring** | Tokio + semaphore ✅ | TLS/HTTP breaks zero-copy anyway; slow path |
| FUSE client | either | Tokio ✅ | `fuser` callbacks are synchronous regardless |
| metrics / health | Tokio | Tokio ✅ | ecosystem-driven, not a data path |

### Data-plane paths

**L1 memory hit** (optional, default off; for hot regions inside large blocks):
decode header + key → look up every `(block_id, page_index)` touched by the
range → send header → plain async socket write from shared `Bytes` pages. L1 is
inclusive: every DRAM page has a whole-block L2 parent, and an L2
eviction/delete/version replacement invalidates all child pages. No disk and no
`sendfile` on an L1 hit; capacity is byte-bounded independently from page size.

**L2 NVMe hit** (primary large-block path): decode header + key → `BlockIndex`
lookup → open cached `.blk` fd → write response header →
`sendfile(cached_fd → socket)` in the blocking helper. Large blocks are never
read into a `Vec<u8>`, avoiding NVMe→userspace and userspace→socket copies, heap
pressure, and buffer bloat. When L1 is enabled, an L2 range hit reads and
promotes only the aligned L1 pages touched by the request, never the whole block.
With L1 disabled the large-block path remains
`NVMe/page-cache → kernel → TCP socket`.

**GET_RANGE hit:** identical to L2 hit but `sendfile` uses `(offset, length)` —
suited to Lance / checkpoint footer / partial reads.

**Paged block hit:** for paged virtual blocks (see §3), a read resolves to the
covered pages via the block's `present_bitmap`. Present pages are served with
one `sendfile` per page (contiguous present pages coalesced into a single call
by offset). Any page in the range that is absent triggers a **page-level miss**:
the loader fetches only those pages' byte ranges from the backend, not the whole
256MB block. In-flight tracking is keyed by `(block_id, page_index)`.

**PUT:** client `splice(file → pipe → socket)`; worker decodes key + `data_len`,
creates a staged file, `splice(socket → pipe → file)`, `sync_all`,
rename-commit, update index. Known tradeoff: because bytes bypass userspace,
streaming `xxh3` checksum is not computed on the zero-copy PUT path (loader path
can, since it downloads into a `Vec`).

### Miss path (deliberately off-ring)

The ring does **not** call the backend directly. On miss for a `blob://` key it
checks in-flight demand loads, submits a `LoadTask` to a **loader thread pool**,
and returns `LOADING` immediately. A loader thread does a blocking HTTP Range GET
into a `Vec`, computes checksum, writes + syncs a staged file, and signals a
completion channel. A ring-side watcher drains the channel (~10 ms), rename-commits
the block, updates `BlockIndex`, optionally populates L1, and evicts if needed.
The client, seeing `LOADING`, tries the next healthy replica or backs off and
retries — then hits the fast path.

Backend HTTP is kept off io_uring on purpose: the blob client is blocking, and
TLS/HTTP would break the file/socket zero-copy anyway. Miss is the slow path;
isolating it to loader threads keeps the data-plane ring responsive.

### LOAD (prewarm) path

Master-initiated, similar to miss but not a client data-plane transfer: master
lists blobs on a background thread, splits into blocks, assigns a primary worker
via jump hash, and sends `LoadBlobs`. Workers' loader threads download the ranges
and commit into cache. Workers pull from the backend themselves; no
client→worker zero-copy involved.

### Why not one mechanism for all data movement

The division of labour, independent of which Layer 1 runtime is in use:

- **Protocol scheduling** (Tokio today, monoio targeted): async TCP, small
  messages, scheduling, timers, metrics.
- **sendfile / splice:** the actual large-block zero-copy movement.
- **spawn_blocking:** isolates the blocking libc zero-copy syscalls off the
  reactor, so a slow client cannot stall protocol scheduling.

Future optimizations (drive by benchmarks, not speculation): fd registration,
pipelined double-buffer splice, and evaluating `IORING_OP_SPLICE` /
send-zero-copy. Current `sendfile`/`splice` is the simpler, stable Linux fast
path.

Two items that were previously listed here have since been measured (#273):

- **Thread-per-core (one ring per core): validated.** 8.05× scaling at 8 rings
  via `SO_REUSEPORT` + CPU pinning. This is the shape a monoio migration should
  take — not a single ring, which caps at ~13k rps/core.
- **Hash-partitioned request affinity: rejected.** Sharding `BlockIndex` per
  ring sounds like the natural companion to thread-per-core, but it loses on
  both axes. `SO_REUSEPORT` hashes connections by TCP 4-tuple while shards key
  on `block_id`, so only 1/N of requests land on the owning ring — measured
  75–87% forwarding at 8 shards, costing **30–67% throughput**. Separately,
  giving each shard its own eviction budget wastes capacity: with 256 MiB blocks
  and 64 GiB per worker there are only ~256 block slots (32/shard at 8 shards),
  far too few for hash uniformity, so some shards evict while others sit below
  capacity — up to **5.1pt hit-rate loss** when the working set is near
  capacity. Sharding the eviction *policy* is free; sharding the *budget* is
  not.

The measured conclusion is to keep worker state **shared with lock-free reads**
(`BlockIndex` is read-mostly), a per-ring buffer for LRU access marking, a
**global** byte account for eviction, and a global `InFlightLoads` for miss
dedup (a correctness requirement — per-shard dedup would refetch the same
256 MiB block once per ring).

## 2. Coordinator

- **Placement:** clients build a deterministic cross-language **Maglev table**
  when healthy worker membership changes. Stable membership makes primary block
  lookup O(1), independent of worker count; top-K probes the same table for
  distinct fallback workers. The coordinator retains equivalent legacy lookup
  compatibility. Assumes stable worker IDs.
- **Replication:** **RF=1** in v1 — the backing store is the durable source.
  Hot blocks may get RF=2 later once miss cost is measured. Avoid blanket
  multi-copy; it burns NVMe.
- **HA:** single coordinator for v1. The post-v1 management plane uses
  active-active, stateless coordinators over a user-selected Kubernetes Lease
  or etcd shared-state backend. The accepted contract and failure semantics are
  defined in
  [`docs/adr/0001-management-plane-ha.md`](docs/adr/0001-management-plane-ha.md).
  Raft remains out of scope unless the coordinator later owns durable,
  non-rebuildable metadata such as write-back state.
- **Writes:** **write-through only** — a write is durable when the origin object
  store has acknowledged it, and never before. Read-your-writes and monotonic
  reads follow from that. Write-back is deferred behind explicit entry
  conditions (replication before acknowledgement, bounded dirty state, proven
  crash recovery), because acknowledging from one node's NVMe is not durability.
  The contract, failure semantics, and the conditions under which write-back
  could be revisited are defined in
  [`docs/adr/0002-write-cache-durability.md`](docs/adr/0002-write-cache-durability.md).
- **Membership:** Kubernetes watch/poll as the membership source; worker
  heartbeats provide liveness + block inventory. No gossip. Timeout at 3–6
  heartbeat windows (e.g. 10s heartbeat → 30–60s to mark unhealthy).
- **Metadata consistency:** placement table is eventually consistent but carries
  an **epoch/version**. Clients cache the ring briefly and refresh on connect
  failure, not-found, wrong-owner, or epoch mismatch, falling back along the
  replica list.

## 3. Worker storage

- **Tiering:** optional byte-bounded, page-granular **L1 DRAM** over an inclusive
  local whole-block **L2 NVMe SSD** store. L1 is empty after restart and promotes
  only pages touched by L2 hits; L2 remains persistent and authoritative for
  cache residency. Disabling L1 retains the whole-block `sendfile` path. No
  `mmap` as the default abstraction.
- **Eviction:** byte-accounted **LRU / segmented-LRU** first. LFU risks pinning
  stale hotspots; TinyLFU is more complex — revisit with real workload data.
  Capacity is per-worker, with support for multiple cache dirs each with its own
  cap.
- **Chunking:** the logical addressing unit is a fixed **256MB block**. Placement,
  the coordinator inventory, `etag/version`, and the cache key all operate at
  block granularity. The cache key includes
  `source_uri + offset + block_size + etag/version` so a source update never
  serves a stale block.
- **Block materialization — whole vs paged:** *superseded for selective reads by
  ADR 0005.* The paged form below was never wired into the serve path; the
  primitives exist but have no caller. Its goal — a point query fetching only
  what it touches — is met instead by `talon-async-worker`, which caches
  variable-length extents with no block or page granularity at all, so there is
  no page size to get wrong. The whole-block form below is what `talon-worker`
  ships and remains correct for sequential scans. The rest of this bullet
  records the original plan.

  Block size stays fixed at 256MB,
  but a block has two physical forms, chosen per block. This decouples the
  *logical addressing unit* from the *physical caching granularity* so a single
  scheme adapts to different workloads without changing placement or the key
  space:
  - **Whole block:** the entire 256MB is cached as one unit. Best for
    sequential-scan workloads (checkpoints, datasets) paired with readahead.
    When a block is loaded as whole, it is backed by a **single `.blk` file** and
    `sendfile(fd, offset, len)` serves it directly — identical to the base
    data-plane path.
  - **Paged virtual block:** only the hot pages within a block are materialized
    on demand. Best for point-query workloads (database lookups). Page size is
    configurable **256KB–4MB** (per-namespace default). A 256MB block therefore
    holds up to 1024 pages (256KB) or 64 pages (4MB). Addressed logically as
    `block_id/page_index`.
  - **Form is decided at LOAD time via a hint** (per-namespace or per-LOAD
    request); e.g. checkpoint prefixes load as whole, database prefixes load as
    paged. Dynamic promotion (paged → whole on detected sequential scan) is
    deferred to a later version to keep the v1 state machine simple.
  - **Page-level miss / in-flight / eviction:** for paged blocks, miss handling,
    `demand_loads_in_flight` tracking, and LRU accounting all descend to
    `(block_id, page_index)` granularity — a point query fetches only the pages
    it touches, never the full 256MB, and cold pages can be evicted while the
    block entry survives. Whole blocks remain a single LRU unit.
  - **Index:** each block entry carries its form,
    `enum { Whole, Paged { page_size, present_bitmap } }`. The present bitmap is
    cheap (1024 bits = 128 bytes worst case) and drives page-hit checks.
  - **Physical layout:** the on-disk form follows the LOAD-time choice. A
    **whole block is a single `.blk` file**. A **paged block is a directory
    `block_id/` with one file per materialized page** (`block_id/page_index`),
    *not* a single sparse file. Each present page is an independent `.page` file
    served by its own `sendfile`. Per-page files keep materialization, commit
    (per-page rename), and eviction (per-page `unlink`) simple and independent,
    avoid sparse-file semantics and fragmentation, and make the on-disk form
    match the logical `block_id/page_index` address. The tradeoff — more inodes
    and `open` calls under high-concurrency point queries — is accepted; mitigate
    with an fd cache for hot pages if benchmarks show pressure.

## 4. talon-fuse client

- **Async bridge:** the `fuser` callback model is synchronous. The FUSE thread
  does only lightweight parsing and hands work to the async runtime over a
  bounded channel / oneshot — never blocking on the reactor. Alternatively, a
  blocking facade over a dedicated runtime pool.
- **Semantics:** read-only cache view. v1 implements
  `lookup / getattr / readdir / open / read / release`. No
  `write / rename / unlink / chmod`. `mmap` relies on the kernel page cache to
  trigger reads; no writable-mmap POSIX guarantees.
- **Key ↔ path mapping:** deterministic and reversible, hierarchical namespace
  prefixed by backend, e.g. `/s3/<bucket>/<object-path>`,
  `/gcs/<bucket>/<object-path>`, `/az/<account>/<container>/<blob-path>`. The
  internal `CacheKey` carries backend + bucket/container + object path + offset +
  block size + etag/version. Not a flat string (escaping/collisions).
- **Client caching / readahead:** rely on the kernel page cache first; the client
  does sequential-read detection and next-N-block readahead. No separate
  client-side disk cache in v1.

## 5. Cross-cutting

- **Backing store:** support all mainstream blob stores — S3, GCS, and Azure
  Blob — behind a single `BackendStore` abstraction, with room for HTTP / local
  file later. Each backend implements the same block-range fetch + metadata
  (etag/version) contract; credentials and endpoint config are per-backend.
  Milvus is not a direct miss source unless object identity + version map
  cleanly; cache the underlying blobs instead.
- **Observability:** full-path Prometheus metrics + tracing. Key metrics:
  hit/miss, bytes served, block-load latency, backend fetch errors, evictions,
  disk usage, worker health, client retry/fallback, placement epoch refresh.
- **Configuration precedence:** `CLI > env > config file > default`. Config file
  for stable service params (port, block size, cache dirs, capacity, backend);
  CLI for local debugging/overrides; env for deployment injection, secrets,
  identity, pod/node metadata.
- **Serialization:** no long-lived ad-hoc JSON on the control plane. Short-term
  internal protocol uses `bincode` / framed binary; move the control plane to
  `prost`/protobuf if version compatibility or cross-language clients are needed.
  The data plane stays small header + raw bytes / splice — never protobuf around
  large objects.

## v1 summary

Single coordinator, K8s membership, Maglev / top-K placement, RF=1, NVMe
block cache, custom TCP data plane, read-only FUSE, and pluggable blob backends
(S3 / GCS / Azure Blob).

Add RF=2 and a protobuf control API once miss cost and compatibility
requirements are demonstrated. Coordinator HA follows
[`ADR 0001`](docs/adr/0001-management-plane-ha.md).

## Follow-up skeleton changes

Decisions above that diverge from the current code, to be addressed in later PRs:

- Replace `CacheKey(String)` with a structured, reversible key
  (`backend + bucket/container + object_path + offset + block_size + etag/version`).
- Add a `BackendStore` trait in `talon-core`, distinct from `ObjectStore`
  (cache access) — S3 / GCS / Azure Blob implementations to follow.
- Adjust `ObjectStore` for block-level, byte-accounted access and an fd/offset
  path for `sendfile`, rather than only returning `Bytes`.
- ~~Model block materialization as `enum { Whole, Paged { page_size,
  present_bitmap } }` in the block index, decided by a LOAD-time hint; add
  page-level miss / in-flight / eviction for paged blocks.~~ Superseded by
  ADR 0005: `talon-async-worker` caches variable-length extents instead, which
  removes the fixed granularity rather than shrinking it.
- Replace control-plane `serde_json` with framed `bincode`; define a data-plane
  frame header.
- Extend `RendezvousPlacement` to top-K + epoch.
- Introduce layered configuration (`CLI > env > config file > default`).
