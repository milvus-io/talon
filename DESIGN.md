# Talon Design (v1)

Talon is a distributed object-store cache. It sits between compute clients and a
durable backing store (Azure Blob first), caching large immutable objects on
local NVMe across a fleet of worker nodes, and exposing them through a read-only
FUSE filesystem.

This document records the v1 architecture decisions. It is intentionally scoped:
several harder problems (replication, coordinator HA, write-back) are explicitly
deferred until workload data justifies them.

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
          |    Coordinator    |   placement, membership, epoch
          +-------------------+
             ^   (control)   ^
             |               |
   heartbeat |               | placement lookup
   inventory |               |
             |               |
   +---------+--+       +----+---------+
   |  Worker    |  ...  |   Client     |
   |  (NVMe)    |<------|  (talon-fuse)|   direct data-plane reads
   +------------+ data  +--------------+
        |
        v  cache miss
   +------------+
   | Azure Blob |   BackendStore
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

## 2. Coordinator

- **Placement:** rendezvous (HRW) hashing, extended to **top-K** to reserve a
  replica-ordering for later. Assumes stable worker IDs. Consistent-hashing
  rings / virtual nodes deferred until worker capacities diverge enough to need
  weighting.
- **Replication:** **RF=1** in v1 — the backing store is the durable source.
  Hot blocks may get RF=2 later once miss cost is measured. Avoid blanket
  multi-copy; it burns NVMe.
- **HA:** single coordinator for v1 — it holds no unrebuildable authoritative
  state. v1.5: K8s leader election + standby. Raft only if the coordinator ever
  owns strongly-consistent, non-loseable metadata (e.g. write-back).
- **Membership:** Kubernetes watch/poll as the membership source; worker
  heartbeats provide liveness + block inventory. No gossip. Timeout at 3–6
  heartbeat windows (e.g. 10s heartbeat → 30–60s to mark unhealthy).
- **Metadata consistency:** placement table is eventually consistent but carries
  an **epoch/version**. Clients cache the ring briefly and refresh on connect
  failure, not-found, wrong-owner, or epoch mismatch, falling back along the
  replica list.

## 3. Worker storage

- **Tiering:** primary store is local **NVMe SSD**. Memory holds only the index,
  small-object cache, and hot metadata. No `mmap` as the default abstraction —
  the Linux page cache already provides the memory tier; explicit
  `pread`/`sendfile` is more controllable.
- **Eviction:** byte-accounted **LRU / segmented-LRU** first. LFU risks pinning
  stale hotspots; TinyLFU is more complex — revisit with real workload data.
  Capacity is per-worker, with support for multiple cache dirs each with its own
  cap.
- **Chunking:** objects are split into blocks. Configurable block size, default
  **256MB** (favors sequential throughput, low metadata). The cache key includes
  `source_uri + offset + block_size + etag/version` so a source update never
  serves a stale block.

## 4. talon-fuse client

- **Async bridge:** the `fuser` callback model is synchronous. The FUSE thread
  does only lightweight parsing and hands work to the async runtime over a
  bounded channel / oneshot — never blocking on the reactor. Alternatively, a
  blocking facade over a dedicated runtime pool.
- **Semantics:** read-only cache view. v1 implements
  `lookup / getattr / readdir / open / read / release`. No
  `write / rename / unlink / chmod`. `mmap` relies on the kernel page cache to
  trigger reads; no writable-mmap POSIX guarantees.
- **Key ↔ path mapping:** deterministic and reversible, hierarchical namespace,
  e.g. `/az/<account>/<container>/<blob-path>`. The internal `CacheKey` carries
  backend + container/bucket + object path + offset + block size +
  etag/version. Not a flat string (escaping/collisions).
- **Client caching / readahead:** rely on the kernel page cache first; the client
  does sequential-read detection and next-N-block readahead. No separate
  client-side disk cache in v1.

## 5. Cross-cutting

- **Backing store:** Azure Blob is the first backend (closest deployment/auth
  path). Keep a `BackendStore` abstraction for later S3 / HTTP / file. Milvus is
  not a direct miss source unless object identity + version map cleanly; cache
  the underlying blobs instead.
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

Single coordinator, K8s membership, rendezvous / top-K placement, RF=1, NVMe
block cache, custom TCP data plane, read-only FUSE, Azure Blob backing store.

Add RF=2, leader election, and a protobuf control API once miss cost and
availability requirements are demonstrated.

## Follow-up skeleton changes

Decisions above that diverge from the current code, to be addressed in later PRs:

- Replace `CacheKey(String)` with a structured, reversible key
  (`backend + container/bucket + object_path + offset + block_size + etag/version`).
- Add a `BackendStore` trait in `talon-core`, distinct from `ObjectStore`
  (cache access) — Azure Blob implementation to follow.
- Adjust `ObjectStore` for block-level, byte-accounted access and an fd/offset
  path for `sendfile`, rather than only returning `Bytes`.
- Replace control-plane `serde_json` with framed `bincode`; define a data-plane
  frame header.
- Extend `RendezvousPlacement` to top-K + epoch.
- Introduce layered configuration (`CLI > env > config file > default`).
