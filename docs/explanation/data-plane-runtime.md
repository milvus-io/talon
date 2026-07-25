# Data-plane runtime: choosing io_uring

Talon's data plane moves large blocks between NVMe and the network. This page
explains how that path is built, why it is split into two layers, and what
happened when the design was measured — including the parts that were measured
and **rejected**.

## The two-layer model

The central constraint is that **large object bytes must never enter userspace**.
A 256 MiB block read into a `Vec<u8>` and written back out costs two copies,
heap pressure, and buffer bloat on whatever runtime is scheduling the
connection. So the data plane splits into two layers with different jobs:

**Layer 1 — protocol scheduling.** Accepts connections, reads and writes 16-byte
frame headers, decodes small control messages, runs timers and metrics. This is
where connection management lives, and it handles a few dozen bytes per request.

**Layer 2 — bulk movement.** Moves the payload with Linux zero-copy syscalls:
`sendfile(2)` from the block file into the socket on a read, `splice(2)` from the
socket through a pipe into a staged file on ingest. The bytes go
`NVMe → page cache → kernel → socket` and are never in a Rust buffer.

The layers are deliberately decoupled. Layer 2 depends on no runtime types —
`send_file_range` and `splice_to_file` take `&impl AsRawFd` — which is what made
the runtime experiment below possible without touching the zero-copy path at all.

### Why sendfile runs off the reactor

`sendfile` and `splice` are **blocking** syscalls. They can block on socket
backpressure, on disk, or on pipe readiness. Running them on the thread that
schedules connections means one slow client stalls every connection multiplexed
onto that thread.

So they run on a blocking helper pool. This matters more, not less, on a
thread-per-core runtime: blocking one worker thread of a work-stealing pool is
survivable, but blocking a single-threaded ring stalls everything it owns.

## The runtime question

Layer 1's job — many concurrent connections, small messages, high syscall rate —
is what `io_uring` is designed for. It submits and completes operations in
batches through shared ring buffers rather than one syscall per operation, and a
thread-per-core design removes cross-thread scheduling from the path entirely.

Talon ships a Tokio Layer 1 and an optional `io_uring` one, selectable per
worker:

```sh
talon-worker --data-plane-rings 0   # one ring per core
talon-worker --data-plane-rings 4   # exactly four
talon-worker                        # portable Tokio path (default)
```

The rest of this page is how that second implementation was designed and what
measuring it revealed.

## Thread-per-core, and how accepts are distributed

`monoio` has no work-stealing scheduler. It scales by running N independent
single-threaded runtimes that share nothing. Each ring thread:

1. **pins itself to a core**, so a connection's protocol scheduling never
   migrates and its state stays in that core's caches;
2. **builds its own ring** with its own blocking pool for `sendfile`;
3. **binds the listen address with `SO_REUSEPORT`**.

That last point is the mechanism the whole design rests on. Every ring binds the
*same* address, and the **kernel** distributes incoming connections across them
by hashing the TCP 4-tuple. There is no shared accept queue, no lock, and no
thundering herd — a connection is assigned to a ring at accept time and stays
there for its lifetime.

Measured scaling on an 8-core host:

| rings | throughput | scaling | per-core |
|---|---|---|---|
| 1 | 13,415 rps | 1.00× | 14,271 |
| 2 | 24,447 rps | 1.82× | 12,867 |
| 4 | 50,587 rps | 3.77× | 13,072 |
| **8** | **107,993 rps** | **8.05×** | **13,935** |
| 16 | 118,137 rps | 8.81× | 12,204 |

Per-core throughput is flat from 1 to 16 rings, which is the number that matters:
it means there is **no cross-ring contention** to amortise. The fall-off at 16 is
SMT — this host has 8 physical cores, so the extra rings land on hyperthread
siblings. The useful setting is one ring per *physical* core.

## What was measured and rejected

Three plausible-sounding design decisions did not survive contact with a
benchmark. They are recorded here because the negative results shaped the
implementation more than the positive ones.

### Sharding cache state per ring — rejected

Thread-per-core suggests an obvious companion: shard `BlockIndex` per ring so
each owns its slice with no locking, using `Rc<RefCell<...>>` instead of
`Arc<Mutex<...>>`. It loses on two independent counts.

**Cross-ring forwarding costs 30–67% of throughput.** Connections are
distributed by TCP 4-tuple; blocks are keyed by `block_id`. The two hashes are
unrelated, so only ~1/N of requests arrive at the ring that owns the block —
**87.5% need forwarding at 8 shards**, and forwarding is not free.

**Per-shard eviction budgets waste capacity.** With 256 MiB blocks and 64 GiB per
worker there are only ~256 block slots, or 32 per shard at 8 rings. That is far
too few for hash uniformity: measured skew reached 47–59%, so some shards evict
while others sit below capacity. Simulated against realistic workloads, this cost
up to **5.1 percentage points of hit rate** when the working set sat near
capacity — and each avoidable miss is a 256 MiB refetch.

Worth separating the two ideas: sharding the eviction *policy* is free — hit rate
was identical to global LRU across every distribution tested, including an
adversarial one concentrating all hot blocks in one shard. Sharding the *budget*
is what costs.

So worker state stays shared behind an `Arc`. This is sound on a thread-per-core
runtime because `!Send` constrains *futures crossing threads*, not shared state
read from within a ring.

### fd ownership across the ring/blocking boundary — not a problem

This was expected to be the hardest part of the design, and it turned out to be
the opposite.

The Tokio path cannot `sendfile` directly onto its socket: a Tokio `TcpStream` is
non-blocking, so a blocking `sendfile` on it would spuriously `EAGAIN`. It
therefore round-trips the socket out of and back into the runtime **on every
single transfer** — `into_std`, `set_nonblocking(false)`, move to a blocking
thread, move back, `set_nonblocking(true)`, `from_std`.

A ring-owned fd needs none of that. It is passed straight to the blocking pool
and the ring resumes on the same stream afterwards. The `io_uring` implementation
of this path is *smaller* than the Tokio one.

### A single ring — rejected

A single ring caps at roughly 13k rps per core regardless of connection count. It
is the simplest thing to build and it is a throughput ceiling on a machine meant
to saturate NVMe and a NIC. Thread-per-core is not an optimisation here; it is
the point.

## Benchmark results, and what they do not show

Two independent measurements exist, at different concurrency levels, and they
disagree in an instructive way.

**At 1024 concurrent connections**, measured in a standalone harness against a
full-integration setup (real framed protocol, real `sendfile`, real loopback TCP,
connection reuse):

| comparison | throughput | per-core | p50 | p99 | RSS |
|---|---|---|---|---|---|
| 1 ring vs 1 Tokio worker | +85% | +23–35% | −46% | −44% | −15% |
| 16 rings vs full Tokio | +35% | +34% | −19% | −26% | −34% |

Latency, CPU efficiency, and memory all improve together — there is no trade
being made.

**At concurrency 1**, measured by the in-repo benchmark
(`cargo bench -p talon-worker --bench dataplane_benches`), the two runtimes are
**statistically indistinguishable**. Across seven runs, medians landed between
218 and 295 µs for both, and the sign of the difference flipped run to run. The
spread *within* one implementation exceeded the gap between them.

That is not a contradiction — it is the expected shape of the result.
`io_uring`'s advantage is amortising syscalls across many in-flight operations.
With one request in flight there is nothing to amortise, and the bulk bytes
bypass the ring via `sendfile` on both paths anyway, leaving a 16-byte header
exchange as the only work that differs.

### Why the default is still Tokio

The `io_uring` data plane is complete, tested, and byte-exact against the Tokio
path — the test suite mirrors both implementations assertion-for-assertion. It is
not the default because **the evidence for flipping it is not yet reproducible in
this repository**. The 1024-connection numbers come from a harness that lives
outside the tree; the in-repo benchmark measures a concurrency where no
difference is expected.

A microbenchmark harness cannot honestly model 1024 concurrent connections — it
drives one client serially, and a synthetic fan-out would mostly measure the
harness's own scheduling. Closing that gap needs a concurrent load test, which is
tracked separately. Until then the default stays on the portable path, and the
faster one is opt-in.

## Coexistence with Tokio

Even on a ring, the worker is not Tokio-free, and the reason is worth stating.

`block_store` runs its filesystem I/O on `tokio::task::spawn_blocking` so a large
read or a write-plus-fsync never stalls the reactor, and parts of the miss path
use Tokio timers and synchronisation primitives. Driven from a bare ring, those
calls panic. Ring threads therefore enter a Tokio runtime handle before running.

This is coexistence by design rather than a workaround: the ring owns protocol
scheduling and hands `sendfile` to its own blocking pool, while Tokio's blocking
pool absorbs filesystem work that belongs on neither. It does mean two blocking
pools share pinned cores, so the per-ring pool is kept deliberately small.

The same reasoning applies at the cluster level. The control plane, coordinator
admin API, management UI, etcd and Kubernetes clients, the miss loader, and the
metrics endpoints all stay on Tokio — they are not the hot path, and their
ecosystems are Tokio-bound. The ring is used precisely where its advantage is
measurable.

## Reproducing this

```sh
# Both data planes, same block, real loopback TCP
cargo bench -p talon-worker --bench dataplane_benches

# Run a worker on rings and confirm SO_REUSEPORT: N listeners, one port
talon-worker --data-plane-rings 2 --listen 127.0.0.1:7001
ss -ltn | grep 7001
```

See [BENCHMARKS.md](../../BENCHMARKS.md) for the harness, the committed
baselines, and why the benchmark CI job is informational rather than gating.
