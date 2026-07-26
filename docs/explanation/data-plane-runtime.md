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

Talon runs the `io_uring` Layer 1 by default, one ring per core, and falls back
to a portable Tokio implementation on hosts that cannot use io_uring:

```sh
talon-worker                        # io_uring, one ring per core (default)
talon-worker --data-plane-rings 4   # exactly four rings

# Pin the Tokio path explicitly (escape hatch)
TALON_WORKER_FORCE_TOKIO_DATA_PLANE=1 talon-worker
```

The fallback is a **runtime capability probe**, not a kernel-version check.
io_uring needs both a recent enough kernel and permission to use it — the
`io_uring_disabled` sysctl, a seccomp filter, or a container runtime's default
profile can each block the syscall on a kernel that nominally supports it. The
worker probes the real capability before binding and logs a warning if it falls
back, so a silent performance cliff is visible in the logs.

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

**Under concurrent load**, measured in-repo by `scripts/dataplane_loadtest.sh`
against a real worker — real `WorkerRuntime`, real `sendfile`, real block store,
64 KiB ranges, 10 s per point after a 3 s warmup:

| conns | io_uring rps | tokio rps | io_uring p50 | tokio p50 | io_uring p99 | tokio p99 |
|---|---|---|---|---|---|---|
| 1 | 5,636 | 4,837 | 161 µs | 191 µs | 259 µs | 293 µs |
| 64 | 54,755 | 55,191 | 900 µs | 1,057 µs | 5,371 µs | 2,853 µs |
| 256 | 42,417 | 56,999 | 1,243 µs | 3,835 µs | 7,423 µs | 10,283 µs |
| **1024** | **51,668** | 41,060 | **1,175 µs** | 19,905 µs | **4,781 µs** | 72,911 µs |

At 1024 connections io_uring delivers **26% more throughput at 17× lower p50 and
15× lower p99**, on comparable CPU and 19% less memory. Throughput, tail
latency, and memory improve together — there is no trade being made.

**The sweep is not monotonic, which is worth stating plainly.** At 64 and 256
connections Tokio sometimes wins: work-stealing balances a moderate load well,
while thread-per-core rings can sit idle. The advantage becomes decisive only at
high connection counts. A cache fleet fronting a compute cluster lives at the
right-hand end of that table, which is why the default follows it — but a
deployment that will only ever see a hundred concurrent readers should measure
rather than assume.

An earlier standalone harness, using a simplified protocol outside the tree,
measured +35% throughput and −26% p99 at the same connection count. The in-repo
figures supersede it: same direction, larger effect, and reproducible with one
command.

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

### Which measurement decides the default

The two results measure different things, and only one of them measures the
thing Talon is built for.

A cache fleet serves many clients at once. That is the whole premise: workers
sit between a compute fleet and an object store, and the interesting regime is
hundreds or thousands of concurrent readers. The 1024-connection measurement is
the one taken in that regime, and it favours `io_uring` on every axis
simultaneously — throughput, per-core efficiency, tail latency, and memory.

The concurrency-1 result is not evidence against that. It is a measurement
taken with an instrument that **cannot detect the effect in question**: with one
request in flight there are no syscalls to batch, so the mechanism that produces
the win is not exercised. A null result from an instrument blind to the effect
says nothing about whether the effect exists.

So `io_uring` is the default, and the Divan benchmark keeps its job as a
**regression floor** — it catches a change that makes single-request latency
materially worse, which is a real thing to guard against, and it is honest about
not being the evidence for the default.

Both measurements now live in the tree and can be re-run:

```sh
cargo bench -p talon-worker --bench dataplane_benches   # latency floor
scripts/dataplane_loadtest.sh                           # concurrent sweep
```

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
# The table above: both planes, sweeping connection counts against a real worker
scripts/dataplane_loadtest.sh
CONNS=1,512,2048 SECONDS_PER=30 scripts/dataplane_loadtest.sh

# Single-request latency floor, both planes, same block over loopback TCP
cargo bench -p talon-worker --bench dataplane_benches

# Drive an already-running worker, sampling its CPU and RSS
talon-loadgen --addr 127.0.0.1:7001 --conns 1024 --server-pid "$(pgrep -x talon-worker)"

# Confirm SO_REUSEPORT: N listeners on one port
talon-worker --data-plane-rings 2 --listen 127.0.0.1:7001
ss -ltn | grep 7001
```

See [BENCHMARKS.md](../../BENCHMARKS.md) for the harness, the committed
baselines, and why the benchmark CI job is informational rather than gating.
