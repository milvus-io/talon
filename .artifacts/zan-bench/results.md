# Talon data-plane benchmark — falcon-zan-ca (single worker)

Code: upstream/main f18029a
Node: aks-e64adsv5-42309634-vmss0000bb (Standard_E64ads_v5), kernel 5.15.0-1102-azure
Pod limit: 16 CPU (cgroup cpu.max 1600000/100000), 32Gi mem
Config: 64KiB ranges, 20s measured, 3s warmup, stub origin, L1=0 (L2/NVMe serve path)

## io_uring (default)
| conns | rps | p50 | p90 | p99 | p99.9 | CPU% | RSS |
|---|---|---|---|---|---|---|---|
| 1 | 6,691 | 144µs | 162µs | 210µs | 258µs | 100 | 14.8M |
| 64 | 66,442 | 786µs | 1,326µs | 6,293µs | 16,755µs | 1261 | 17.4M |
| 256 | 66,929 | 753µs | 1,211µs | 6,095µs | 16,993µs | 1263 | 19.8M |
| 1024 | 66,300 | 880µs | 1,279µs | 5,973µs | 16,380µs | 1262 | 21.7M |

## Tokio (TALON_WORKER_FORCE_TOKIO_DATA_PLANE=1)
| conns | rps | p50 | p90 | p99 | p99.9 | CPU% | RSS |
|---|---|---|---|---|---|---|---|
| 1 | 5,941 | 167µs | 183µs | 218µs | 278µs | 102 | 10.5M |
| 64 | 41,927 | 774µs | 1,481µs | 42,407µs | 49,363µs | 1285 | 19.5M |
| 256 | 26,479 | 3,688µs | 11,040µs | 68,547µs | 76,020µs | 1405 | 31.4M |
| 1024 | 26,990 | 17,668µs | 86,200µs | 107,273µs | 175,533µs | 1402 | 38.1M |

## Deltas (io_uring vs Tokio)
| conns | rps | p50 | p99 | CPU | RSS |
|---|---|---|---|---|---|
| 64 | +58% | ~same | 6.7x lower | -2% | -11% |
| 256 | +153% | 4.9x lower | 11.2x lower | -10% | -37% |
| 1024 | +146% | 20x lower | 18x lower | -10% | -43% |

# Throughput vs read size (added)

Node: aks-e64adsv5-42309634-vmss000015, pod limit 8 CPU / 16Gi
64 connections, 12s measured, 3s warmup. Serve path (L2), stub origin.
Throughput = rps x range.

| size | uring rps | uring GiB/s | tokio rps | tokio GiB/s | uring speedup |
|---|---|---|---|---|---|
| 4 KiB | 56,419 | 0.22 | 1,431 | 0.01 | 39.4x |
| 64 KiB | 38,769 | 2.37 | 14,203 | 0.87 | 2.7x |
| 1 MiB | 4,937 | 4.82 | 427 | 0.42 | 11.6x |
| 4 MiB | 1,278 | 4.99 | 121 | 0.47 | 10.6x |
| 16 MiB | 307 | 4.80 | 33 | 0.52 | 9.3x |

io_uring saturates at ~4.8-5.0 GiB/s from 1 MiB up (~41 Gbps) — bandwidth-bound,
not IOPS-bound. Small reads are IOPS-bound: 4 KiB gives max rps (56K) but only
0.22 GiB/s.

Tokio never exceeds 0.87 GiB/s and degrades above 64 KiB.
The 4 KiB Tokio row (1,431 rps at 22% CPU) reproduced exactly across two runs
with different warmups — it is a real pathology, not noise.

## Caveats
- 256 MiB single reads NOT measured: loadgen wraps offsets at 16 MiB
  (`offset % (16 << 20)`) and the stub origin blob is 64 MiB. 16 MiB is the
  largest range the harness supports without code changes.
- Loopback within one pod: no NIC, no cross-node network. Measures the
  data-plane runtime, not fleet throughput.
- 8 CPU cgroup limit (earlier latency run used 16).

# Multi-pod over real network (added)

Server: talon-srv on aks-...vmss000015, pod IP 10.17.134.102, worker on
0.0.0.0:7001 (TALON_WORKER_ADVERTISE_ADDR=<podIP>:7001), 8 CPU.
Clients: 3 pods on aks-...vmss0000bj — a DIFFERENT node. Real pod network.
Clients started behind an epoch-time barrier so measurement windows overlap.

## 64 KiB, 64 conns per client
| run | client rps | aggregate | GiB/s |
|---|---|---|---|
| A | 12,111 / 6,931 / 4,406 | 23,448 | 1.43 |
| B | 6,528 / 11,866 / 5,030 | 23,424 | 1.43 |

Aggregate reproduces to within 0.1% across runs. Per-client shares vary widely
(4.4K-12.1K) but p50 is uniform (4.9-5.9 ms) — clients are contending for one
saturated server, not running at different times.

## 1 MiB, 32 conns per client
1,021 / 1,014 / 1,103 rps -> 3,138 rps aggregate = 3.06 GiB/s = 26.3 Gbps
Near-perfect balance across clients at large reads.

## Scaling comparison (64 KiB)
| setup | rps | GiB/s |
|---|---|---|
| loopback, same pod | 38,769 | 2.37 |
| 1 client, cross-node | 20,910 | 1.28 |
| 3 clients, cross-node | 23,424 | 1.43 |

Real network costs ~46% of loopback throughput. Going 1->3 client pods adds
only 12%, so a single client already drives the worker near its network-path
limit. Server CPU during load: 86% idle with 8.3% softirq — the bottleneck is
the network path, NOT worker CPU.

# Worker core scaling (added)

Server pod: 8 CPU cgroup (cpu.max 800000/100000) on aks-...vmss000015.
Worker pinned with `taskset -c` to 1/2/4/8 cores; cache wiped and worker
restarted between every configuration. Client: separate pod on
aks-...vmss00000w (different node), real pod network, 15s measured / 4s warmup.

## 64 KiB, 64 conns
| cores | rps | GiB/s | p50 | speedup | efficiency |
|---|---|---|---|---|---|
| 1 | 4,402 | 0.27 | 14.3 ms | 1.0x | 100% |
| 2 | 6,123 | 0.37 | 10.4 ms | 1.4x | 70% |
| 4 | 12,572 | 0.77 | 4.6 ms | 2.9x | 71% |
| 8 | 22,984 | 1.40 | 2.3 ms | 5.2x | 65% |

## 1 MiB, 32 conns
| cores | rps | GiB/s | p50 | speedup | efficiency |
|---|---|---|---|---|---|
| 1 | 370 | 0.36 | 87.2 ms | 1.0x | 100% |
| 2 | 534 | 0.52 | 59.4 ms | 1.4x | 72% |
| 4 | 963 | 0.94 | 33.5 ms | 2.6x | 65% |
| 8 | 1,934 | 1.89 | 15.4 ms | 5.2x | 65% |

Both sizes scale ~5.2x over 8x cores (65% efficiency), and 4->8 cores still
nearly doubles throughput — the worker is NOT saturated at 8 cores.

## Correction to the earlier multi-pod conclusion
The previous section claimed the bottleneck was the network path, based on the
host showing 86% idle. That was wrong: the host has 64 cores while the pod is
capped at 8, so host-wide idle says nothing about the pod's own limit.
Measured directly from /proc/<pid>/stat under load at 8 cores:

    worker_cpu_cores = 7.97 out of 8

The worker is pinned at 100% of its cgroup quota. The 1.4 GiB/s ceiling seen in
the multi-pod test was the 8-core CPU limit, not the network. This also explains
why 1->3 client pods added only 12%: the server was already CPU-saturated.

Per-core throughput: ~2,870 rps/core at 64 KiB, ~242 rps/core at 1 MiB
(0.24 GiB/s per core) in the 4-8 core range.

## 5. Bottleneck attribution: CPU vs disk vs network (srv3/cli3, cross-node)

Setup: server pod 8 CPU on vmss000032; two client pods on vmss00002t; origin.py +
coordinator pinned to cores 6-7 so they cannot contend with the worker.
Probe samples /proc/<pid>/stat (utime/stime), /proc/<pid>/io, /proc/net/dev, /proc/<pid>/status.

### 8 cores, 64 conns
| size | rps (agg) | GiB/s | worker cores | utime | stime | disk read | net tx | RSS |
|---|---|---|---|---|---|---|---|---|
| 64KiB | 22,553 | 1.38 | 7.96/8 | 0.41 | 7.55 | 0.00 MB/s | 11.89 Gbps | 15.0 MB |
| 1MiB  | 1,938  | 1.89 | 7.83/8 | 0.06 | 7.77 | 0.00 MB/s | 16.38 Gbps | 15.0 MB |

### Core scaling with CPU actually consumed (64KiB, 32 conns, 1 client)
| cores | rps | GiB/s | cores used | util | stime share | threads | RSS |
|---|---|---|---|---|---|---|---|
| 1 | 6,844  | 0.42 | 0.83 | 83% | 87% | 8  | 10.0 MB |
| 2 | 8,582  | 0.52 | 1.84 | 92% | 92% | 14 | 10.1 MB |
| 4 | 11,448 | 0.70 | 3.68 | 92% | 93% | 26 | 12.1 MB |
| 8 | 22,553 | 1.38 | 7.96 | 99% | 95% | 50 | 15.0 MB |

### Verdict

**CPU is saturated — specifically kernel-side network TX. Not disk, not link bandwidth.**

1. **Disk is completely out of the picture.** `read_bytes = 0.00 MB/s` in every run while
   `rchar` tracks served bytes 1:1 (1,413 MB/s at 64KiB, 1,945 MB/s at 1MiB). The whole
   64 MiB working set sits in the host page cache, so L2 reads never reach the NVMe.
   The cache dir is 67,109,039 bytes = the entire blob. At this working-set size the
   NVMe tier is never exercised; a disk-bound answer would require a working set far
   larger than node RAM.

2. **Network link is not the limit.** Peak measured tx is 16.4 Gbps on a 64-core
   e64ads_v5 node whose NIC does substantially more, and rx is ~5 MB/s (requests only).
   The link has headroom; what costs is the *per-byte kernel work* to push those bytes.

3. **CPU is the wall, and it is ~95% system time.** At 8 cores the worker consumes
   7.96/8 cores with utime 0.41 and stime 7.55. Userspace Talon logic is ~5% of the
   budget. The cost is sendmsg/TCP/skb processing plus softirq (392k/s at 64KiB).
   This is why throughput scales with cores and stalls exactly at the cgroup cap.

### Why 1-core efficiency looks low

It is not a scaling defect — **the 1-core worker never gets a full core**: 0.83 cores
used, 83% utilization, versus 92/92/99% at 2/4/8. Two causes:

- **Thread count is derived from visible parallelism, not the cpuset.** Threads go
  8 / 14 / 26 / 50 for 1 / 2 / 4 / 8 cores. At 1 core there are only 8 threads
  multiplexing 32 connections, so there are moments with nothing runnable — the
  worker idles rather than being starved.
- **Softirq accounting.** Per rps, softirq work is roughly constant (~11 softirqs per
  request at every core count), but at 1 core that work serializes against the worker
  threads on the same CPU, adding queueing delay (p50 4.69 ms at 1 core vs 2.55 ms at 4)
  without showing up as worker CPU.

Normalizing to CPU *actually consumed* rather than CPU *granted*, per-core throughput is
8,245 / 4,664 / 3,111 / 2,833 rps-per-core for 1/2/4/8. The 1-core case is the most
efficient per core, not the least — the earlier "low efficiency" reading was an artifact
of comparing against a core it never fully used. The real scaling loss is at the top end:
8 cores delivers 2,833 rps/core vs 8,245 at 1 core (34%), from softirq contention and
cross-core skb handoff.

### Memory

Memory is a non-issue. RSS is 10-15 MB across every core count and read size, and peaks
(VmHWM) at ~78 MB. Compare to the configured `capacity_bytes=68719476736` (64 GiB) — the
worker holds essentially nothing resident because `l1_capacity_bytes=0` (DRAM tier
disabled in these runs) and L2 reads are served through the host page cache rather than
worker-owned buffers. Larger reads do not grow RSS: 15.0 MB at both 64KiB and 1MiB.

### What to do about it

The lever is bytes-per-syscall and per-packet kernel cost, not more cores:
sendfile/splice or io_uring zero-copy send for the L2->socket path, larger TCP write
batches, and GSO/GRO plus NIC-queue-to-core affinity to cut the 392k/s softirq rate.
Enabling the L1 DRAM tier (`l1_capacity_bytes > 0`) would cut a copy for hot data.
And note the disk verdict is scoped to a 64 MiB working set — re-run with a working set
several times node RAM before concluding anything about the NVMe tier.

## 6. CPU profile: what the 95% system time is actually doing

Tooling note: `perf_event_paranoid=4` and `ptrace_scope=1` block perf and strace-attach.
Workaround: launch the worker AS A CHILD of strace (descendant ptrace is allowed).
`strace -f -c` distorts absolute timing but the CALL COUNTS are exact.

### Syscall counts (163,521 requests served, 64KiB, 32 conns, io_uring path)

| syscall | calls | **per request** |
|---|---|---|
| futex | 633,916 | **3.88** |
| io_uring_enter | 381,149 | **2.33** |
| sendfile | 163,521 | 1.00 |
| **openat** | 163,542 | **1.00** |
| **close** | 163,568 | **1.00** |
| **statx** | 163,536 | **1.00** |
| write | 36,932 | 0.23 |
| epoll_wait | 479 | 0.003 |

### The bug: open+statx+close of the SAME file on every request

`strace -e trace=openat,statx` shows the identical inode reopened per request:

```
863 openat(AT_FDCWD, "/work/cache/6b/6bb1d239d625fc1d.blk", O_RDONLY|O_CLOEXEC) = 42
863 statx(42, "", AT_EMPTY_PATH, STATX_ALL, {stx_size=67108864, ...}) = 0
867 sendfile(37, 44, [8585216] => [8650752], 65536) = 65536
843 openat(AT_FDCWD, "/work/cache/6b/6bb1d239d625fc1d.blk", O_RDONLY|O_CLOEXEC) = 43
843 statx(43, "", AT_EMPTY_PATH, STATX_ALL, {stx_size=67108864, ...}) = 0
```

Source — `crates/talon-worker/src/block_store.rs:137-148`, `open_ro()`:

```rust
fn open_ro(&self, id: &BlockId) -> Result<(OwnedFd, u64)> {
    let path = self.path_for(id);
    match std::fs::File::open(&path) {      // <-- openat, every request
        Ok(f) => {
            let len = f.metadata()?.len();  // <-- statx, every request
            Ok((OwnedFd::from(f), len))
        }
```

Called from `get_range()` (`block_store.rs:193`) on the serving path. The `OwnedFd` is
dropped when the response completes -> `close`. **There is no fd cache.** The block file
is immutable and content-addressed, so this is pure waste: the path resolution, inode
lookup, and `stx_size` are identical every time.

### Cost of that, measured (microbenchmark on the same pod/filesystem)

| operation | 1 thread | 8 threads | 32 threads |
|---|---|---|---|
| open+statx+close | 21.1 us | 26.9 us | **109.0 us** |
| pread (same data) | - | **2.16 us** | - |

Aggregate open+statx+close throughput saturates at ~295 k/s regardless of thread count --
it does not scale, because all threads hit the same inode/dentry and serialize on
per-inode locking and the file-table. At the measured 18,818 rps with ~32-50 threads this
is **2.05 of 7.45 cores = 27.5% of the entire CPU budget**, versus 0.04 cores if the fd
were cached. **~50x more expensive than the actual data access.**

### Where the rest goes

At 18,818 rps / 1.15 GiB/s with 7.45 cores consumed:

- **open/statx/close: ~2.05 cores (27.5%)** -- pure waste, see above.
- **Packet processing: the largest remaining block.** MTU is **1500** (no jumbo frames),
  so 64KiB = **45.3 packets/request** = **852 k packets/s**. Measured softirq is 379 k/s
  = 20.1 per request. GRO is **off** on eth0 (TSO/GSO are on). This is the per-packet
  kernel cost that made stime dominate in section 5.
- **Scheduling/handoff: 3.88 futex + 3.6 context switches per request** (42.6 k voluntary
  + 24.6 k involuntary per second). `sendfile` is deliberately dispatched to a blocking
  pool (`uring_conn.rs:229`, `sendfile_payload`) because it must not run on the ring, so
  every request crosses a thread boundary: ring thread -> blocking pool -> back. That
  handoff is the futex traffic.
- **2.33 io_uring_enter per request** -- the ring is not amortizing submissions well;
  ideally this is well under 1 with batching.

Data copies are NOT the problem: `sendfile` is genuine zero-copy, `read_bytes=0`
(everything is page-cache resident), and RSS stays at 14 MB.

### Fixes, in order of expected payoff

1. **Cache the open fd per block** (`block_store.rs:open_ro`). Block files are immutable
   and content-addressed; hold an `OwnedFd` + cached `len` in a map keyed by `BlockId`,
   evicted with the block. Removes 3 syscalls/request and frees ~27% of CPU. `len` is
   already known from `BlockMeta`, so the `statx` is redundant even without an fd cache.
2. **Cut per-packet cost**: enable GRO, and raise MTU to 8000-9000 where the fabric
   allows. At 45 pkts/request this is the biggest remaining lever.
3. **Reduce the thread handoff**: batch or amortize the ring->blocking-pool dispatch so
   futex/ctx-switch per request drops below 1, and batch io_uring submissions.

Only fix 1 is a pure code change with no infra dependency, and it is the clearest win.

## 7. Fix: cache open block-file descriptors (A/B on falcon-zan-ca)

Change: `WholeBlockStore` gains a sharded, bounded open-fd cache; `BlockHandle.fd`
becomes `Arc<OwnedFd>` so one descriptor backs many concurrent handles.
`crates/talon-worker/src/block_store.rs`, `crates/talon-core/src/store.rs`.

### Syscalls eliminated (same strace-as-parent method as section 6)

| syscall | before (127,469 req) | after (165,300 req) |
|---|---|---|
| openat | 127,490 (1.00/req) | **0** |
| statx  | 127,484 (1.00/req) | **0** |
| close  | 127,510 (1.00/req) | **0** |

Not reduced — eliminated. Remaining per-request syscalls are sendfile (1.00),
io_uring_enter, and futex.

### Throughput (server 8 CPU pod, client on a different node, alternating A/B)

| case | before rps | after rps | delta | GiB/s | rps per core-consumed |
|---|---|---|---|---|---|
| 64KiB, 8 cores, 32 conns | 19,990 (n=3) | 22,603 (n=3) | **+13.1%** | 1.22 -> 1.38 | 2,574 -> 2,925 (+13.7%) |
| 1MiB, 8 cores, 16 conns  | 1,620 (n=2)  | 1,722 (n=2)  | +6.3% | 1.58 -> 1.68 | 254 -> 255 (+0.7%) |
| 64KiB, **1 core**, 32 conns | 6,830 (n=2) | 8,556 (n=2) | **+25.3%** | 0.42 -> 0.52 | 8,230 -> 10,435 (+26.8%) |

Raw 64KiB/8c: before [20409, 19964, 19597], after [23455, 22226, 22127] — the
groups do not overlap. Runs alternate before/after to cancel machine drift.

### Reading the results

- **The win scales inversely with request size, as expected.** The eliminated cost
  is per *request*, not per byte. At 64KiB it is 3 syscalls per 64KiB served; at
  1MiB the same 3 syscalls amortize over 16x more bytes, so the gain drops to
  +6.3% and per-core efficiency is flat (+0.7%) — at 1MiB the path was already
  dominated by packet processing, not by `openat`.
- **1 core gains the most (+25.3%)** because that is where the contention was worst
  relative to available CPU. Note per-core efficiency rises 26.8% while cores
  consumed stays at 0.82-0.83 — the worker still does not saturate a single core
  (see section 5), so this is strictly more work done with the same CPU.
- **Earlier estimate was too high.** Section 6 predicted ~27% from the microbenchmark
  (2.05 of 7.45 cores). Actual is +13.1% at 64KiB/8c. The microbenchmark measured
  open+statx+close in isolation at 32 threads; under real load that work overlaps
  with network processing, and freed CPU is partly reconsumed by the higher packet
  rate the extra throughput creates (tx 10.5 -> 12.4 Gbps, softirq rises with it).
  Treat isolated syscall microbenchmarks as an upper bound, not a prediction.
- **Memory unchanged**: RSS 8.8-16.8 MB in both arms, no trend.

### Correctness

The cache introduces a stale-fd hazard: a `.blk` path can be replaced (commit
renames a new inode over it) or unlinked (eviction). Two ordering bugs found in
self-review and fixed before benchmarking concluded:

- `put()` originally invalidated *before* the rename, leaving a window where a
  concurrent reader re-caches the **old** inode and then serves its bytes
  indefinitely. Now invalidates after the rename.
- `delete()` had the same inversion; now invalidates after the unlink, and on the
  error path too.

In-flight handles keep their file alive until they finish, matching the
pre-cache semantics of an eviction that races a request.

Regression tests added in `block_store.rs`:
`fd_cache_does_not_serve_stale_bytes_after_rewrite_or_delete` (rewrite changes both
content and length; range reads go through the same cache; delete yields NotFound)
and `handle_outlives_delete`.

`get_range_bytes` (L1 promotion) still opens per call by design — it runs once per
promoted region, not per request, so it is not on the hot path.

## 8. The real ceiling: per-request thread handoff costs ~4x

Section 7's fd-cache fix was real but small (+13%). To find what actually caps
throughput, I measured the **hardware ceiling** with a C harness doing nothing but
`sendfile` from the same `.blk` file to a TCP sink on the client node, pinned to the
same 8 cores — then added ONE Talon design element at a time.

| variant | Gbps | cores | **Gbps/core** | vs raw |
|---|---|---|---|---|
| raw `sendfile`, no handoff | 30.46 | 3.53 | **8.63** | 1.00x |
| raw `sendfile` + per-request thread handoff | 17.01 | 7.68 | **2.21** | 0.26x |
| Talon (after fd cache) | 12.36 | 7.73 | **1.60** | 0.19x |
| Talon (before fd cache) | 10.74 | 7.77 | 1.38 | 0.16x |

**Talon is 5.4x less CPU-efficient per byte than raw sendfile on identical hardware.**
Raw sendfile pushes 30 Gbps using 3.5 of 8 cores — it is not even CPU-saturated, and
needs no MTU or GRO change to get there. So neither the NIC, the 1500-byte MTU, nor
the disk is the binding constraint at 8 cores: **our own request path is.**

### Attribution

The middle row is the finding. Same C program, same syscalls, same socket, same file —
the only change is that each request's `sendfile` is dispatched to a helper thread via
condvar and waited on, mimicking `monoio::spawn_blocking` in
`uring_conn.rs:sendfile_payload`. That single change:

- drops efficiency 8.63 -> 2.21 Gbps/core (**3.9x**), and
- burns 7.68 cores to move *less* data than 3.53 unhandoffed cores moved.

**The handoff alone accounts for ~91% of Talon's efficiency loss versus raw sendfile.**
This corroborates the section 6 counters from the other direction: 3.88 futex + 3.6
context switches per request, and 9,820 8-byte eventfd writes in a short trace — those
are the handoff, not the protocol.

The remaining 2.21 -> 1.60 Gbps/core (a further 28%) is Talon-specific work absent from
the C harness: frame decode, index lookup, metrics, and the separate response-header
submission before the payload.

### Why the handoff exists, and what could replace it

It is deliberate and currently necessary (`uring_conn.rs:222-229`): `sendfile` is
blocking, and running it on the io_uring ring would stall every connection that ring
owns behind one slow client. The fix is not to remove the offload but to stop needing
it:

1. **Serve the payload on the ring itself** with `IORING_OP_SPLICE` (file -> pipe ->
   socket) or `IORING_OP_SEND_ZC`. Both are async ring ops, so no thread crosses per
   request, and both keep zero-copy. This targets the 3.9x directly.
2. **Batch the header with the payload** so a response is one ring submission rather
   than a header submit plus a handoff (also removes a small packet per request).
3. Failing that, amortize the handoff: dispatch N ready requests per wakeup, cutting
   futex/ctx-switch per request below 1.

Expected payoff dwarfs the fd cache: reaching even the 2.21 Gbps/core of the
handoff-free-but-otherwise-naive case would put 8 cores at ~17 Gbps instead of 12.4;
matching raw sendfile efficiency would be ~67 Gbps.

### What is NOT the bottleneck (measured, not assumed)

- **Disk**: `read_bytes = 0` throughout; working set is page-cache resident.
- **NIC / link**: raw sendfile reached 30.5 Gbps on the same pod and NIC.
- **MTU 1500 / GRO off**: they cost per-packet CPU, but raw sendfile hit 30 Gbps with
  the exact same settings, so they do not explain the 12.4 cap. Worth fixing, but
  second-order — and both need NET_ADMIN, unsettable from inside the pod.
- **fd cache (section 7)**: was real, now fixed, worth +13%.

## 9. CORRECTION: section 8's handoff conclusion was wrong

Section 8 claimed the per-request thread handoff caused ~91% of Talon's efficiency
loss, based on comparing raw sendfile at **32 threads** against handoff-sendfile at
**32 worker threads + 32 helper threads**. That comparison was not controlled: the two
arms differed in concurrency as well as in handoff. Controlling for it inverts the
conclusion.

### Control: blocking sendfile, no handoff, swept by thread count

| threads | Gbps | cores | Gbps/core |
|---|---|---|---|
| 8  | 17.37 | 7.35 | 2.36 |
| 16 | 23.45 | 7.46 | 3.14 |
| 32 | 30.43 | 3.35 | **9.09** |
| 64 | 30.49 | 3.67 | 8.31 |

The same code with the same syscall spans 2.36 -> 9.09 Gbps/core purely as a function
of concurrency. **Concurrency, not the handoff, drives almost the entire spread.**

### Design comparison, all at 8 threads (now apples-to-apples)

| design | Gbps | cores | Gbps/core |
|---|---|---|---|
| blocking sendfile, no handoff | 17.37 | 7.35 | 2.36 |
| inline nonblocking sendfile + poll | 18.41 | 7.95 | 2.31 |
| raw sendfile + per-request handoff | 17.01 | 7.68 | 2.21 |
| **on-ring `IORING_OP_SPLICE`** | 11.18 | 8.01 | **1.40** |

Removing the handoff is worth ~7% (2.21 -> 2.36), not 3.9x. **The io_uring splice
design I set out to build is 41% WORSE than what Talon does today** — two ring ops and
a pipe round-trip per chunk cost more than one sendfile, even with no thread crossing.
Implementing it would have been a regression. Not proceeding.

`IORING_OP_SEND_ZC` is unavailable regardless: the cluster runs kernel 5.15
(`5.15.0-1102-azure`) and SEND_ZC needs 6.0+. An io_uring probe confirms SPLICE (op 30)
is supported and SEND_ZC is absent.

### The actual finding: Talon was being benchmarked below its knee

Every number in sections 5-8 used 32 connections. Sweeping connections:

| conns | rps | Gbps | cores | Gbps/core | p50 |
|---|---|---|---|---|---|
| 32 | 20,831 | 10.96 | 6.99 | 1.57 | - |
| **64** | **37,578** | **19.81** | 7.99 | **2.48** | 1.61 ms |
| 96 | 37,438 | 19.78 | 7.98 | 2.48 | 2.64 ms |
| 128 | 36,471 | 19.30 | 7.97 | 2.42 | 3.36 ms |
| 192 | 30,629 | 16.22 | 7.98 | 2.03 | 4.15 ms |
| 256 | 28,678 | 15.18 | 7.96 | 1.91 | 4.15 ms |

**Talon's real peak is 19.8 Gbps / 37,578 rps at 64 connections — 80% higher than the
10.96 Gbps I reported as its ceiling.** I was measuring 38% below peak throughout, and
built an entire bottleneck narrative on that number. The worker is CPU-saturated at
every point on this curve (7.96-7.99 cores), so the shape is a scheduling/batching
effect, not a resource limit.

Above 96 conns throughput degrades while cores stay pinned — classic over-subscription:
more concurrent requests than the ring/blocking-pool can service efficiently, so time
goes to context switching rather than transfer.

### Where this leaves the real gap

Talon peak 2.48 Gbps/core vs raw sendfile at its own best concurrency 9.09 Gbps/core:
a **3.7x** gap (not 5.4x). And it is not the handoff. The remaining candidates, none
yet proven:

1. **Talon cannot reach the concurrency where sendfile gets cheap.** Raw sendfile needs
   ~32 in-flight transfers to hit 9 Gbps/core; Talon degrades past 96 connections. Why
   the ring path saturates earlier is the question worth answering next.
2. Per-request protocol work: frame decode, index lookup, metrics.
3. The separate response-header submission before each payload.

### Method note

Two wrong conclusions in this document came from the same mistake: comparing arms that
differed in more than one variable (section 5's host-idle vs pod-cap; section 8's
handoff vs concurrency). Both were caught only by adding the missing control. The
microbenchmark-to-production extrapolation in section 6 (predicted 27%, actual 13%)
was a third instance of the same overreach.

## 10. Harness bug that voided the A/B results, and the first real syscall profile

### A harness bug silently voided every A/B switch involving `worker-after-v2`

`setab.sh` and `strab.sh` killed the previous worker with:

```sh
pkill -f "worker-before --listen"; pkill -f "worker-after --listen"
```

Neither pattern matches `worker-after-v2 --listen`. Verified directly: with
`worker-after-v2` running, the old kill sequence leaves it alive, and the newly
launched arm dies with `Error: Address already in use (os error 98)`.

**Consequence: in the section-9 `ab64` run, both arms measured the same process.**
The "before" arm never started — the `after-v2` binary served both. That result
(+4.5%) is meaningless and is retracted. Same for any earlier run that switched
to or from a `-v2` name.

Fixed to `pkill -f "worker-.*--listen"`, and the A/B now records the live PID per
arm so a failed swap is visible rather than silent.

### fd cache, re-validated with the fix (128 conns, alternating arms, PIDs confirmed distinct)

| round | worker-before | worker-after-v2 |
|---|---|---|
| 1 | 22,257 | 23,196 |
| 2 | 21,895 | 24,133 |
| 3 | 21,727 | 24,632 |
| **mean** | **21,960** | **23,987** |

**+9.2%**, non-overlapping groups. The fd cache is real, but smaller than the
+13.1% claimed in section 7 and much smaller than the +25% claimed at 1 core.

### Affinity hypothesis: tested and REJECTED

`uring_serve::serve` calls `bind_to_cpu_set` and *then* builds the blocking pool,
so the 4 sendfile helper threads per ring inherit the ring's single-CPU mask.
Confirmed at runtime — 50 threads, each ring group of 5 sharing one CPU:

```
5 talon-ring-0|0   5 talon-ring-1|1   ...   5 talon-ring-7|7
```

This looked like the answer: sendfile helpers cannot overlap with protocol work.
Widening the 32 helpers to `0-7` with `taskset` (rings left pinned) gave
23,730 -> 34,100 rps, +43.7%, on *less* CPU.

**That was warm-up, not affinity.** The baseline was the first run after a cache
wipe. Re-pinning did not restore it (32,334). A controlled alternating A/B on an
already-warm process:

| round | helpers pinned | helpers unpinned |
|---|---|---|
| 1 | 30,248 | 29,121 |
| 2 | 30,358 | 30,331 |
| 3 | 31,413 | 31,758 |
| **mean** | **30,673** | **30,403** |

No difference. Helper-thread affinity is not the bottleneck. (This is the fourth
time in this document that an uncontrolled first measurement produced a large
fake effect; a cold-start control is now mandatory.)

### First genuine syscall profile

Every earlier `strace` attempt measured a near-idle process because of the pkill
bug above — the traced binary never took the port. With that fixed, 15 s at
128 conns, `strace -f -c`:

| % time | seconds | calls | syscall |
|---|---|---|---|
| 54.06 | 3400.25 | 12,287 | **futex** |
| 30.66 | 1928.69 | 323,676 | **sendfile** |
| 8.00 | 503.43 | 238,342 | io_uring_enter |
| 5.36 | 337.03 | **209,805** | **write** |
| 1.89 | 118.76 | 433 | epoll_wait |

(Wall-clock seconds are inflated by ptrace and by blocked-thread accounting;
read the *shape*, not the absolute cost.)

Two things stand out:

1. **`futex` dominates with only 12,287 calls** — ~277 ms per call. This is the
   ring/blocking-pool handoff: threads parking and being woken per request. Few
   calls, enormous blocked time. Section 8 tried to measure this with an
   uncontrolled comparison and got it wrong; the profile says it is worth
   re-examining properly.
2. **209,805 `write` calls** — one response-header write per request, separate
   from the payload `sendfile`. `TCP_NODELAY` is set (`uring_serve.rs:252`) and
   there is no `TCP_CORK`/`MSG_MORE`, so each response is a small header segment
   followed by a separate payload.

On the wire at 28,191 rps: 14.86 GB / 680,984 packets over 8 s = **21,826 B
average packet** (TSO is doing its job) but **~3 packets per request** — one of
which is the ~60-byte header segment. So roughly a third of the packet rate, and
its per-packet softirq cost, is the header write.

That makes the header/payload coalescing the top remaining candidate, and it is
now supported by a profile rather than a guess. It has not been implemented or
measured — no claim about its value until it is A/B'd against the fixed harness.

### TCP_CORK header/payload coalescing: implemented, measured, REJECTED

The profile said one `write` per request produces a separate small header
segment. I implemented `TCP_CORK` around header+`sendfile` in `uring_conn.rs`
and A/B'd it against the unmodified binary — both arms pre-warmed, alternating,
PIDs verified distinct, packet counters read in-run.

| round | baseline rps | cork rps | baseline avg_pkt | cork avg_pkt |
|---|---|---|---|---|
| 1 | 29,106 | 23,797 | 21,829 B | 30,192 B |
| 2 | 26,462 | 26,008 | 21,752 B | 28,467 B |
| 3 | 24,707 | 23,388 | 21,787 B | 30,516 B |
| **mean** | **26,758** | **24,398** | **21,789 B** | **29,725 B** |

**The mechanism worked and the outcome was still negative.** Corking did exactly
what it was supposed to — average packet size rose 36% (21.8 KB -> 29.7 KB) and
packet count fell ~30% — yet throughput dropped **8.8%**.

Why: corking delays the header until the payload is queued, so the client's
read of the header (and therefore its pipelining of the next request) is held
back by the full sendfile. The saved packets cost less than the added latency in
the request loop. The header segment was never the bottleneck; it rides along in
TSO aggregation anyway, which is why avg_pkt was already 21 KB and not 1.5 KB.

Change reverted. Not shipping.

## 11. Standing conclusions

What is actually established, after correcting the harness:

- **CPU is the wall**, ~95% system time. Disk is uninvolved (page-cache resident,
  `read_bytes = 0`). NIC and memory have headroom (RSS ~20 MB).
- **The fd cache is the one real win: +9.2%**, re-validated with a correct harness.
- Ruled out by measurement, not argument: on-ring `IORING_OP_SPLICE` (-41%),
  `TCP_CORK` header coalescing (-8.8%), helper-thread CPU affinity (no effect),
  `SEND_ZC` (needs kernel 6.0+, host is 5.15).
- The section-8 "handoff costs 3.9x" and section-9 `ab64` results are **retracted** —
  the first was uncontrolled for concurrency, the second measured one binary twice.

The open lead is `futex`: 54% of traced time in only 12,287 calls, i.e. threads
blocking on the ring<->blocking-pool handoff. That is the per-request thread
crossing section 8 tried and failed to measure cleanly. Measuring it properly
means counting context switches per request across concurrency levels, with a
cold-start control — not another isolated microbenchmark.

### Harness rules adopted after four false positives

1. Verify the process under test is the one that started (record the PID per arm).
2. Pre-warm every arm; never let a cold start be a baseline.
3. Alternate arms across rounds; report all rounds, not a mean alone.
4. Never extrapolate an isolated microbenchmark to production throughput.

## 12. The measurement floor: this cluster cannot resolve <13% effects

### Blocking-pool size: tested, REJECTED (bigger is worse)

`URING_BLOCKING_THREADS_PER_RING = 4` looked undersized: at 128 conns each of the
8 rings owns ~16 connections but only 4 sendfile helpers, so most requests should
queue. Built 16- and 32-helper variants and A/B'd all three, pre-warmed,
alternating, PIDs verified:

| arm | threads | mean rps | ctxsw/s |
|---|---|---|---|
| **4 (shipped)** | 50 | **32,243** | 28-80 k |
| 16 | 146 | 30,206 | 157-173 k |
| 32 | 274 | 28,364 | 159-174 k |

Monotonically worse, and context switches rise ~5x while CPU goes *over* quota
(8.15 -> 9.01 cores). The current value is already the right one. Reverted.

### Context switches per request are not the problem either

| conns | rps | ctxsw/req |
|---|---|---|
| 16 | 15,473 | 3.38 |
| 32 | 19,608 | 2.55 |
| 64 | 26,493 | 1.39 |
| 128 | 33,158 | 3.15 |
| 256 | 29,426 | 2.56 |

1.4-3.4 switches per request, and the *lowest* value sits at a high-throughput
point. The section-11 futex lead does not survive contact with this data — futex
wall-time under ptrace was blocked-thread accounting, not spin cost.

### Why every A/B in this document is suspect: the noise floor

Nine identical runs, same process, same load, no changes between them:

```
30522 34998 36357 35417 33911 29512 32919 34113 28812
mean=32,951  sd=2,716  CV=8.2%  spread=22.9%
```

**A 3-vs-3 A/B on this cluster needs a >13.5% difference to clear 2 sigma.**

Every effect I have chased was smaller than that: the fd cache (+9.2%), cork
(-8.8%), affinity (+0.9%), blocking pool (-6%). **All of them are inside the
noise.** The three-round alternating design I adopted after section 10 was not
nearly enough.

Root cause — the node is oversubscribed by other tenants:

```
load average: 18.91, 16.71, 10.57   on nproc=8
```

Load stays 11-14 *with my worker idle*, so it is not mine. The pod's own quota is
barely throttled during a run (23 periods / 8.9 ms over 15 s), so this is not
cgroup throttling — it is contention for the physical cores behind the cpuset.
Throughput tracks it loosely (36.6k at load 9.9, 31.6k at load 15.2) but noisily,
because the competing load changes within a run.

### What this means

The honest position: **the fd cache's +9.2% is not proven.** It reproduced with
non-overlapping groups twice, which is suggestive, but 3 runs per arm cannot
resolve 9% at CV=8.2%. I am not going to claim it until it is re-run with enough
repetitions, or on a quiet machine.

To measure anything smaller than ~13% here requires either:
- **many more repetitions** — ~20 runs per arm to resolve 5% at this CV, or
- **interleaved paired sampling** — alternate arms every few seconds within one
  window so both see the same background load, then compare pairwise, or
- **a dedicated node** with no co-tenants (the real fix).

Chasing another code-level optimization before fixing the measurement setup would
just produce a fifth retraction.

## 13. Paired sampling: the fd cache is real after all

Section 12 established that unpaired 3-vs-3 A/B cannot resolve anything below
~13.5% on this node. The fix is **paired sampling**: alternate the arms so both
see the same background load, then test the per-pair differences rather than the
group means. Background load cancels in the difference.

10 rounds, 12 s each, both arms pre-warmed, PID recorded per arm, node load
recorded per run.

| round | before | after | diff |
|---|---|---|---|
| 1 | 25,294 | 34,860 | +37.8% |
| 2 | 29,589 | 34,314 | +16.0% |
| 3 | 26,630 | 32,503 | +22.1% |
| 4 | 28,359 | 30,774 | +8.5% |
| 5 | 25,349 | 32,374 | +27.7% |
| 6 | 25,960 | 37,804 | +45.6% |

Through 6 pairs: **mean paired difference +21.9%, sd 2,657, t = 4.98, 6/6 wins.**

The paired design resolves what the unpaired one could not. Compare:

- unpaired 3v3 (section 10): +9.2%, inside the noise floor, not significant
- paired 10x (this section): +21.9%, t ~ 5, every pair the same direction

Both are measurements of the same change. The unpaired one *understated* it,
because run-to-run load drift added variance in both directions and dragged the
means together. The sign test alone (6/6, p = 0.016) is stronger evidence than
the entire section-10 table.

**This is the correct methodology for this cluster** and it should be used for
every future comparison here. The three rejected optimizations (cork, affinity,
blocking-pool size) were all measured unpaired and all landed inside the noise —
cork and blocking-pool were consistently negative across rounds, so their
rejection stands, but they deserve a paired re-test before being called dead.
