# Small-store shared metadata: memory and read comparison

Measured on 2026-11-05 (Asia/Shanghai), using the existing QTP two-vCPU Worker
`10.5.66.32`. The control is Worker `6109fff`; the optimized Worker is `815be6e`.
Both enable small store. All GB and MB values below are decimal.

Readiness RSS falls by 46.62%, from 2.119 GB to
1.131 GB. The 1 GB target remains unmet. These measurements cover
recovery and cache-hit reads; optimized fresh-fill RSS was not measured.

| Measurement | Control | Optimized |
|---|---:|---:|
| Median RSS at readiness | 2.119 GB | 1.131 GB |
| Maximum startup high-water RSS | 2.286 GB | 1.298 GB |
| Maximum sampled read RSS | 2.130 GB | 1.140 GB |
| Median cache recovery | 4.975 s | 3.817 s |

Each variant has three warm starts. Readiness RSS ranges are
[2119372800, 2119536640] bytes for the control and [1131278336, 1131409408] bytes for the optimized Worker.
End-to-end readiness includes coordinator registration and can add a five-second
heartbeat interval; cache recovery is the startup comparison above.
Observed end-to-end readiness: control [10.049, 5.042, 4.999] seconds;
optimized [8.88, 8.947, 3.878] seconds.

| Startup phase, median | Control | Optimized |
|---|---:|---:|
| Small-store journal | 0.779 s | 0.768 s |
| Locator reconstruction | 0.994 s | 1.816 s |
| Policy membership | 1.984 s | 0.990 s |
| Total LRU preparation and recovery | 3.201 s | 1.242 s |

Policy membership is part of total LRU time, so these rows are not additive.
Locator reconstruction is now the largest startup phase and is slower than the
control; the reduction in policy preparation more than offsets it.

## Read comparison

The fixture contains 1,934,886 small objects in 3,512 segments and one exactly-1-MiB
paged block: 1,934,887 objects and 234,272,852,992 payload bytes in total. Both versions
use a strict 1 MiB small-object threshold, 64 MiB segments, 400 GiB capacity, two rings,
and disabled L1, TTI, stale GC and ownership GC. No payload refill or cache clearing
was performed between variants.

The versions run in alternating order across three repetitions. Each repetition
waits for file cleanup to finish and verifies all bytes of the same 17,123 sampled objects,
then tests up-to-4-KiB and whole-object version-pinned reads at 1, 64 and 128 connections.
Each point uses five seconds of warmup and twenty measured seconds. The middle repetition
reverses workload/concurrency order. Results below are medians of three points.

| Read | Connections | Control QPS | Optimized QPS | Change | Control P99 | Optimized P99 |
|---|---:|---:|---:|---:|---:|---:|
| Up to 4 KiB | 1 | 1,242 | 1,559 | +25.47% | 0.848 ms | 0.684 ms |
| Up to 4 KiB | 64 | 47,648 | 48,422 | +1.62% | 2.167 ms | 1.998 ms |
| Up to 4 KiB | 128 | 50,801 | 51,165 | +0.72% | 3.906 ms | 3.997 ms |
| Whole object | 1 | 1,353 | 1,443 | +6.67% | 1.220 ms | 1.278 ms |
| Whole object | 64 | 7,341 | 7,341 | +0.00% | 21.663 ms | 21.712 ms |
| Whole object | 128 | 7,359 | 7,339 | -0.27% | 89.161 ms | 89.667 ms |

Concurrent throughput differs from the control by -0.27% to +1.62%.
Single-connection ranges overlap substantially, so their median differences do not
establish a stable improvement.

At 128 connections, median Worker CPU time per request is:
4k 38.90 → 38.77 µs (control → optimized).
full 72.02 → 72.56 µs (control → optimized).

Single-connection throughput varies between runs:

- 4k: control [1229.05, 1595.85] QPS; optimized [1222.1, 1709.0] QPS.
- full: control [1103.65, 1716.7] QPS; optimized [1315.3, 1458.05] QPS.

All 36 measured points passed: 13,969,618 successful measured requests, zero errors,
zero backend fetch bytes, zero cache misses, zero evictions and no concurrent cleanup scans.
Measured physical disk reads total 0 bytes. This is a warm cache-hit
comparison, not a cold-disk or origin benchmark. The byte-exact verification repeated
after the heap capture also passed.

## Retained heap

A separate heaptrack startup capture retained 1,016,979,005 requested bytes
at readiness. Its RSS is excluded from the preceding measurements. Killing the profiled
test process at readiness preserves live allocation traces; these are not a leak diagnosis
or a partition of RSS. Allocation ownership is grouped without double-counting:

| Allocation owner | Live requested MB |
|---|---:|
| Shared identity wrappers and strings | 409.426 |
| Unified locator and policy directory | 303.490 |
| Journal model record trees | 121.517 |
| Access recency state | 92.875 |
| LRU ordered queue | 50.659 |
| Runtime segment offset trees and segment metadata | 36.828 |
| Other | 2.185 |

## Implementation and validation

Journal records, locators and eviction queues now share the same BlockKey and string
buffer. Borrowed lookups avoid encoding an identity on each read; journal and access-log
bytes retain their existing format. Small-store directories keep locator and policy state
in dense reusable slots, eliminating their separate policy-only directory and whole-unit
allocations. Access clocks are allocated only when age tracking is enabled.

Policy insertion and locator publication hold the same LRU mutex. A paused-publication
regression verifies that a concurrent policy removal cannot release an unpublished locator.
Deletion keeps the shared record until both locator and policy are retired; compaction
preserves the access token and readers retain their data-file descriptors.

The development container passed rustfmt checking, 673 Worker library tests (8 existing
ignored tests), Clippy with warnings denied, and the release build. Tests cover identity
sharing after live admission and recovery, disk-byte compatibility, retirement/readmission,
late policy removal, and enabling/restoring access clocks. Independent adversarial review
identified the publication race and rechecked its fix. No cold-page redesign was included.

The interrupted preliminary run is retained separately and excluded from these tables.
The original Worker service was restored and verified ready; the test Worker, test
coordinator and origin services returned to their original inactive states. All changes
remain in the local stack.

Evidence: `target/benchmarks/small-store-opt-20261104/` contains the driver, raw startup
logs, metrics, per-case samples, heap traces, summaries and restoration audit.

| Binary | SHA-256 |
|---|---|
| packed-worker | `ca70b892a78fc535fa978a88555d68bf8aa887ba6829f917bfa3fb17369b8bd1` |
| small-read-probe | `2f506473462a7b4e933d6e91901e78ea54e03f276310cfe226e2f106d1fd598e` |
| optimized-worker | `c320c32a57d211130f7e47f6c0b9d452432c6fe2fb78fd727bc4d0b539ab3459` |
