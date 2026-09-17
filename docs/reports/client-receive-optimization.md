# Native SDK receive coalescing

> Historical development snapshot. Later ownership, scheduling and performance
> results are documented in [the final hot-path report](client-hotpath-validation.md).

## Change

This follow-up in `perf/client-uring` optimizes native range-response reception.
Plain, version-pinned and cache-only range reads share the path. Request dispatch,
thread counts, connection pooling and retry policies retain their previous design.

Previously, the client completed a 16-byte header receive before submitting the
body receive, even when the whole response was already queued in the socket.
It now submits one native `readv` into separately owned header and body buffers.
The initial body space is `min(requested_length, 64 KiB)`; data already available
can fill both in one completion. Short reads finish the header first, validate
frame/type/length/error limits, and only then grow and receive the remaining body.
A short error response never waits for the requested successful range length.
An interrupted first receive retries; EOF and malformed replies discard the
exchange. Unexpected prefetched bytes after a reply are rejected, not discarded
before returning the connection to the pool. Empty range, control and write
responses retain the existing header-first reader.

Monoio's published `VecBuf` owns both buffers and iovec metadata through the
operation; the pinned dependency's `ReadVec` also owns the FD reference through
completion. No new unsafe implementation, dependency or Monoio patch was added.
Body bytes are not shifted to strip the header. A validated response larger than
the prefix may reallocate its body buffer while completing the remainder.

Costs: the bounded speculative body is allocated and zero-initialized before the
header arrives, and `VecBuf` adds temporary vector metadata allocations. A stalled
large-range receive can therefore hold up to 64 KiB before receiving its header.
This trades memory/allocation work for fewer receive operations when bytes arrive
together; fragmented traffic and large responses need additional completions.

## Verification

Warnings-denied workspace tests: **1,330 passed / 36 ignored**. Explicit native
checks: **14 passed**, including six new receive tests and the existing eight
native RPC/full SDK checks. Seccomp fallback: **1 passed**. Python: **4 passed**.
Workspace all-target/all-feature Clippy, rustdoc and formatting passed.

The real-ring receive probe verifies a queued 4 KiB response completes with
**one native readv and zero further reads**. Other cases cover partial headers,
header-only first fragments, partial bodies, a complete 64 KiB prefix followed by
more body data, short typed errors on a kept-open socket, invalid advertised
lengths, trailing bytes and interrupted first receives. Existing native tests
exercise cancellation and caller-buffer safety on the changed path.

```sh
cargo test -p talon-cache-client --lib --all-features --locked --offline monoio_client::receive_tests -- --ignored
cargo test -p talon-cache-client --test native_rpc --all-features --locked --offline -- --ignored
cargo test -p talon-rust-client --test native_client --all-features --locked --offline -- --ignored
```

The [native SDK CI workflow](../../.github/workflows/client-uring.yml) includes
the new receive tests. No remote workflow has been triggered for this local work.

## Before/after measurements

Both binaries were built with the same benchmark and release settings. Only the
production receive implementation differs. The benchmark needed one timing fix:
the measurement now starts at the earliest caller's timestamp after the warmup
barrier. Previously the parent could resume after requests had already started,
under-counting elapsed time and overstating QPS. Historical QPS percentages from
the original harness must not be used as acceptance evidence.

Environment: Linux 5.15.0-139-generic x86_64, Rust 1.96.1. All process threads were
restricted to CPU 0 for this diagnostic. The unchanged four-thread Tokio mock
server/caller and one Monoio thread therefore share that CPU; this is not a
production CPU layout. Byte validation remains enabled for both binaries.
No task compilation or other task test ran during the measurements.

Run order: before / after / after / before. Each invocation measures all three
backends with the original alternating backend order and three repetitions;
each caller warms up 20 requests, then measures 250. Values below are medians
of six runs per version/point; P50/P99 are medians of per-run percentiles.

| Bytes | Concurrency | Entry | QPS before / after | QPS change | P50 us before / after | P99 us before / after |
|---:|---:|---|---:|---:|---:|---:|
| 4,096 | 1 | MonoioNative | 53,161.5 / 67,376.5 | +26.7% | 16.5 / 12 | 18.5 / 14 |
| 4,096 | 1 | MonoioFacade | 28,108.0 / 43,142.0 | +53.5% | 34 / 20 | 41 / 25 |
| 4,096 | 1 | Tokio | 51,871.5 / 77,492.5 | +49.4% | 14.5 / 10 | 18 / 11 |
| 4,096 | 64 | MonoioNative | 70,413.0 / 70,712.5 | +0.4% | 889 / 882 | 1095.5 / 1024 |
| 4,096 | 64 | MonoioFacade | 23,237.5 / 21,102.5 | -9.2% | 2956 / 3232 | 3358.5 / 3892 |
| 4,096 | 64 | Tokio | 83,833.0 / 83,710.0 | -0.1% | 715 / 712 | 1035 / 984 |
| 65,536 | 1 | MonoioNative | 17,506.5 / 17,416.0 | -0.5% | 22 / 22 | 25 / 25 |
| 65,536 | 1 | MonoioFacade | 15,123.0 / 14,993.0 | -0.9% | 31 / 31 | 35.5 / 36 |
| 65,536 | 1 | Tokio | 17,978.0 / 17,678.0 | -1.7% | 20 / 20 | 27.5 / 26.5 |
| 65,536 | 64 | MonoioNative | 16,344.5 / 15,705.5 | -3.9% | 3856.5 / 4013 | 4287.5 / 4561 |
| 65,536 | 64 | MonoioFacade | 10,741.5 / 10,007.5 | -6.8% | 6031 / 6631 | 7797 / 8227.5 |
| 65,536 | 64 | Tokio | 17,533.0 / 17,205.0 | -1.9% | 3465 / 3476.5 | 4963.5 / 5201.5 |

[Raw measurements](client-receive-optimization.csv).

The mechanical reduction from two receives to one is established by the real-ring
test. Small serial requests show lower observed latency, but the unchanged Tokio
I/O control also shifts substantially at that point. Concurrent points show no
consistent throughput improvement, and some decrease. These short loopback runs
therefore **do not establish a stable end-to-end speedup**, nor show that all prior
SDK performance regression is fixed. They document the receive optimization and
its costs without attributing all timing variation to it. Real-network and
production-cluster acceptance remains unmeasured.
