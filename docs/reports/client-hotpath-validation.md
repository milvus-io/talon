# Client native hot-path overhead reduction

## Scope

This continues the local `perf/client-uring` layer above PR #591. The C TCP read
path is compared to the original Tokio client with unchanged Workers, protocol,
working set and load generator. Direct writes into caller-provided destinations
and cancellation safety remain required.

Publication base: `main` at `c391171`, after #590 and #591 merged. Its
Git tree is identical to the benchmark parent `f3a63e9`; this changes ancestry,
not the tested implementation.

## Changes relative to the previous direct-destination candidate

- Single-block reads skip the block aggregation stream and empty tail splits.
- Local Monoio and explicit Tokio range executors borrow the destination instead
  of constructing an owned cross-runtime loan for each RPC.
- One allocation combines the destination handle, recovery notification and root
  access state. A release/acquire user count gates recovery; an owner in Drop no
  longer accesses payload after releasing that count. Cancelled CQEs still retain
  storage. Real subdivisions/cross-runtime loans retain their own access state.
- Native header/iovec storage shares one stable allocation and is cached locally
  after completion (256 entries per thread). Neither a live nor a cancelled
  in-flight receive returns metadata prematurely. No payload buffer is cached or
  copied by this mechanism.
- Hosted tasks are spawned on the selected runtime directly. Ring-local callback
  continuations bypass the cross-thread command queue and separate callback/buffer
  boxes. The native lane owns one client rather than cloning its caches/pools per
  task. A local shutdown registry replaces the outer FuturesUnordered scheduler.
- Remove the added global admission semaphore and per-request lane load counters.
  External native submissions rotate lanes; native continuations keep affinity.
  As with the original Tokio submission, callers bound their own concurrency.
- Tokio hosted submissions use runtime.spawn directly. The C binding transfers
  its already-owned URI/version metadata without an additional clone.
- Native short exchanges share an initial 10 ms timer wakeup, then enforce the
  original absolute deadline for slow requests. No deadline is extended.
- Tracing and native deadline wrappers store the operation future once, using
  structural pinning instead of nested async moves. Native range reads have a
  dedicated RPC path without upload/file state. Compiler layout inspection on
  x86_64 reduced the intermediate development candidate's C native task from
  35,072 to 10,752 bytes; this is structural
  evidence, not a CPU-speed claim.
- Native range send and receive are submitted concurrently using separate Monoio
  stream halves; no send CQE is required before arming the first readv. One
  operation per direction remains the invariant; failed exchanges discard sockets.

## Necessary remaining work per operation

io_uring still requires submission/completion bookkeeping and buffer retention
through completion. The destination control object is not a staging payload.
External threads still hand work to an owning ring. The exact aggregate per-peer
idle limit still coordinates rings at pool take/return; it has not been increased
or relaxed to improve a benchmark. Shared placement/telemetry/request identifiers
already present in the original client are retained. This is ordinary TCP receive,
not hardware zero-copy RX and not a claim of zero allocations for every SDK API.

## Validation

The final native SDK/cache/C regression suites cover direct buffer addresses,
partial responses, errors, timeout/cancellation, pool reuse, concurrent progress,
subscriber isolation and inline callback owner cancellation. Added regressions
cover receive metadata reuse and short/shared/absolute native deadlines. Python
native and Tokio destination tests pass. Process-local io_uring denial passes for
the cache client, hosted SDK and C multi-block uninitialized destination.

Final workspace tests excluding Python pass; the native/cache/C suites, the
borrowed-range timeout/reuse regression, Python and process-local fallback tests
also pass. Workspace all-target/all-feature Clippy and no-dependency rustdoc deny
warnings.
Build/cache/temp/log paths are under `/data/yuruiz`; no host storage paths were
relocated or removed. Exact receipts and the deployed source manifest are in
`target/client-hotpath-20260917/`.

## Performance validation

Deployed ARM library SHA256:
`7e47deb295d54fe65b9ac48edea4fa12e1d007b2d76d02a640fe727f3cde0389`.

The same-environment dual-Worker matrix uses a 1 TiB hot working set, 4 KiB
reads, concurrency 512/1024, three rotated orders, 3 s warmup and 30 s measurement
per point. Only the client library changes; Worker FD cache remains 8192 and
both client and Workers retain 16 rings each. Values below are medians of three
runs; latency values are medians of each run's percentile, not merged percentiles.

| Concurrency | Client | QPS | P50 ms | P99 ms | Client CPU us/request |
|---:|---|---:|---:|---:|---:|
| 512 | Original Tokio | 572,782 | 0.881 | 2.383 | 20.30 |
| 512 | Previous direct-buffer Monoio | 537,581 | 0.889 | 3.955 | 25.49 |
| 512 | Optimized Monoio | 625,717 | 0.818 | 2.240 | 17.96 |
| 1024 | Original Tokio | 602,044 | 1.491 | 5.320 | 21.65 |
| 1024 | Previous direct-buffer Monoio | 536,924 | 1.685 | 7.372 | 27.50 |
| 1024 | Optimized Monoio | 700,670 | 1.190 | 7.598 | 18.55 |

At concurrency 512, throughput improves 9.2% and CPU per request falls 11.5% versus the contemporaneous Tokio control.

At concurrency 1024, throughput improves 16.4% and CPU per request falls 14.3% versus the contemporaneous Tokio control.

At 1024 concurrency the new client has higher P99 than Tokio and increments
ENA inbound bandwidth allowance counters. This does not establish an unlimited
client throughput ceiling or an across-the-board latency win. No single-change
ablation was run, so gains cannot be attributed to one removed cost.

All 18 formal windows passed: zero request/submission errors, origin GETs and
lost latency samples; both Workers served traffic and issued physical NVMe reads.
Before/after checks compared 288 Talon reads against 144 conditional S3 reads
by SHA256, covering 4 KiB, 64 KiB and 1 MiB. Formal load checks status, length and
ETag, not every payload byte. CPU quota throttling was zero.
Independent test services/samplers were stopped; original services remain ready.
Cold reads, serial latency, FUSE mount acceptance and business E2E were not run.

The complete report, raw records, hashes, resource samples and cleanup receipts
are retained at `/data/yuruiz/artifacts/talon-client-hotpath-benchmark-20260917/`.
Intermediate candidates and invalid sampling attempts are retained separately and
excluded from the final statistics. Production sources match the deployed source
archive; only this report and an additional tested timeout regression changed
after freezing the build. Publication also adds historical-report cross-links;
these documentation changes do not affect the measured library.
