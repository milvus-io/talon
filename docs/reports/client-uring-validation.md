# Native Monoio SDK transport

> Historical development snapshot. Later ownership, scheduling and performance
> results are documented in [the final hot-path report](client-hotpath-validation.md).

## Scope and execution model

Local stack layer `perf/client-uring` is based on #591 (`perf/ring-splice`,
`f3a63e92351dc4291df81fde87560540ee10c1b2`). Rust, C, Python, FUSE and Gateway
share the changed Talon TCP path. Java and HTTP/S3 origin traffic are excluded.
The published Monoio 0.2.4 patch from #591 is reused without modification; no new
registry package is introduced.

The initial per-I/O Tokio stream adapter has been removed. This implementation
has three entry points into the same native RPC engine:

- `ClientBuilder::build_hosted()` / `HostedClient`: C/Python execute complete SDK
  operations on a client-owned Monoio execution group. Python waits without Tokio; C invokes
  callbacks inline or through the existing user executor. Whole-operation
  cancellation and initialization-only Tokio fallback are covered in the
  [hosted entry validation](client-hosted-entry-validation.md).
- `ClientBuilder::build_native()` / `NativeClient`: complete Rust SDK stat,
  membership discovery, placement, version-pinned block reads and borrowed reads
  run in a caller-owned, timer-enabled Monoio runtime, without a Tokio runtime.
  The client is thread-local; create, poll and drop it on that runtime. Lower-level
  worker, write and coordinator clients also accept a native `MonoioClient` pool.
- Existing Rust `build()`, FUSE and Gateway: one owned request crosses to a
  shared ring and one owned result crosses back. Sockets, response parsing,
  timers and staged-file reads remain on the ring. No per-read/write channel or
  Tokio `AsyncRead`/`AsyncWrite` adapter is used.

Auto remains the Linux default and falls back on ring initialization failure.
Strict IoUring propagates initialization errors; Tokio explicitly selects the
portable implementation. Network and protocol failures never switch backends.
The explicit native builder does not fall back and rejects a conflicting Tokio
setting. `TALON_CLIENT_FORCE_TOKIO=1` applies to the existing default entry points;
explicit builder selections override it.

## Ownership, bounds and compatibility

- Each exchange exclusively owns a socket. Pools retain per-peer idle limits
  and lazy TTL expiration. The shared dispatcher has 64 queued and at most
  1,024 active requests; admission waits up to the configured connect deadline.
  Direct native callers control their own concurrency. This is not concurrent
  request multiplexing on a single socket.
- Monoio owns each receive/send allocation until completion. Successful range
  lengths and control/error limits are checked before allocating beyond a bounded
  speculative prefix. Native ranges receive header and up to 64 KiB of body in
  one `readv`; separate owned buffers avoid shifting the body to remove the header.
  A single-block `read()` returns the owned receive buffer; multiple blocks are
  assembled in order. Borrowed `read_into()` copies only completed responses:
  it can retain an extra owned allocation per in-flight block range, potentially
  as large as that requested range. It is not receive-side zero-copy.
- Cancellation drops the native exchange; the existing Monoio operation model
  retains pending buffers and descriptors through completion. Caller memory is
  never passed to kernel I/O. Pool disposal closes idle sockets; the shared ring
  exits after its last pool and active requests are gone. A pool can outlive and
  be reused across replacement caller Tokio runtimes.
- Owned PUT bodies use `Bytes`. Staged-file PUT opens once, validates length,
  and reuses a 64 KiB owned buffer with native explicit-offset reads. A reused
  socket may retry a transport failure once while replay remains safe. Local
  file failures and PUT/DELETE failures after the send phase never replay writes.
- Hostname dials use two lazy system-resolver threads and a queue of 64 lookups;
  a full DNS queue reports `WouldBlock`. Cancelled queued jobs skip resolution.
  A running system lookup cannot be interrupted, but its result is discarded
  after the request deadline. Numeric addresses bypass DNS entirely.
- Existing public Tokio pool checkout/fresh/release methods retain their raw
  socket behavior. Default generic parameters preserve existing client types.
  C/Python ABI, wire operations, typed errors and version contracts are unchanged.

## Verification

All outputs, caches and temporary/test files are under `/data/yuruiz/` on
`/dev/nvme1n1p1`. Rust 1.96.1; `RUSTFLAGS=-Dwarnings` and
`RUSTDOCFLAGS=-Dwarnings`. Bypass loopback proxies with
`NO_PROXY=127.0.0.1,localhost,::1` and its lowercase counterpart.

Before the hosted entry update, validation passed: workspace **1,330 passed / 36 ignored**; Python **4 passed**;
explicit native suites **14 passed**; isolated seccomp fallback **1 passed**;
standalone SDK consumer **5 passed**. Of the 36 ignored cases, fifteen are the native
and seccomp tests executed separately. Workspace Clippy (all targets/features),
rustdoc (no dependencies), formatting and diff whitespace checks passed. The new
workflow YAML parsed successfully and changed documents' relative links resolve.

The dedicated [native SDK workflow](../../.github/workflows/client-uring.yml)
runs both forced native tests and process-local fallback on PRs, including stacked
bases. It fails if a runner cannot create a real ring. This workflow has been
added locally; no remote run has been triggered or claimed.

```sh
cargo fmt --all --check
cargo test --workspace --exclude talon-python --all-features --locked --offline
cargo test -p talon-python --features telemetry --locked --offline
cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings
cargo doc --workspace --no-deps --all-features --locked --offline
cargo test -p talon-cache-client --lib --all-features --locked --offline monoio_client::receive_tests -- --ignored
cargo test -p talon-cache-client --test native_rpc --all-features --locked --offline -- --ignored
cargo test -p talon-rust-client --test native_client --all-features --locked --offline -- --ignored
```

The explicit io_uring suite is ignored by ordinary tests so portable hosts can
run the standard suite. It requires a real ring and fails if initialization is
unavailable. Tests cover native control/versioned/cached reads and writes,
streamed files, stalled-peer progress, timeout, cancellation, untrusted response
lengths, concurrent pool disposal, idle expiry, split frames, stale-socket retry,
caller-runtime replacement, and the full Rust SDK with hostname resolution and
multi-block reads while no Tokio runtime exists. Existing tests cover C buffer
contracts, write retry guards, exact-version failures and telemetry propagation.

For a process-local fallback check, produce the cache-client unit-test executable
with `cargo test -p talon-cache-client --lib --all-features --locked --offline
--no-run --message-format=json`, then pass its `executable` artifact path to:

```sh
python3 scripts/test_client_no_uring.py /data/yuruiz/.../talon_cache_client-...
```

The script denies only `io_uring_setup` using seccomp in the test process. Auto
performs a real TCP exchange through Tokio; strict IoUring preserves EPERM.
It changes no host-wide setting.

## Local performance comparison before receive coalescing

The rows below preserve the original native rewrite measurement. A later audit
found that its QPS clock could start after callers had already begun work, so
**the historical QPS values and QPS percentage comparisons are not reliable**.
Per-RPC latency timestamps do not have that issue. The receive-only optimization
and comparisons using the corrected clock are documented in
[the receive optimization report](client-receive-optimization.md).

Linux x86_64, kernel `5.15.0-139-generic`, 64 visible logical CPUs. Release build,
four-thread Tokio mock server/caller and one Monoio client ring. All three modes
exercise `WorkerClient::fetch_range` with the same payload validation. Direct
Monoio callers share one local pool; facade callers use full-request dispatch.
The host is neither exclusive nor CPU-pinned. No other build or test from this
task ran during measurement.

For each size/concurrency, every caller warms up 20 requests and then measures
1,000 requests. Three repetitions alternate backend order. QPS includes byte
validation and harness overhead; P50/P99 measure RPC time. Each table entry is
the median of that metric across runs, not a pooled latency distribution.

| Bytes | Concurrency | Entry | QPS | P50 us | P99 us | QPS vs Tokio |
|---:|---:|---|---:|---:|---:|---:|
| 4,096 | 1 | Tokio | 44,406 | 16 | 43 | baseline |
| 4,096 | 1 | MonoioFacade | 14,020 | 62 | 131 | -68.4% |
| 4,096 | 1 | MonoioNative | 26,905 | 30 | 57 | -39.4% |
| 4,096 | 64 | Tokio | 113,287 | 552 | 901 | baseline |
| 4,096 | 64 | MonoioFacade | 25,605 | 2409 | 4623 | -77.4% |
| 4,096 | 64 | MonoioNative | 72,075 | 876 | 1064 | -36.4% |
| 65,536 | 1 | Tokio | 10,789 | 33 | 52 | baseline |
| 65,536 | 1 | MonoioFacade | 3,663 | 174 | 604 | -66.0% |
| 65,536 | 1 | MonoioNative | 8,177 | 55 | 231 | -24.2% |
| 65,536 | 64 | Tokio | 36,577 | 1680 | 2722 | baseline |
| 65,536 | 64 | MonoioFacade | 18,071 | 3465 | 6208 | -50.6% |
| 65,536 | 64 | MonoioNative | 14,855 | 4146 | 5220 | -59.4% |

[Raw rows](client-uring-loopback.csv) and
[the executable harness](../../crates/talon-cache-client/examples/client_io_bench.rs):

```sh
cargo run --release -p talon-cache-client --example client_io_bench --locked --offline -- 1000
```

The historical latency samples were higher than Tokio even for the native entry;
the QPS regression percentages must not be used after the clock issue above was
identified. Removing the stream adapter does not by itself establish a speedup.
The cross-runtime entry also retains substantial scheduling overhead, and the
single ring is a scaling boundary. Auto uses Monoio as requested; **performance
acceptance remains open**. These results replace the discarded stream adapter's
measurements and must not be presented as a production improvement.

This framed-TCP loopback mock does not measure real Worker cache/disk behavior,
a remote network, HTTPS origins, or deployed scalability. Actual multi-language
cluster acceptance, Docker packaging and kernel FUSE mounts were not run for
this layer. No #591 Worker throughput gain is attributed to the SDK.
