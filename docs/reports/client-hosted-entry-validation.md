# Whole-operation C/Python SDK entry

> Historical development snapshot. Later ownership, scheduling and performance
> results are documented in [the final hot-path report](client-hotpath-validation.md).

## Change

On `perf/client-uring`, above #591 (`perf/ring-splice`), C and Python now use
`ClientBuilder::build_hosted()`. One command contains a complete read/stat/list;
stat fallback, discovery, placement, concurrent block reads and retries execute
on a selected thread of a client-owned Monoio execution group. Clones share the
group, placement/membership caches and refresh coordination. Socket ownership
remains local to each ring. Control and data pools use separate shared per-peer
idle budgets, so increasing the ring count does not multiply either limit.

Parallelism defaults to available CPUs. Configuration precedence is Rust
`with_io_threads(n)`, `TALON_CLIENT_IO_THREADS`, `TOKIO_WORKER_THREADS`, then
`available_parallelism()`. All explicit values must be positive. External submissions pick
the least-loaded lane and prefer the first lane on ties. Native callbacks
submitting another operation to the same client retain their current ring
and pool; the affinity never crosses independent clients. Each operation stays on its assigned native ring.
This removes the previous Tokio-to-Monoio handoff for each child TCP RPC.

C retains its callback executor contract: without an executor, the callback runs
**inline on the SDK operation thread**; with an executor, the original `submit`
hook schedules the callback. There is no default completion thread pool and no
C Tokio task waiting for the result. C transfers a private destination handle to
the operation; kernel receives retain owned buffers and completed blocks copy
into the caller destination through the existing `read_into` implementation.
It does not allocate an additional assembled whole-range result. The caller must
keep the client, buffer and callback context valid through completion, as before.

Python releases the GIL and uses an executor-independent wait for its owned
result. Conversion to Python `bytes` still copies. The language API is unchanged.

On Linux Auto selects Monoio at construction. If ring initialization fails,
partially started rings are cancelled and joined;
one owned multi-thread Tokio runtime uses the same parallelism and shared forced
Tokio pools. A partially initialized or mixed-backend group is never published.
Strict IoUring returns the initialization error. Network errors do not
switch backends. `TALON_CLIENT_FORCE_TOKIO=1` still forces the portable path.
No Tokio runtime is started for these bindings' normal native operation path;
optional telemetry exporters have their own independent lifecycle.

Rust `build()` and the FUSE/Gateway callers keep their per-RPC bridge. Rust
`build_native()` still runs directly on the caller's Monoio runtime. This update
changes the C/Python entry and adds a reusable hosted Rust entry.

## Lifetime and capacity

- Nonblocking submission enqueues owned arguments. The queue is unbounded, as was
  the previous C Tokio task admission; at most 1,024 complete operations execute
  at once. This is not a byte budget or a cap on child block RPCs.
- Dropping a result future cancels queued/active work. Pending futures retain both
  the queue and the runtime lifetime, so they can finish after public handles drop.
- Callback operations require a live client. Dropping its final owner cancels
  outstanding operations; native operation buffers remain owned through kernel
  completion. Runtime/socket teardown runs on the owning thread.
- Inline callbacks must remain short. Expensive or blocking callbacks should use
  the existing user-supplied executor.

## Validation before the multi-ring update

All build/cache/temp paths resolved under `/data/yuruiz/`, backed by the data NVMe.
Build/test commands used `--locked --offline`; Rust and rustdoc warnings were
errors. Real TCP/io_uring tests ran outside the socket-restricted sandbox.

- Workspace tests excluding Python: **1,333 passed / 41 ignored**.
- Explicit native Rust SDK tests: **4 passed**, covering caller-owned and hosted
  Monoio, stat/discovery, concurrent multiblock reads, DNS, EOF, pool teardown,
  progress with a stalled request, cancellation, and inline callback placement.
- Explicit native C entry: **1 passed**, asserting inline callback thread identity
  and absence of a Tokio runtime there. The regular C suite also covers C header
  ABI, read buffers, exact-version/EOF behavior, and user executor dispatch.
- Python with telemetry, including explicit native entry: **6 passed**. Two
  concurrent Python calls must release the GIL before the server responds.
- Process-local seccomp denial of `io_uring_setup`: hosted SDK **1 passed**,
  C read/inline callback **1 passed**, Python concurrent stat **1 passed**.
  The hosted test also checks strict IoUring preserves EPERM.
- Workspace all-target/all-feature Clippy, no-dependency rustdoc, formatting and
  diff whitespace checks passed.

The lifetime test caught and verified a fix: keeping only the runtime shutdown
owner alive was insufficient when all queue senders had dropped. Pending result
futures now retain both, and complete successfully after client handles drop.

The [native SDK workflow](../../.github/workflows/client-uring.yml) includes the
strict native entry and denied-ring binding tests. These workflow changes have
not been run remotely. Local evidence does not establish deployment acceptance
or a throughput/latency gain; no new performance or cluster benchmark was run.

## Multi-ring validation

The follow-up restores multi-core execution within one Client. Regression tests
cover three simultaneous synchronous inline callbacks on distinct native ring
threads and distinct Tokio worker threads, shared membership refresh across three
rings, aggregate idle budget enforcement and release on pool teardown, isolated
thread-count environment precedence, and joining unpublished startup threads.
The denied-ring test also checks three-thread parallel execution after Auto
fallback. No deployment benchmark was run for this change.

Final multi-ring source checks (tests use `TALON_CLIENT_IO_THREADS=2`; explicit
three-ring cases override it):

- Workspace excluding Python: **1,337 passed / 45 ignored**.
- Native SDK integration target, including portable cases: **10 passed** (the
  seccomp-only case is run separately).
- Explicit native RPC suite: **8 passed**, including the three-ring idle budget.
- C including its strict native entry: **17 passed**; Python including its strict
  native entry: **6 passed**.
- Isolated denied-ring hosted SDK, C and Python cases: **1 passed each**.
- Workspace all-target/all-feature Clippy and no-dependency rustdoc, formatting,
  changed-document local links and workflow YAML validation passed.

The Rust thread-count test launches isolated subprocesses to check CPU defaults,
legacy environment compatibility, override precedence and invalid input without
mutating the environment of concurrent tests. This is functional and concurrency
validation, not an end-to-end throughput improvement claim.

## Hosted scheduling follow-up

Native inline callback continuations retain the same client's ring and warm
connection pool. The callback remains included in outstanding load until it
returns, so external submissions still account for blocked callbacks. A unique
group identity prevents affinity from crossing independent clients.

Hosted futures are pinned before entering the active queue. Disabled tracing
avoids dispatcher installation when the current dispatcher is already inactive.
Captured active subscribers still apply on every poll; explicit suppression
also overrides global subscribers installed after client construction or
submission. The C entry applies the same inactive-dispatcher fast path.

Validation adds callback-chain affinity, cross-client isolation, per-operation
subscriber propagation, and explicit suppression with a global subscriber. The
global-subscriber regression failed before the fix; isolated child processes
avoid modifying other tests' global tracing state. Final native integration
checks pass 16 cases, with the process-local denied-ring case run separately.
C/Python native entry checks, affected SDK regular tests, all-target/all-feature
Clippy and formatting pass. Python tests use the repository's normal test
configuration; enabling the extension-module feature for an executable test
does not link Python and is not a supported test invocation.

Deployment performance evidence is recorded separately from these functional
checks. Screening or CPU-profile runs do not establish throughput acceptance.
