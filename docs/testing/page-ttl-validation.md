# Page TTL validation

## Page-handle hot-read optimization (2026-09-21, local validation only)

This follow-up replaces block-locked read guards with stable per-page handles in
existing index access entries. Hits avoid the lifecycle block lock and second
page-table lookup, and no longer update the block revision. An atomic state word
arbitrates readers, deletion, successful-access invalidation and unlink retries.
TTL-disabled hits skip the clock, timestamp and dirty marker; TTL-enabled hits
coalesce timestamp/dirty writes within the same millisecond. They still read the
clock, and guards still retain an Arc and use atomic pin/unpin operations.

Checkpoint sampling consumes each page's dirty marker before reading its age.
Concurrent access stays dirty for the next snapshot; failed construction or
publication restores consumed markers. The directory-shard format and file-sync,
rename, directory-sync protocol from the preceding repair remain unchanged.

- Local `wt-build` development container, Worker library: **312 passed, 1 ignored,
  33 filtered out**. The ignored metadata scale probe was not run; native io_uring,
  accept and splice groups remain excluded due to the previously established
  container restriction. No remote native tests were run for this version.
- Eight new regressions cover read/delete arbitration, same-millisecond access
  and candidate reselection, cancelled retry selection, checkpoint/access races,
  failed snapshot marker restoration, block-lock-free indexed reads and partial
  range pin rollback, TTL-disabled read behavior, and access-only persistence
  retries without structural changes. Existing deletion/recreation and crash
  recovery tests were adapted and passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`,
  `cargo fmt --all --check`, and `git diff --check`: passed.
- Commands, source digest and local log paths are recorded under
  `/data/yuruiz/artifacts/talon-ttl-page-handle-20260921/validation.json`.

**Performance tests are deferred at the user's request while another agent is
using the test nodes.** This iteration did not deploy to those nodes, restart
services, touch their caches or run performance probes. The measurements in the
following section describe the preceding implementation and do not validate this
new page-handle implementation. Its throughput, P99 and metadata memory impact
remain unmeasured.

## Directory-shard checkpoint repair (2026-09-21)

The repair reuses stable lifecycle handles from `BlockIndex` on read hits and
replaces per-block checkpoint publication with directory-shard snapshots. See
[page TTL](../explanation/page-ttl.md) for migration, pacing, limits and recovery.
The baseline for the real-disk comparison is `6d18bf2`; the pre-repair PR head is
`d0eb9ee`. Historical metadata probes below describe the earlier per-block format.

- Worker library: 337 passed, 1 existing ignored on the ARM64 test Worker,
  Linux `6.12.100-125.179.amzn2023.aarch64`, including native io_uring/socket tests.
- Local development container: 304 passed, 1 ignored, 33 native io_uring/splice
  tests excluded after confirming the container denies io_uring with EPERM.
- Added regressions cover preservation of clean sibling records, legacy migration
  and corrupt-shard precedence, stale-record pruning after deletion/restart,
  active temporary-file protection even with TTL disabled, cancelled checkpoint
  ownership, stable index handles across deletion/recreation, and shard codec
  validation/failure boundaries. Existing SIGKILL boundary tests use the new format.
- Transport library: all 76 tests passed on the same ARM Worker, including
  the 20 io_uring cases denied by the local container.
- Workspace Clippy (all targets/features, warnings denied), documentation build
  (warnings denied), and formatting passed. Python's four tests passed after
  supplying the image's existing Python library under a data-backed linker path.
- Independent content verification: 144 Talon reads matched 72 direct S3
  `If-Match` range reads by SHA-256, covering 4 KiB, 64 KiB and 1 MiB;
  both Workers served requests with zero backend fetches and request errors.

**Performance acceptance is not met.** Two forward/reverse rounds use the same
1 TiB real-S3 paged cache, original C SDK/load generator, two ARM Workers and a
separate Coordinator on the approved node. TTL-on is 24 hours; checkpoints are
60 seconds. L1 is disabled. Twelve final windows passed data/measurement checks,
including physical NVMe reads on both Workers, no backend GETs/errors, no dropped
latency samples and no ENA allowance increases.

| Concurrency | TTL setting | QPS vs main, rounds 1 / 2 | P99 vs main, rounds 1 / 2 |
| ---: | --- | --- | --- |
| 512 | off | -0.79% / -0.90% | +13.71% / +15.54% |
| 512 | on | -0.33% / -2.17% | +14.57% / +12.36% |
| 1 | off | -16.89% / -0.79% | +22.65% / +3.35% |
| 1 | on | -5.10% / -2.58% | +4.84% / +2.19% |

High-concurrency windows ran for 180 seconds; low-concurrency windows for 90
seconds, after 10 seconds of warmup. Repeating the initial low-concurrency stream
warmed it into the kernel page cache: its TTL-off window had zero physical reads
on one Worker and was rejected. Fresh low-concurrency rounds issue scoped
`POSIX_FADV_DONTNEED` to the fixed benchmark's page files before **every** variant,
without deleting data or dropping global caches. All six rerun windows have real
physical reads. Invalid and unpaired earlier windows remain in the raw artifacts.

Final TTL-on physical writes across both Workers are 3.16/3.32 MiB/s at c512 and
2.72/2.76 MiB/s at c1. High-concurrency checkpoint dirty age stayed near 60 seconds
without observed accumulation or errors. These are node-level physical writes,
not just encoded checkpoint bytes. The earlier 301–347 MiB/s measurements belong
to the separately controlled pre-repair diagnosis, not this round.

TTL-on relative to TTL-off on the **same repaired binary** did not show a repeated
P99 increase (+0.76%/-2.76% at c512; -14.52%/-1.12% at c1). That does not establish
no regression against main: both repaired configurations still have higher P99.
Low-concurrency magnitudes vary substantially; the remaining cause is unresolved.
A same-binary diagnostic with smaller, more frequent scans at approximately the
same work rate worsened P99 in both orders and was not adopted.

Final Coordinator-node average CPU was 0.45%–0.58%; earlier partial experiments
included a busy Coordinator-node window and are reported separately. The complete
report and raw logs are in `/data/yuruiz/artifacts/talon-ttl-fix-20260921/`.
[Portable result summary](../../.artifacts/page-ttl/repair-20260921.json) records
version identity, individual windows, physical I/O and the rejected-window rule.
This is targeted repair validation, not the full runbook matrix, sustained expiry
load or production performance acceptance.

## Historical implementation validation

Implementation worktree based on `318eaa4`. Local Linux 5.15.0-139-generic,
x86_64, Rust 1.96.1. Measurements below use the unoptimized test profile and
synthetic access metadata, not production traffic.

## Checks

- `cargo test -p talon-core -p talon-worker --lib --bins`: 364 tests passed
  (74 core, 279 worker library, 2 loadgen, 9 worker binary); the manual scale
  probe is ignored by this command and was run separately for both sizes.
- The unit suite includes real Tokio and io_uring socket reads before and after
  TTL collection. io_uring requires execution outside the restricted sandbox;
  the initial sandbox run returned EPERM and the unrestricted rerun passed.
- `cargo check --workspace --all-targets --all-features --locked`: passed.
- `cargo clippy -p talon-core -p talon-worker --all-targets -- -D warnings`: passed.
- `cargo fmt --all --check`, `git diff --check`: passed.
- Configuration generated with coordinator features `etcd,kubernetes` matches
  `docs/reference/configuration.md` exactly.
- Alert YAML parsed successfully; Prometheus `promtool` is not installed in this
  environment, so rule expressions were not executed by Prometheus here.

## Metadata scale probe

Run each size in a separate process so the RSS baseline does not include a
previous size's retained allocator arenas:

```sh
TALON_TTL_BENCH_PAGES=100000 cargo test -p talon-worker --lib page_ttl_metadata_scale -- --ignored --nocapture
TALON_TTL_BENCH_PAGES=1000000 cargo test -p talon-worker --lib page_ttl_metadata_scale -- --ignored --nocapture
```

[Raw JSONL](../../.artifacts/page-ttl/metadata.jsonl) preserves all 12 records.

| Pages | Process RSS before / after registry construction (KiB) | Cold / hot / mixed / expired scan (ms) | Checkpoint bytes | Serial checkpoint time (ms) |
| ---: | ---: | --- | ---: | ---: |
| 100,000 | 4,596 / 16,680 | 13.736 / 13.420 / 17.758 / 22.233 | 1,216,293 | 75.180 |
| 1,000,000 | 4,596 / 94,560 | 111.243 / 96.028 / 113.246 / 139.000 | 12,163,452 | 620.956 |

`PageEntry` is 40 bytes on this target. RSS includes B-tree nodes, registry,
block identities, allocator overhead and other process memory; it is not an
active-heap measurement and cannot be equated with 40 bytes per page.

The probe groups up to 1,024 page records per block, measures 10,000 guard/access
operations per scenario, scans in batches of 65,536, and writes real checksummed
checkpoint files with file sync, rename and directory sync. A touched first page
in each block explains why the expired scenario has 99,902 / 999,023 candidates
rather than every page. Scan times exclude the production scheduler's one-second
interval between batches. Checkpoint writes in this probe are serial; the worker
at that revision used at most two concurrent checkpoint tasks.

No corresponding page data files are created by this scale probe. Its metadata
operation rate/p99 are not request throughput/p99; it does not compare full data
plane performance with TTL off/on or measure origin traffic and physical page
unlink throughput. The runtime and socket tests cover those paths functionally.
Production performance and GC budgets still need validation on representative
storage, page sizes, concurrency and access distributions.

## Review fixes validation (2026-09-09)

After correcting the five local review findings:

- `cargo test -p talon-core -p talon-worker --lib --bins --locked`: 373 tests
  passed (74 core, 288 worker library, 2 loadgen, 9 worker binary); the existing
  manual metadata scale probe remains ignored.
- New regressions cover GC progress past a failed deletion and empty-directory
  cleanup candidates, capacity replacement after protected/failed victims,
  cancelled page and whole admissions (including paged fallback), and stale or
  newly pinned whole-block candidates.
- The io_uring regression runs alone in a child process for reliable FD counts.
  It exercises multiple idle stop checks, forces successful accept to race
  cancellation, and verifies FD counts return to baseline. Shutdown without a
  new connection also completes. Socket/io_uring tests ran outside the sandbox.
- Worker/core Clippy with warnings denied, workspace all-target/all-feature
  locked compilation, formatting, diff whitespace, and generated configuration
  consistency checks passed.

The metadata measurements above and all raw JSONL records are unchanged. These
fixes were not used to rerun the scale probe or measure production data-plane
performance; the previously stated performance limitations still apply.

## Orphan cleanup validation (2026-09-09)

After adding startup disk discovery and bounded background cleanup:

- `cargo test -p talon-core -p talon-worker --lib --bins --locked`: 382 tests
  passed (74 core, 297 worker library, 2 loadgen, 9 worker binary); the manual
  metadata scale probe remains ignored. Socket/io_uring tests ran outside the
  sandbox.
- Nine new regressions cover scan/delete budgets of one, conservative handling
  of live pages, unknown files and symlinks, failed deletion rediscovery after
  restart without block metadata, and rechecking newly created pages between
  partial metadata cleanup batches.
- Runtime coverage includes TTL disabled, paged reads disabled, corrupt block
  metadata, background retries and cleanup metrics. A held mutation gate prevents
  cleanup from removing an active checkpoint temporary file; foreground block
  mutations sharing a directory-lock stripe remain concurrent.
- A subprocess is killed with SIGKILL at four real mutation boundaries: before
  checkpoint file sync, before rename, after rename but before directory sync,
  and after unlinking the last page. Two recovery passes verify idempotence,
  removal of owned temporary files and metadata-only directories, and retention
  of readable live pages and valid formal access snapshots.
- Worker/core Clippy with warnings denied, workspace all-target/all-feature
  locked compilation, formatting, diff whitespace, and generated configuration
  consistency checks passed. Alert YAML parsed successfully (14 rules);
  `promtool` remains unavailable, so PromQL rule execution was not validated.

The SIGKILL tests cover process crashes, not power-loss durability. Startup
traversal cost and background cleanup performance on production-sized page
directories were not benchmarked. The original scale measurements and raw JSONL
records above are unchanged. Eventual cleanup depends on the worker continuing
to scan and the filesystem permitting deletion; persistent failures remain
observable through cleanup error/pending metrics and alerts.

## PR base synchronization (2026-09-09)

Rebased on `81e0949` (upstream PR #577). The io_uring shutdown path preserves
the shared pre-accept admission budget and can also stop while that budget is
saturated. A new regression checks shutdown of a waiting admission and capacity
reuse without leaking a permit.

- `cargo test -p talon-core -p talon-worker --lib --bins --locked`: 386 passed
  (74 core, 301 worker library, 2 loadgen, 9 worker binary), with one manual scale
  probe ignored. Real socket/io_uring tests ran outside the sandbox.
- Worker/core Clippy with warnings denied, workspace all-target/all-feature
  locked compilation, formatting, diff whitespace and generated configuration
  consistency checks passed again after integration.
- The original benchmark artifact and historical validation records are retained
  unchanged; production performance and PromQL validation limitations still apply.

## CI alert contract repair (2026-09-09)

PR #580's initial `test` job failed three `talon-observability` tests: new alert
metric names were absent from the contract list, and their runbook URLs did not
target headings in `docs/operations/runbook.md`. The earlier core/worker-only
test runs did not execute this crate's tests; YAML parsing and workspace
compilation did not catch these contract violations.

All three failures were reproduced locally. The repair registers the 12 alert
metric names after checking their worker exporters and adds operational runbook
sections for all five page maintenance alerts, preserving the existing tests.

- `cargo test -p talon-observability --all-features --locked`: 17 tests passed.
- The CI test command, `cargo test --workspace --exclude talon-python
  --all-features --locked`, passed locally: 1,306 passed and 22 ignored, with
  localhost proxy bypass and socket/io_uring permissions. This includes unit,
  integration and doc tests;
  opt-in external-service tests and manual probes remain ignored.
- `cargo clippy -p talon-observability --all-targets --all-features --locked --
  -D warnings`, formatting and diff whitespace checks passed.

PromQL execution with `promtool` and production performance remain unvalidated.

## Main merge compatibility (2026-09-17)

Merged upstream `main` at `c391171` into PR #580. Deferred deletion now selects
candidates through the second-chance queue and logical-block version index.
Candidates remain charged until unlink succeeds; later access, replacement, or
recency consumption invalidates an older snapshot. Page and whole admissions
publish their stable access handles, and owned commit tasks retain their tracing
scope. The shutdown wrapper follows the ring-owned splice API without the removed
blocking-helper argument. The TTL test origin implements conditional version
reads required by the updated backend contract.

- `cargo test --workspace --exclude talon-python --all-features --locked`:
  1,373 passed, 22 ignored. This includes 329 worker library tests, real
  socket/io_uring coverage, TTL crash recovery, and telemetry integration tests.
- `cargo test -p talon-python --locked`: 4 passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`:
  passed.
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked`: passed.
- `cargo fmt --all --check`, `git diff --check`, and the four dependency patch
  preparation tests: passed.
- Four new eviction regressions cover deferred byte accounting, pinned/excluded
  candidates, stable-handle access across repeated selection and replacement,
  and version-index snapshots before unlink.

All build, dependency cache, temporary data, and test logs used paths under
`/data/yuruiz`. External-service E2E tests and production performance benchmarks
were not run; ignored tests retain their existing opt-in requirements.

## Page-handle benchmark follow-up (2026-09-22)

Tested the local page-handle/checkpoint repair against main `6d18bf2` using
client `10.15.3.101` (c6in.8xlarge, 32 x86_64 vCPU), two 16-ring ARM Workers,
and an independent Coordinator on `10.15.64.171`. All variants shared the
baseline SDK, 1 TiB physical L2 cache, recovered/reordered source manifest, and
4 KiB random reads with L1 disabled. Each window began with scoped
`POSIX_FADV_DONTNEED`; c512 measured 180 seconds after 10 seconds of warmup,
spanning multiple 60-second checkpoints. TTL was 0 or 24h.

Concurrency 512 completed three paired rounds in different orders; after the
user stopped the third attempt, a new complete round reran all variants using
identical source and binary hashes. The partial attempt remains diagnostic.
The following are medians of per-run metrics, not pooled-request P99.

| Variant | QPS | QPS vs main | P99 ms | P99 vs main | Worker CPU us/request |
| --- | ---: | ---: | ---: | ---: | ---: |
| baseline | 593,636.9 | +0.00% | 2.040 | +0.00% | 32.38 |
| ttl-off | 574,338.8 | -3.25% | 2.313 | +13.38% | 33.52 |
| ttl-on | 575,934.7 | -2.98% | 2.287 | +12.11% | 33.81 |

The results do not establish absence of a regression. See paired changes and
per-run ranges in the artifact; P99 varied between rounds. Concurrency 1
completed two opposite-order rounds without observed throughput or P99
regression. Concurrency 1024 hit connection-admission saturation and roughly
30-second maximum request latencies in all variants; those three windows are
retained only as diagnostics. Its second round stopped before measurement on
a control-plane TLS timeout, and further saturated runs were not pursued.

All 20 recovered windows had zero logical/submission errors, S3 GETs, dropped
latency samples, and ENA allowance increments, with both Workers physically
reading NVMe. The 15 complete c1/c512 comparison windows had no admission
saturation; the other five comprise three saturated windows and two windows
from the interrupted incomplete round. Checkpoint, GC and cleanup errors were
zero; only TTL-on configurations wrote checkpoint bytes. Four payload passes
across the original and resumed runs each matched 144 Talon reads against 72
fresh direct S3 conditional ranges. The resumed payload script initially
referenced the deleted Coordinator address; that test-script error was fixed
before formal measurements and both resumed payload passes then succeeded.

Owned services, temporary Coordinators, client tmpfs mounts and temporary
credentials were cleaned; original services remained ready and data remained.
This is a targeted steady-state follow-up, not the full runbook matrix,
expiration-under-load validation, business E2E, fault recovery or proof for all
production workloads. Changed client hardware/manifest ordering prevents
direct comparison of absolute latencies against older runs. Exact hashes,
paired results, resource counters and audit evidence are recorded in
[`handles-20260922.json`](../../.artifacts/page-ttl/handles-20260922.json).
