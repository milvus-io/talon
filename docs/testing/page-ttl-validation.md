# Page TTL local validation

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
uses at most two concurrent checkpoint tasks.

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
