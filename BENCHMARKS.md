# Benchmarks

Talon uses [Divan](https://docs.rs/divan) microbenchmarks plus a thin harness
(`scripts/bench.py`) that turns Divan's human-only table into structured JSON and
diffs a run against a committed baseline. The goal is a fast, machine-readable
performance signal so that changes — whether made by a human or a coding agent —
get timely feedback.

## Quick start

```sh
just bench            # run all benches, write bench/results/latest.json
just bench-save main  # promote the latest run to the committed baseline
just bench-check      # run + diff vs baseline; non-zero exit on regression
```

Scope a run to one crate to iterate faster:

```sh
just bench -p talon-core
just bench-check -p talon-coordinator --threshold 15
```

## The workflow

1. **Establish a baseline.** On a known-good commit: `just bench && just bench-save main`.
   `bench/baselines/main.json` is committed and is the reference everything
   compares against.
2. **Iterate.** After a change, `just bench-check`. It prints a markdown table
   (`benchmark | baseline | current | Δ | verdict`) and exits non-zero if any
   benchmark regresses beyond the threshold (default ±10%, above typical
   microbench noise). Use `--soft` to report without failing.
3. **Intentional perf change?** Re-run `just bench-save main` to move the
   baseline, and commit it in the same PR so the diff is reviewable.

## What is measured

Deterministic, CPU-bound hot paths (low variance, good for regression
detection):

- `talon-core` (`core_benches`): key path ↔ id conversion, `PresentBitmap`
  operations, `BlockId::page_count`.
- `talon-coordinator` (`placement_benches`): `RendezvousPlacement::locate`
  across 8/64/256 nodes — the per-request placement lookup.

The zero-copy data plane (`sendfile`/`splice`) is I/O-bound and higher-variance;
those benches are added in a separate tier when the transport layer lands.

- `talon-worker` (`dataplane_benches`): the Tokio and io_uring data planes
  serving the same resident block over real loopback TCP (#285).

  **Read this one with care.** It drives a single client serially, so it
  measures a per-request latency floor, not scaling. At concurrency 1 the two
  planes are statistically indistinguishable — across seven runs the medians
  overlapped and the sign of the difference flipped run to run. That is
  expected: io_uring amortizes syscalls across many in-flight operations, and
  with one request in flight there is nothing to amortize, while the bulk bytes
  bypass the ring via `sendfile` on both paths. The 35% win measured in #273 was
  at 1024 concurrent connections.

  A Divan harness cannot model that concurrency honestly — a synthetic fan-out
  inside `bench()` would mostly measure the harness's own scheduling. So this
  bench is a **regression floor**, not the evidence for how the planes compare
  under load. That question is answered by the load test below.

## Concurrent load test

`scripts/dataplane_loadtest.sh` drives a real worker with many connections in
flight and reports the latency distribution. This is the measurement Divan
cannot make, and the evidence behind the io_uring default.

```sh
scripts/dataplane_loadtest.sh                        # sweep 1/64/256/1024
CONNS=1,512 SECONDS_PER=30 scripts/dataplane_loadtest.sh
```

It starts a coordinator, a local blob origin, and a worker on each data plane in
turn, then sweeps connection counts against each. Results on a 16-core EPYC
7763, 64 KiB ranges, 10 s measured after a 3 s warmup:

| conns | io_uring rps | tokio rps | io_uring p50 | tokio p50 | io_uring p99 | tokio p99 |
|---|---|---|---|---|---|---|
| 1 | 5,636 | 4,837 | 161 µs | 191 µs | 259 µs | 293 µs |
| 64 | 54,755 | 55,191 | 900 µs | 1,057 µs | 5,371 µs | 2,853 µs |
| 256 | 42,417 | 56,999 | 1,243 µs | 3,835 µs | 7,423 µs | 10,283 µs |
| **1024** | **51,668** | 41,060 | **1,175 µs** | 19,905 µs | **4,781 µs** | 72,911 µs |

At 1024 connections io_uring delivers **26% more throughput at 17× lower p50 and
15× lower p99**, using comparable CPU and 19% less memory.

**The middle of the sweep is not monotonic, and that is worth knowing.** At 64
and 256 connections Tokio sometimes wins — work-stealing balances a moderate
load well, while rings can sit idle. The advantage becomes decisive at high
connection counts, which is the regime a cache fleet actually runs in. Do not
read a single row as the whole picture.

Notes on running it:

- **Manual, not CI.** Shared runners are too noisy for absolute-time
  comparison, the same reason the bench job is informational.
- **Percentiles, not means.** The difference lives in the tail; a mean hides it.
- **Warmup is excluded** from recorded samples, so the numbers describe the
  serve path rather than the first-fetch miss path.
- **`talon-loadgen` can drive an existing worker** directly:
  `talon-loadgen --addr host:7001 --conns 1024 --server-pid <pid>`. With
  `--server-pid` it samples the worker's CPU and RSS from `/proc` across the
  measured window; `--json` emits JSON Lines for scripted comparison.
- A run that reports **no samples** is a failed run, not a fast one — the tool
  refuses to turn error frames into latency numbers and prints the first error
  verbatim.

## For coding agents

- One command surface: `just bench`, `just bench-save`, `just bench-check`.
- `bench-check` output is a markdown table and its exit code is the verdict
  (0 = within threshold, 1 = regression). Parse either.
- `bench/results/latest.json` and `bench/baselines/<name>.json` are stable
  `{ "binary::name[/arg]": median_ns }` maps for programmatic diffing.
- Prefer scoping with `-p <crate>` while iterating; run the full suite before
  saving a baseline.

## CI

The `bench` CI job runs `bench-check --soft` and posts the table to the job
summary. It is **informational only** (`continue-on-error`) — shared CI runners
are too noisy for absolute-time gating, so benchmarks never block a merge. The
committed baseline is the source of truth; refresh it deliberately.
