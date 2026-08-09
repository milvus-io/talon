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
  `talon-loadgen --addr host:7001 --backend s3 --conns 1024 --server-pid <pid>`.
  Set `--backend` to the backend configured on the target worker (the default is
  `az`). With `--server-pid` it samples the worker's CPU and RSS from `/proc`
  across the measured window; `--json` emits JSON Lines for scripted comparison.
- A run that reports **no samples** is a failed run, not a fast one — the tool
  refuses to turn error frames into latency numbers and prints the first error
  verbatim.

## Where the serve path actually saturates

The sweep above compares data planes against each other. It does not answer a
prior question: *what is the ceiling, and what is holding it?* These runs do.
They matter because an optimization that helps on loopback can be worth nothing
on a real cluster, where a different resource runs out first.

All numbers below: `talon-srv8` on a managed Kubernetes cluster
(`Standard_E64ads_v5` class node, worker capped at 8 CPU by its cgroup),
64 KiB ranges, all cache hits, 32 connections, 20 s measured after a 6 s
warmup, `talon-loadgen --depth 16`.

### Loopback: the kernel is the bottleneck, not Talon

Client and worker on the same node, over `127.0.0.1`:

| metric | value |
|---|---|
| throughput | 167,278 rps = **11.0 GB/s** (87.7 Gbps) |
| worker CPU | 5.78 of 8 cores |
| **kernel time** | **85% of worker CPU** (12,873 of 15,079 ticks) |
| user time | 15% (2,206 ticks) |

Tuning the ring count and connection count raises the peak further, on the same
8-core host, at depth 16 with 64 connections:

| rings | throughput | GB/s | worker CPU | kernel share |
|---|---|---|---|---|
| 1 | 51,718 rps | 3.39 | 1.00 | 88% |
| 2 | 71,907 rps | 4.71 | 1.99 | 88% |
| 4 | 138,144 rps | 9.05 | 3.98 | 88% |
| **8** | **189,379 rps** | **12.41** | 5.71 | 89% |

189,379 rps reproduced within 1% across runs (187,553 on a repeat). Note that the
8-ring peak uses 5.71 of 8 cores: the system stops being CPU-bound before it runs
out of cores, because a single pipelined ring already spends 88% of its time in
the kernel and additional rings contend for one shared network stack.

![Throughput and CPU used across 1, 2, 4 and 8 io_uring rings](docs/assets/bench/ring-scaling.svg)

CPU was sampled from `utime`/`stime` in `/proc/<pid>/stat` across the measured
window. The split is the finding: **six sevenths of the worker's CPU is spent
inside the kernel** — `sendfile` copying bytes and the TCP stack — and only one
seventh in Talon's own code (frame decode, cache lookup, response assembly).

![Worker CPU split: 85% kernel time, 15% Talon's own code](docs/assets/bench/cpu-split.svg)

The practical consequence is that user-space optimizations on this path have a
small denominator. Halving *all* of Talon's user-space work on the serve path
would move total CPU by about 7%. Work that removes copies or syscalls
(zero-copy send, batched submission) acts on the 85%.

### Across the network: the NIC is the bottleneck, and it binds first

Same worker, driven from `talon-cli7-0` on a **different node**, over the pod
network:

| path | rps | GB/s | Gbps | p50 | p99 |
|---|---|---|---|---|---|
| loopback | 167,278 | 11.0 | 87.7 | 4,617 µs | 35,244 µs |
| **cross-node** | **44,662** | **2.93** | **23.4** | 20,269 µs | 49,905 µs |

Cross-node throughput is **27% of the loopback rate at the same 32 connections**
(24% of the 189,379 rps peak reached at 64 connections), and 23.4 Gbps against a
25 GbE link is the wire, saturated. On this cluster the NIC runs out well before
the worker does — the worker is not the constraint at all.

![Loopback vs cross-node throughput against the 25 GbE line rate](docs/assets/bench/throughput-ceilings.svg)

**Read the loopback number as a component ceiling, not a capacity claim.** It
measures how fast the serve path can go when the network is free. Any change
that moves the loopback number but not the cross-node number has not made the
deployed system faster.

### Pipelining: the one change that moved the network number

Depth is requests kept in flight per connection (`--depth`). Cross-node:

| depth | rps | GB/s | p50 |
|---|---|---|---|
| 1 | 22,315 | 1.46 | 762 µs |
| **16** | **44,592** | **2.92** | 20,250 µs |

Pipelining **doubles cross-node throughput**, because at depth 1 each connection
pays a full network round trip per request and the link sits idle waiting. It
buys throughput with latency: p50 rises 27×, as depth-16 queueing means a
request waits behind 15 others. That is the expected trade, not a defect —
depth 1 is latency-optimal, depth 16 is bandwidth-optimal.

### Multiplexing: measured, and not merged

Serving a connection's requests concurrently with out-of-order replies
(multiplexing, as opposed to in-order pipelining) was implemented and measured
against the same baseline. It did not pay:

| | baseline | multiplexed | Δ |
|---|---|---|---|
| loopback rps | 167,278 | 160,725 | **−3.9%** |
| loopback user ticks | 2,206 | 2,484 | **+12.6%** |
| loopback kernel ticks | 12,873 | 12,895 | +0.2% |
| loopback max latency | 51.7 ms | **5,111 ms** | 99× worse |
| cross-node rps | 44,662 | 44,612 | −0.1% |

The kernel time is unchanged — multiplexing does not remove a single copy or
syscall — while user time rises by an eighth for task spawning, semaphore
admission, and write-lock acquisition. Multiplexing exists to remove head-of-line
blocking, which requires *variance* in per-request service time; a uniform
all-cache-hit workload has none to remove, so only the overhead lands.

The max-latency blowup is structural and worth recording. Two responses' bytes
must never interleave on the wire, and `sendfile` cannot be paused mid-transfer,
so the write half must be held exclusively for the duration of each response.
Multiplexing therefore reintroduces at the writer exactly the serialization it
removes at the reader — and adds lock queueing on top.

Cross-node the two are indistinguishable, because there the NIC decides.

**Conclusion: not merged.** Multiplexing may still pay on a workload with genuine
service-time variance (mixed cache hits and origin misses on one connection),
which is the condition to test before revisiting it. It does not pay here.

### Method notes

- **Paired and order-swapped.** Baseline and candidate were run alternately, and
  the order reversed between rounds, so drift in the shared cluster cannot
  masquerade as an effect.
- **Baselines built from the exact parent commit**, not from a nearby branch.
  Comparing against a tree that already contains another change produces a
  confident and wrong number; this was caught happening.
- **The measurement client can be the bottleneck.** An earlier version of
  `talon-loadgen` matched pipelined replies positionally, which serialized its
  own reader and capped every depth-16 result near 85k rps — half the true
  figure. Numbers recorded before that fix understate the worker and should not
  be compared against numbers here. When a candidate and its baseline both sit
  at a suspiciously equal ceiling, suspect the harness.

## Charts

The figures above are generated, not hand-drawn. `bench/data/dataplane.json`
holds the measurements and is the single source of truth for both the charts and
the tables on this page; `scripts/bench_charts.py` renders it to SVG:

```sh
just bench-charts           # re-render docs/assets/bench/*.svg
just bench-charts --check   # non-zero exit if a chart is stale
```

Dependencies are declared inline (PEP 723), so `uv` builds a throwaway
environment on demand — there is nothing to install and matplotlib is not a
project dependency.

**To record a new measurement, edit the JSON and re-run — never edit an SVG.**
Keeping one source means a re-measurement cannot leave the charts saying one
thing and the prose another. Output is byte-reproducible (fixed `svg.hashsalt`,
no render timestamp), so an unchanged run produces no diff and `--check` is
meaningful. `--check` renders to a scratch directory rather than the committed
paths, so it reports drift instead of silently repairing it.

## For coding agents

- One command surface: `just bench`, `just bench-save`, `just bench-check`,
  `just bench-charts`.
- `bench-check` output is a markdown table and its exit code is the verdict
  (0 = within threshold, 1 = regression). Parse either.
- `bench/results/latest.json` and `bench/baselines/<name>.json` are stable
  `{ "binary::name[/arg]": median_ns }` maps for programmatic diffing.
- `bench/data/dataplane.json` is the structured record of the data-plane
  measurements — read numbers from there rather than parsing prose or SVGs.
- Prefer scoping with `-p <crate>` while iterating; run the full suite before
  saving a baseline.

## CI

The `bench` CI job runs `bench-check --soft` and posts the table to the job
summary. It is **informational only** (`continue-on-error`) — shared CI runners
are too noisy for absolute-time gating, so benchmarks never block a merge. The
committed baseline is the source of truth; refresh it deliberately.
