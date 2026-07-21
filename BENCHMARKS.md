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
