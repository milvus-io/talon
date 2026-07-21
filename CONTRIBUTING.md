# Contributing to Talon

Thanks for your interest in Talon — a distributed object-store cache written in
Rust. This guide covers how to get set up, the standards we hold code to, and
how to get a change merged.

Before writing code, please read [`DESIGN.md`](DESIGN.md): it records the v1
architecture and the decisions behind it. Changes should fit that design, or
propose an amendment to it.

## Table of contents

- [Code of conduct](#code-of-conduct)
- [Ways to contribute](#ways-to-contribute)
- [Project layout](#project-layout)
- [Development setup](#development-setup)
- [Local checks (mirror CI)](#local-checks-mirror-ci)
- [Performance feedback loop](#performance-feedback-loop)
- [Coding standards](#coding-standards)
- [Commit and PR conventions](#commit-and-pr-conventions)
- [Review process](#review-process)
- [Reporting bugs and requesting features](#reporting-bugs-and-requesting-features)
- [Security issues](#security-issues)
- [License](#license)

## Code of conduct

Be respectful, assume good intent, and keep discussion technical. Harassment or
abuse is not tolerated. Maintainers may remove comments, commits, and
contributors that violate this.

## Ways to contribute

- **Fix a bug** — see [issues labeled `bug`](https://github.com/milvus-io/talon/issues).
- **Implement a design item** — the "Follow-up skeleton changes" in `DESIGN.md`
  track planned work (e.g. the NVMe worker store, the transport data plane,
  top-K placement, layered config).
- **Improve docs, tests, or benchmarks.**
- **Discuss design** — open an issue before large or cross-cutting changes so we
  can align on approach first.

Good first contributions are small, self-contained, and touch one crate.

## Project layout

Talon is a Cargo workspace:

| Crate | Role |
|---|---|
| `talon-core` | Shared types, keys, block forms, and the `ObjectStore` / `BackendStore` traits. Everything depends on it. |
| `talon-coordinator` | Cluster membership and object placement (rendezvous hashing). |
| `talon-worker` | Cache storage: block index and the local object store. |
| `talon-fuse` | Read-only FUSE client exposing the cache as a filesystem. |

Supporting files: `DESIGN.md` (architecture), `BENCHMARKS.md` (perf harness),
`scripts/bench.py` (harness), `Justfile` (task runner).

Because `talon-core` gates the other crates, changes to its public types usually
touch downstream crates in the same PR — keep the workspace compiling.

## Development setup

Requirements:

- **Rust** — the toolchain is pinned in `rust-toolchain.toml` (currently 1.96.1
  with `rustfmt` and `clippy`). `rustup` picks it up automatically.
- **[`just`](https://github.com/casey/just)** — task runner: `cargo install just`.
- **Python 3** — for the benchmark harness (`scripts/bench.py`). Standard
  library only, no packages to install.

Clone and build:

```sh
git clone https://github.com/milvus-io/talon.git
cd talon
cargo build --workspace
```

Run `just` with no arguments to list available recipes.

## Local checks (mirror CI)

CI runs four gates on every PR: `fmt`, `clippy`, `test`, and `doc`. Reproduce
all of them before pushing:

```sh
just ci        # fmt-check + clippy + test
```

Individually:

```sh
just fmt-check                              # cargo fmt --all --check
just clippy                                 # clippy, warnings denied
just test                                   # cargo test --workspace --locked
cargo doc --workspace --no-deps             # doc build (RUSTDOCFLAGS=-D warnings in CI)
```

CI denies all warnings (`RUSTFLAGS=-D warnings`), so a clean local `just ci` is
required for a green PR. Run `just fmt` to auto-format.

## Performance feedback loop

Talon has a microbenchmark harness for fast, machine-readable perf signal. If
your change touches a hot path (keys, placement, block/page indexing, the data
plane), check it:

```sh
just bench-check          # run benches, diff vs committed baseline, verdict table
just bench -p talon-core  # scope to one crate while iterating
```

`bench-check` exits non-zero on a regression beyond the threshold (default
±10%). If a regression is **intended** (e.g. a correctness fix that costs
cycles), refresh and commit the baseline in the same PR:

```sh
just bench && just bench-save main   # commit bench/baselines/main.json
```

See [`BENCHMARKS.md`](BENCHMARKS.md) for details. CI runs the check
informationally (it never blocks a merge — shared runners are too noisy for
absolute-time gating).

## Coding standards

- **Formatting:** `rustfmt` per `rustfmt.toml` (stable options, `max_width = 100`).
- **Lints:** `clippy` clean with warnings denied. Don't `#[allow(...)]` without a
  comment justifying it.
- **Errors:** use the crate `Error`/`Result` in `talon-core::error`; add a
  variant rather than stringly-typing when a case is meaningful. No `unwrap()`
  or `expect()` in library code paths that can fail at runtime (tests and clear
  invariants are fine).
- **Async:** traits use `async_trait`; keep blocking work off async runtimes
  (see the runtime model in `DESIGN.md`).
- **Public API:** document every public item with a doc comment (the `doc` CI
  gate denies broken intra-doc links).
- **Dependencies:** prefer the workspace-pinned deps in the root `Cargo.toml`;
  discuss before adding a new third-party crate — keep the dependency surface
  small (e.g. `talon-core` stays free of transport/syscall deps).
- **Tests:** add unit tests next to the code (`#[cfg(test)] mod tests`). Cover
  new behavior and edge cases; keep them deterministic.

## Commit and PR conventions

We follow [Conventional Commits](https://www.conventionalcommits.org/). CI
validates the format (see below), so please match it.

**Format**

```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

- **type** (required) — one of:
  - `feat` — a new feature or capability
  - `fix` — a bug fix
  - `refactor` — code change that neither fixes a bug nor adds a feature
  - `perf` — a performance improvement
  - `docs` — documentation only
  - `test` — adding or fixing tests
  - `bench` — benchmarks or the perf harness
  - `build` — build system, dependencies, or `Cargo.toml`
  - `ci` — CI configuration and workflows
  - `chore` — miscellaneous maintenance with no src/test impact
- **scope** (optional but encouraged) — the affected area, typically a crate
  without the `talon-` prefix: `core`, `coordinator`, `worker`, `fuse`. Other
  useful scopes: `bench`, `ci`, `deps`. Omit for repo-wide changes.
- **description** (required) — imperative mood, lower-case start, no trailing
  period, ≤ 72 chars for the whole subject line.
- **body** (optional) — explain the *why*, not just the *what*. Wrap at ~72 cols.
- **breaking changes** — append `!` after the type/scope and/or add a
  `BREAKING CHANGE:` footer, e.g. `feat(core)!: replace CacheKey with BlockId`.

**Examples**

```
feat(worker): add page-level eviction to the block index
fix(coordinator): stabilize rendezvous hash for renamed nodes
perf(core): avoid allocation in ObjectId::from_path
docs: document the whole-vs-paged block layout
refactor(core)!: replace CacheKey with structured BlockId
ci: enforce conventional commit format on PR titles
```

**Branches**

- Branch off `main`; use a short descriptive name (e.g. `worker-store`,
  `fix-placement-hash`).

**Pull requests**

- **The PR title must follow the same Conventional Commits format** — because we
  **squash merge**, the PR title becomes the commit subject on `main`. CI checks
  the PR title.
- Fill out the PR template (summary, design alignment, test plan).
- Keep PRs small and single-purpose — easier to review, faster to merge.
- Ensure `just ci` passes locally and the workspace compiles.
- Link the issue the PR addresses (`Closes #123`).
- Reference `DESIGN.md` sections your change implements or amends; if it
  diverges from the design, say so and why.
- Rebase on the latest `main` before requesting review; resolve conflicts
  locally.

Because merges are squashed, individual commit messages on a PR branch are not
required to pass the check — only the PR title is enforced — but keeping commits
conventional helps reviewers.

## Review process

- At least one maintainer approval is required to merge.
- Address review comments with follow-up commits (don't force-push mid-review
  unless asked — it makes re-review harder). The branch is squashed on merge, so
  intermediate commits don't pollute history.
- CI must be green (`fmt`, `clippy`, `test`, `doc`). The `bench` job is
  informational and won't block.
- Be responsive; stale PRs may be closed after inactivity and can be reopened.

## Reporting bugs and requesting features

Use the issue templates:

- **Bug report** — steps to reproduce, expected vs. actual, environment.
- **Feature request** — the problem, proposed solution, and how it fits
  `DESIGN.md`.

Search existing issues first to avoid duplicates. For open-ended design
discussion, open a feature/design issue before writing code.

## Security issues

**Do not open a public issue for security vulnerabilities.** Instead, use
GitHub's [private vulnerability reporting](https://github.com/milvus-io/talon/security/advisories/new)
so we can address it before disclosure.

## License

By contributing, you agree that your contributions are licensed under the
project's [Apache License 2.0](LICENSE).
