# Talon — working agreement (READ FIRST)

## Fork-based PR workflow (MANDATORY — this overrides any earlier habit)

Remotes:
- `origin` = **your own personal fork** — push branches here. Each contributor
  uses their *own* fork, not somebody else's: check `git remote -v` (or
  `gh api repos/<owner>/talon --jq .permissions.push`) and use whatever fork
  your credentials can actually push to. On this devbox that is
  **Kazimierzsier/talon**.
- `upstream` = **milvus-io/talon** — push is DISABLED. This is where PRs and issues live.

**The loop — ONE PR at a time, repeat for every sub-task:**
1. Branch from `upstream/main` (NOT fork main): `git fetch upstream main && git checkout -B <branch> upstream/main`.
2. Implement + commit.
3. Push the branch to the **fork**: `git push -u origin <branch>`.
4. Open the PR to **upstream**: `gh pr create --repo milvus-io/talon --base main --head <your-fork-owner>:<branch> ...`.
   - upstream CI runs on the PR. Do NOT open PRs inside the fork (`--repo <your-fork-owner>/talon`) — fork PR CI does not auto-trigger, and it is the wrong target.
5. **Wait for upstream CI to go green** (a Docker Hub timeout is infra flake → `gh run rerun <run-id> --failed`, not a code issue).
6. **Merge on upstream**: `gh pr merge <n> --repo milvus-io/talon --squash --delete-branch`. Close the sub-issue.
7. **Go back to step 1** for the next sub-task — always re-branch fresh from the now-updated `upstream/main`. Never stack the next branch on the previous one.

Issues (parent epics + sub-issues) are created and closed on **upstream** (`--repo milvus-io/talon`), because the fork has Issues disabled.

**Never:**
- Never push to a fork that is not yours. If `git push` returns 403, do not
  "fix" it by switching to somebody else's fork — get credentials for your own.
- Never open a PR with base = a fork. Always base = `milvus-io/talon`.
- Never push to the fork's `main` to "trigger CI". Branch from `upstream/main` and PR to upstream.
- Never rebase a feature branch onto the fork's squashed `main` — its history diverges from upstream and causes massive conflicts. Always base feature branches on `upstream/main`.

**Base every feature branch on `upstream/main`, target every PR at `upstream`, push branches to `origin` (fork).**

## Commit / PR conventions
- Conventional-commits lint requires `doc:` (singular, not `docs:`), plus `feat:`/`fix:`/`test:`/`build:`/`ci:`/`chore:`.
- PR title ≤ 72 chars.
- Never commit secrets; secrets come from env only.

## Auth gotcha: `workflow` scope

Branching from `upstream/main` while your fork's `main` is behind means the
pushed branch carries upstream's `.github/workflows/**` as *new* files, even
when your own commits never touch them. GitHub then rejects the push:

    refusing to allow an OAuth App to create or update workflow
    `.github/workflows/ci.yml` without `workflow` scope

Fix: `gh auth refresh -h github.com -s workflow` (then re-push). Syncing the
fork first (`gh repo sync <your-fork> --source milvus-io/talon --branch main`)
needs the same scope, so grant it once and both work.

## Client API rules

These rules apply to `clients/*`, `crates/talon-cache-client`, and the
`crates/talon-client` CLI. Keep `CLAUDE.md` and `AGENTS.md` synchronized when
changing project instructions.

- Distinguish client configuration from request parameters. Coordinator addresses,
  connection pools, concurrency limits, and deployment discovery belong to client
  configuration. Namespace and trace-parent selection belong to individual requests.
- Extend existing request options and operation signatures when adding optional
  request parameters. Do not add parallel operation APIs such as `*_with_options`,
  `*_with_namespace`, or `*_v2` merely to carry another parameter. Rust `read`,
  `read_into`, `stat`, `list`, `load`, and `batch_load` take request options directly.
- Namespace selection is per request, never a `ClientBuilder` setting or mutable
  state on the shared SDK client. An omitted namespace means `default`, independently
  of prior calls. One client must support concurrent requests to different namespaces
  while reusing discovery, placement state, connection pools, and resource budgets.
- Rust: configure `RequestOptions::default()` with fluent `with_*` methods; keep
  fields private and preserve other settings when deriving options. Do not introduce
  a separate `RequestOptionsBuilder` or `.build()` step. Keep the `RequestOptions`
  implementation and its tests in `clients/rust/src/client.rs`; `lib.rs` declares
  modules and re-exports APIs.
  Preserve reusable base options and the lifetimes of borrowed request data.
- Follow each language's existing style: Java uses immutable request options and
  fluent camelCase methods; C uses its versioned request-options struct and passes
  options to existing operations; Python uses optional keyword arguments. Do not
  impose a Rust-style builder on C or Python. Keep semantics consistent across SDKs.
- Carry request options through internal metadata queries, block reads, prewarming,
  and retries. Namespace routing must work with tracing disabled. Never switch a
  shared client's state to select a request's namespace or recreate SDK clients or
  pool resources per request. C must copy borrowed option data before submission returns
  and access versioned fields only when `struct_size` covers them.
- Validate added request options before dispatch, including zero-length reads and empty
  batches. Invalid namespaces must fail rather than silently use `default`. Reuse
  existing server APIs and protocol carriers for namespace selection.
- Do not retain duplicate APIs or compatibility wrappers solely to avoid an API/ABI
  change unless the task explicitly requires compatibility. Update all affected SDK
  bindings, C headers, examples, and test callers when changing signatures.
- Verify defaults, same-client mixed-namespace concurrency, reusable options,
  trace/namespace independence, retries, and asynchronous ownership when affected.
  Tests should check observable requests or results, not just configuration fields.
- Do not update README files, `docs/`, or committed design documents unless the task
  explicitly requests documentation changes. Source API comments remain appropriate.
