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
