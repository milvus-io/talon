# Talon — working agreement (READ FIRST)

## Fork-based PR workflow (MANDATORY — this overrides any earlier habit)

Remotes:
- `origin` = **beinan/talon** (the contributor fork) — push branches here.
- `upstream` = **milvus-io/talon** — push is DISABLED. This is where PRs and issues live.

**The loop — ONE PR at a time, repeat for every sub-task:**
1. Branch from `upstream/main` (NOT fork main): `git fetch upstream main && git checkout -B <branch> upstream/main`.
2. Implement + commit.
3. Push the branch to the **fork**: `git push -u origin <branch>`.
4. Open the PR to **upstream**: `gh pr create --repo milvus-io/talon --base main --head beinan:<branch> ...`.
   - upstream CI runs on the PR. Do NOT open PRs inside the fork (`--repo beinan/talon`) — fork PR CI does not auto-trigger, and it is the wrong target.
5. **Wait for upstream CI to go green** (a Docker Hub timeout is infra flake → `gh run rerun <run-id> --failed`, not a code issue).
6. **Merge on upstream**: `gh pr merge <n> --repo milvus-io/talon --squash --delete-branch`. Close the sub-issue.
7. **Go back to step 1** for the next sub-task — always re-branch fresh from the now-updated `upstream/main`. Never stack the next branch on the previous one.

Issues (parent epics + sub-issues) are created and closed on **upstream** (`--repo milvus-io/talon`), because the fork has Issues disabled.

**Never:**
- Never open a PR with base = the fork (`beinan/talon`). Always base = `milvus-io/talon`.
- Never push to the fork's `main` to "trigger CI". Branch from `upstream/main` and PR to upstream.
- Never rebase a feature branch onto the fork's squashed `main` — its history diverges from upstream and causes massive conflicts. Always base feature branches on `upstream/main`.

**Base every feature branch on `upstream/main`, target every PR at `upstream`, push branches to `origin` (fork).**

## Commit / PR conventions
- Conventional-commits lint requires `doc:` (singular, not `docs:`), plus `feat:`/`fix:`/`test:`/`build:`/`ci:`/`chore:`.
- PR title ≤ 72 chars.
- Never commit secrets; secrets come from env only.
