<!--
Thanks for contributing to Talon! Keep PRs small and single-purpose.
Please fill out each section. Delete any that genuinely don't apply.
-->

## Summary

<!-- What does this change do, and why? 1-3 sentences. -->

## Related issues

<!-- e.g. "Closes #123" / "Part of #45". -->

## Type of change

- [ ] Bug fix
- [ ] New feature / functionality
- [ ] Refactor (no behavior change)
- [ ] Performance
- [ ] Docs / tests / tooling

## Design alignment

<!--
Which DESIGN.md area does this touch? Reference the section(s).
If this diverges from or amends the design, explain why.
-->

## Changes

<!-- Bullet the notable changes, grouped by crate if it helps. -->

-

## Test plan

<!-- How did you verify this? Commands and results. -->

- [ ] `just ci` passes locally (fmt + clippy + test)
- [ ] Workspace compiles (`cargo build --workspace`)
- [ ] Added/updated tests for new behavior

## Performance impact

<!--
Did this touch a hot path (keys, placement, block/page index, data plane)?
If so, paste the `just bench-check` verdict table. If a regression is
intentional, note it and confirm the baseline was refreshed and committed.
-->

- [ ] Not performance-sensitive, OR
- [ ] `just bench-check` run; results below (baseline refreshed if the change is intentional)

## Checklist

- [ ] PR title is an imperative summary (it becomes the squash-merge commit)
- [ ] Public items are documented; no broken intra-doc links
- [ ] No new dependencies without discussion
- [ ] Rebased on the latest `main`
