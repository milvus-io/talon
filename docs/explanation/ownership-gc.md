# Cache ownership cleanup in stale GC

Workers can periodically reclaim clean local cache entries that no longer belong
to their placement candidates. This extends `DESIGN.md` section 3 (Worker storage)
and runs as a policy within the shared `stale_gc` service. This feature does not move data between Workers,
delete origin objects, or touch staging/WAL data.

## Configuration

```toml
ownership_gc_interval_secs = 3600
ownership_gc_replicas = 1
ownership_gc_scan_batch_size = 1024
ownership_gc_delete_batch_size = 1024
```

The interval defaults to `0` (disabled). Other defaults are shown above. Every
field supports its uppercase `TALON_WORKER_` environment variable; environment
values override TOML. Changes require a Worker restart. Batch limits and replica
count must be positive. The example schedules one round per hour. The first
round starts immediately; subsequent rounds wait the configured interval after
the previous round finishes. `stale_gc_enabled` controls backend-deletion checks;
`ownership_gc_interval_secs` independently controls ownership cleanup. Disabling
either policy leaves the other enabled policy running in the same scheduler task.

Set `ownership_gc_replicas` to **at least the largest candidate count used by any
reader or load planner**. It is a retention policy, not a replication command.
The Worker retains candidates from both the global placement table and its own
zone's table. This supports mixed global and zone-affine readers conservatively;
it can deliberately retain extra cache. There is no automatic negotiation of
reader replica counts. Arbitrary explicit prewarming on non-candidate Workers
is subject to cleanup too.

## Membership and availability

Each round fetches `MembershipQuery` from the Coordinator. Placement uses the
retained logical membership, including offline and conflicted Workers, and excludes
retired members. Process restarts and address changes do not change placement or
trigger a full cache scan. There is no separate Worker stability timer. Once a
scheduled round sees an authoritative membership change, it checks ownership
using that view.

Every continuation batch fetches membership again. If it differs during an
active round, the Worker ends that round and waits for the next scheduled run.
Logical Worker IDs and zones participate in equality; response order, addresses,
and instance availability do not.
This stops partially executed rounds from continuing across observed disturbances.
Polling cannot detect transitions entirely between observations, and cleanup does
not add protection for Workers explicitly retired from logical membership.

Query failures, unsupported replies, empty membership, duplicate IDs, or absence
of this Worker end the current round. Last-good membership is never used for
deletion after a failed query. The Coordinator must support
retained membership; on secured deployments it must also allow that read on its
authenticated Worker listener. Upgrade Coordinators before enabling this feature.
No wire schema change is required.

Each fresh observation authorizes at most one bounded batch with a one-second
execution budget. Waiting for deletion rate, disk permits, page GC permits, or
block gates is bounded by this deadline; mutations recheck it before unlinking.
An already-started unlink may finish after the budget, including its
index/accounting transaction. Each continuation batch obtains another membership
snapshot.
Membership can change immediately after any observation; this is a conservative
cache policy, not a distributed ownership transaction. It requires no peer-copy
confirmation because cached bytes remain recoverable from the origin.

## Cost and progress

When enabled, startup constructs an in-memory ordered admission index with one
entry per resident block. New blocks and newly materialized pages mark their
block for inspection. Cache hits do not update this index. A membership change
resets the scan cursor; otherwise only admissions and unfinished deletions after
the cursor are examined. Removed blocks leave no queued metadata behind.

For B cached blocks and N Workers:

- Once a pass is complete, stable membership with no new admissions or pending
  deletions performs an **O(1) local scan-watermark check**, independent of B.
- Membership polling still transfers O(N) information and canonicalizes it in
  O(N log N). The whole task is not O(1) in cluster size. Maglev tables are rebuilt
  only when membership changes.
- Enabled admission bookkeeping costs O(log B) per new cache unit, with O(B)
  additional metadata, including a copy of each BlockId. Disabled Workers do not
  allocate this ordered index. No placement computation is added to cache hits.
- Actual scans enumerate at most the configured block budget in chunks of 64;
  they never clone the full block index or walk the disk tree. Ownership hashes
  are computed per block, not per page. Page enumeration is ordered and bounded.
- Deletion attempts are bounded separately by cache-unit count and sequential.
  Continuation batches are paced at one second, independently of the configured
  hourly round interval. Each round freezes its admission watermark and each
  partial block freezes its page upper bound, so concurrent fills cannot extend
  the round indefinitely. Each batch releases its scheduler slot before the
  continuation delay. Scan/delete batch sizes are capped by the corresponding
  `background_*_batch_size` settings. All deletions share the background disk
  concurrency and deletion rate budget; paged deletions also share the page GC I/O
  semaphore. Larger caches can trade cleanup latency for lower foreground I/O
  interference. Physical release can lag unlink while readers hold open FDs.

Read protections, policy pins, concurrent admissions and deletion failures can
defer reclamation. Partial blocks resume from a page cursor within the same round;
protected/failed pages and new admissions after the round watermark wait for the
next scheduled round. Admission and retry entries coalesce per block,
so sustained failures cannot create an unbounded retry queue. Ownership unlink
failures never become ordinary TTI retries: only a new authorized ownership batch
may retry them. Normal TTI/capacity policies remain independent.

Ownership cleanup runs under task `stale_gc` in `WorkerBackground`, so global task
concurrency and per-task metrics include it. Shutdown stops admission of new
batches, finishes an active batch and its owned mutations, then flushes access
metadata under the existing Worker drain deadline. The two policies have separate
round clocks and cursors; an ongoing backend round does not require ownership
cleanup to wait for the full round. Scheduler completion metrics count batches.

Unlike backend-deletion cleanup, ownership cleanup does not require 24 hours of
idle time or issue HEAD requests. It checks ownership per block, then reclaims
unprotected pages in bounded batches; protected pages can remain for the next
round. Backend-deletion cleanup continues to claim and retire an entire block.

## Observation and validation

`talon_worker_ownership_gc_checked_total` counts blocks examined;
`talon_worker_ownership_gc_attempted_total` counts selected cache units.
Page reclamation adds `reason="ownership"` to the existing
`talon_worker_page_gc_reclaimed_total` and reclaimed-byte counters. Whole-block
deletions use the existing general eviction counter. Query failures are logged
and pause cleanup. Compare filesystem usage separately from logical residency.

Regression coverage includes stable no-op ticks, incremental admissions,
membership changes between/during rounds, query failure state, missing/duplicate members,
top-k and zone retention, whole/paged deletion, hourly scheduling with bounded
batch continuation, finite rounds under concurrent admissions, reader
protection, unlink failures and expiration while waiting for mutation gates.
These tests do not establish production CPU/latency or disk throughput numbers.
