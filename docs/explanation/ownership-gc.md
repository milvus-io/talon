# Cache ownership cleanup in stale GC

Workers can reclaim clean local cache blocks outside their placement candidates.
This extends `DESIGN.md` section 3 (Worker storage). Ownership is a predicate in
the existing `StaleGc` block scan, alongside backend deletion. Both predicates
share one cursor, pending queue, round boundary, scheduler registration and
whole-block deletion transaction. The existing Worker control-plane task refreshes
membership and publishes a shared snapshot. `StaleGc` consumes that snapshot for
placement checks; it owns no sockets, TLS channel or discovery client. There is
no separate ownership-GC module, service or block index.
Origin objects, staging and WAL data are outside its scope.

## Configuration

```toml
ownership_gc_interval_secs = 3600
ownership_gc_replicas = 1
ownership_gc_scan_batch_size = 1024
ownership_gc_delete_batch_size = 1024
```

The interval defaults to `0`, disabling ownership cleanup. When enabled, it sets
the **shared stale-GC round interval**, including backend checks if enabled.
Without ownership cleanup, backend rounds retain their one-hour interval.
The first round starts immediately; subsequent rounds wait the interval after
the previous round finishes. Within a round, `StaleGc` processes bounded batches
with a one-second delay. The existing fixed-delay scheduler sees one invocation
per complete round; the task slot remains occupied until the round ends.

`stale_gc_enabled` independently enables backend-deletion checks. Disabling it
leaves ownership cleanup available; disabling both removes the task. The scan
and deletion settings above cap blocks scanned and block candidates per shared
batch. They are further capped by the page-GC and worker-wide background limits,
and scans never exceed 4096 blocks. A paged block counts as one candidate;
**every resident page still consumes deletion-rate credit** before reclamation.
All fields support uppercase `TALON_WORKER_` environment variables, overriding
TOML. Changes require restart; batch sizes and replica count must be positive.

Set `ownership_gc_replicas` to at least the largest candidate count used by any
reader or load planner. The Worker retains the union of global and local-zone
top-k candidates, conservatively supporting mixed global and zone-affine readers.
There is no automatic replica-count negotiation. Explicit prewarming outside
these candidates is subject to cleanup.

## Scan and membership

When ownership cleanup is enabled, the existing heartbeat control task refreshes
retained membership using `MembershipQuery` at `heartbeat_interval_ms`. Heartbeats
and membership requests run in independently paced loops, each with a three-second
request timeout. A slow or failed membership query does not delay heartbeats or
change their readiness result. TLS and connection handling stay in the control
plane. Disabling ownership cleanup also disables membership refresh.

The publisher validates and canonicalizes retained logical Workers, including
offline and conflicted instances, and excludes retired members. Response ordering,
addresses and instance availability do not affect placement. Every snapshot expires
at query start plus the Coordinator's `valid_for_ms`; network time consumes its
validity. Query failure or invalid data revokes the last snapshot. If the publisher
stops, expiry prevents indefinite reuse. There is no additional stability timer.

Each GC batch reads the current local snapshot. A membership change between rounds
resets the ownership watermark. A change during a round, missing/expired snapshot,
or missing local Worker pauses ownership for the rest of that round and resets
its watermark. Backend checks can continue. The next round again reads the local
snapshot; GC never triggers or waits for discovery, including at startup before
any snapshot is available. Coordinator upgrade must precede enabling cleanup:
retained membership queries must also be allowed on its authenticated Worker
listener. No wire schema changes are needed.

The shared scanner checks ownership before applying backend idle and HEAD
filters. Misplaced blocks do not require 24-hour idleness or a HEAD. Remaining
blocks need more than 24 hours of idleness and authoritative backend `NotFound`,
with the existing positive-check cooldown and bounded HEAD work.

Ownership remembers a scalar checked-through watermark in the existing
admission-ordered block queue. With unchanged membership, already checked owned
blocks need no placement computation. Deferred misplaced blocks move beyond the
frozen round boundary in that same queue; new blocks already enter it on admission.
No ownership bookkeeping is added to cache hits or page fills. A full round has a
finite admission boundary, so concurrent fills and protected blocks cannot keep
it alive indefinitely.

With backend checks disabled, stable membership and no new/deferred blocks need
O(1) local scan work. With backend checks enabled, the existing cold-block scan
still runs, skipping ownership calculations before its watermark. Membership
transfer and canonicalization cost O(N log N) in the control plane; GC reuses the
same snapshot between publications and rebuilds placement tables only on membership
changes. Deferring a block costs O(log B), reusing existing O(B)
block metadata. The task does not clone the complete resident index or scan disk.

## Reclamation and protection

Both reasons use the same whole-block mutation. Before deleting, it rechecks
admission revision, block activity and pins under the mutation gate, then claims
all resident pages. Any active reader defers the entire block. Successful removal
retires all files, L1 entries, lifecycle metadata, index entries and accounting.
Failed deletion retains conservative accounting and releases page claims. Failed
ownership cleanup requires a valid snapshot next round; TTI does not replay it.

Ownership charges deletion credits before final authorization, then reads the latest
published snapshot and requires unchanged placement. This allows a background
refresh to renew validity while a large block waits for deletion-rate credits.
Subsequent I/O, page-GC semaphore and block-gate waits are bounded by that snapshot's
expiry. Under the mutation gate, the reclaimer checks both expiry and the latest
published placement before unlinking; a revoked or changed view defers the block.
An already-started unlink completes its owned accounting transaction even if its
caller is cancelled or the snapshot subsequently expires or changes.

Polling cannot detect every membership transition, and membership can change
immediately after an observation. Cleanup is a cache policy, not a distributed
ownership transaction, and needs no peer-copy confirmation because the origin
retains the data. Logical retirement does not add a separate grace period.

## Observation and validation

`talon_worker_ownership_gc_checked_total` counts ownership block checks;
`talon_worker_ownership_gc_attempted_total` counts selected cache units.
Paged reclamation uses `reason="ownership"` in existing page/byte counters;
whole-file removal uses the general eviction counter. Scheduler task `stale_gc`
counts complete rounds. Shutdown uses the existing maintenance and owned-mutation
drain: an admitted round and its mutations finish before the scheduler joins.

Regression tests cover shared scans, bounded round continuation, stable ownership
watermarks, new admissions, membership changes/failures, independent backend
checks, top-k/zone retention, whole/paged cleanup, whole-block reader/pin protection,
background refresh independent of GC, heartbeat progress during stalled discovery,
rate-wait snapshot renewal, and expiration or changed membership behind disk permits
or mutation gates.
The existing stale-block tests cover concurrent reads/fills, unlink failures,
cancellation, HEAD cooldown and request bounds. These are local correctness
checks, not production throughput or latency acceptance.
