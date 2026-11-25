# Event-driven cache ownership cleanup

Workers can reclaim clean local cache blocks outside their placement candidates.
This extends `DESIGN.md` section 3 (Worker storage). The existing Worker
control-plane task refreshes membership and publishes local snapshots. `OwnershipGc`
consumes those snapshots; it owns no discovery client, socket or TLS channel.
Origin objects, staging and WAL data are outside its scope.

## Configuration

```toml
ownership_gc_enabled = true
ownership_gc_replicas = 1
```

Ownership cleanup defaults to enabled. It registers `ownership_gc` for logical
membership changes and hourly supplemental admission checks. Set
`ownership_gc_enabled = false` or `TALON_WORKER_OWNERSHIP_GC_ENABLED=false` to
stop both ownership maintenance and its control-plane membership refresh.
`stale_gc_enabled` independently enables backend
object deletion checks, which retain their one-hour delay after each completed
backend round. Each setting controls its own task.

Ownership scans continuously through the event boundary. An ordinary scan works
one candidate at a time. A compact Small Store scan retains at most one bounded
batch of new candidates. Every selected packed entry or resident page consumes
deletion-rate credit. Both fields support uppercase
`TALON_WORKER_` environment variables overriding TOML. Changes require restart;
replica count must be positive.

Set `ownership_gc_replicas` to at least the largest candidate count used by any
reader or load planner. The Worker retains the union of global and local-zone
top-k placement candidates, supporting mixed global and zone-affine readers.
There is no automatic replica-count negotiation. Prewarming outside these
candidates can be reclaimed by a later membership event or supplemental pass.

## Membership events and scan boundaries

When ownership cleanup is enabled, the heartbeat control task refreshes retained
membership through `MembershipQuery` at `heartbeat_interval_ms`. Heartbeats and
membership requests run in independently paced loops, each with a three-second
request timeout. Discovery failure does not delay heartbeats or change readiness.
TLS and connections stay in the control plane. Disabling ownership cleanup also
disables membership refresh.

The publisher validates and canonicalizes retained logical Workers, including
offline and conflicted instances, and excludes retired members. Logical IDs and
zones determine changes; ordering, addresses and instance health do not. A snapshot
expires at query start plus the Coordinator's `valid_for_ms`, so network time
consumes validity. Missing/invalid observations expire the previous snapshot,
but retain its topology and event boundary. Same-topology recovery renews validity
and wakes pending work without expanding its scope.

On each logical membership change, publication captures the ordinary block
admission sequence and the compact Small Store admission epoch for each initialized
cache namespace and credential scope. It assigns a new local generation. The first
valid observation is an event too, covering cache present at publication, including
recovered blocks. GC scans only that fixed prefix for ownership. It also excludes
blocks whose admission revision is at or beyond the limit: a post-event replacement
or page fill conservatively excludes the entire block. Reads do not change the
admission revision. New data never expands an event's scan, including data written
between publication and delayed GC startup. A packed segment enters the scan
order when its first row becomes visible. A writer that reserved a file earlier
can therefore publish after the event without adding work to its scan. An
already visible segment freezes its published byte ranges on the first later
admission. Concurrent appends can finish out of order, leaving gaps for
unfinished writes. The event skips those gaps even after their writes finish.
Compaction retains the logical scan position and translates the captured
ranges to the surviving rows in the replacement.

A complete pass marks the generation converged once all selected misplaced entries
have been reclaimed or superseded by a later admission and the generation remains
valid. Reader and pin protection, activity races and unlink failure leave the event
pending. After one second, the sweep retries protected or failed ordinary entries by
queue position. Packed retries keep deduplicated admission addresses that move with
the locator. Each retry batch obtains fresh tickets, and a later admission removes
the old retry. A journaled deletion that still has a readable, charged locator
remains discoverable until accounting completes or recovery applies the tombstone.
The cursor and current candidate survive cancellation. Once converged, ownership
waits for the next event or supplemental check.

A newer event supersedes the old generation, including A -> B -> A changes observed
by the publisher between GC invocations. Missing, expired or self-excluding membership
pauses ownership. Recovery or another topology event wakes it. GC never performs or
waits for discovery. Upgrade the Coordinator before Workers using the default-on
cleanup: retained membership queries must be allowed on its authenticated Worker
listener. Until then, explicitly disable ownership cleanup on Workers; failed
membership queries otherwise defer cleanup. No wire schema changes are needed.

## Low-frequency supplemental cleanup

Clients may continue using old placement while discovering a topology change.
Their page fills and refills fall outside the event boundary. One hour after a
completed pass, ownership checks whether any cache admissions have occurred since
its completed boundary. Without admissions, it returns without traversing the
resident index, hashing blocks, or doing disk/backend I/O. This check is constant
time in resident-cache size; normal membership validation still depends on the
number of Workers. Reads and capacity-policy rotations do not advance this marker.

When admissions exist, a supplemental pass captures a fixed new upper boundary
and traverses only that range of the ordinary in-memory ownership index. Only blocks whose
latest admission revision is below the new boundary are hashed and checked
against the cached placement tables. Unchanged residents are not revisited and
there is no disk-directory scan.

The ownership index retains one entry per resident block. Each page admission
updates the block revision for deletion safety, but repeated admissions coalesce
at the same queue position until ownership observes the block. Observation marks
the notification consumed under the same lock used by admission. The next fill
moves the existing entry to a new position, reusing its BlockId; subsequent fills
again update only the revision. Observed entries remain indexed for full scans on
membership changes. Removing the last resident unit removes the entry too.

Queue position and admission revision are deliberately separate. If a coalesced
block has a post-boundary fill, the scanner moves its entry to the latest revision
and leaves it pending for a later pass. This preserves the fixed event boundary
without moving the index for every page. Protected retries use queue positions,
while deletion still validates the latest revision, activity and pins.

Small Store groups admissions by segment and epoch. Each ordinary append
inherits its segment's first epoch or stores an offset exception for a later epoch.
Immutable re-admissions update the epoch even when their payload locator is
unchanged. Publication advances the epoch after capturing the event boundary.
Hourly supplements visit segment headers up to the captured extent. They scan all
rows in segments with newer admissions and hash only entries in the captured epoch
range. Without relocation, a pass costs the number of segment headers plus the
rows in those segments. A single immutable re-admission can require scanning every
row in its segment, with at most 64 segment or row steps per slice.
A segment replacement keeps its logical place
in the scan and transfers row epochs and tickets to the copied extents. If the
current segment moves between slices, the scan resumes at the beginning of its
replacement.

Supplemental passes use the same yielding, deletion budgets and protection checks
as event passes. Reader/pin protection and unlink failure retry the fixed bounds
after one second. Cancellation or a same-topology discovery outage preserves those
bounds. Admissions during the pass are deferred to the next hourly pass, including
page fills into older blocks. A new membership generation supersedes a pending
supplemental pass and checks the new event's entire prefix immediately. Hourly
timing restarts on completion; invalid or self-excluding membership pauses work.
No timer waits occupy a background task slot. Continued admissions require work
proportional to notified blocks, so cleanup under write load is not constant time.

## Independent tasks and bounded work

`ownership_gc` and `stale_gc` have separate modules, scheduler registrations and
scan cursors. Ownership never issues backend requests; stale GC never consumes
membership. Both tasks use the ordinary block index and the compact Small Store
catalog. They share deletion budgets and the same guarded mutations.

Ownership retains the active pass cursor, retry keys and current candidate across
scheduler slices and cancellation, along with its configuration, cached placement
tables, last converged generation/admission boundary and supplemental deadline.
Maglev tables rebuild only when placement changes. Each eligible block is hashed
and checked against those tables. Reads do not update ownership notifications.
Membership canonicalization costs O(N log N); scans use the resident index without
cloning the full index or walking cache directories.

A slice checks at most 64 ordinary entries or spends a 64-step budget traversing
packed segments and rows. It yields between batches or after about 10 ms in
the deletion loop. Each deletion rechecks membership. Actual deletions consume
shared page/disk credits; pending
protected blocks retry after one second. The event scheduler coalesces changes
received during active work. Once converged, ownership waits for a membership
event or its hourly admission check without occupying a task slot.

Independent `stale_gc` retains its hourly round delay, one-second batch pacing,
24-hour idle filter and positive HEAD cooldown. In a mixed namespace it alternates
ordinary and packed HEAD waves while both have pending work. Only authoritative `NotFound`
authorizes backend-deletion reclamation. Each batch permits at most 32 distinct
HEADs within a five-second backend work deadline. Neither ownership activity nor
membership failure changes this schedule or policy.

## Reclamation and protection

For ordinary blocks, both reasons retire the whole file or all resident pages.
Under the mutation gate the reclaimer rechecks admission revision, activity and
pins, then claims all resident pages. An active page reader defers the block.
Successful deletion clears L1, lifecycle, index and accounting entries. Failures
retain conservative accounting and release page claims.

For a packed entry, the reclaimer checks its locator ticket, pins, read activity
and membership at tombstone publication. It retires just that whole object or
exact range. An exact-range retirement leaves any ordinary whole block, paged
block or L1 data for the same block identity available.

Ownership charges all deletion credits before reading the latest valid snapshot
of the expected generation. This permits renewal while a large block waits for
credits. Subsequent semaphore, disk-permit and mutation-gate waits are bounded by
that snapshot's expiry. Before unlinking, the reclaimer verifies expiry and the
current generation. An already-started unlink finishes its owned accounting
transaction even if the caller is cancelled or membership subsequently changes.

Polling cannot detect every remote transition; the boundary reflects local
observation. Cleanup is cache policy, not a distributed ownership transaction.
Origin data remains authoritative, so cleanup needs no peer-copy confirmation or
additional retirement grace period.

## Observation and validation

`talon_worker_ownership_gc_checked_total` counts block ownership checks;
`talon_worker_ownership_gc_attempted_total` counts selected cache units.
Paged removal uses `reason="ownership"` in page/byte counters; whole-file removal
uses the general eviction counter. Scheduler tasks `ownership_gc` and `stale_gc` report separate invocations.
Shutdown stops admission and drains active rounds and owned mutations.

Tests cover event boundaries, post-event admissions/page fills, convergence,
A -> B -> A, recovery without widening scope, cancellation, reader/pin and unlink
retries, supplemental cleanup of late fills/refills, unchanged-admission no-ops,
supplemental cancellation/recovery boundaries, events during active work, idle scheduler slots, independent task
schedules, heartbeat progress, placement replicas/zones, and authorization after rate,
I/O and mutation-gate waits. Existing stale-block tests cover HEAD cooldown and
bounds, concurrent reads/fills and deletion cancellation. These are local
correctness checks, not production performance acceptance.
