# Background maintenance and cache eviction

The Worker uses one scheduler for TTI GC, access checkpoints, orphan-file cleanup,
stale GC, and cache watermark eviction. This extends `DESIGN.md`,
section 3 (Worker storage): maintenance shares resource budgets while foreground
capacity enforcement remains
available when a cache fill reaches capacity. Network fetches and uploads are
outside this scheduler.

## Shared scheduling and resource budgets

Configure the Worker with TOML or the matching `TALON_WORKER_` environment variable
(uppercase field name):

```toml
page_gc_interval_minutes = 10
file_cleanup_interval_hours = 1
background_task_concurrency = 2
background_io_concurrency = 4
background_scan_batch_size = 65536
background_delete_batch_size = 1024
background_io_max_mb_per_sec = 8
background_delete_max_per_sec = 1024
```

These are the defaults. Task and I/O concurrency and batch sizes must be positive.
A zero rate disables that rate limit. MB means 1,000,000 bytes.

- **Task concurrency** bounds active invocations across all registrations. Each task
  has at most one invocation. Ready tasks are admitted in round-robin order; there
  is no queue of missed timer events. A task becomes eligible again one configured
  interval after its previous invocation finishes. The first invocation is eligible at
  startup. Resource contention can delay admission.
- **I/O concurrency** is shared by checkpoint writes, page/whole-block eviction,
  and orphan cleanup. A slot stays owned until disk and metadata changes finish,
  including when a caller is cancelled. Existing page-GC concurrency remains an
  additional limit for page deletion and cleanup.
  All rate waits use asynchronous timers, including directory cleanup and each
  checkpoint write chunk; pacing never occupies a blocking-pool thread. Cleanup
  and checkpoint filesystem operations use asynchronous APIs, while snapshot
  encoding and synchronous temporary-file operations run in short blocking jobs.
  The admitted mutation retains its I/O slot and required directory/shard gates
  across waits to preserve serialization and cancellation safety.
- **TTI GC (`tti_gc`)** completes a full in-memory scan and all selected deletion attempts
  per invocation, then waits `page_gc_interval_minutes` (default 10 minutes). It ignores
  scan/deletion batch limits, but retains disk concurrency and deletion rate limits.
  Candidates are collected before deletion, so candidate memory grows with the
  number of eligible pages. Pages added behind the cursor may wait for the next pass.
  This task also retries failed capacity/superseded page deletions and cleans up
  empty page directories, so it continues running when TTI is disabled. The
  scheduler task label is now `tti_gc` (previously `page_gc`); update queries
  filtering the background-task metrics by that label. Existing `page_gc_*`
  configuration names and shared page-reclamation metrics remain unchanged.
- **File cleanup** completes a full disk traversal per invocation, then waits
  `file_cleanup_interval_hours` (default one hour), independently of TTI GC. Startup
  recovery still completes a pass before serving requests. Directory iteration
  streams entries without collecting a global file list; deletion concurrency,
  pacing, and directory/shard gates remain in effect. Failed removals are retried
  on the next pass. Existing deployments that used `page_gc_interval_ms` to tune
  cleanup must set the new option explicitly.
- **Batch sizes** cap scanned entries and deletion candidates for watermark
  eviction and stale GC. The legacy `page_gc_scan_batch_size` and
  `page_gc_delete_batch_size` settings also apply to both tasks; the
  smaller limit wins for backend-deletion checks. Ownership cleanup uses its own
  batch sizes capped by the shared background limits. Neither TTI GC nor file cleanup is truncated by these
  limits. Checkpoints retain their atomic one-shard transaction, bounded by the
  existing 64 MiB format limit.
- **Disk throughput** meters application data bytes before each checkpoint write
  chunk (at most 64 KiB), using one shared rate clock with no idle burst credit.
  This is local disk read/write budgeting, not network bandwidth or measured
  physical device traffic. Current maintenance reads directory metadata and writes
  checkpoints; filesystem metadata, fsync, page-cache writeback, and write
  amplification cannot be expressed as application-byte traffic.
- **Deletion rate** meters maintenance work items, independently of byte traffic.
  One item is a page or whole-block eviction including its associated metadata
  cleanup, an empty-block cleanup attempt, or one orphan file/directory removal.
  Failed and stale attempts also consume credit. It is not an exact syscall or
  device-IOPS limit. Deleting a 1 GiB cache block does not charge 1 GiB to the disk
  byte budget.

Limits apply to scheduled maintenance and its final checkpoint flush. Startup
recovery and foreground request/capacity work are outside these shared budgets.
Fair admission does not preempt a running invocation; a slow filesystem operation or
low rate can delay other tasks, especially with task concurrency set to one.

The scheduler stops admitting work on shutdown, finishes admitted invocations, drains
owned mutations, then flushes dirty access metadata with the same disk budget.
Admission stops as soon as the worker starts draining requests. Maintenance and
the final checkpoint share the process's 20-second drain deadline with request
and control handling. If the deadline expires, the process exits while retaining
the cache-directory lock until termination; restart uses the last valid checkpoint.
Dropping the scheduler handle also stops admission and lets active batches finish.

Metrics `talon_worker_background_task_active`,
`talon_worker_background_task_completed_total`,
`talon_worker_background_task_panics_total`, and
`talon_worker_background_task_seconds` use a `task` label. Invocation duration includes
resource waits, including the complete TTI GC pass. A panicking registration is
logged and retried after its interval; other registrations continue. Existing
GC/checkpoint error and progress metrics remain available.

## Cache watermarks

Enable watermark eviction with:

```toml
async_eviction_enabled = true
async_eviction_high_watermark = 0.85
async_eviction_low_watermark = 0.75
async_eviction_check_interval_minutes = 1
```

```sh
TALON_WORKER_ASYNC_EVICTION_ENABLED=true
TALON_WORKER_ASYNC_EVICTION_HIGH_WATERMARK=0.85
TALON_WORKER_ASYNC_EVICTION_LOW_WATERMARK=0.75
TALON_WORKER_ASYNC_EVICTION_CHECK_INTERVAL_MINUTES=1
```

Defaults are disabled, high `0.9`, low `0.8`, and `1` minute between batches.
Watermarks must satisfy `0 < low < high < 1`; the interval must be positive.
See the generated [configuration reference](../reference/configuration.md#async_eviction_enabled).

Occupancy is tracked resident cache data bytes divided by `capacity_bytes`, for
both whole-block and paged L2. It excludes metadata, temporary files, other
applications, and filesystem overhead. A zero capacity disables watermark eviction.

Reaching the high watermark starts a reclamation cycle. Each invocation performs
one bounded Second-Chance scan/deletion batch. If pins, recent accesses, ongoing
fills, errors, or batch limits prevent reaching the low watermark, the cycle stays
active across subsequent invocations, even below the high watermark. Reaching or
falling below the low watermark ends it. Start thresholds round up and stop
thresholds round down; deleting one unit can cross below the low watermark.

The first batch handles occupancy recovered from disk, independently of requests,
page TTI, and coordinator registration. Candidate revalidation and reader
protection apply before deletion. Failed deletion keeps cache accounting; successful
L2 eviction also invalidates inclusive L1 copies. Existing capacity-eviction
metrics include watermark deletions.

Background headroom does not reserve space for concurrent fills or handle every
filesystem `ENOSPC` condition. Foreground capacity enforcement remains the fallback
when new cache admissions exceed `capacity_bytes`.

Maintenance durations use integer minutes or hours. For renamed configuration keys,
unit conversion, and environment-variable migration, see [Page TTI](page-tti.md#使用方式).

## Stale GC: deleted backend objects

`stale_gc_enabled = true` (the default) enables the backend-deletion policy in
`stale_gc`. The first round is eligible at startup; after a complete round finishes,
it waits **one hour** before starting another, unless ownership cleanup configures
the shared interval below. Rounds never overlap.
Set `TALON_WORKER_STALE_GC_ENABLED=false` to disable backend-deletion checks.

The same scan includes optional [ownership cleanup](ownership-gc.md).
`ownership_gc_interval_secs` defaults to `0` (disabled); enabling it sets the
shared round interval. Both predicates share one cursor and whole-block reclaimer.
Ownership is checked first using the valid membership snapshot published by the
Worker control-plane task, without an idle-age or HEAD requirement. GC does not
perform membership communication. Disabling both predicates removes the task.

For backend-deletion checks, only blocks idle for **more than 24 hours** are eligible. Reads and admissions
refresh a process-local, monotonic block timestamp, shared by all of its pages.
The task walks resident metadata, without scanning cache directories or listing
the backend. Each round traverses one fixed queue generation in batches. Each
batch scans at most 4096 blocks (or the smaller configured scan budget) and retains
at most the configured deletion batch size in block candidates. A paged block
counts as one candidate regardless of how many pages it contains.
A cursor resumes the admission-ordered block index, independently of the
capacity-eviction queue. Blocks admitted during a round wait for the next round.

The worker issues serial HEAD requests, capped at 32 distinct objects and a
five-second backend-work deadline per batch. Unprocessed candidates carry over
to the next batch in the same round so slow objects cannot starve the remainder.
Batches within a round have a one-second delay after completion; this is pacing,
not the interval between full scans. Local deletion waits can extend a batch
beyond the HEAD deadline. The task keeps its background scheduler slot throughout
the round, including continuation delays. The `stale_gc` task metrics count
complete scan rounds.
The existing disk concurrency and deletion rate budgets apply to unlinks, not
HEAD traffic. A whole-block removal charges deletion work for all resident pages,
even though they are removed together as a directory.

A successful HEAD suppresses further GC verification for 24 hours across every
cached offset and version of the same `(backend, bucket, key)`. A changed version
still counts as an existing object. Only an explicit backend `NotFound` permits
local reclamation; permission errors, transport failures, and timeouts retain the
cache. Negative results and errors are shared only within the current batch.
The task never deletes origin data.

Before unlinking, the worker rechecks block admission identity, reader pins, and
block activity under the mutation gate. For paged blocks, it claims every resident
page before deleting anything: if any page has an active reader, the entire block
is deferred and all partial claims are released. Reads or page fills invalidate
old block candidates. Successful reclamation removes the whole block file or the
entire pages directory, then removes all of the block's L1 entries, page lifecycle
state, index entries, and resident-byte accounting together.

An unlink failure retains accounting and reopens page handles; stale GC requires
a fresh HEAD on retry. Directory removal can fail after partially removing files;
accounting remains conservative until normal miss recovery or a later GC succeeds.
TTI GC does not independently replay the stale NotFound. Paged reclamation still
counts the number of reclaimed pages under reason `backend_deleted` in the
existing GC metrics; scheduler metrics expose task `stale_gc` for both forms.

Idle timestamps and successful checks are in memory and retire with resident
entries. Restart conservatively starts a fresh 24-hour idle window, including for
recovered cache files. This is eventual space reclamation: hot blocks remain
cached, and deletion discovery is delayed by idle age, positive-check cooldown,
and scan progress. It does not change read-path version semantics.
