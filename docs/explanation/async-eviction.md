# Background maintenance and cache eviction

The Worker uses one scheduler for page GC, access checkpoints, orphan-file cleanup,
and cache watermark eviction. This extends `DESIGN.md`, section 3 (Worker storage):
maintenance shares resource budgets while foreground capacity enforcement remains
available when a cache fill reaches capacity. Network fetches and uploads are
outside this scheduler.

## Shared scheduling and resource budgets

Configure the Worker with TOML or the matching `TALON_WORKER_` environment variable
(uppercase field name):

```toml
background_task_concurrency = 2
background_io_concurrency = 4
background_scan_batch_size = 65536
background_delete_batch_size = 1024
background_io_max_mb_per_sec = 8
background_delete_max_per_sec = 1024
```

These are the defaults. Task and I/O concurrency and batch sizes must be positive.
A zero rate disables that rate limit. MB means 1,000,000 bytes.

- **Task concurrency** bounds active batches across all registrations. Each task
  has at most one invocation. Ready tasks are admitted in round-robin order; there
  is no queue of missed timer events. A task becomes eligible again one configured
  interval after its previous batch finishes. The first batch is eligible at
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
- **Batch sizes** cap scanned entries and deletion candidates for GC, cleanup,
  and eviction. Existing page-GC batch limits also apply; the smaller limit wins.
  Checkpoints retain their atomic one-shard transaction, bounded by the existing
  64 MiB format limit, rather than truncating the shard to the scan budget.
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
Fair admission does not preempt a running batch; a slow filesystem operation or
low rate can delay other tasks, especially with task concurrency set to one.

The scheduler stops admitting work on shutdown, finishes admitted batches, drains
owned mutations, then flushes dirty access metadata with the same disk budget.
The process retains its existing 10-second maintenance shutdown deadline; after
that deadline, restart uses the last valid checkpoint. Dropping the scheduler
handle also stops admission and lets active batches finish.

Metrics `talon_worker_background_task_active`,
`talon_worker_background_task_completed_total`,
`talon_worker_background_task_panics_total`, and
`talon_worker_background_task_seconds` use a `task` label. Batch duration includes
resource waits. A panicking registration is logged and retried after its interval;
other registrations continue. Existing GC/checkpoint error and progress metrics
remain available.

## Cache watermarks

Enable watermark eviction with:

```toml
async_eviction_enabled = true
async_eviction_high_watermark = 0.85
async_eviction_low_watermark = 0.75
async_eviction_check_interval_secs = 30
```

```sh
TALON_WORKER_ASYNC_EVICTION_ENABLED=true
TALON_WORKER_ASYNC_EVICTION_HIGH_WATERMARK=0.85
TALON_WORKER_ASYNC_EVICTION_LOW_WATERMARK=0.75
TALON_WORKER_ASYNC_EVICTION_CHECK_INTERVAL_SECS=30
```

Defaults are disabled, high `0.9`, low `0.8`, and `60` seconds between batches.
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
