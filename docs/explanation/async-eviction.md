# Background cache eviction

The Worker can reclaim cached data in the background before reaching its configured
capacity. This leaves space for new cache fills and reduces the occasions when a
miss must wait for capacity eviction. It applies to both whole-block and paged L2
caches and uses the existing byte-accounted Second-Chance policy described in
`DESIGN.md`, section 3 (Worker storage). The change adds a background trigger
while retaining foreground capacity enforcement.

Enable it in the Worker TOML configuration:

```toml
async_eviction_enabled = true
async_eviction_high_watermark = 0.85
async_eviction_low_watermark = 0.75
async_eviction_check_interval_secs = 30
```

The corresponding environment variables are:

```sh
TALON_WORKER_ASYNC_EVICTION_ENABLED=true
TALON_WORKER_ASYNC_EVICTION_HIGH_WATERMARK=0.85
TALON_WORKER_ASYNC_EVICTION_LOW_WATERMARK=0.75
TALON_WORKER_ASYNC_EVICTION_CHECK_INTERVAL_SECS=30
```

Defaults are disabled, a high watermark of `0.9`, a low watermark of `0.8`, and a
check interval of `60` seconds. Watermarks must satisfy
`0 < low < high < 1`; the check interval must be positive. See the generated
[configuration reference](../reference/configuration.md#async_eviction_enabled).

## Watermarks and lifecycle

Occupancy means tracked resident cache data bytes divided by `capacity_bytes`.
It does not measure filesystem utilization, temporary files, metadata, or other
applications' data. For a 1 TiB cache, the example starts eviction at 85% occupancy
and attempts to reduce it to 75%. Thresholds round up for starting and down for
stopping; individual cache units can take occupancy below the low watermark.

The Worker checks immediately after starting the service, including data recovered
from disk, and then at the configured interval. Reaching the high watermark starts
a cycle. Each check makes a bounded reclamation pass; if reads, pins, concurrent
fills, or deletion errors prevent reaching the low watermark, the cycle remains
active for the next check even when usage has fallen below the high watermark.
Reaching or dropping below the low watermark ends the cycle.

The background loop awaits each pass before polling its timer again, so passes
do not overlap. An overdue tick can run immediately after a long pass; missed
intervals are delayed rather than replayed in a burst. Foreground eviction and
page TTI collection can still run concurrently with the background loop.

The collector runs independently of requests, page TTI, and coordinator
registration. Existing read protection and candidate validation also apply to
background eviction. Failed deletions remain charged to the cache; successful L2
eviction invalidates inclusive L1 copies. During shutdown the service stops
scheduling work, and the Worker drains owned disk/metadata mutations before the
final access checkpoint.

## Capacity fallback and disk space

Writes continue to enforce `capacity_bytes` after committing data. Below that
limit they do not wait for background watermark reclamation. If fills outpace the
collector and exceed the limit, the existing foreground capacity fallback still
runs. Neither path can guarantee reclamation while every candidate is protected
or cannot be deleted.

Set cache capacity below available physical storage, leaving room for concurrent
fills, staging files, and metadata. Files held open by active readers can delay
physical space release after unlink. Background eviction does not implement
filesystem-full (`ENOSPC`) recovery or reserve space for in-flight fills.

Existing `talon_worker_evictions_total` counts successful reclamations. Paged
watermark eviction is included in `talon_worker_page_gc_reclaimed_total` and
`talon_worker_page_gc_reclaimed_bytes_total` with `reason="capacity"`.
