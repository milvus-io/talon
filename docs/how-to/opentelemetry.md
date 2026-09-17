# OpenTelemetry tracing

Talon tracing is opt-in. Existing builds and deployments remain off. Build
Worker, Coordinator, Gateway, CLI, FUSE or a native SDK with `--features telemetry` to
include recording and OTLP export. The lightweight carrier and v2 decoder are
available without the exporter. No client constructor installs a subscriber.

## Configure a process

```sh
export TALON_TELEMETRY_MODE=standard
export TALON_TELEMETRY_SAMPLE_RATIO=0.01
export TALON_TELEMETRY_ENDPOINT=http://127.0.0.1:4318/v1/traces
export TALON_TELEMETRY_V2_ENDPOINTS=coordinator:7000,worker-1:7001,worker-2:7001
```

`off`, `propagate`, `standard`, and `diagnostic` are accepted. Only confirmed
v2 endpoints belong in the allowlist. `*` means the operator guarantees every
destination supports v2. Unknown destinations use v1; no probe or automatic
v1 replay follows a failed v2 attempt. A carrier and an allowlisted destination
are both required to send v2. The receiving server ignores remote diagnostic
hints and applies its own policy.

`TALON_TELEMETRY_MAX_SPANS` defaults to 256 (range 1–4096).
Diagnostic mode additionally requires `TALON_TELEMETRY_DIAGNOSTIC_SECONDS`
(1–3600) and `TALON_TELEMETRY_DIAGNOSTIC_REQUESTS` (positive). Expired or
exhausted diagnostics fall back to standard detail. Root sampling is decided
once; valid parent sampling flags are preserved. `RUST_LOG` filters the log
layer independently of the OTel layer.

Export uses OTLP HTTP/protobuf, a dedicated SDK batching thread, 4096 admitted
spans including in-progress exports, batches of 256, a one-second interval and
a three-second HTTP timeout. Attributes, events and links have SDK limits;
Talon attribute strings are limited to 256 bytes. Repeated byte counters use
fixed storage and are emitted once at span completion; other details are
deduplicated and limited to 23 keys, reserving space for summary and terminal
attributes within the production 48-attribute limit. Omitted details mark the
request partial. Collector failures never
wait on request threads. Existing metrics endpoints additionally expose
`talon_telemetry_queue_size`, `talon_telemetry_dropped_total` and
`talon_telemetry_export_failures_total`. The queue gauge includes the active
export batch. It is not a per-request statistic. Explicit shutdown waits up to
the SDK's five-second timeout after business drain. SDK 0.28 flushes only one
batch on shutdown; remaining spans count as drops. This does not guarantee a
complete trace during shutdown.

## Native SDKs

Rust hosts explicitly call `talon_telemetry::configure`, then install a
compatible `tracing-opentelemetry` layer, or use
`talon_telemetry::export::init_scoped` and bind its returned dispatcher to
Talon futures. The host owns the returned export owner and calls `shutdown`
from a blocking thread after draining reads. Automatic Rust inheritance occurs
when the future is first polled, not when it is constructed.

```rust,ignore
let parent = TraceContext::from_w3c(traceparent, Some(tracestate));
let options = RequestOptions {
    parent: parent.as_ref().map(TraceParent::Explicit).unwrap_or(TraceParent::Root),
};
client.read_into_with_options(&object, offset, &mut buffer, None, &options).await?;
```

C/C++ hosts call `talon_telemetry_init()` explicitly after setting the
environment. It installs a Talon-scoped dispatcher, leaving the host's global
subscriber untouched. Initialize `talon_request_options` with
`talon_request_options_init`, fill the W3C strings and submit through
`talon_read_async_with_options` / `talon_stat_async_with_options`. Strings and
options may be destroyed after submission returns. The destination buffer and
client retain their original callback lifetime. NULL/NONE and all legacy C
entrypoints select no external parent. Unknown ABI versions, flags and modes
fail synchronously; invalid W3C strings merely discard context. New symbols
require the matching new shared library. Set `TALON_BUILD_TELEMETRY=1` when
running `clients/c/package.sh` to include export support.

Python accepts `trace_context={"traceparent": ..., "tracestate": ...}` on
`read` and `stat`. Without explicit data it captures the installed Python
OpenTelemetry propagator before releasing the GIL. `talon.configure_telemetry()`
explicitly loads the process policy and, in a wheel built with `telemetry`,
installs a Talon-scoped exporter. `talon.shutdown_telemetry()` waits for bounded
export shutdown without holding the GIL; call it after draining reads. The
partial version/size metadata lookup and subsequent read share the Python
operation scope. It does not replace the Python provider.

Java preserves its dependency-free artifact and supports request-options
overloads. Install an adapter with `Telemetry.configure(confirmedEndpoints,
contextSupplier)`; the supplier injects the current Java OTel context into W3C
strings and returns `TraceContext.fromW3c(...)`. It is called on the submitting
thread. `RequestOptions.explicit(...)` and `ROOT` override inheritance. This
bridge propagates the caller's context; it does not create a Java exporter.
The ordinary HTTP Java agent alone cannot instrument Talon's custom TCP wire.
FUSE creates local Talon traces; kernel FUSE requests do not carry an application
OTel parent.

## Collector and queries

`docker compose -f deploy/observability/otel/compose.yaml up -d` starts a local
Collector and Tempo with bounded Collector queues. The pinned versions are
[Collector 0.123.0](https://github.com/open-telemetry/opentelemetry-collector-releases/releases/tag/v0.123.0)
and [Tempo 2.7.2](https://github.com/grafana/tempo/releases/tag/v2.7.2).
The example uses an isolated development network and ephemeral trace storage.
Mount `talon-traces-datasource.yaml` into your existing Grafana provisioning
directory, adjusting its Tempo URL for that network. Import
`deploy/observability/grafana/talon-traces-health.json` for queue and loss panels.

Filter spans by `talon.read.id`; use the trace timeline for wall time. Count
`talon.refill` spans by unique `talon.refill.id`, and inspect their outcomes.
Follower waits link to the captured refill context; deduplicate links across
pages before calculating shared dependencies. An unrecorded leader or a wait
that spans multiple flight generations is incomplete. The initial bridge
reports `talon.refill.shared_dependencies` as a deduplicated known lower bound
and keeps `talon.dependencies.complete=false` when any shared wait occurs.

HTTP spans cover body EOF/error/drop and retain partial downloaded bytes.
`talon.http.attempt_boundary=reqwest.execute` means one Reqwest execution,
which can include internal retries/redirects. It is not proof of one physical
send or of origin receipt. Explicit Talon retries and timeout/backoff stages
are recorded separately. Do not count both levels as physical attempts.

`talon.details.complete` concerns locally recorded details only. Export drops,
unsampled peers, old protocol hops and backend retention can still make a trace
partial. Missing GET spans do not prove zero GETs. Compare downloaded,
validated, committed and response bytes where present; omitted attributes are
unknown, not zero. Concurrent span durations sum work, not read wall time.

## Rollout and acceptance

Deploy dual-version servers first, then capable clients, then enable the
allowlist. For rollback, disable active v2 sends, drain their connections and
only then revert servers. Disabling recording keeps the v2 decoder available.

Functional tests are not a zero-overhead certificate. Keep the default off
until the design's interleaved A/B workload matrix, allocation checks, CPU/RSS,
tail latency and Collector-failure measurements pass on an isolated system.
No acceptable regression percentage is implied by the implementation. The
design's P5 production rollout and performance acceptance remain deployment
work; this example is not a production deployment or a performance report.

The [local overhead measurements](../reports/telemetry-overhead-20260909.md)
found measurable costs even after allocation reductions. On a short TCP stub
RPC, 1% root sampling added about 6% and full sampling about 61–63%; these are
client-side probe results without real Worker I/O or OTLP export. They do not
justify enabling tracing by default or establish zero overhead when off.


## Query examples and interpretation

In Tempo Explore, start with `{ span.talon.read.id = "READ_ID" }`,
`{ name = "talon.refill" }`, or `{ span.talon.outcome = "timeout" }`.
A Worker SERVER summary counts only its local work. SDK summaries do not add
remote Worker totals: join traces/read IDs and deduplicate refill IDs before
cross-worker aggregation. `talon.cache.tier` reports observed local tiers;
`mixed` can include origin plus local cache. `unknown` is not a cache miss.
`server.address` on a native RPC contains the configured endpoint.
HTTP `talon.response.bytes` at the Gateway counts body bytes yielded toward
Hyper, not proof of remote receipt. Worker response bytes are recorded only
after successful writes; partial failed writes may be unknown.

HTTP detail fields describe the locked Reqwest execute boundary. No attempt
changes redirect, retry, signing, ETag validation, cache admission or durability.
Do not interpret span attributes as billing-grade origin receipt accounting.

See [validation evidence](../reports/opentelemetry-validation.md) for checks run
against this implementation and the remaining deployment acceptance work.
