# Real-Time Tenant Traffic Observability

## Status

Design proposal. Companion to
[Eventual Global Tenant Rate Limits](eventual-global-tenant-rate-limits.md).
That document defines how a tenant's aggregate traffic is *bounded*; this one
defines how operators *see* each tenant's live traffic across the cluster. The
two share signal sources and the same eventual-consistency envelope.

## Problem

A Talon cluster serves many tenants from many workers. Operators need a
near-real-time answer to "what is each tenant doing right now, cluster-wide?":

- read IOPS admitted,
- client egress bytes,
- origin (cache-miss) read bytes,
- how close the tenant is to its configured limits (headroom), and
- whether the tenant is currently being throttled, and by how much.

Two properties of the system make this non-trivial:

1. A tenant's traffic is spread across the whole fleet, and in the
   eventually-consistent limiter no single node holds a tenant's global state
   on the hot path. "Current global rate for tenant X" is therefore not
   directly readable from any one worker.
2. The metrics stack enforces low cardinality. A raw tenant identifier must not
   become an unbounded Prometheus label (the same rule that already bans
   `bucket`, `object_path`, and `block_id` labels). Per-tenant drill-down has to
   go through an aggregate-plus-top-N path, not a label explosion.

The aggregation must not put a synchronous query on the data hot path, must
respect the cardinality rule, and should work *both* before and after the
distributed rate-limit owner layer exists — so that per-tenant visibility can
ship alongside the first, local-only limiter stage.

## Tenant identity model

For the purpose of this design a **tenant** is the authenticated resource owner
carried by `TenantContext`, established at the object-store gateway from the
request credential (the gateway's provider-account / principal). The gateway
attaches it to each request and it is propagated to the worker on the data
plane. Clients that connect directly to a worker and bypass the gateway (FUSE
and the native clients) carry no credential; their traffic is attributed to a
single reserved **`unattributed`** tenant so that it is still visible and
bounded rather than silently uncounted. Tenant identity, its propagation, and
the unattributed fallback are specified by the rate-limit design; this document
only consumes the resulting `(tenant, metric)` signal.

## Contract

This is a *monitoring* view: near-real-time, eventually consistent, and
bounded-stale. It is explicitly **not** a billing ledger or an exact historical
meter.

- Reported values lag reality by at most one aggregation period — on the order
  of the heartbeat interval (Tier 1) or the rate-limit owner sync interval
  (Tier 2).
- The view is top-N per source. A tenant that is individually small on every
  worker but collectively non-trivial may be summarized into an `other`
  aggregate rather than named. Truncation is reported, never silent.
- Numbers are rates over a sliding window, not cumulative billable totals.

Anything that needs exact, durable, never-dropped per-tenant accounting (usage
billing, chargeback) belongs in a separate metering pipeline and is out of
scope here.

## Signals measured on each worker

Every worker already owns atomic per-`(tenant, metric)` counters for the
limiter. Observability reuses them; it introduces no new hot-path work beyond a
periodic read. For each active `(tenant, metric)` the worker maintains a
windowed rate meter (EWMA or fixed-window counter) over the same events the
limiter charges:

- `read_iops` — client read requests admitted;
- `client_egress_bytes` — bytes sent to clients;
- `origin_read_bytes` — bytes fetched from the object store on miss;
- write-path metrics when those paths are enabled.

Alongside the rates, the worker tracks per tenant:

- admission outcome counts: `admitted`, `queued`, `rejected_rate`, `bypass`;
- current local view `remaining` and tenant queue depth;
- a `throttled` flag (queue non-empty or recent rate rejections).

These are cheap reads off counters that already exist for enforcement.

## Two aggregation tiers

Cluster-wide per-tenant state is assembled from workers in two tiers. Tier 1
works from day one and is always available; Tier 2 becomes available once the
rate-limit owner layer exists and supersedes Tier 1 for keys with an active
owner.

### Tier 1 — heartbeat top-N summaries (available in the local-only stage)

Each worker already reports a `NodeMetricsSnapshot` into the coordinator's
leased cluster-state store on the heartbeat cadence. Tier 1 adds a **bounded
per-tenant traffic summary**: each worker selects its top-N tenants by recent
traffic (N small and configurable, initially 32–64) and reports, per tenant, a
compact record:

```text
TenantTrafficSample {
  tenant,
  read_iops,
  client_egress_bytes,
  origin_read_bytes,
  throttled,
  window_ms,
}
```

Because the node-status record is size-bounded (a hard 16 KiB cap), the summary
is carried as its own bounded control message on the heartbeat cadence rather
than inflating `NodeStatus`; the top-N cap and a byte ceiling keep it bounded
regardless of tenant count.

The coordinator merges the per-worker summaries into a cluster view: for each
tenant it sums the per-worker rates to a cluster-wide rate and records the
contributing worker set. A tenant hot on a few workers is captured exactly; a
tenant spread thinly below every worker's top-N is approximated and the
truncation is surfaced (an `other` aggregate plus a "N tenants omitted" count).
This costs one small periodic message per worker, scales with workers × N (not
with IOPS), and adds nothing to the data hot path.

### Tier 2 — owner-authoritative view (with the rate-limit owner layer)

Once `RateKey` owners exist, each owner already holds the *exact* aggregated
global consumption for its tenant×metric keys — it sums every worker's usage
reports to maintain the authoritative meter. A management aggregator on the
coordinator pulls each owner's current per-key meter state (remaining, target
rate, observed rate, active-reporter count) at the owner snapshot cadence. This
yields an **exact** cluster-wide per-tenant rate for every key with an active
owner, at 2–5 ms freshness, and reuses the same consistent-hash ring and
membership epoch the limiter already computes to know which worker owns which
key.

Tier 2 supersedes Tier 1 for keys that have an active owner; Tier 1 remains the
fallback for keys with no recent activity and during owner failover, when the
authoritative meter is briefly unavailable. The exposure layer marks each
tenant's numbers as `exact` or `approximate` accordingly.

## Exposure

### Management API (coordinator)

New read-only endpoints on the existing coordinator management API, mirroring
the shape and safety of the current `/api/v1/nodes` surface (bounded page size,
fail-closed to 503, a response envelope carrying snapshot age and revision):

```text
GET /api/v1/tenants
    -> paged list of tenants with cluster-wide current rates per metric,
       configured limits, headroom, throttle state, freshness (exact|approx),
       sorted by a requested metric (top-N by rate), filterable.

GET /api/v1/tenants/{tenant}
    -> per-tenant drill-down: per-metric rates, per-worker breakdown,
       headroom vs policy, recent throttle/reject counts, contributing owners.
```

This is the primary path for high-cardinality per-tenant inspection, precisely
because it does not live in the Prometheus label space.

### Prometheus (`/metrics`)

Only aggregate and enum-labeled series are exported — never a raw tenant label:

- cluster totals per metric (`read_iops`, `client_egress_bytes`,
  `origin_read_bytes`);
- count of currently-throttled tenants;
- the estimated-excess gauge (the limiter's `N*b + R*d` envelope);
- report/snapshot aggregation delay histograms and aggregation staleness.

For the handful of tenants an operator wants on a dashboard, a small
operator-configured **allow-list** grants those specific tenants their own
labeled series; every other tenant rolls into an `other` bucket, with the full
ranking available through the API. This bounded-label allow-list is the one
net-new piece the metrics layer needs, and it keeps the existing
"no unbounded tenant labels" guardrail intact.

### Dashboards and alerts

A cluster dashboard shows per-metric totals, a top-N tenant table (sourced from
the management API or the allow-listed series), throttle rate, and the excess
envelope. Alerts cover sustained per-tenant throttling, an excess-envelope
breach, and aggregation staleness. Dashboard panels obey the banned-label rule;
no raw tenant identifier appears in a panel except an allow-listed one.

## Cardinality and safety

- No unbounded tenant identifier is ever emitted as a Prometheus label; the
  drill-down path is the management API plus top-N, with an explicit allow-list
  for named series.
- Every per-worker summary and per-owner pull is size-bounded (top-N plus a
  byte ceiling), so a tenant-count spike cannot blow up control-plane traffic.
- The management API reads coordinator snapshots only. It never touches the data
  hot path and never issues a per-request query.

## Failure semantics

| Event | Behaviour |
|---|---|
| Worker unreachable | Its last summary ages out; cluster totals for affected tenants are marked partial and the stale worker is dropped from the contributing set. |
| Coordinator failover | Aggregation state is transient and rebuilt from the next heartbeat round; no durable per-tenant store is kept. |
| Owner missing (Tier 2) | Fall back to the Tier 1 approximation for that key and mark the tenant `approximate`. |
| Summary truncated (tenant below every top-N) | Rolled into `other` with an omitted-tenant count; never silently dropped. |
| Metrics scrape during a reconfigure | `/metrics` renders from the last consistent snapshot; it never blocks on the store. |

## Initial parameters

```text
per-worker top-N tenants:        32--64
summary report cadence:          heartbeat interval (Tier 1)
owner pull cadence:              owner snapshot interval, 2--5 ms (Tier 2)
rate window:                     1--5 s sliding window
management API page limit:       reuse the existing bounded default
allow-listed tenant series:      operator-configured, small
```

## Non-goals

- Billing-grade or exact historical per-tenant accounting. That is a separate
  metering pipeline, not this monitoring view.
- Per-request, per-tenant tracing on the hot path. Hot-path decisions stay as
  counters, consistent with the zero-copy data-plane design.
- Sub-millisecond global accuracy. The view is bounded-stale by construction.
- Enforcement. This document only observes; bounding is defined by the
  rate-limit design.

## Delivery plan

Staged so that visibility ships with, and slightly behind, the corresponding
enforcement stage:

1. With the local-limiter stage: per-`(tenant, metric)` windowed rate meters on
   the worker, the bounded top-N summary, and its heartbeat-cadence carrier.
2. Coordinator aggregation of summaries and the `/api/v1/tenants[/{tenant}]`
   endpoints.
3. Bounded Prometheus aggregates, the allow-list helper, and the dashboard and
   alerts.
4. With the owner stage: the Tier 2 owner-authoritative pull, with the API and
   dashboards preferring exact values when an owner is present.

## References

- [Eventual Global Tenant Rate Limits](eventual-global-tenant-rate-limits.md) —
  the enforcement design and the source of the `(tenant, metric)` signal, the
  consistent-hash owner ring, and the excess envelope.
- Distributed Tenant Cache Quotas (the companion cache-capacity design) — its
  observability section defines the same bounded-label, top-N, and
  management-API drill-down discipline that this document follows for traffic
  metrics.
