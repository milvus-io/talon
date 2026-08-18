# Eventual Global Tenant Rate Limits

## Status

Design proposal. This document describes a high-throughput, eventually
consistent tenant rate-limiting mode. It intentionally does **not** provide a
strict, never-exceed global limit.

## Problem

A Talon cluster may serve requests for many tenants from many workers. A noisy
tenant must not be able to consume all request processing capacity, client
egress bandwidth, or object-store origin bandwidth. The policy must apply to
the aggregate of all workers, rather than independently permitting the full
limit on every worker.

A synchronous, strongly consistent global counter would place a control-plane
round trip on the data path. That conflicts with Talon's low-latency,
zero-copy read path. This proposal instead follows the asynchronous `GLOBAL`
model used by [Gubernator](https://github.com/gubernator-io/gubernator): local
decisions on the hot path, asynchronously aggregated consumption, and
owner-published state snapshots.

## Contract

This mode is a bounded, eventually consistent limit. It is appropriate for
tenant fairness and noisy-neighbour protection, not for a contractual hard
cap on billable origin egress.

For a metric with target rate `R`, configured global burst `B`, `N` active
workers, local burst cap `b`, and propagation delay `d`, the implementation
must expose and size for a conservative excess envelope:

```
excess <= N * b + R * d
```

The exact observed excess depends on batching, membership convergence, and
network loss. This is deliberately a sizing rule and SLO, not a strict safety
proof. Policies that require a never-exceed bound need a synchronous credit or
lease mode instead.

## Rate keys and ownership

Each independently limited resource has a key:

```
RateKey = (tenant_id, metric, policy_generation)
```

Initially, metrics are:

- `read_iops`: client read requests admitted by workers;
- `client_egress_bytes`: bytes sent from a worker to a client;
- `origin_read_bytes`: bytes fetched from an object store on cache misses;
- `write_iops`, `client_ingress_bytes`, and `origin_write_bytes` when the
  relevant write paths are enabled.

Every Talon worker runs a QoS peer service. A consistent-hash ring over the
worker membership selects one logical owner for each `RateKey`. There is no
fixed central limiter: different tenants and metrics distribute across the
peer fleet. The owner is authoritative only for its current in-memory view of
the key.

The membership ring has an epoch. Rate messages include that epoch and the
owner rejects messages for a key it no longer owns. Owner restart and ring
migration are explicitly sources of bounded inaccuracy; this mode does not
persist buckets.

## Policy distribution and owner bootstrap

The coordinator is the authoritative control-plane source for tenant rate
*policy*, distinct from an owner's ephemeral bucket state. It stores or fronts
a durable, versioned policy record such as:

```
TenantRatePolicy {
  tenant_id,
  metric,
  generation,
  target_rate,
  global_burst,
  local_burst_cap,
  enabled,
}
```

The coordinator distributes policy updates to workers by watch/push or by
short-TTL polling. Workers keep a local, versioned policy cache, so request
admission never reads the coordinator. `policy_generation` in `RateKey`
selects an immutable cached policy version; a policy update creates a new
generation rather than changing the meaning of an existing key. A worker must
not admit work for an unknown generation: it queues within the tenant bound or
rejects until it has obtained the policy. This makes a control-plane outage
restrict traffic instead of silently applying a permissive default.

When membership chooses a new owner, that owner resolves the `RateKey` from
its local policy cache or, if necessary, fetches it from the coordinator. Only
after it has the matching policy may it accept reports and publish snapshots.
It initializes a fresh, conservative bucket (exhausted, then refilling at the
configured target rate), rather than synthesizing a full global burst. Retried
usage reports can be applied to that new bucket; duplicate reports after a
restart can only make the limiter more restrictive. The new owner's first
snapshot establishes the new `owner_epoch` and lets reporting workers resume.

The coordinator does not reconstruct `remaining`, unacknowledged local use,
or the old owner's idempotency state. Those are deliberately ephemeral runtime
state. Consequently, this bootstrap minimizes a new burst after failover but
does not turn the mode into a strict global limit; the bounded inaccuracy in
the contract still applies.

## Hot path

Authentication resolves a request to a non-forgeable `TenantContext` before
it reaches the worker. The worker uses that tenant identity for all QoS
decisions.

The hot path never contacts the rate owner.

1. A request consumes one unit from the local `read_iops` view. If no unit is
   available, it waits in the bounded tenant queue or receives a rate-limit
   response when its deadline expires.
2. A response is split into bounded transfer chunks. Each chunk consumes bytes
   from the local `client_egress_bytes` view before it is submitted.
3. The chunk is still served through `sendfile`; splitting the transfer only
   gives the scheduler points at which to switch tenants.
4. A cache miss additionally consumes local `origin_read_bytes` capacity and
   a bounded origin-loader concurrency slot before it is started.

Each worker uses sharded or atomic local state so that the io_uring data-plane
rings do not serialize on a global QoS lock. A failed or short `sendfile`
returns the unused part of the locally charged byte amount.

## Asynchronous usage reporting

Each worker accumulates use per active `RateKey`. It reports aggregated deltas
to the key owner every `sync_interval`, or earlier after a byte/request
threshold is reached:

```
UsageReport {
  rate_key,
  worker_id,
  worker_epoch,
  sequence,
  iops_delta,
  byte_delta,
}
```

Reports are retried until acknowledged. `worker_epoch` and `sequence` make
reports idempotent at the owner. Reporting must be batched by owner and key,
so peer traffic scales with active workers and sync intervals, not with Talon
IOPS.

For egress, a worker pre-charges its local view before submitting a chunk and
reports the actual bytes accepted by `sendfile`. A short send refunds local
capacity; the resulting correction is included in a later report.

## Owner and snapshot propagation

The owner applies aggregated deltas to a leaky bucket or GCRA-style meter for
the `RateKey`. It maintains the target rate, burst, remaining capacity, owner
timestamp, and a monotonically increasing version.

The owner pushes `RateSnapshot` updates at the sync interval, on important
threshold crossings (especially exhaustion), and after a batch reaches its
size limit:

```
RateSnapshot {
  rate_key,
  owner_epoch,
  version,
  remaining,
  observed_at,
  ack_sequence_for_receiver,
}
```

Unlike a broadcast to every cluster member, Talon should send snapshots only
to workers that reported this key recently. A worker keeps an active-reporter
lease for a short interval at the owner. This limits fan-out for tenant keys
that are used by only a subset of the fleet.

A worker does not overwrite its local state with `remaining` directly. Its
effective local availability is:

```
snapshot_remaining - locally consumed, not-yet-acknowledged usage
```

This avoids forgetting consumption performed after the snapshot was created.

## Bounding drift

The principal trade-off of this design is that multiple workers can act on
stale state. To keep this controlled:

- configure a per-worker local burst cap;
- divide or cap that burst by the owner-observed active-worker count;
- use short reporting and snapshot intervals (initially 2--5 ms);
- immediately publish exhaustion transitions instead of waiting for the next
  normal batch;
- use a snapshot TTL. A worker with a stale view must stop expanding traffic,
  queue requests, or reject them; it must not continue spending an old,
  positive view indefinitely;
- use a small bootstrap allowance for a previously inactive worker, rather
  than allowing every new worker to spend the whole global burst before it has
  received an owner snapshot.

Chunk size is part of the envelope. A smaller egress chunk lowers the maximum
unreported overshoot but increases scheduling and syscall overhead. This value
must be chosen alongside the permitted excess SLO.

## Failure semantics

| Event | Behaviour |
|---|---|
| Worker cannot reach owner | It can consume only its non-stale local view. At TTL expiry it slows or rejects new work. |
| Usage report is lost | It is retried with the same worker epoch and sequence. |
| Owner fails or ownership moves | Workers use their non-stale view briefly, then fail closed or slow down until the new owner publishes a snapshot. A bounded excess is possible. |
| Owner restarts | In-memory bucket state is lost. This mode accepts the resulting temporary inaccuracy. |
| Policy is reduced | New snapshots apply the smaller rate. The old local views remain valid only until their short TTL. |

## Initial parameters

The first implementation should expose conservative defaults and make the
excess envelope visible in telemetry:

```
sync interval:                 2--5 ms
usage-report threshold:        32 IOPS or 64 KiB
snapshot TTL:                  10--20 ms
tenant queue bound:            1,024 requests
egress scheduling chunk:       256 KiB--1 MiB
```

Telemetry must include local decisions, owner-applied use, report/snapshot
delay, active reporter count, stale-view rejections, and estimated excess.

## Non-goals

- A strict, never-exceed cluster-wide rate cap.
- Persistent quota accounting or billing-grade origin usage.
- Per-tenant physical NVMe IOPS accounting. The initial controls are request,
  client egress, and origin traffic; Linux page cache and `sendfile` make
  exact physical-device attribution a different problem.

## References

- [gubernator-io/gubernator](https://github.com/gubernator-io/gubernator):
  reference implementation of the asynchronous `GLOBAL` rate-limiting model
  that informed this proposal.
