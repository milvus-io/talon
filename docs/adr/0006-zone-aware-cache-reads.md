# ADR 0006: Zone-Aware Cache Reads

- Status: Accepted; updated for retained membership and strict local reads
- Original date: 2026-08-11
- Tracking issue: #531
- Related: ADR 0001 (membership and placement), ADR 0005 (gateways)

## Context

Global cache placement sends reads to Workers in other availability zones.
Deployments need to bound those transfers independently of availability policy.
Readers and Workers can move with Kubernetes scheduling; a zone assigned to an
entire application instance may not describe each Pod. Cache data is read-through,
so each zone can warm its own working set without cross-zone replication.

The original schema-5 discovery design is superseded by schema 6 persistent
membership. Current clients do not negotiate older membership messages. This
record describes the current implementation; historical protocol text is in Git.

## Decision

### Persistent zone identity

Worker/Gateway startup and default SDK construction use `TALON_ZONE` when
provided, otherwise a Kubernetes Node label lookup using `TALON_NODE_NAME` and
`get nodes` permission. Rust/C/Python reuse the backend resolver; Java uses an
equivalent authenticated HTTPS lookup. Missing metadata yields an unknown zone
without disabling affinity. FUSE requires explicit zone configuration; explicit
Rust/Java `ZoneOptions` bypass environment settings and automatic discovery.

A Worker's logical ID and zone live in `MemberRegistry`, independent of leased
instance health. Registering the same ID with a different zone fails. An operator
must fence the old instance and update the complete registry with revision-checked
compare-and-swap to migrate a zone. Heartbeats cannot perform that migration.
The topology token includes active member identity and zone; retiring a member
removes it from that topology. Availability-only updates do not change placement.

### Strict local placement

Affinity remains opt-in for compatibility with existing global deployments.
With affinity enabled, READ and LOAD rank retained members in the reader's zone.
Unknown zones and absent local membership fail closed. Offline or
conflicting owners remain in placement and return an availability error; they do
not silently remap ownership.

There is no cross-zone retry switch. READ candidates remain in the same-zone
table even when all local owners fail; LOAD and BatchLoad use only its primary.
The [operations reference](../operations/zone-affinity.md) specifies local
candidate retry limits and error classification for each implementation.

There is no cross-zone replica placement or background replication. Zones warm
independently through read-through; each needs adequate cache capacity. Cache
membership and local read-through do not guarantee durable replicas.

### Kubernetes deployment and observability

Spread Coordinators and Workers over the intended zones, with an HA state backend.
The Helm multi-AZ mode validates replicas, constrains nodes to an explicit zone
list and uses hard topology spread with `minDomains`. Worker and Gateway zone
lookup permissions are independent of the Coordinator's chosen state backend.

Gateway sidecars keep the consumer hop on loopback. For centralized Gateways,
Kubernetes topology-aware Service preferences do not guarantee locality. Strict
first-hop locality needs zone-scoped endpoints or sidecars. Worker-read metrics
cover only the Gateway-to-Worker hop; network-level evidence is needed for other
hops and unsuccessful attempts.

## Alternatives and consequences

- Active cross-zone replication is outside this read-through design. It adds
  placement, transfer and repair machinery and is not needed for local warming.
- Automatically choosing a global owner when the zone is unavailable is rejected
  as the default: it silently changes the operator's transfer policy.
- Filtering placement to healthy Workers is rejected: transient failures must
  not rehash retained ownership.
- Cloud instance metadata is not required; Pod access is frequently restricted.
- Explicit per-zone capacity and operational migration are required. Local-zone
  failures can reduce availability even while other zones remain healthy.
- Origin access is a separate network/cost policy; regional origin storage does
  not imply every configured endpoint is free of cross-zone transfer charges.

## Validation and rollout

Regression tests cover local selection, absent/unknown zones, retained offline
and conflicting owners, recovery, and local errors without remote retries. Helm
rendering tests cover both HA backends and rejected configurations. Kubernetes
failure drills remain a deployment acceptance step, not a conclusion from unit
or render tests. Follow the [multi-AZ runbook](../operations/kubernetes-multi-az.zh-CN.md)
for setup, fenced membership migration, failure drills and rollback.
