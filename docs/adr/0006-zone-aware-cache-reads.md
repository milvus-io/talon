# ADR 0006: Zone-Aware Cache Reads

- Status: Accepted
- Date: 2026-08-11
- Tracking issue: #531
- Related: extends ADR 0001 (membership and placement version); complements
  ADR 0005 (object-store gateways)

## Context

Cache traffic is large, and on every major cloud a byte that crosses an
availability zone is billed in both directions while a byte served within the
zone is free. Talon has no topology concept anywhere: `NodeInfo` carries only
id, address, and role; block placement is a deterministic Maglev ranking over
the full worker set computed locally by every reader; and the deployment has
no zone-aware hop. With three zones, roughly two thirds of cache reads leave
the reader's zone.

Two deployment facts shape the solution space:

- Consumers move. Milvus query nodes are scheduled per pod across zones and
  are not pinned, so any design that assigns a zone per *instance* (for
  example, the control plane rendering a per-instance gateway address) writes
  configuration that is wrong for some of that instance's pods some of the
  time.
- The cache is read-through. A worker that does not hold a block fetches it
  from the origin object store, which is regional: an in-zone origin fetch
  costs no transfer fee and permanently seeds the zone. Replicating blocks
  across zones inside Talon would add placement and fill machinery to reach
  the same steady state that per-zone read-through reaches on its own.

## Decision

### 1. One zone field, self-discovered, never load-bearing for startup

Each worker and gateway resolves its own zone at startup: the `TALON_ZONE`
environment variable wins; otherwise the process asks the Kubernetes API for
its node's standard `topology.kubernetes.io/zone` label (node name injected
via the downward API as `TALON_NODE_NAME`; requires a `get nodes` RBAC grant;
the label was verified present on EKS, ACK, and AKS). Cloud instance-metadata
endpoints are deliberately not used: they are unreachable from pods in common
EKS configurations (IMDS hop limit). Resolution failure yields "zone unknown"
and the process starts normally with today's behavior.

### 2. Zone reporting rides the existing status heartbeat

Workers put the zone into the `labels` map of the schema-v2
`NodeStatusHeartbeat` (`labels["zone"]`), which already flows into the shared
state store and is therefore visible to every coordinator replica. No wire
format changes on the reporting path.

### 3. Zoned membership is two new schema-v5 messages

`bincode` encodes fields positionally, so `NodeInfo` and `MembershipList`
cannot grow fields compatibly. Following the schema-v2/v4 precedent, schema v5
appends `MembershipQueryV2 {}` and `MembershipListV2 { nodes:
Vec<ZonedNodeInfo> }`, where `ZonedNodeInfo` pairs the unchanged `NodeInfo`
with an optional zone. Coordinators answer from membership joined with the
zone reported per node.

Placement epochs remain computed over the node set only. ADR 0001 §7.1
anticipated folding a future failure-domain field into the canonical
placement version; this ADR deliberately takes the other fork: a zone
arriving late for an unchanged node set (reporting lag during rollout) is a
client-local concern, so churning the cluster-wide version for it would
invalidate every reader's cache to fix one reader's view. Instead the
membership cache hashes the observed `(address, zone)` pairs alongside the
epoch and rebuilds its table when that token moves even though the epoch
does not. A pod that moves zones re-registers as a new node and changes the
epoch as before.

Readers request V2 first and fall back to the V1 query automatically when the
round trip fails (an older coordinator drops the connection on an unknown
schema), remembering the failure for a cooldown period so connection churn
stays bounded. Mixed-version clusters are therefore safe in both upgrade
orders, though coordinator-first remains the documented order.

### 4. Zone affinity is an off-by-default read-side filter

With `TALON_ZONE_AFFINITY=true` and a known local zone, a reader builds its
placement table from the same-zone subset of membership; the hash itself is
unchanged. All readers in one zone therefore agree on owners within that zone,
and each zone warms independently through read-through. When the same-zone
subset is empty (no workers, or none healthy) the reader falls back to the
full membership — availability over cost — and counts the event. Every worker
read is classified `same`/`cross`/`unknown` against the reader's zone and
surfaced through an observer so each binary counts into its own registry.

### 5. The consumer-to-gateway hop is Kubernetes' job

Milvus stays a plain S3 client against one cluster-wide gateway Service.
Same-zone steering for that hop uses Kubernetes Topology Aware Routing
(`service.kubernetes.io/topology-mode: Auto`) plus
`topologySpreadConstraints` keeping at least two gateway replicas per zone.
Kubernetes silently disables the feature when replicas are unbalanced, so the
cross-zone read metrics from section 4 are the required alarm, not an
optional nicety. Reference manifests and the rollout order live in the
operations documentation.

### 6. Rollout and rollback

Upgrade coordinators first, then workers and gateways (zone reporting starts;
behavior unchanged). Verify zones populate on the membership surface, enable
the Service annotation, then set `TALON_ZONE_AFFINITY=true` on readers. Rollback
at any point is turning the flag off.

## Rejected alternatives

- **Zone-spread replication with nearest-replica reads** (the Kafka/HDFS
  shape): requires replication factor ≈ zone count plus active fill to be
  effective, reaching the same per-zone capacity cost as read-through with
  strictly more machinery. Rejected for a read-through cache.
- **Control plane renders per-zone gateway addresses per instance**: broken by
  pod-level consumer mobility (see Context). Rejected.
- **Cloud instance metadata as the zone source**: unreachable from pods on
  default EKS node settings. Rejected in favor of the node-label lookup.
- **Growing `NodeInfo` in place**: incompatible under bincode's positional
  encoding; every mixed-version pairing misparses. Rejected for appended
  messages.

## Consequences

- Steady-state cache reads stop crossing zones; the hot set occupies disk in
  every zone that reads it, and each zone pays one origin fetch per block to
  warm. The trade is favorable whenever a block is read across zones more
  than a handful of times per month.
- Zone capacity follows worker placement: uneven worker distribution across
  zones produces uneven cache capacity. Spread constraints on workers are the
  operational lever.
- A zone with readers but no workers runs permanently on the full-membership
  fallback; the fallback counter makes this visible.
- The conformance vectors gain schema-v5 cases, and `CONTROL_SCHEMA_VERSION`
  moves to 5.
