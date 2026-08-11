# Zone-aware cache reads

Cross-zone traffic is billed in both directions on every major cloud, while
the origin object store is regional and free to read from any zone. Zone
affinity (ADR 0006) keeps steady-state cache reads inside the reader's zone:
each zone's readers compute placement over the same-zone worker subset, and a
block first read in a zone is fetched once from the origin and stays there.

The switch is off by default; enabling it changes read placement only. Blocks
are never replicated across zones by Talon — each zone warms independently
through the ordinary read-through path.

## How a process learns its zone

1. `TALON_ZONE`, when set, wins (worker, gateway, and FUSE client alike).
   The FUSE client uses only this variable — it often runs outside
   Kubernetes, so it never attempts the node-label lookup below.
2. Otherwise workers and gateways ask the Kubernetes API for their node's
   `topology.kubernetes.io/zone` label. This needs two deployment pieces,
   both in the reference manifests: the downward-API env
   `TALON_NODE_NAME` (from `spec.nodeName`) and the `talon-node-zone-reader`
   ClusterRole bound to the pod's ServiceAccount. Cloud instance-metadata
   endpoints are deliberately not used (unreachable from pods under common
   EKS settings).
3. Neither available: the zone is unknown and behavior stays exactly as
   today. Zone resolution never blocks or fails startup.

## Configuration

| Variable | Default | Applies to | Meaning |
|---|---|---|---|
| `TALON_ZONE` | unset | worker, gateway, FUSE | Explicit zone; skips the node-label lookup. |
| `TALON_NODE_NAME` | unset | worker, gateway | Node name for the label lookup; inject via the downward API. |
| `TALON_ZONE_AFFINITY` | `false` | gateway, FUSE | Compute placement over the same-zone worker subset. |

Workers need no affinity flag: they only *report* their zone, inside the
status heartbeat they already send. Coordinators store and serve it; they
have no zone logic and need no zone of their own (the control plane carries
no data bytes, and its replicas should simply be spread across zones for
availability like any HA deployment).

## Behavior contract

- Readers with an unknown zone, and zones with no workers, use the full
  membership: availability wins over transfer cost, and the event is counted.
- A worker that is healthy but does not hold a block fetches it from the
  origin itself (regional, no transfer fee) — that is the ordinary
  read-through path, not a fallback.
- Every served read is classified against the reader's zone and counted as
  `same`, `cross`, or `unknown` — including with affinity off, so the
  cross-zone baseline is measurable before the switch is thrown. Gateway
  metrics: `talon_gateway_zone_reads_total{zone_match}`,
  `talon_gateway_zone_read_bytes_total{zone_match}`, and
  `talon_gateway_zone_affinity_fallback_total`. The FUSE mount has no
  metrics endpoint; it logs a throttled warning while the fallback
  persists.
- Mixed versions are safe in both upgrade orders: a new reader against an
  older coordinator falls back to the zone-less membership query
  automatically (with a cooldown), and older readers ignore zones entirely.

## The consumer-to-gateway hop

Zone affinity covers gateway-to-worker reads. The reference sidecar
templates need nothing more: a sidecar gateway shares the consumer's pod,
so that hop is loopback and same-zone by construction.

For a *centralized* gateway Deployment behind a Service, steering clients
(for example Milvus query nodes) to a same-zone gateway is Kubernetes' job:

- annotate the gateway Service with
  `service.kubernetes.io/topology-mode: Auto`;
- keep at least two gateway replicas in every zone with a
  `topologySpreadConstraints` block (`topologyKey:
  topology.kubernetes.io/zone`, `whenUnsatisfiable: DoNotSchedule`).

Kubernetes silently reverts to cluster-wide routing when a zone's replicas
are missing or unbalanced. The cross-zone byte metrics above are the required
alarm for that regression, not an optional dashboard.

## Rollout

1. Upgrade coordinators, then workers and gateways. Zone reporting starts;
   behavior is unchanged.
2. Confirm zones appear on the membership surface (management UI / status
   API) for every worker.
3. Annotate the gateway Service and verify the spread constraints hold.
4. Set `TALON_ZONE_AFFINITY=true` for gateways (and FUSE mounts where used).
   Watch `zone_match="cross"` bytes drop to zero and the fallback counter
   stay flat.

Rollback at any step is unsetting `TALON_ZONE_AFFINITY`.
