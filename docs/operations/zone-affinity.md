# Zone-aware cache reads

Talon supports zone-local placement in Gateway, FUSE and the Rust, C, Python
and Java SDKs. Enable affinity explicitly to keep Worker reads and retries
inside the local zone. For the deployment team's Kubernetes instructions, migration
procedure and acceptance matrix, see the
[Chinese multi-AZ runbook](kubernetes-multi-az.zh-CN.md).

## Configuration and discovery

| Variable | Default | Applies to | Meaning |
|---|---|---|---|
| `TALON_ZONE` | unset | Worker, Gateway, FUSE, SDKs | Explicit zone, matching membership exactly. |
| `TALON_NODE_NAME` | unset | Worker, Gateway, SDKs | Resolve the node's `topology.kubernetes.io/zone` label at startup. Requires downward API injection and `get nodes` RBAC. |
| `TALON_ZONE_AFFINITY` | `false` | Gateway, FUSE, SDKs | Restrict primary placement to retained same-zone members. |

Explicit `TALON_ZONE` takes precedence over Kubernetes discovery. Without it,
Worker, Gateway and Rust/C/Python/Java SDKs resolve the Node zone using
`TALON_NODE_NAME`, `KUBERNETES_SERVICE_HOST/PORT`, and the mounted ServiceAccount
token/CA. Inject `spec.nodeName` through the Downward API and grant that Pod's
ServiceAccount `get nodes`. This works without Kubernetes 1.35 topology-label
propagation. FUSE still requires an explicit zone.

SDK lookup runs once per client construction, with a 2-second connect timeout
and 5-second HTTP request timeout. Rust/C/Python reuse the Worker/Gateway resolver
on a joined helper thread, so synchronous construction also works inside a Tokio
runtime. Java implements the same HTTPS lookup without native dependencies.
Missing prerequisites, denied requests, missing labels and failed lookups leave
the zone unknown; affinity is never disabled by a lookup failure. There is no
per-read lookup or background zone refresh; reconstruct the client after fixing
an unsuccessful initial lookup.

Rust `ClientBuilder::with_zone_options(ZoneOptions)` and Java's `connect`
overload override both environment settings and automatic zone discovery per
client, including an explicitly absent zone. C/Python constructors inherit
Rust's environment configuration without ABI changes. SDKs validate boolean
and zone inputs at construction. Set environment before constructing clients.

Workers resolve their zone before registering it in the persistent membership
registry. Heartbeats publish liveness; they do not mutate a member's zone.
Reusing an ID with a different zone is rejected. Unknown-zone startup remains
possible, but strict readers cannot use that Worker as a known local owner.

## Placement and failure contract

| Affinity | Local zone / membership | READ | LOAD / BatchLoad |
|---|---|---|---|
| off | any | Global placement | Global primary |
| on | known local members | Same-zone candidates only; local failure remains a failure | Same-zone primary only |
| on | unknown zone or no local members | Fail closed | Fail closed |

Placement includes retained offline/conflicting logical members. Availability
changes do not rehash ownership, so local failure cannot silently choose a global
primary. Affinity filters logical membership before ranking; it does not filter
to only healthy endpoints. Retirement/zone edits change topology; recovery of
an existing member restores its endpoint without changing placement.

Rust's shared reader retains its existing candidate retry classification within
the selected zone: transport/protocol and nonterminal worker failures can try
the next local candidate, up to `replicas_k`. Java attempts one local primary.
Both preserve terminal version mismatch, not found, invalid request, origin
failure and rate limiting errors. No zone-affine read can select a nonlocal
candidate, including when the local candidate list is empty or exhausted.

No background cross-zone replication is promised. Each zone warms independently
through normal read-through, and must have capacity for its own working set.
Origin access and any caller-level fallback are separate policies; network paths
and cloud charges depend on the provider and endpoints.

## Two network hops and observability

A Gateway sidecar makes the consumer-to-Gateway hop loopback. The affinity policy
controls Gateway-to-Worker traffic. A centralized Gateway Service introduces a
separate routing policy: Kubernetes topology hints / traffic distribution are
preferences and can route outside the zone. They do not enforce the strict
contract above. Use a sidecar or an explicitly zone-scoped Service and endpoints
when the first hop must also remain local.

Gateway records served Worker reads in
`talon_gateway_zone_reads_total{zone_match="same|cross|unknown"}` and
`talon_gateway_zone_read_bytes_total{zone_match="same|cross|unknown"}`.
FUSE and SDKs currently do not export these Gateway counters.

These metrics do not measure consumer-to-Gateway traffic, failed attempts, or
origin transfers. Validate those paths with endpoint-to-node/zone mapping and
network telemetry. Zero cross-zone bytes without successful requests is not an
acceptance result.

## Upgrade and rollback

Current discovery uses schema 6 `MembershipQuery` / `MembershipList` with
persistent members. There is no negotiation fallback to the historical schema-5
or lease-only membership. Upgrade only within the supported retained-membership
protocol contract; see [rolling upgrades](../how-to/rolling-upgrade.md).

Before enabling zone discovery on an existing Worker, inspect its retained zone.
Migrate zone-less/mismatched records using the runbook's fenced, revision-checked
registry update. Preserve IDs, PVCs, retirement markers and unrelated members.
Then enable affinity on readers.

Turning affinity off restores global placement and permits cross-zone traffic;
it is not a cost-preserving rollback. For strict-zone requirements, keep
affinity enabled and restore local capacity or roll back to a compatible binary.
