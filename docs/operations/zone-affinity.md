# Zone-aware cache reads

Talon supports zone-local placement in Gateway, FUSE and the Rust, C, Python
and Java SDKs. Enable affinity explicitly; **cross-zone READ retries default
to disabled**. For the deployment team's Kubernetes instructions, migration
procedure and acceptance matrix, see the
[Chinese multi-AZ runbook](kubernetes-multi-az.zh-CN.md).

## Configuration and discovery

| Variable | Default | Applies to | Meaning |
|---|---|---|---|
| `TALON_ZONE` | unset | Worker, Gateway, FUSE, SDKs | Explicit zone, matching membership exactly. |
| `TALON_NODE_NAME` | unset | Worker, Gateway | Resolve the node's `topology.kubernetes.io/zone` label at startup. Requires downward API injection and `get nodes` RBAC. |
| `TALON_ZONE_AFFINITY` | `false` | Gateway, FUSE, SDKs | Restrict primary placement to retained same-zone members. |
| `TALON_ZONE_FALLBACK` | `false` | Gateway, FUSE, SDKs | Explicitly permit bounded nonlocal READ attempts. Has no effect with affinity disabled; never changes LOAD ownership. |

Explicit `TALON_ZONE` takes precedence over Kubernetes discovery. FUSE and SDKs
use only the explicit zone; they do not query the Kubernetes API. The Downward
API can inject `spec.nodeName`, but cannot read a Node's zone label into a Pod
environment variable. SDK applications must receive a verified zone through
per-zone scheduling/configuration or an external injector.

Rust `ClientBuilder::with_zone_options(ZoneOptions)` and Java's `connect`
overload can override the environment per client. C/Python constructors inherit
Rust's environment configuration without ABI changes. Both SDKs validate boolean
and zone inputs at construction. Set environment before constructing clients.

Workers resolve their zone before registering it in the persistent membership
registry. Heartbeats publish liveness; they do not mutate a member's zone.
Reusing an ID with a different zone is rejected. Unknown-zone startup remains
possible, but strict readers cannot use that Worker as a known local owner.

## Placement and failure contract

| Affinity | Read fallback | Local zone / membership | READ | LOAD / BatchLoad |
|---|---|---|---|---|
| off | either | any | Global placement | Global primary |
| on | off (default) | known local members | Same-zone candidates only; local failure remains a failure | Same-zone primary only |
| on | off (default) | unknown zone or no local members | Fail closed | Fail closed |
| on | on (explicit opt-in) | known local members | Local candidates first, then bounded nonlocal candidates on eligible failures | Same-zone primary only |
| on | on (explicit opt-in) | unknown zone or no local members | Nonlocal/full-membership READ candidates | Fail closed |

Placement includes retained offline/conflicting logical members. Availability
changes do not rehash ownership, so local failure cannot silently choose a global
primary. Affinity filters logical membership before ranking; it does not filter
to only healthy endpoints. Retirement/zone edits change topology; recovery of
an existing member restores its endpoint without changing placement.

Rust's shared reader uses its existing replica retry classification (transport
and protocol failures, and nonterminal worker failures). Java's optional remote
attempt is narrower: only `Unavailable`/`Timeout`. Both stop on version mismatch,
not found, invalid request, origin failure and rate limiting. Rust bounds each
stage by `replicas_k`; Java attempts at most one owner per stage. With affinity enabled, neither retries
across zones unless fallback is explicitly enabled.

No background cross-zone replication is promised. Each zone warms independently
through normal read-through, and must have capacity for its own working set.
A remote fallback target need not contain the block. Ownership GC can reclaim
non-owner cache copies, so fallback is not a durable replica guarantee. Origin
access and any caller-level fallback are separate policies; network paths and
cloud charges depend on the provider and endpoints.

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
`talon_gateway_zone_affinity_fallback_total` records entry into an explicitly
permitted fallback path, including unknown/no-local-zone fallback. It is not a
success counter. FUSE emits a throttled fallback warning. SDKs currently do not
export these Gateway counters.

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
Then enable affinity on readers with fallback explicitly false.

Turning affinity off restores global placement and permits cross-zone traffic;
it is not a cost-preserving rollback. For strict-zone requirements, keep both
policy flags fixed and restore local capacity or roll back to a compatible binary.
