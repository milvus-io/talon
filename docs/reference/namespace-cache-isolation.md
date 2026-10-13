# Namespace cache isolation

Every request has an effective namespace. Omitted identities, legacy unscoped
requests and the legacy `Unattributed` identity select `default`. An explicitly
named namespace without a configuration entry inherits the worker's global cache
settings and retains its own independent directory and cache state.

A namespace is a caller-selected scope for cache state, cache settings and rate
limits. It can represent a workload, dataset, collection or tenant; Talon does
not require a one-to-one mapping to a business identity. Different namespaces
keep independent cache copies, even when they read the same origin object.

`NamespaceId` is distinct from `ObjectNamespace`, the source-object range
`backend/bucket[/path-prefix]` used by `ObjectNamespacePolicy` for privileged
control-plane authorization. A cache namespace can cover multiple object ranges,
and the same object range can be read through multiple cache namespaces.
Selecting a cache namespace neither changes origin keys nor grants access.

## Worker configuration

Add optional tables to the worker TOML configuration:

```toml
# Global defaults for every namespace.
block_size = 268435456
l2_page_size_bytes = 1048576
l1_page_size_bytes = 65536
page_tti_hours = 24

[namespaces.analytics]
l2_page_size_bytes = 262144
l1_page_size_bytes = 32768
page_tti_hours = 6
page_gc_interval_minutes = 5
page_access_checkpoint_interval_minutes = 10
paged_miss_run_concurrency = 4

[namespaces.default]
page_tti_hours = 48
```

All namespace fields are optional and inherit the resolved global value, including
CLI/environment overrides. These six fields are the supported namespace overrides;
unknown fields and invalid combinations fail configuration validation. A
`[namespaces.default]` entry applies only to the default namespace: `analytics` and
unconfigured namespaces inherit the global settings, not that entry. Namespace tables
are file-only and apply on worker restart. All namespaces use the same fixed logical
block size; it cannot be overridden by a namespace or a request. L1 and L2 page sizes remain namespace settings.

TTI and page GC apply to paged L2 storage. A zero TTI disables idle expiration;
a zero L2 page size selects whole-block storage. Checkpoint and GC intervals
are scheduled separately for each namespace. Ownership GC shares the worker's
membership observation across namespaces, while keeping separate admission
boundaries and scan progress for each namespace. New admissions after a topology
observation are handled by the hourly supplemental scan; all scans use the
process-wide background I/O and deletion budgets.

## Files and state

```text
cache_root/
  default/shared/<hash-prefix>/<hash>.blk
  analytics/shared/<hash-prefix>/<hash>.blk
  analytics/shared/paged/<hash-prefix>/<hash>.pages/...
```

Namespace identity does not enter `BlockId` or its hash. Equal object/version/block
geometry produces equal block IDs and placement across namespaces. Separate
runtimes own each namespace's disk files, L1 entries, index, in-flight fills,
version cache, access checkpoints and garbage collection. Streamed PUT staging
also uses the namespace's `shared` directory. PUT and DELETE affect the requesting
namespace's local cache. Origin keys and credentials remain shared: namespace selection
does not create a separate object-store namespace or isolate origin mutations.

Disk capacity, L1 capacity, LOAD concurrency, the LOAD payload byte budget and
background I/O/deletion budgets remain process-wide. Open-file caches are shared
across namespaces, retaining at most 1024 whole-block and 8192 page descriptors,
excluding active readers.
This feature does not reserve a capacity quota or guarantee
fair eviction for each namespace. Existing per-worker read rate limits remain in
`[rate_limits]`; an override named `default` also covers omitted identities.
Namespace metadata is bounded at 4096 namespaces per worker (including
configured and recovered namespaces); further names fail without falling back.

Page GC, checkpoint and file-cleanup gauges carry a `namespace` label so a healthy
namespace cannot hide another namespace's pending work. TTI and checkpoint-period
gauges report each namespace's resolved policy. Their counters and histograms
remain process-wide. Deploy the updated checkpoint-lag alert with this version;
it matches process uptime against each namespace's checkpoint period. L1 residency
gauges remain process-wide and use incremental accounting on the request path.

Namespace names are 1–255 ASCII characters from letters, digits, `.`, `_`, `-`;
`.` and `..` are rejected. `worker_identity` and `.worker.lock` are reserved for
worker metadata and are rejected by request and configuration validation.
Names are case-sensitive. Namespace directories and
`shared` directories must be real directories, not symlinks.

## Client compatibility

The C, Rust, Python and Java SDK APIs remain unchanged. Requests from these
clients carry no cache namespace, so the worker selects `default` for reads,
metadata, cache admission, PUT, DELETE, LOAD and BatchLoad. Gateway and FUSE
also continue using their existing unscoped requests. Applications do not need
a new request option or a rebuilt C options structure to use the default cache.

The worker also accepts the existing namespace-scoped read messages and protocol
v3 namespace metadata. Explicit names select independent caches; omitted names
and the legacy `Unattributed` identity select the same `default` runtime.
Namespace IDs are declared routing identities, not authentication credentials.

## Naming migration

The cache scope formerly called tenant is now namespace. Rust core and transport
types use `NamespaceId` and namespace-named messages. The existing low-level
`WorkerClient::with_tenant` method is renamed to `with_namespace`; its wire
encoding and client-scoped behavior are unchanged. The high-level SDKs do not
add namespace selection APIs in this change.

Protocol message numbers, legacy namespace encoding and `BlockId` hashing remain
unchanged by the rename. The source-object authorization policy still uses
`namespace_policy_path`; its Rust type is now `ObjectNamespacePolicy`.

## Upgrade and recovery

Upgrade coordinators and workers to enable the isolated cache layout. Existing
clients continue using v1/v2 requests and land in `default`. Protocol v3 requires
namespace metadata and is rejected by older servers; the new worker also accepts
legacy namespace-scoped reads.

Use a new empty cache root when upgrading an existing flat cache layout. Startup
rejects legacy nonempty cache subdirectories; it does not migrate, delete, or
silently reuse their files. Configure the new root and retain the old root until
it can be removed through the operator's normal cache maintenance process.
Worker identity and lock files remain process-wide at the root.

Startup recovers configured and previously created namespaces, including names
with no current override. Each namespace persists its block/L2-page geometry;
changing it against existing cache data fails startup. Use a new empty cache
root for geometry changes. Request cancellation does not cancel publication of
an initializing namespace, and shutdown drains those initializers before cache
checkpoints.
