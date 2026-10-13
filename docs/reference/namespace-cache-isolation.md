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
block_size = 67108864
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
CLI/environment overrides. These seven fields are the supported namespace overrides;
unknown fields and invalid combinations fail configuration validation. A
`[namespaces.default]` entry applies only to the default namespace: `analytics` and
unconfigured namespaces inherit the global settings, not that entry. Namespace tables
are file-only and apply on worker restart. All workers serving a namespace must use
the same block geometry. Configure SDK block sizes to match the namespace's block
size, including for LOAD and cache admission; configuration is not automatically
discovered by clients.

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

## Selecting a namespace per request

A reusable client owns connection pools and discovery, not a namespace. Every
operation selects its own namespace, and omission always selects `default`.
Concurrent operations on one client can use different namespaces and block sizes;
request options do not mutate that client's defaults. One BatchLoad invocation
has one namespace; submit separate concurrent batches to target different namespaces.

The Rust SDK validates the namespace once and borrows its name through metadata,
read and prewarm calls. Request contexts contain only that borrowed identity and
the resolved block size; selecting a namespace does not clone the client, reader,
coordinator or their shared resources. The cache-client `*_with_context` methods
accept the same validated `NamespaceContext`, including across retries.

Rust uses `RequestOptions` on `read_with_options`, `read_into_with_options`,
`stat_with_options`, `list_with_options`, `load_with_options` and
`batch_load_with_options`:

```rust
let options = talon_rust_client::RequestOptions {
    namespace: Some("analytics"),
    block_size: Some(67108864),
    ..Default::default()
};
let bytes = client.read_with_options(&object, 0, Some(4096), None, &options).await?;
// The same client's ordinary read(...) uses the default namespace.
```

Python accepts keyword arguments on operations, not the constructor:

```python
client = Client("coordinator:7000")
a = client.read("s3://bucket/key", length=4096, namespace="analytics", block_size=67108864)
b = client.read("s3://bucket/key", length=4096, namespace="other")
c = client.read("s3://bucket/key", length=4096)  # default
client.load("s3://bucket/key", version="etag", size=4096,
            namespace="analytics", block_size=67108864)
```

`read`, `load` and `batch_load` accept `namespace` and optional `block_size`;
`stat` and `list` accept `namespace`. If `block_size` is omitted, the operation uses
the client planning default. It must match the selected namespace's worker policy.

Java operations accept immutable `RequestOptions`, including `list`:

```java
RequestOptions options = RequestOptions.INHERIT
    .withNamespace("analytics").withBlockSize(67108864);
byte[] bytes = client.read("s3://bucket/key", 0, 4096, options);
```

C keeps the original `talon_client_new` constructor. Pass `namespace_id` and
`block_size` in the existing request options with the `*_async_with_options`
entrypoints for reads, stat, LOAD and BatchLoad:

```c
talon_request_options options;
talon_request_options_init(&options, sizeof(options));
options.namespace_id = "analytics";
options.block_size = 67108864;
```

Submission copies the namespace and trace strings before returning. NULL options
or a NULL namespace_id select `default`. A zero request block size uses the client's
default. The request options structure is extended directly; callers must
rebuild against the current header and library.

The Rust cache-client layer exposes `client.request(&namespace)?` handles on
`BlockReader`, `WorkerClient`, `WriteClient` and `CoordinatorClient`. These
immutable handles share the original client's pools and budgets, and support
reads, metadata, admission, write/delete and prewarm without changing the
original client. For example, `writer.request(&namespace)?.put_object(...)` scopes
one write, and `writer.delete_object(...)` uses `default`.

Gateway uses `TALON_GATEWAY_NAMESPACE_ID` as its service request namespace; FUSE uses
`TALON_FUSE_NAMESPACE_ID` for the mount's requests, including write/delete. Unset
service variables select `default`. Namespace IDs are declared routing identities,
not authentication credentials. Gateway authentication does not dynamically map
each principal to a namespace.

## Naming migration

The cache scope formerly called tenant is now namespace. Update `[tenants.NAME]`
to `[namespaces.NAME]`, Rust/Python request option `tenant` to `namespace`, Java
`withTenant` to `withNamespace`, and C `talon_request_options.tenant` to
`namespace_id` (because `namespace` is a C++ keyword). Gateway and FUSE service
variables are now `TALON_GATEWAY_NAMESPACE_ID` and `TALON_FUSE_NAMESPACE_ID`.
The FUSE configuration key is `namespace_id`; `TALON_FUSE_NAMESPACE_ID` overrides
the file value, and omission selects `default`. These are source/configuration
renames; the former names are not aliases. Update service environment variables
explicitly so an omitted namespace does not select `default` unexpectedly.

Protocol versions, message numbers, namespace encoding, `BlockId` hashing and
cache directory layout are unchanged by this rename. Keep the same namespace
name to reuse an existing isolated cache directory. The source-object policy
configuration `namespace_policy_path` and its environment variables also remain
unchanged; its Rust policy type is now `ObjectNamespacePolicy`.

## Upgrade and recovery

Upgrade coordinators and workers before enabling named namespaces in new clients.
Named requests use protocol v3 with required namespace metadata for every operation;
older servers reject v3 rather than silently using the default namespace. New
servers still accept v1/v2 unscoped requests and legacy namespace-scoped reads.
Default clients continue using the existing v1/v2 encoding.

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
