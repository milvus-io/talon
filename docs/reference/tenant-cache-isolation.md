# Tenant cache isolation

Every request has an effective tenant. Omitted identities, legacy unscoped
requests and the legacy `Unattributed` identity select `default`. An explicitly
named tenant without a configuration entry inherits the worker's global cache
settings and retains its own independent directory and cache state.

## Worker configuration

Add optional tables to the worker TOML configuration:

```toml
# Global defaults for every tenant.
block_size = 268435456
l2_page_size_bytes = 1048576
l1_page_size_bytes = 65536
page_tti_hours = 24

[tenants.analytics]
block_size = 67108864
l2_page_size_bytes = 262144
l1_page_size_bytes = 32768
page_tti_hours = 6
page_gc_interval_minutes = 5
page_access_checkpoint_interval_minutes = 10
paged_miss_run_concurrency = 4

[tenants.default]
page_tti_hours = 48
```

All tenant fields are optional and inherit the resolved global value, including
CLI/environment overrides. These seven fields are the supported tenant overrides;
unknown fields and invalid combinations fail configuration validation. A
`[tenants.default]` entry applies only to the default tenant: `analytics` and
unconfigured tenants inherit the global settings, not that entry. Tenant tables
are file-only and apply on worker restart. All workers serving a tenant must use
the same block geometry. Configure SDK block sizes to match the tenant's block
size, including for LOAD and cache admission; configuration is not automatically
discovered by clients.

TTI and page GC apply to paged L2 storage. A zero TTI disables idle expiration;
a zero L2 page size selects whole-block storage. Checkpoint and GC intervals
are scheduled separately for each tenant.

## Files and state

```text
cache_root/
  default/shared/<hash-prefix>/<hash>.blk
  analytics/shared/<hash-prefix>/<hash>.blk
  analytics/shared/paged/<hash-prefix>/<hash>.pages/...
```

Tenant identity does not enter `BlockId` or its hash. Equal object/version/block
geometry produces equal block IDs and placement across tenants. Separate
runtimes own each tenant's disk files, L1 entries, index, in-flight fills,
version cache, access checkpoints and garbage collection. Streamed PUT staging
also uses the tenant's `shared` directory. PUT and DELETE affect the requesting
tenant's local cache. Origin keys and credentials remain shared: tenant selection
does not create a separate object-store namespace or isolate origin mutations.

Disk capacity, L1 capacity, LOAD concurrency and background I/O/deletion budgets
remain process-wide. This feature does not reserve a capacity quota or guarantee
fair eviction for each tenant. Existing per-worker read rate limits remain in
`[rate_limits]`; an override named `default` also covers omitted identities.
Tenant namespace metadata is bounded at 4096 tenants per worker (including
configured and recovered tenants); further names fail without falling back.

Tenant names are 1–255 ASCII characters from letters, digits, `.`, `_`, `-`;
`.` and `..` are rejected. Names are case-sensitive. Tenant directories and
`shared` directories must be real directories, not symlinks.

## Selecting a tenant per request

A reusable client owns connection pools and discovery, not a tenant. Every
operation selects its own tenant, and omission always selects `default`.
Concurrent operations on one client can use different tenants and block sizes;
request options do not mutate that client's defaults. One BatchLoad invocation
has one tenant; submit separate concurrent batches to target different tenants.

The Rust SDK validates the tenant once and borrows its name through metadata,
read and prewarm calls. Request contexts contain only that borrowed identity and
the resolved block size; selecting a tenant does not clone the client, reader,
coordinator or their shared resources. The cache-client `*_with_context` methods
accept the same validated `TenantContext`, including across retries.

Rust uses `RequestOptions` on `read_with_options`, `read_into_with_options`,
`stat_with_options`, `list_with_options`, `load_with_options` and
`batch_load_with_options`:

```rust
let options = talon_rust_client::RequestOptions {
    tenant: Some("analytics"),
    block_size: Some(67108864),
    ..Default::default()
};
let bytes = client.read_with_options(&object, 0, Some(4096), None, &options).await?;
// The same client's ordinary read(...) uses the default tenant.
```

Python accepts keyword arguments on operations, not the constructor:

```python
client = Client("coordinator:7000")
a = client.read("s3://bucket/key", length=4096, tenant="analytics", block_size=67108864)
b = client.read("s3://bucket/key", length=4096, tenant="other")
c = client.read("s3://bucket/key", length=4096)  # default
client.load("s3://bucket/key", version="etag", size=4096,
            tenant="analytics", block_size=67108864)
```

`read`, `load` and `batch_load` accept `tenant` and optional `block_size`;
`stat` and `list` accept `tenant`. If `block_size` is omitted, the operation uses
the client planning default. It must match the selected tenant's worker policy.

Java operations accept immutable `RequestOptions`, including `list`:

```java
RequestOptions options = RequestOptions.INHERIT
    .withTenant("analytics").withBlockSize(67108864);
byte[] bytes = client.read("s3://bucket/key", 0, 4096, options);
```

C keeps the original `talon_client_new` constructor. Pass `tenant` and
`block_size` in the existing request options with the `*_async_with_options`
entrypoints for reads, stat, LOAD and BatchLoad:

```c
talon_request_options options;
talon_request_options_init(&options, sizeof(options));
options.tenant = "analytics";
options.block_size = 67108864;
```

Submission copies the tenant and trace strings before returning. NULL options
or a NULL tenant select `default`. A zero request block size uses the client's
default. The request options structure is extended directly; callers must
rebuild against the current header and library.

The Rust cache-client layer exposes `client.request(&tenant)?` handles on
`BlockReader`, `WorkerClient`, `WriteClient` and `CoordinatorClient`. These
immutable handles share the original client's pools and budgets, and support
reads, metadata, admission, write/delete and prewarm without changing the
original client. For example, `writer.request(&tenant)?.put_object(...)` scopes
one write, and `writer.delete_object(...)` uses `default`.

Gateway uses `TALON_GATEWAY_TENANT_ID` as its service request tenant; FUSE uses
`TALON_FUSE_TENANT_ID` for the mount's requests, including write/delete. Unset
service variables select `default`. Tenant IDs are declared routing identities,
not authentication credentials. Gateway authentication does not dynamically map
each principal to a tenant.

## Upgrade and recovery

Upgrade coordinators and workers before enabling named tenants in new clients.
Named requests use protocol v3 with required tenant metadata for every operation;
older servers reject v3 rather than silently using the default namespace. New
servers still accept v1/v2 unscoped requests and legacy tenant-scoped reads.
Default clients continue using the existing v1/v2 encoding.

Use a new empty cache root when upgrading an existing flat cache layout. Startup
rejects legacy nonempty cache subdirectories; it does not migrate, delete, or
silently reuse their files. Configure the new root and retain the old root until
it can be removed through the operator's normal cache maintenance process.
Worker identity and lock files remain process-wide at the root.

Startup recovers configured and previously created tenants, including names
with no current override. Each tenant persists its block/L2-page geometry;
changing it against existing cache data fails startup. Use a new empty cache
root for geometry changes. Request cancellation does not cancel publication of
an initializing namespace, and shutdown drains those initializers before cache
checkpoints.
