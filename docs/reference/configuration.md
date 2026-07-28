# Configuration reference

> **Generated file — do not edit by hand.** Produced from the `ConfigVar` schema tables in the code by `talon-gen-config-docs`; CI fails if it drifts. To change it, edit the schema next to the parser and regenerate.

Every setting is resolved from four layers, highest precedence first: **CLI flag** > **environment variable** > **config file (TOML)** > **default**. Each setting below lists its environment variable, default, and CLI flag (when it has one); environment variables always apply. Secrets are read only from the environment and are never written to a config file or logged.

## Coordinator

### `listen`

Control-plane bind address (workers/clients).

- **Environment variable:** `TALON_COORDINATOR_LISTEN`
- **Default:** `127.0.0.1:7000`
- **CLI flag:** `--listen`

### `admin_listen`

Admin HTTP bind address: metrics, health, API, UI.

- **Environment variable:** `TALON_COORDINATOR_ADMIN_LISTEN`
- **Default:** `127.0.0.1:8000`
- **CLI flag:** `--admin_listen`

### `admin_advertise`

Admin address advertised in node status.

- **Environment variable:** `TALON_COORDINATOR_ADMIN_ADVERTISE`
- **Default:** `<admin_listen>`
- **CLI flag:** `--admin_advertise`

### `cluster_id`

Logical cluster identity.

- **Environment variable:** `TALON_COORDINATOR_CLUSTER_ID`
- **Default:** `default`
- **CLI flag:** `--cluster_id`

### `node_id`

Stable coordinator node identity.

- **Environment variable:** `TALON_COORDINATOR_NODE_ID`
- **Default:** `<listen>`
- **CLI flag:** `--node_id`

### `state_backend`

Shared-state backend: memory | etcd | kubernetes.

- **Environment variable:** `TALON_COORDINATOR_STATE_BACKEND`
- **Default:** `memory`
- **CLI flag:** `--state_backend`

### `ha_enabled`

Enable active-active mode; rejects the memory backend.

- **Environment variable:** `TALON_COORDINATOR_HA_ENABLED`
- **Default:** `false`
- **CLI flag:** `--ha_enabled`

### `coordinator_replicas`

Expected coordinator replica count.

- **Environment variable:** `TALON_COORDINATOR_REPLICAS`
- **Default:** `1`
- **CLI flag:** `--coordinator_replicas`

### `heartbeat_interval_ms`

Node heartbeat interval (ms).

- **Environment variable:** `TALON_COORDINATOR_HEARTBEAT_INTERVAL_MS`
- **Default:** `5000`
- **CLI flag:** `--heartbeat_interval_ms`

### `unhealthy_after_ms`

Silence before a node is unhealthy (ms); must exceed heartbeat.

- **Environment variable:** `TALON_COORDINATOR_UNHEALTHY_AFTER_MS`
- **Default:** `15000`
- **CLI flag:** `--unhealthy_after_ms`

### `lease_ttl_ms`

Node lease TTL (ms); must exceed unhealthy_after.

- **Environment variable:** `TALON_COORDINATOR_LEASE_TTL_MS`
- **Default:** `30000`
- **CLI flag:** `--lease_ttl_ms`

### `request_timeout_ms`

Deadline for one authoritative backend operation (ms).

- **Environment variable:** `TALON_COORDINATOR_REQUEST_TIMEOUT_MS`
- **Default:** `3000`
- **CLI flag:** `--request_timeout_ms`

### `(env only)` 🔒

Bearer token (>= 16 chars) enabling API/UI authentication; unset disables auth.

- **Environment variable:** `TALON_COORDINATOR_AUTH_TOKEN`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)
- **Secret:** read only from the environment; never written to a config file or logged

### `(env only)`

Honor X-Forwarded-For for audit attribution behind a trusted proxy.

- **Environment variable:** `TALON_COORDINATOR_TRUST_FORWARDED`
- **Default:** `false`
- **CLI flag:** not settable via CLI (config file or environment only)

### `etcd.endpoints`

Comma-separated etcd host:port endpoints.

- **Environment variable:** `TALON_COORDINATOR_ETCD_ENDPOINTS`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)

### `etcd.username`

etcd username (requires password).

- **Environment variable:** `TALON_COORDINATOR_ETCD_USERNAME`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)

### `etcd.password` 🔒

etcd password; keep in a Secret, not the config file.

- **Environment variable:** `TALON_COORDINATOR_ETCD_PASSWORD`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)
- **Secret:** read only from the environment; never written to a config file or logged

### `etcd.tls.ca_cert_path`

PEM CA certificate path; enables TLS.

- **Environment variable:** `TALON_COORDINATOR_ETCD_CA_CERT_PATH`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)

### `etcd.tls.client_cert_path`

PEM client certificate path; mutual TLS.

- **Environment variable:** `TALON_COORDINATOR_ETCD_CLIENT_CERT_PATH`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)

### `etcd.tls.client_key_path`

PEM client key path; mutual TLS.

- **Environment variable:** `TALON_COORDINATOR_ETCD_CLIENT_KEY_PATH`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)

### `kubernetes.namespace`

Namespace holding Talon Lease objects.

- **Environment variable:** `TALON_COORDINATOR_K8S_NAMESPACE`
- **Default:** `talon`
- **CLI flag:** not settable via CLI (config file or environment only)

## Worker

### `listen`

Data-plane bind address.

- **Environment variable:** `TALON_WORKER_LISTEN`
- **Default:** `127.0.0.1:7001`
- **CLI flag:** `--listen`

### `advertise_addr`

Routable address advertised to the coordinator.

- **Environment variable:** `TALON_WORKER_ADVERTISE_ADDR`
- **Default:** `<listen>`
- **CLI flag:** `--advertise_addr`

### `admin_listen`

Admin HTTP bind address: metrics, health, status.

- **Environment variable:** `TALON_WORKER_ADMIN_LISTEN`
- **Default:** `127.0.0.1:8001`
- **CLI flag:** `--admin_listen`

### `coordinator`

Coordinator control-plane address to register with.

- **Environment variable:** `TALON_WORKER_COORDINATOR`
- **Default:** `127.0.0.1:7000`
- **CLI flag:** `--coordinator`

### `cluster_id`

Logical cluster advertised in status.

- **Environment variable:** `TALON_WORKER_CLUSTER_ID`
- **Default:** `default`
- **CLI flag:** `--cluster_id`

### `node_id`

Stable worker node identity.

- **Environment variable:** `TALON_WORKER_NODE_ID`
- **Default:** `<listen>`
- **CLI flag:** `--node_id`

### `heartbeat_interval_ms`

Heartbeat interval (ms).

- **Environment variable:** `TALON_WORKER_HEARTBEAT_INTERVAL_MS`
- **Default:** `5000`
- **CLI flag:** `--heartbeat_interval_ms`

### `block_size`

Logical block size (bytes).

- **Environment variable:** `TALON_WORKER_BLOCK_SIZE`
- **Default:** `268435456`
- **CLI flag:** `--block_size`

### `cache_dirs`

Colon-separated cache directories.

- **Environment variable:** `TALON_WORKER_CACHE_DIRS`
- **Default:** `/var/cache/talon`
- **CLI flag:** not settable via CLI (config file or environment only)

### `capacity_bytes`

Worker cache capacity (bytes).

- **Environment variable:** `TALON_WORKER_CAPACITY_BYTES`
- **Default:** `68719476736`
- **CLI flag:** not settable via CLI (config file or environment only)

### `l1_capacity_bytes`

L1 DRAM cache capacity in bytes; 0 disables L1.

- **Environment variable:** `TALON_WORKER_L1_CAPACITY_BYTES`
- **Default:** `0`
- **CLI flag:** not settable via CLI (config file or environment only)

### `l1_max_entry_bytes`

Largest whole block eligible for the L1 DRAM cache.

- **Environment variable:** `TALON_WORKER_L1_MAX_ENTRY_BYTES`
- **Default:** `4194304`
- **CLI flag:** not settable via CLI (config file or environment only)

### `backend`

Object-store backend: azure (default), s3, or gcs.

- **Environment variable:** `TALON_WORKER_BACKEND`
- **Default:** `azure`
- **CLI flag:** not settable via CLI (config file or environment only)

### `azure_account`

Azure blob storage account name (required to serve data).

- **Environment variable:** `TALON_WORKER_AZURE_ACCOUNT`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)

### `azure_endpoint`

Azure endpoint host override (emulator/proxy); enables path-style addressing.

- **Environment variable:** `TALON_WORKER_AZURE_ENDPOINT`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)

### `backend_delay_ms`

Synthetic base backend latency in ms (test/latency lab).

- **Environment variable:** `TALON_WORKER_BACKEND_DELAY_MS`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)

### `backend_jitter_ms`

Synthetic per-request latency jitter upper bound in ms (test/latency lab).

- **Environment variable:** `TALON_WORKER_BACKEND_JITTER_MS`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)

### `backend_throughput_bytes`

Synthetic backend bandwidth ceiling in bytes/sec (test/latency lab).

- **Environment variable:** `TALON_WORKER_BACKEND_THROUGHPUT_BYTES`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)

### `backend_max_retries`

Retries after the first attempt for a transient backend failure (0 disables).

- **Environment variable:** `TALON_WORKER_BACKEND_MAX_RETRIES`
- **Default:** `3`
- **CLI flag:** not settable via CLI (config file or environment only)

### `backend_retry_base_ms`

Exponential backoff base in ms; the wait is jittered over [0, base * 2^attempt].

- **Environment variable:** `TALON_WORKER_BACKEND_RETRY_BASE_MS`
- **Default:** `100`
- **CLI flag:** not settable via CLI (config file or environment only)

### `backend_retry_max_delay_ms`

Ceiling on a single backoff wait in ms; also clamps an origin Retry-After hint.

- **Environment variable:** `TALON_WORKER_BACKEND_RETRY_MAX_DELAY_MS`
- **Default:** `5000`
- **CLI flag:** not settable via CLI (config file or environment only)

### `backend_timeout_floor_ms`

Fixed part of the per-attempt backend deadline in ms (connect + first byte).

- **Environment variable:** `TALON_WORKER_BACKEND_TIMEOUT_FLOOR_MS`
- **Default:** `5000`
- **CLI flag:** not settable via CLI (config file or environment only)

### `backend_min_throughput_bytes`

Throughput floor in bytes/sec used to extend the deadline by transfer size (0 = flat).

- **Environment variable:** `TALON_WORKER_BACKEND_MIN_THROUGHPUT_BYTES`
- **Default:** `10485760 (10 MiB/s)`
- **CLI flag:** not settable via CLI (config file or environment only)

### `data_plane_rings`

io_uring rings for the data plane; 0 = one per core. Falls back to Tokio if io_uring is unavailable.

- **Environment variable:** `TALON_WORKER_DATA_PLANE_RINGS`
- **Default:** `0 (one ring per core)`
- **CLI flag:** `--data_plane_rings`

### `s3_region`

S3 region (required when backend=s3).

- **Environment variable:** `TALON_WORKER_S3_REGION`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)

### `s3_endpoint`

S3 endpoint host override (MinIO/LocalStack); http:// selects plaintext.

- **Environment variable:** `TALON_WORKER_S3_ENDPOINT`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)

### `s3_access_key_id`

S3 access key id (the secret key is env-only).

- **Environment variable:** `TALON_WORKER_S3_ACCESS_KEY_ID`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)

### `s3_path_style`

S3 path-style addressing (true for most S3-compatible emulators).

- **Environment variable:** `TALON_WORKER_S3_PATH_STYLE`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)

### `gcs_endpoint`

GCS endpoint host override (fake-gcs-server); http:// selects plaintext.

- **Environment variable:** `TALON_WORKER_GCS_ENDPOINT`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)

### `(env only)` 🔒

S3 secret access key; env-only, never from a config file or logged.

- **Environment variable:** `TALON_WORKER_S3_SECRET_ACCESS_KEY`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)
- **Secret:** read only from the environment; never written to a config file or logged

### `(env only)` 🔒

S3 STS session token; env-only, never from a config file or logged.

- **Environment variable:** `TALON_WORKER_S3_SESSION_TOKEN`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)
- **Secret:** read only from the environment; never written to a config file or logged

### `(env only)` 🔒

GCS OAuth2 bearer token; env-only, never from a config file or logged.

- **Environment variable:** `TALON_WORKER_GCS_BEARER_TOKEN`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)
- **Secret:** read only from the environment; never written to a config file or logged

### `(env only)` 🔒

Azure SAS token; env-only, never from a config file or logged.

- **Environment variable:** `TALON_WORKER_AZURE_SAS`
- **Default:** none
- **CLI flag:** not settable via CLI (config file or environment only)
- **Secret:** read only from the environment; never written to a config file or logged

## FUSE client

### `mountpoint`

Directory to mount the Talon filesystem at.

- **Environment variable:** `TALON_FUSE_MOUNTPOINT`
- **Default:** `/mnt/talon`
- **CLI flag:** `--mountpoint`

### `coordinator`

Coordinator address for placement and membership.

- **Environment variable:** `TALON_FUSE_COORDINATOR`
- **Default:** `127.0.0.1:7000`
- **CLI flag:** `--coordinator`

### `block_size`

Logical block size (bytes); must match the cluster.

- **Environment variable:** `TALON_FUSE_BLOCK_SIZE`
- **Default:** `268435456`
- **CLI flag:** `--block_size`

### `placement_ttl_ms`

Placement-cache entry TTL (ms).

- **Environment variable:** `TALON_FUSE_PLACEMENT_TTL_MS`
- **Default:** `5000`
- **CLI flag:** not settable via CLI (config file or environment only)

### `readahead_blocks`

Client-side readahead depth in blocks.

- **Environment variable:** `TALON_FUSE_READAHEAD_BLOCKS`
- **Default:** `4`
- **CLI flag:** not settable via CLI (config file or environment only)

