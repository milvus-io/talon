# Configuration reference

> **Generated file — do not edit by hand.** Produced from the `ConfigVar` schema tables in the code by `talon-gen-config-docs`; CI fails if it drifts. To change it, edit the schema next to the parser and regenerate.

Every setting is resolved from four layers, highest precedence first: **CLI flag** > **environment variable** > **config file (TOML)** > **default**. A ✓ in the *CLI* column means the setting also has a `--<key>` flag; environment variables always apply. Secrets are read only from the environment and are never written to a config file or logged.

## Coordinator

| Key | Environment variable | Default | CLI | Description |
|-----|----------------------|---------|-----|-------------|
| `listen` | `TALON_COORDINATOR_LISTEN` | `127.0.0.1:7000` | ✓ | Control-plane bind address (workers/clients). |
| `admin_listen` | `TALON_COORDINATOR_ADMIN_LISTEN` | `127.0.0.1:8000` | ✓ | Admin HTTP bind address: metrics, health, API, UI. |
| `admin_advertise` | `TALON_COORDINATOR_ADMIN_ADVERTISE` | `<admin_listen>` | ✓ | Admin address advertised in node status. |
| `cluster_id` | `TALON_COORDINATOR_CLUSTER_ID` | `default` | ✓ | Logical cluster identity. |
| `node_id` | `TALON_COORDINATOR_NODE_ID` | `<listen>` | ✓ | Stable coordinator node identity. |
| `state_backend` | `TALON_COORDINATOR_STATE_BACKEND` | `memory` | ✓ | Shared-state backend: memory | etcd | kubernetes. |
| `ha_enabled` | `TALON_COORDINATOR_HA_ENABLED` | `false` | ✓ | Enable active-active mode; rejects the memory backend. |
| `coordinator_replicas` | `TALON_COORDINATOR_REPLICAS` | `1` | ✓ | Expected coordinator replica count. |
| `heartbeat_interval_ms` | `TALON_COORDINATOR_HEARTBEAT_INTERVAL_MS` | `5000` | ✓ | Node heartbeat interval (ms). |
| `unhealthy_after_ms` | `TALON_COORDINATOR_UNHEALTHY_AFTER_MS` | `15000` | ✓ | Silence before a node is unhealthy (ms); must exceed heartbeat. |
| `lease_ttl_ms` | `TALON_COORDINATOR_LEASE_TTL_MS` | `30000` | ✓ | Node lease TTL (ms); must exceed unhealthy_after. |
| `request_timeout_ms` | `TALON_COORDINATOR_REQUEST_TIMEOUT_MS` | `3000` | ✓ | Deadline for one authoritative backend operation (ms). |
| `(env only)` | `TALON_COORDINATOR_AUTH_TOKEN` 🔒 | — |  | Bearer token (>= 16 chars) enabling API/UI authentication; unset disables auth. |
| `(env only)` | `TALON_COORDINATOR_TRUST_FORWARDED` | `false` |  | Honor X-Forwarded-For for audit attribution behind a trusted proxy. |
| `etcd.endpoints` | `TALON_COORDINATOR_ETCD_ENDPOINTS` | — |  | Comma-separated etcd host:port endpoints. |
| `etcd.username` | `TALON_COORDINATOR_ETCD_USERNAME` | — |  | etcd username (requires password). |
| `etcd.password` | `TALON_COORDINATOR_ETCD_PASSWORD` 🔒 | — |  | etcd password; keep in a Secret, not the config file. |
| `etcd.tls.ca_cert_path` | `TALON_COORDINATOR_ETCD_CA_CERT_PATH` | — |  | PEM CA certificate path; enables TLS. |
| `etcd.tls.client_cert_path` | `TALON_COORDINATOR_ETCD_CLIENT_CERT_PATH` | — |  | PEM client certificate path; mutual TLS. |
| `etcd.tls.client_key_path` | `TALON_COORDINATOR_ETCD_CLIENT_KEY_PATH` | — |  | PEM client key path; mutual TLS. |
| `kubernetes.namespace` | `TALON_COORDINATOR_K8S_NAMESPACE` | `talon` |  | Namespace holding Talon Lease objects. |

## Worker

| Key | Environment variable | Default | CLI | Description |
|-----|----------------------|---------|-----|-------------|
| `listen` | `TALON_WORKER_LISTEN` | `127.0.0.1:7001` | ✓ | Data-plane bind address. |
| `advertise_addr` | `TALON_WORKER_ADVERTISE_ADDR` | `<listen>` | ✓ | Routable address advertised to the coordinator. |
| `admin_listen` | `TALON_WORKER_ADMIN_LISTEN` | `127.0.0.1:8001` | ✓ | Admin HTTP bind address: metrics, health, status. |
| `coordinator` | `TALON_WORKER_COORDINATOR` | `127.0.0.1:7000` | ✓ | Coordinator control-plane address to register with. |
| `cluster_id` | `TALON_WORKER_CLUSTER_ID` | `default` | ✓ | Logical cluster advertised in status. |
| `node_id` | `TALON_WORKER_NODE_ID` | `<listen>` | ✓ | Stable worker node identity. |
| `heartbeat_interval_ms` | `TALON_WORKER_HEARTBEAT_INTERVAL_MS` | `5000` | ✓ | Heartbeat interval (ms). |
| `block_size` | `TALON_WORKER_BLOCK_SIZE` | `268435456` | ✓ | Logical block size (bytes). |
| `cache_dirs` | `TALON_WORKER_CACHE_DIRS` | `/var/cache/talon` |  | Colon-separated cache directories. |
| `capacity_bytes` | `TALON_WORKER_CAPACITY_BYTES` | `68719476736` |  | Worker cache capacity (bytes). |
| `azure_account` | `TALON_WORKER_AZURE_ACCOUNT` | — |  | Azure blob storage account name (required to serve data). |
| `(env only)` | `TALON_WORKER_AZURE_SAS` 🔒 | — |  | Azure SAS token; env-only, never from a config file or logged. |

## FUSE client

| Key | Environment variable | Default | CLI | Description |
|-----|----------------------|---------|-----|-------------|
| `mountpoint` | `TALON_FUSE_MOUNTPOINT` | `/mnt/talon` | ✓ | Directory to mount the Talon filesystem at. |
| `coordinator` | `TALON_FUSE_COORDINATOR` | `127.0.0.1:7000` | ✓ | Coordinator address for placement and membership. |
| `block_size` | `TALON_FUSE_BLOCK_SIZE` | `268435456` | ✓ | Logical block size (bytes); must match the cluster. |
| `placement_ttl_ms` | `TALON_FUSE_PLACEMENT_TTL_MS` | `5000` |  | Placement-cache entry TTL (ms). |
| `readahead_blocks` | `TALON_FUSE_READAHEAD_BLOCKS` | `4` |  | Client-side readahead depth in blocks. |

