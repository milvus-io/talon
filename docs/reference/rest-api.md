# REST API reference

> **Generated file — do not edit by hand.** Rendered from `crates/talon-coordinator/src/openapi.json` by `talon-gen-api-docs`; CI fails if it drifts. To change it, edit the OpenAPI spec and regenerate.

Versioned cluster views and revision-checked membership administration. Worker discovery uses independent topology/state tokens; legacy node views retain their backend revision semantics.

**API version:** `v1`

## Endpoints

### `GET /api/v1/backend`

Backend status

**Responses:**

| Status | Body | Description |
|--------|------|-------------|
| `200` | [`BackendStatus`](#backendstatus) | Backend status. |
| `503` | — | (see BackendUnavailable) |

### `GET /api/v1/capabilities`

Cluster capabilities

**Responses:**

| Status | Body | Description |
|--------|------|-------------|
| `200` | [`CapabilitiesView`](#capabilitiesview) | Advertised capabilities. |
| `503` | — | (see BackendUnavailable) |

### `GET /api/v1/cluster`

Cluster summary

**Responses:**

| Status | Body | Description |
|--------|------|-------------|
| `200` | [`ClusterSummary`](#clustersummary) | Cluster summary. |
| `503` | — | (see BackendUnavailable) |

### `GET /api/v1/nodes`

List nodes

**Parameters:**

| Name | In | Required | Type | Description |
|------|----|----------|------|-------------|
| `role` | query | no | string |  |
| `health` | query | no | string |  |
| `offset` | query | no | integer |  |
| `limit` | query | no | integer |  |

**Responses:**

| Status | Body | Description |
|--------|------|-------------|
| `200` | [`NodeList`](#nodelist) | A page of nodes. |
| `503` | — | (see BackendUnavailable) |

### `GET /api/v1/nodes/{node_id}`

Node detail

**Parameters:**

| Name | In | Required | Type | Description |
|------|----|----------|------|-------------|
| `node_id` | path | yes | string |  |

**Responses:**

| Status | Body | Description |
|--------|------|-------------|
| `200` | [`NodeView`](#nodeview) | One node's detail. |
| `404` | [`ApiError`](#apierror) | No such node in the current snapshot. |
| `503` | — | (see BackendUnavailable) |

### `GET /api/v1/openapi.json`

This OpenAPI document

**Responses:**

| Status | Body | Description |
|--------|------|-------------|
| `200` | — | The OpenAPI 3.0 contract. |

### `GET /api/v1/worker-discovery`

Retained members and sole serving process observations

**Responses:**

| Status | Body | Description |
|--------|------|-------------|
| `200` | — | WorkerDiscovery JSON. Freshness is at most 500ms; responses are not a durable serving lease. |
| `503` | — | Registry or instance state unavailable. |

### `GET /api/v1/worker-membership`

Persistent Worker registry and opaque CAS revision

**Responses:**

| Status | Body | Description |
|--------|------|-------------|
| `200` | — | registry_revision (nullable string), topology_token and registry (format_version, members with worker_id, nullable zone, retired). |
| `503` | — | Backend unavailable or invalid registry. |

### `PUT /api/v1/worker-membership`

Import, retire or explicitly reactivate members

**Responses:**

| Status | Body | Description |
|--------|------|-------------|
| `204` | — | Applied. |
| `400` | — | Malformed membership update request. |
| `409` | — | Registry revision changed; reload before constructing another request. |
| `503` | — | Backend unavailable, invalid membership update, or active instance prevents retirement. |

## Schemas

### ApiError

| Field | Type | Required |
|-------|------|----------|
| `error` | string | yes |
| `message` | string | yes |

### BackendStatus

| Field | Type | Required |
|-------|------|----------|
| `backend` | string | yes |
| `meta` | [`ResponseMeta`](#responsemeta) | yes |
| `ready` | boolean | yes |
| `revision` | string | yes |
| `snapshot_age_ms` | integer (int64) | yes |

### CapabilitiesView

| Field | Type | Required |
|-------|------|----------|
| `advertised` | array of string | yes |
| `meta` | [`ResponseMeta`](#responsemeta) | yes |
| `revision` | integer (int64) | yes |
| `store_reachable` | boolean | yes |

### ClusterSummary

| Field | Type | Required |
|-------|------|----------|
| `cluster_id` | string | yes |
| `coordinator_count` | integer | yes |
| `healthy_worker_count` | integer | yes |
| `meta` | [`ResponseMeta`](#responsemeta) | yes |
| `node_count` | integer | yes |
| `total_block_count` | integer (int64) | yes |
| `total_capacity_bytes` | integer (int64) | yes |
| `total_resident_bytes` | integer (int64) | yes |
| `worker_count` | integer | yes |

### NodeList

| Field | Type | Required |
|-------|------|----------|
| `limit` | integer | yes |
| `meta` | [`ResponseMeta`](#responsemeta) | yes |
| `nodes` | array of [`NodeView`](#nodeview) | yes |
| `offset` | integer | yes |
| `total` | integer | yes |

### NodeView

| Field | Type | Required |
|-------|------|----------|
| `address` | string | yes |
| `admin_address` | string | no |
| `block_count` | integer (int64) | yes |
| `build_version` | string | yes |
| `bytes_served_total` | integer (int64) | yes |
| `cache_hits_total` | integer (int64) | yes |
| `cache_misses_total` | integer (int64) | yes |
| `capacity_bytes` | integer (int64) | yes |
| `errors_total` | integer (int64) | yes |
| `health` | string | yes |
| `labels` | object | yes |
| `node_id` | string | yes |
| `ready` | boolean | yes |
| `reported_at_unix_ms` | integer (int64) | yes |
| `requests_total` | integer (int64) | yes |
| `resident_bytes` | integer (int64) | yes |
| `role` | string | yes |
| `started_at_unix_ms` | integer (int64) | yes |

### ResponseMeta

| Field | Type | Required |
|-------|------|----------|
| `api_version` | string | yes |
| `backend` | string | yes |
| `generated_at_unix_ms` | integer (int64) | yes |
| `snapshot_age_ms` | integer (int64) | yes |
| `snapshot_revision` | string | yes |

