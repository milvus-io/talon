# Worker configuration

Workers reload a local TOML file every five seconds, independently of Coordinator
heartbeats. In Kubernetes, project a mutable ConfigMap as a directory and set
`TALON_WORKER_CONFIG` to its `worker.toml` file. The Worker reopens the
path on each check, so kubelet's atomic symlink replacement is observed.

There is no Coordinator configuration publication API, configuration state store,
or special heartbeat delivery protocol. Operations update the ConfigMap through
the Kubernetes API and its existing RBAC/concurrency controls. Pods sharing that
ConfigMap eventually receive the same file. Propagation is asynchronous; updating
a ConfigMap does not prove that every Worker has applied it.

The same TOML document supplies all ordinary startup settings and live tuning,
using the existing `WorkerConfigPatch::from_toml` parser and field names.
JSON configuration files are not supported. Credentials are rejected in
this file and must come from Secrets. `node_id` and `advertise_addr` are rejected
in the shared document, including empty strings. The deployment injects each Pod's
IP through `TALON_WORKER_ADVERTISE_ADDR`; the stable Worker identity is read from
its cache PVC's `worker_identity`. Explicit identity imports remain available
through local environment/CLI configuration. Pod identity and Secrets are
injected independently of the shared ConfigMap. Precedence is CLI > environment > file >
compiled defaults. Legacy `--config` TOML remains available when
`TALON_WORKER_CONFIG` is unset; it is not watched.

## Configuration example

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: talon-worker-config
  namespace: talon
data:
  worker.toml: |
    coordinator = "talon-coordinator:7000"
    listen = "0.0.0.0:7001"
    admin_listen = "0.0.0.0:8001"
    cache_dirs = ["/var/cache/talon"]
    capacity_bytes = 8589934592
    paged_miss_run_concurrency = 4
    background_scan_batch_size = 4096
    background_delete_batch_size = 256
    background_io_max_mb_per_sec = 100
    background_delete_max_per_sec = 1000
    async_eviction_high_watermark = 0.8
    async_eviction_low_watermark = 0.6

    [namespaces.analytics]
    page_tti_hours = 24
    rate_limits = { read_iops = { rate = 2000, burst_ratio = 1.0 } }
```

Edit the target namespace in `worker.toml`, preserving the others. Each file is
a complete snapshot: removing an override restores inherited values. An empty TOML file resets
file settings to environment/CLI overrides and compiled defaults; startup-only
changes remain pending until restart. No application-managed revision counters
are required; restoring a previous file is a valid rollback.

| Hot setting | Behavior |
| --- | --- |
| `page_tti_hours` | Idle expiry in hours; zero disables only idle collection |
| `paged_miss_run_concurrency` | Bound newly launched paged-miss runs |
| `background_scan_batch_size`, `background_delete_batch_size` | Candidate batch limits in watermark eviction and stale GC |
| `background_io_max_mb_per_sec`, `background_delete_max_per_sec` | Worker-wide background pacing; zero removes the rate limit |
| `async_eviction_high_watermark`, `async_eviction_low_watermark` | Paired thresholds with `0 < low < high <= 1` |
| `rate_limits` | Namespace `read_iops`/`read_throughput`; `{}` means unlimited, omitted inherits |

Only `page_tti_hours` and `rate_limits` may be hot-updated per namespace.
Existing namespace storage geometry and checkpoint/GC intervals may also be
specified in this document but require restart. Concurrency,
background batch sizes and rate caps, and eviction watermarks are Worker-global
settings supplied at the document root; namespace entries reject these fields.
Root `rate_limits` uses the existing policy shape with `enabled`, `default` and
`overrides`; omitted uses the resolved policy from the remaining layers. Namespace TTI and rate limits
take precedence over inherited settings. The Worker-wide disk semaphore, aggregate
rate caps and capacity watermarks remain shared across all namespaces.
Background capacity eviction must already be enabled at startup.

Existing and future runtimes of a namespace use the same overrides. Changing one
namespace's admission policy preserves unchanged namespaces' consumed credits.
In-flight work and already reserved operations finish under their earlier settings.
Idle deletion candidates recheck TTI after waiting for permits. Access recording,
access-age recovery and checkpoints remain enabled even when TTI is zero, so
turning TTI on later retains prior access history. Positive TTI requires paged L2
or small-object storage and must cover the namespace checkpoint interval.

Listeners, storage geometry/directories, cache capacity, client configuration,
resource-pool sizes and task-registration switches are read from this same file
at startup. Changes are validated and reported as pending restart; they do not
rebuild resources or restart Pods automatically. A missing or invalid file at
startup fails startup. Later read or validation failures preserve the last valid
settings.
Unknown fields, invalid bounds and files over 64 KiB are rejected. Read errors,
missing files and invalid updates keep the previous valid configuration. Sensitive backend credentials belong in Secrets, outside this schema.

## Deployment

The Helm chart creates and mounts a mutable ConfigMap by default:

```yaml
worker:
  dynamicConfig:
    enabled: true
    existingConfigMap: ""
    config:
      namespaces:
        analytics:
          page_tti_hours: 24
```

To use an operator-managed ConfigMap, set
`worker.dynamicConfig.existingConfigMap`; it must contain `worker.toml`. To turn
file reloading off, set `worker.dynamicConfig.enabled=false`. The raw
`deploy/kubernetes/worker.yaml` includes the same ConfigMap and directory mount.
The ConfigMap payload is not part of the Pod template: updating only its contents
through Helm or Kubernetes does not trigger a Pod restart. If Helm owns the
ConfigMap, retain operational changes in Helm values to avoid overwriting them
on a later upgrade. An external ConfigMap supports separate GitOps ownership.

Do not use `subPath`, environment-variable values, or immutable ConfigMaps for the
dynamic payload. Kubelet refreshes projected files asynchronously, followed by up
to five seconds until the next Worker check. See the [Kubernetes ConfigMap
contract](https://kubernetes.io/docs/concepts/configuration/configmap/).

## Application status

The existing Worker admin port exposes read-only endpoints:

- `GET /api/v1/config`: full ordinary `effective_config`, resolved `desired_config`,
  `restart_required`, `pending_restart_fields`, `observed_hash`, `applied_hash`,
  `last_error`, and the applied hot subset `config`. Credentials are omitted.
- `GET /api/v1/namespaces/analytics/config`: effective namespace TTI/read limits.

`observed_hash` is SHA-256 of the last accepted file bytes. `applied_hash` matches
it only when no restart is required; it is null while startup-only changes remain
pending. Invalid updates preserve the prior accepted views/hashes and set
`last_error`. Compare every expected Worker's hash with the desired file, verify
`restart_required` is false and `last_error` is null. The ordinary heartbeat
includes `config_hash`, `config_restart_required` when applicable, and bounded
`config_error` on failure. Read-only admin endpoints use the existing admin
network policy and have no configuration mutation API.

The namespace view reports effective TTI and read rate limits. Worker-global
settings are visible in the applied document returned by `/api/v1/config`.
