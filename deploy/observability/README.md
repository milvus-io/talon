# Talon observability assets

Deployable Prometheus rules and a Grafana dashboard for a Talon cluster. These
files are the source of truth; the `talon-observability` crate embeds and
validates them in CI (`cargo test -p talon-observability`) so they cannot drift
from the metric contract exposed by the worker (#76) and coordinator (#77).

## Files

| File | Purpose |
|------|---------|
| `prometheus/talon.rules.yml` | Recording rules: request rates, error ratios, cache hit rate, capacity saturation, latency quantiles, snapshot freshness. |
| `prometheus/talon.alerts.yml` | Alerts: coordinator quorum loss, scrape-missing vs. stale-data, state-store errors, capacity pressure, backend errors, cache degradation. |
| `grafana/talon-cluster-overview.json` | Cluster / coordinator / worker / storage / backend dashboard, scoped by a `cluster` template variable. |

## Prometheus

Point Prometheus at the rule files and give it a `cluster` external label so a
single Prometheus can scrape several Talon clusters without blending them:

```yaml
global:
  external_labels:
    cluster: prod-us-east
rule_files:
  - /etc/prometheus/talon/talon.rules.yml
  - /etc/prometheus/talon/talon.alerts.yml
scrape_configs:
  - job_name: talon-coordinator
    static_configs: [{ targets: ["coordinator:9090"] }]
  - job_name: talon-worker
    static_configs: [{ targets: ["worker-1:9090", "worker-2:9090"] }]
```

Validate locally with the upstream tool (also run structurally in CI):

```sh
promtool check rules prometheus/talon.rules.yml prometheus/talon.alerts.yml
```

### Missing scrape vs. stale data

Two failure modes are alerted separately on purpose:

- **Scrape missing** (`up == 0`) — Prometheus cannot reach the process.
- **Stale data** (`TalonClusterViewStale`) — the process is scraped fine but its
  freshest cluster snapshot is old (a wedged state-store watch). This reads the
  `talon_coordinator_state_snapshot_age_seconds` gauge, which only exists on a
  successful scrape, so it can never be confused with absence of scrape.

## Grafana

Import `grafana/talon-cluster-overview.json` and select a Prometheus data
source. The dashboard is provisionable via the standard file provider:

```yaml
# /etc/grafana/provisioning/dashboards/talon.yml
apiVersion: 1
providers:
  - name: talon
    type: file
    options:
      path: /var/lib/grafana/dashboards/talon
```

### Panels

- **Cluster**: ready coordinators, healthy workers, cache hit rate, freshest
  cluster-view age.
- **Coordinator**: control request rate and error ratio by instance,
  state-store error rate, live nodes by role and health.
- **Worker & Storage**: P99 request latency, capacity saturation, bytes-served
  rate, backend fetch error ratio.

All panels are aggregated (by cluster/instance/role); there are no per-object or
per-block panels, keeping series cardinality bounded on large clusters.

## Runbook

Alert `runbook_url` annotations link to `docs/operations/runbook.md` (operator
runbook, #90), one anchor per alert.
