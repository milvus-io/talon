# Colocated and sidecar deployment

The other pages describe *what* Talon caches. This one is about *where it runs*,
and it is the case that changed how the data plane was built.

A cache is most useful next to the compute it serves — same node as the GPUs,
same pod as the query engine. But a cache that sits next to a training job is
also **competing with it**, for CPU, for memory, and for the memory bandwidth
that job is trying to saturate. The question is not "how fast can the cache go"
but "how little can it take while still being worth having".

## The measured answer

A release build serving 64 concurrent readers taking 64 KiB ranges, with the
worker confined to a fixed number of CPUs:

| CPUs | throughput | per-CPU | p50 | p99 | CPU used | RSS |
|---|---|---|---|---|---|---|
| **1** | **49,356 rps** | 49,356 | 1.28 ms | 2.19 ms | **98%** | **7.1 MB** |
| 2 | 84,542 rps | 42,271 | 0.74 ms | 1.42 ms | 196% | 7.1 MB |
| 4 | 155,834 rps | 38,958 | 0.47 ms | 1.06 ms | 378% | 7.7 MB |

Three properties matter more than the throughput number:

**It takes exactly what it is given.** CPU used tracks the allocation — 98%,
196%, 378% — rather than expanding to fill the machine. One ring per allowed
CPU, each pinned, so the cost is a number you choose rather than one you
discover in production.

**Memory is flat under load.** RSS moves ~0.2 MB between idle and saturation.
Block payloads never enter userspace — `sendfile(2)` moves them
`NVMe → page cache → kernel → socket` — so there is no buffer pool that grows
with traffic. An 8.9 MB binary with a ~7 MB resident set is a rounding error
next to a model.

**Per-CPU throughput barely degrades as CPUs are added** (49k → 42k → 39k),
because the rings share no state on the request path. Scaling the cache up does
not make each unit of it worse.

## Colocating with GPU training

A training job's bottleneck is the accelerator and the data loader feeding it,
not one CPU core. Spending a core on a local cache is a good trade when it
removes re-reading the dataset from object storage on every epoch — the second
epoch onward is served from local NVMe.

What makes this safe is predictability rather than efficiency. A work-stealing
runtime sizes its thread pool to the machine and takes CPU as offered, which on
a shared node means taking it from the job you are supposed to be helping. The
thread-per-core data plane cannot do that: it starts N rings, pins each to one
CPU, and stops there.

```sh
# One ring, one CPU, ~7 MB — the rest of the node belongs to the job
talon-worker --data-plane-rings 1
```

The pinning respects the CPU set the process was actually given, so a worker
launched under a `taskset`, a Kubernetes CPU manager policy, or a NUMA
restriction stays inside it. (This was a real bug: rings previously pinned to
CPU *i* regardless of the allowed set, silently landing on CPUs belonging to
other workloads. Fixed in #300.)

## Running as a Kubernetes sidecar

The same properties make Talon viable as a sidecar rather than a DaemonSet:
small enough to attach to a pod, bounded enough to declare in `resources`.

```yaml
containers:
  - name: talon-cache
    image: ghcr.io/milvus-io/talon-worker:latest
    args:
      - "--coordinator"
      - "talon-coordinator:7000"
      - "--listen"
      - "127.0.0.1:7001"        # pod-local: only this pod's containers reach it
      - "--admin-listen"
      - "127.0.0.1:8001"
    resources:
      requests: { cpu: "1", memory: "128Mi" }
      limits:   { cpu: "1", memory: "256Mi" }
    env:
      - name: TALON_WORKER_CACHE_DIRS
        value: /var/cache/talon
      - name: TALON_WORKER_CAPACITY_BYTES
        value: "8589934592"      # 8 GiB — keep under the volume size
      # Backend credentials come from a Secret; never inline them.
      - name: TALON_WORKER_AZURE_ACCOUNT
        valueFrom:
          secretKeyRef: { name: talon-worker-backend, key: azure-account }
      - name: TALON_WORKER_AZURE_SAS
        valueFrom:
          secretKeyRef: { name: talon-worker-backend, key: azure-sas }
    volumeMounts:
      - { name: cache, mountPath: /var/cache/talon }
```

Note the memory limit covers the process, not the cache: cached blocks live on
the mounted volume, and `TALON_WORKER_CAPACITY_BYTES` bounds that separately.
See [`deploy/kubernetes/worker.yaml`](../../deploy/kubernetes/worker.yaml) for a
complete node-level manifest to adapt.

Ring count defaults to the CPU budget the container actually has, so
`--data-plane-rings` is usually unnecessary — the worker reads **both** limits
that apply. They are not the same number: the affinity mask says *which* CPUs
the process may run on, while a cgroup quota (`cpu.max`) says *how much* CPU
time it may consume. A pod allowed on 16 CPUs but granted 15 CPUs of quota gets
15 rings, not 16, because 16 rings competing for 15 CPUs of quota would throttle
each other and make latency unpredictable.

### Sidecar versus DaemonSet

A sidecar gives per-pod isolation and a cache lifecycle tied to the workload; a
node-level DaemonSet gives one shared cache and a much higher hit rate when pods
read overlapping data. **The hit rate usually decides it.** Sidecars each fetch
their own copy, so N pods reading the same dataset pay N origin fetches — a
node-level worker pays one. Prefer a sidecar when pods read disjoint data or
need hard isolation, and a DaemonSet when they share a working set.

## Other deployments this enables

**Multi-tenant nodes.** Because the CPU cost is declared rather than
discovered, an operator can state "this cache uses two CPUs" as a commitment
that holds — useful where several teams share hardware and the usual answer is
"it probably will not use much".

**Edge and constrained nodes.** A ~7 MB resident set and a single CPU fit on
hardware that cannot host a conventional caching tier, putting a cache in front
of a distant object store where there was previously no option.

**Developer machines.** One ring on a laptop is enough to make an object-store
dataset feel local. Adoption does not start with a cluster.

**Elastic and short-lived workers.** There is no work-stealing scheduler to warm
up: startup is N threads and N rings, with a small resident set. Workers can be
created for a job and discarded after it.

## Practical notes

- **Cache directory sizing matters more than CPU.** The interesting limit for a
  colocated worker is usually local NVMe capacity relative to the working set,
  not cores. Per-worker capacity and hit rate are exported to Prometheus.
- **io_uring is not required.** Where it is unavailable — older kernels,
  restrictive seccomp profiles, some container runtimes — the worker falls back
  to the portable Tokio data plane automatically and logs the fallback. The
  measurements above are the io_uring path; under the same 1-CPU budget the
  Tokio path serves ~26% less. Both work; one costs less per CPU.
- **These numbers are a shape, not a promise.** They come from loopback TCP on
  one host with CPU affinity restricting the worker, not from a cgroup CPU
  quota under a real container runtime, and RSS is measured rather than
  constrained by a memory limit. Treat them as the cost profile to expect, and
  measure your own hardware before sizing to them.
