# Namespace authorization policy

Coordinators and workers load the same operator-owned TOML file through
`namespace_policy_path` or the corresponding environment variable. The file is
read and validated at process startup.

```toml
version = 1

[[workers]]
node_id = "worker-a"
control_address = "worker-a.talon.svc:7002"
grants = [
  "s3/datasets/training",
  "gcs/checkpoints",
]

[[workers]]
node_id = "worker-b"
control_address = "worker-b.talon.svc:7002"
grants = ["az/models"]
```

Each grant is a canonical `backend/bucket[/path-prefix]` namespace. Backends
are `s3`, `gcs`, and `az`. Prefix matching is path-component aware: a grant for
`s3/datasets/training` includes `s3/datasets/training/v1`, but not
`s3/datasets/training-private`.

The `node_id` must match the worker's stable configured identity. A missing
policy, an unknown worker, an empty grant list, or a namespace outside every
grant denies authorization. A configured file that is missing or malformed
prevents the process from starting.

`control_address` is the worker's privileged mTLS listener used by coordinators
to propagate mapping revisions. It is optional for compatibility, but a worker
without it receives no revision updates and therefore cannot safely activate
revision-fenced namespace operations.
