# talon-client

Python client for [Talon](https://github.com/milvus-io/talon), a distributed
object-store cache.

Reads objects through a Talon cache cluster instead of from the origin, so
repeated reads across a fleet are served from local NVMe.

Wheels are `abi3`, so one artifact per platform covers CPython 3.8 and newer:
Linux x86_64 and aarch64 (manylinux 2.28), and macOS x86_64 and arm64. Windows
is not built — the worker is Linux-only, so a Windows client would be talking to
a cluster it cannot host.

```python
import talon

with talon.Client("coordinator-host:7000", max_idle_per_addr=32) as client:
    info = client.stat("az://container/datasets/train.parquet")
    print(info.size, info.version)

    # A ranged read; spans block boundaries transparently.
    chunk = client.read("az://container/datasets/train.parquet",
                        offset=0, length=1 << 20)

    for entry in client.list("az/container/datasets"):
        print(entry.path, entry.size)
```

URIs use the same namespaces as the FUSE mount — `s3://`, `gcs://`, `az://` —
so a path addresses the same object through either client.

Blocking calls release the GIL, so threaded loaders are limited by the network
rather than serialised on the interpreter.

`max_idle_per_addr` defaults to 8 and must be positive. It controls idle
connections retained per peer address in each coordinator/worker pool, not
the number of concurrent requests. The idle timeout remains 30 seconds.

Passing both `version` and `size` to `read` skips the metadata lookup and pins
the read to that exact source generation; Talon never substitutes newer bytes.
Supplying only one raises `ValueError` because both must describe the same
generation. Omit both to resolve current metadata with `stat`.

**Read-only in this release.** Writes go through the FUSE mount or the Rust
client; `put`/`delete` are tracked separately.

See the [documentation](https://milvus-io.github.io/talon/) for cluster setup
and the [use cases](https://milvus-io.github.io/talon/use-cases/overview.html)
this is built for.
