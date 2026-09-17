# Talon Java client

Pure-JVM client for [Talon](https://github.com/milvus-io/talon), a distributed
object-store cache. **No native dependency** — a plain jar that drops into any
JVM build, with no per-platform artifact, no `System.loadLibrary`, and no JNI
crash surface.

```java
try (TalonClient client = TalonClient.connect("coordinator-host:7000", 8 << 20, 32)) {
    byte[] data = client.read("az://container/dataset.parquet",
                              "0x8DABCDEF",   // object version (ETag)
                              0, 1 << 20);
}
```

`maxIdlePerAddr` defaults to **8** when omitted and must be positive. It limits idle TCP
connections retained per address, independently in the coordinator and worker
pools; concurrent requests may open more connections. Idle connections expire
after 30 seconds and are discarded on checkout. Failed exchanges close their
connections; a reused connection that disconnects is retried once on a fresh
connection. `close()` closes idle connections and prevents in-flight connections
from returning to the pools.

The existing `TalonClient.connect(coordinator, blockSize)` and
`TalonClient.connect(coordinator)` calls use the default idle limit. The default
block size is 256 MiB and must match the workers' configuration.

URIs use the same namespaces as the FUSE mount — `s3://`, `gcs://`, `az://` —
so a path addresses the same object through either client.

The supplied version is exact: if that source generation is no longer
available, the read fails instead of silently returning replacement bytes.

## How correctness is maintained

The wire protocol is implemented twice: here and in Rust. That duplication is
the cost of a native-free jar, and the failure mode of drift is subtle — a
client that occasionally reads a stale version rather than one that crashes.

So this client is validated against **conformance vectors** generated from the
Rust implementation. A change that alters the wire fails a test rather than
silently breaking a deployment.

```sh
JAVA_HOME=/path/to/jdk scripts/java_client_e2e.sh
```

That runs the vectors and local TCP pool checks, followed by an end-to-end read
against a live cluster.

## Current scope

Read-only. `read` works; `stat` and `list` are implemented but not yet usable,
because no server implements those endpoints
([#318](https://github.com/milvus-io/talon/issues/318)). Until that lands,
`read` takes the object version explicitly, since blocks are keyed by it.

Requires Java 17 or newer.

See the [wire protocol reference](https://milvus-io.github.io/talon/reference/wire-protocol.html)
for the format this implements.
