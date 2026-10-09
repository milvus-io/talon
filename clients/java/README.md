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
connections. Data reads retry a transport failure on a reused connection once,
using a fresh connection to the same discovered instance, only while the original
discovery remains valid before and after dialing. Worker refusals, protocol errors,
and failures on fresh connections are not retried. Coordinator requests retry a
reused connection that disconnects once. `close()` closes idle connections and prevents in-flight connections
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

Supports `read`, `stat`, `list`, and cache prewarming through `load` / `batchLoad`.
Prewarming does not modify the origin object.

Requires Java 17 or newer.

See the [wire protocol reference](https://milvus-io.github.io/talon/reference/wire-protocol.html)
for the format this implements.

## Rolling upgrades

Schema-6 clusters with persistent membership preserve block owners while Workers restart. Reads select one logical owner and resolve its current process. A stale pooled connection can be retried once within the original discovery lifetime; this does not refresh discovery or switch owners. Availability failures return `TalonException` with `code()` equal to `UNAVAILABLE` or `TIMEOUT`. `fallbackEligible()` is advisory and never accesses origin storage. Version mismatch, rate limits, origin failures and protocol failures remain distinct. Discovery expires after at most 500 ms and refresh failures are coalesced; idle sockets belong to a single process incarnation.

## Prewarm

```java
LoadResult result = client.load("s3://bucket/file", "etag-1", 4096);
List<LoadResult> results = client.batchLoad(List.of(
    new LoadRequest("s3://bucket/a", "etag-a", 4096),
    new LoadRequest("s3://bucket/b", "etag-b", 8192)));
```

`LoadRequest` accepts either a URI or `ObjectId`. Supply the exact source version
and its size; no HEAD is issued. Batch LOAD groups blocks by worker and sends up
to 1024 instructions per frame, splitting earlier for the control-frame byte
limit. Results follow input order, including empty files. Calls block until
completion; concurrent calls on a client share eight LOAD RPC permits. Workers
load blocks concurrently and retry transient origin failures. A failure may
leave completed cache fills; successful completion does not pin residency.

`load(LoadRequest, RequestOptions)` and `batchLoad(List<LoadRequest>, RequestOptions)`
accept explicit tracing options. Expired instance discovery must refresh before
dispatch; offline or conflicting owners produce `TalonException(UNAVAILABLE)`.
Worker rejections and malformed acknowledgements are not retried.
