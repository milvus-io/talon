# Client SDKs

Talon can be reached four ways, and which one fits depends less on language
than on how much control the workload needs.

| | best for | needs |
|---|---|---|
| **FUSE mount** | unmodified applications, POSIX tooling | a privileged mount and `/dev/fuse` |
| **Python client** | training loaders, notebooks, data pipelines | a wheel |
| **Java client** | JVM query engines and data tooling | a jar |
| **C client** | C/C++ engines that need async ranged reads | a header and shared/static library |

## Why a native client rather than the mount

The FUSE mount is the least invasive option — existing code opens paths and
keeps working. It also carries constraints a native client avoids:

- **It needs a privileged mount** and access to `/dev/fuse`, which many
  container platforms restrict.
- **It imposes POSIX semantics** on what is really a ranged-read API. A read
  becomes a file offset, an object becomes an inode, and errors become errnos.
- **It cannot express cache-aware behaviour.** A client that wants to know
  which worker holds a block, or to read many ranges of one object without
  re-resolving its version, has nowhere to say so.

If the application already speaks in terms of objects and byte ranges, a client
is a closer fit.

## Different architectures, deliberately

**Python binds the Rust core.** Block splitting, placement caching, replica
fallback, and connection pooling already exist and are exercised by the FUSE
client; reimplementing them in Python would reimplement their bugs. `abi3`
wheels give one artifact per platform across CPython versions, which the
ecosystem already expects.

**Java is a pure jar.** No JNI, no FFM, no native artifact — it drops into any
JVM build with no per-platform matrix, no `System.loadLibrary` failure mode, and
no JNI crash surface. The cost is that the wire protocol is implemented twice.

**C binds the Rust read path behind a C ABI.** It exposes async `read` and
`stat` with callback dispatch, while the Rust side keeps ownership of placement
caching, replica fallback, and connection pooling. Callers own read buffers so
the binding does not allocate an intermediate result buffer for range bytes.

That duplication is why the [wire protocol reference](../reference/wire-protocol.md)
exists as a specification with **conformance vectors** rather than as prose. The
vectors are generated from the Rust implementation and asserted by the Java
client, so a change that alters the wire fails a test rather than silently
breaking a deployment.

It has already earned that: the Java client's first conformance run failed
because it sent the envelope schema as the newest version it understood, where
Rust sends the minimum version that can represent the message. Nothing would
have crashed — an older coordinator would simply have rejected requests it could
have served, invisibly from both ends.

## Rust client execution

`ClientBuilder::build()` creates the Rust client's fixed Tokio I/O threads.
Rust, C and Python use this same executor; awaiting a Rust operation does not
require a caller Tokio runtime. Each thread owns separate coordinator and worker
connection pools. A connection stays on its owning OS thread for its lifetime;
this does not pin the thread to a CPU core. Inline callback continuations submitted to
the same client stay on that thread too.

Client clones share metadata caches, refresh coordination and the active-read
budget. `max_idle_per_addr` remains an aggregate limit across all I/O threads,
not a separate allowance for each thread. Rust callers can set the thread count
with `with_io_threads(n)`; otherwise `TOKIO_WORKER_THREADS` or the available CPU
count is used.

Rust `read_into` takes ownership of a writable buffer (`AsMut<[u8]> + Send +
'static`) and returns `(bytes_written, buffer)` on success. It receives directly
into that buffer without an intermediate allocation or payload copy. For example:

```rust,ignore
let buffer = vec![0; 4096];
let (written, buffer) = client.read_into(&object, 0, buffer, None).await?;
// Consume &buffer[..written], then reuse the buffer.
```

This replaces the borrowed `&mut [u8]` Rust signature. Passing a `Vec` or boxed
slice moves ownership without copying its contents. On error the buffer is
dropped; unread trailing bytes remain unchanged on success. Dropping a pending
request cancels its I/O, and the task retains the buffer until it stops using it.
Forgetting the future may leak resources but cannot cause a background write to
freed memory. `read` returns its owned result directly.
`read_into_owned_with_callback` and the C read API also receive directly into
storage kept valid until the callback; the C and Python APIs are unchanged.

Keep a client clone alive until callbacks finish. Dropping the last clone stops
the executor and cancels outstanding work. Shutdown waits for the I/O threads
when initiated outside the executor; from a callback it signals shutdown without
joining those threads, avoiding self-deadlock. C/Python do not own extra runtimes.

## Current scope

All native clients are **read-only** in this release.

`list` is implemented in both but not yet usable, because listing needs a
capability the storage backends do not have yet
([#332](https://github.com/milvus-io/talon/issues/332)). Write-through
(`put`/`delete`) is deliberately deferred — its error and version semantics
deserve a design pass rather than being rushed into a first release.

- [Python client](./python.md)
- [Java client](./java.md)
- [C client](./c.md)
