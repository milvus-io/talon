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
caching, replica fallback, and connection pooling. Callers own the final read
buffer. The transport receives directly into that destination. Its handle remains
alive through kernel completion, including cancellation, before the callback runs.

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

## Rust-based TCP transport

Rust, C, Python, FUSE and Gateway clients prefer io_uring on Linux and fall back
to Tokio when ring initialization is unavailable. C and Python submit complete
SDK operations to a client-owned group of Monoio runtimes: even a multi-block read crosses
the queue only once. Python releases the GIL while waiting. C runs callbacks
inline on the operation thread unless the caller supplies a callback executor.
Neither binding starts a Tokio runtime on the normal io_uring path.

Existing Rust `build()`, FUSE and Gateway callers use Tokio and submit one complete
RPC to a shared Monoio runtime. Socket I/O, parsing,
timeouts and file uploads execute natively there. Java and HTTP/S3 retain their
existing transports. Set `TALON_CLIENT_FORCE_TOKIO=1` before creating clients to
force the portable transport, including from C and Python.

Rust can select this transport explicitly:

```rust,ignore
use talon_rust_client::{ClientBuilder, ClientIoBackend};
let client = ClientBuilder::default()
    .with_coordinator("127.0.0.1:9001")
    .with_io_backend(ClientIoBackend::IoUring) // fail if a ring cannot start
    .build()?;
```

For a thread-safe client that owns its runtime, use `build_hosted()` instead of
`build()`. The returned `HostedClient` exposes read/stat/list futures that can be
awaited without a caller Tokio runtime. Clones share its execution group and caches.
`io_backend()` reports the backend selected at construction. Auto initialization
failure starts the portable runtime; strict IoUring returns the error. C and
Python use this entry internally, with their existing public interfaces.

Hosted parallelism defaults to the CPUs available to the process. Set Rust
`with_io_threads(n)`, or set `TALON_CLIENT_IO_THREADS=n` before constructing C/Python
clients. `TOKIO_WORKER_THREADS` remains a compatible fallback override. All values
must be positive; the Rust setting takes precedence over both environment
variables. `HostedClient::io_threads()` reports the selected count. Each native
thread has its own ring; the Tokio fallback uses the same number of worker
threads. `max_idle_per_addr` remains a total per-peer limit for each client pool,
not a limit multiplied by the ring count. Metadata caches are shared across rings.

For an application already using Monoio on Linux, run the complete SDK directly
on that runtime. No Tokio runtime or cross-thread request dispatch is needed:

```rust,ignore
let mut runtime = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
    .enable_timer()
    .build()?;
runtime.block_on(async {
    let client = talon_rust_client::ClientBuilder::default()
        .with_coordinator("coordinator:9001")
        .build_native()?;
    let object = talon_rust_client::parse_uri("s3://bucket/key")?;
    let bytes = client.read(&object, 0, Some(4096), None).await?;
    Ok::<_, talon_rust_client::Error>(bytes)
})?;
```

`NativeClient` is thread-local: create, poll and drop it in its Monoio runtime;
clone it for local concurrent tasks. Use a separate client on each ring to scale.
The native builder has no fallback and rejects `with_io_backend(Tokio)`.
Hostname resolution is offloaded from the ring. Lower-level users can share
`MonoioClient` with native `WorkerClient`, `WriteClient` and `CoordinatorClient`.

For `build()`, the backend choices are `Auto`, `Tokio`, and `IoUring`; explicit
settings override the environment. Connection reuse, typed errors and write
retry guards remain in force. A network failure does not switch backends.
`read()` allocates the final result once; block ranges receive directly into
non-overlapping regions. `read_into()` takes ownership of the destination and
returns the same buffer with a result:

```rust,ignore
let buffer = vec![0; 4096];
let (result, buffer) = client.read_into(&object, offset, buffer, Some(&stat)).await;
let written = result?;
// Use buffer[..written]; buffer's allocation has not changed.
```

This replaces the borrowed `&mut [u8] -> Result<usize, Error>` Rust API with
`B -> (Result<usize, Error>, B)`, where `B: ReadDestination`. `Vec<u8>`, `Box<[u8]>`, boxed arrays and
`BytesMut` are supported. Inline arrays must be boxed before submission to avoid
copying their contents when returning ownership. Custom foreign
buffer owners can implement its unsafe stable-storage contract; C and Python use
this to receive into uninitialized memory without clearing or staging it first.
The same change applies to the lower-level WorkerClient and BlockReader `*_into`
methods. On error, contents can be partly modified. Dropping the future retains
the destination until kernel operations retire, rather than returning ownership.
C's pointer/callback ABI and Python's `bytes` result type are unchanged. Python
receives into its final unpublished bytes allocation, without copying a Rust Vec.

These paths avoid intermediate userspace payload copies; normal TCP still copies
from kernel socket buffers.

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
