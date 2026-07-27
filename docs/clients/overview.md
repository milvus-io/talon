# Client SDKs

Talon can be reached three ways, and which one fits depends less on language
than on how much control the workload needs.

| | best for | needs |
|---|---|---|
| **FUSE mount** | unmodified applications, POSIX tooling | a privileged mount and `/dev/fuse` |
| **Python client** | training loaders, notebooks, data pipelines | a wheel |
| **Java client** | JVM query engines and data tooling | a jar |

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

## Two different architectures, deliberately

**Python binds the Rust core.** Block splitting, placement caching, replica
fallback, and connection pooling already exist and are exercised by the FUSE
client; reimplementing them in Python would reimplement their bugs. `abi3`
wheels give one artifact per platform across CPython versions, which the
ecosystem already expects.

**Java is a pure jar.** No JNI, no FFM, no native artifact — it drops into any
JVM build with no per-platform matrix, no `System.loadLibrary` failure mode, and
no JNI crash surface. The cost is that the wire protocol is implemented twice.

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

## Current scope

Both clients are **read-only**: `read` and `stat`.

`list` is implemented in both but not yet usable, because listing needs a
capability the storage backends do not have yet
([#332](https://github.com/milvus-io/talon/issues/332)). Write-through
(`put`/`delete`) is deliberately deferred — its error and version semantics
deserve a design pass rather than being rushed into a first release.

- [Python client](./python.md)
- [Java client](./java.md)
