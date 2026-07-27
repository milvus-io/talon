# Java client

A pure-JVM client: a plain jar with **no native dependency**. No JNI, no FFM, no
per-platform artifact, no `System.loadLibrary` failure mode.

Requires Java 17 or newer.

## Build

```sh
cd clients/java
mvn package
```

The jar has no runtime dependencies, so it drops into any classpath without
pulling anything else in. There is also no build-tool dependency — the sources
compile with `javac` alone, which is what CI does:

```sh
javac -d classes clients/java/src/main/java/io/milvus/talon/*.java
```

## Reading

```java
import io.milvus.talon.ObjectStat;
import io.milvus.talon.TalonClient;

try (TalonClient client = TalonClient.connect("coordinator-host:7000", 8 << 20)) {
    ObjectStat info = client.stat("az://container/datasets/train.parquet");
    System.out.println(info.size() + " " + info.version());

    byte[] chunk = client.read("az://container/datasets/train.parquet", 0, 1 << 20);
}
```

The second argument to `connect` **must match the workers' configured block
size**. Placement is computed per block, so a mismatch addresses blocks that do
not exist.

URIs use the same namespaces as the FUSE mount — `s3://`, `gcs://`, `az://`.

## Reading many ranges of one object

`read(uri, offset, length)` resolves the version with a `stat` first. When
reading many ranges of the same object, resolve it once and use the overload
that takes it:

```java
ObjectStat info = client.stat(uri);
for (long offset = 0; offset < info.size(); offset += chunkSize) {
    byte[] part = client.read(uri, info.version(), offset, chunkSize);
}
```

## Threads

Instances are safe for concurrent use. Each call opens its own connection, so a
slow read cannot block an unrelated one; the placement cache is shared and
synchronised.

```java
try (ExecutorService pool = Executors.newFixedThreadPool(8)) {
    for (long offset : offsets) {
        pool.submit(() -> client.read(uri, info.version(), offset, chunkSize));
    }
}
```

## Errors

`IOException` for transport and worker failures, with the worker's message
preserved. `ProtocolException` when bytes on the wire are not what the protocol
specifies — usually a version mismatch between client and cluster rather than a
transient fault, so **retrying will not help**. `IllegalArgumentException` for a
malformed URI, raised before any I/O.

## How this client stays correct

The wire protocol is implemented twice: here and in Rust. The failure mode of
drift is subtle — a client that occasionally reads a stale version rather than
one that crashes — so this client is validated against **conformance vectors**
generated from the Rust implementation:

```sh
scripts/java_client_e2e.sh
```

That runs both suites: byte-equality against the vectors, and an end-to-end read
against a live cluster. Both run in CI on every pull request.

See the [wire protocol reference](../reference/wire-protocol.md) for the format
this implements.

## Not yet available

`list` is implemented but returns an error until the backends gain a listing
capability ([#332](https://github.com/milvus-io/talon/issues/332)). Writes go
through the FUSE mount or the Rust client.
