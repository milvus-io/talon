# Experimental Monoio origin and cache I/O

Set `TALON_WORKER_ORIGIN_IO=uring` to use native io_uring for Worker origin
HTTP(S) downloads and whole-block/page cache commits. The default remains
`tokio`. Unknown values and io_uring initialization failures stop startup;
the experiment never silently switches to an epoll or blocking implementation.

```sh
TALON_WORKER_ORIGIN_IO=uring target/release/talon-worker
```

The existing Worker configuration, credentials, namespace policy and cache
directories apply. This does not change the cache format. Choose the data-plane
runtime consistently: origin `uring` requires the native Monoio data plane.
`TALON_WORKER_FORCE_TOKIO_DATA_PLANE=1` with origin `uring`, or unavailable
io_uring, is rejected at startup. Non-ring native I/O callers receive an error.

## Execution and ownership

With the native data plane, each connection's accepting Monoio ring also runs
LOAD scheduling, signing, retries, HTTP/TLS, backend response validation,
whole-block/page commits, cache-index publication and the response write.
Resident cache byte reads and cold descriptor open/stat operations also use native
I/O. Buffered descriptors and the existing file-to-socket splice path are preserved. Owned
mutation tasks use `monoio::spawn` on that ring. Timer waits use its Monoio timer.
Tokio sync primitives (semaphores, locks and completion channels) remain, but
waiting on them does not dispatch request work to a Tokio scheduler.

The existing data-plane affinity policy pins rings to allowed logical CPUs when
possible; affinity failure is logged. This does not confine kernel/device work
to that CPU, or guarantee an exclusive physical core. Origin I/O reuses these
rings; it creates no additional runtime, thread or
cross-thread queue. The existing Send interfaces use ring-local tasks and
completion channels to keep Monoio futures and buffers on their owning thread.

LOAD/BatchLoad retain their existing shared payload-byte budget through retries
and cache publication, plus their existing RPC and scheduling limits. This
engine adds no operation-count admission limit. READ misses retain their
existing policy; they do not acquire the LOAD payload budget.

Network I/O uses `monoio-http` HTTP/1.1
codecs and `monoio-rustls` 0.4 with rustls 0.23. TLS verifies certificate chains
and hostnames against WebPKI trust roots. Idle connections stay on their own
ring, isolated by authority and trust configuration, capped at 64 per ring and
reused for up to 90 seconds. Only complete responses return to the pool.
Connecting (DNS, TCP and TLS) has a 10-second limit; the whole request has a
300-second limit, additionally bounded by the backend retry policy.

Existing backend signing, Range/Content-Range checks, version preconditions,
retry policy, single-flight and LOAD byte reservations still wrap this transport.
Dropping a timed-out HTTP attempt cancels its ring-local future. Accepted disk
commits continue even if their caller is cancelled. Worker mutation tasks retain
the LOAD reservation and finish the disk/index transaction before releasing it.
Each data-plane ring drains detached work after stopping connections and before
dropping its runtime. No fallback executor is created.

HTTP Range bodies are received into a single allocation with a 4 KiB aligned
start. Headers are consumed separately: the Monoio rustls extension consumes
only header lines from its plaintext reader, leaving body bytes for the final
receive buffer. Plain HTTP uses nonblocking socket peeks to locate framing and
then consumes exactly that framing; peeks can include a small body prefix.
Known-length chunked Range responses fill successive windows of the same
allocation rather than assembling copied chunks. Non-Range chunked APIs retain
the growable-body implementation.

After a complete Range is validated, `Bytes::from_owner` and page slices retain
that allocation through native open/write/fsync/rename. Payload files use
`O_DIRECT` only when both their nonzero length and buffer address are multiples
of 4096. Every other file uses buffered I/O for its entire length: no tail
reopen, padded write, truncate, or alignment copy. Whole-block mode decides per
block file; paged mode decides per page file. Ordinary page sizes preserve
alignment across coalesced downloads. Unaligned external cache admissions or
slices retain buffered writes. Metadata sidecars always use buffered I/O.
Filesystems must support 4 KiB direct I/O when selected; failures are reported,
not silently retried as buffered writes. File synchronization and atomic rename
retain the previous commit semantics. Writes use unique staging files with the
existing `.tmp.<pid>.<sequence>` naming convention. Published file descriptors
are invalidated after rename; residency is published by the existing runtime.
Both configured namespaces and namespaces created on demand use this execution
policy. Whole-block sidecar failures retain the existing best-effort behavior;
paged commits require a sidecar before publishing their page.

## Scope and limitations

- This changes the I/O engine, **not** the complete-Range buffering contract.
  It is not streaming download-to-disk or end-to-end zero-copy. Rustls still
  decrypts in userspace and copies plaintext into the application's receive
  buffer. The safe monoio-rustls adapter also copies received ciphertext from
  its owned socket buffer into rustls. This removes the extra body assembly/
  alignment copy, not TLS's copies, and does not establish a lower total copy
  count than reqwest/tokio-rustls.
  Aligned direct writes bypass the data page cache; other writes populate it.
  Reads deliberately retain buffered descriptors and file-to-socket splice,
  so subsequent reads may populate page cache even after a direct write.
- Hostname resolution uses Tokio's resolver; IP literals skip it. Existing
  connections need no DNS lookup. Startup scanning, first-time dynamic namespace
  initialization, maintenance and control RPCs retain their
  existing implementations. Foreground capacity eviction can still reach blocking
  filesystem operations and Tokio timers. MonoioClient does not support streamed
  request bodies or file uploads: `execute_body` and `execute_file` return errors.
- The experimental HTTP transport uses HTTP/1.1 directly. It does not implement
  HTTP/2, proxy environment variables, automatic redirects, or EOF-delimited
  response bodies. Configure the final object-store endpoint. Bodies must have
  Content-Length or chunked framing; ambiguous framing is rejected. HEAD
  responses are not returned to the connection pool. Chunked framing and
  trailers are consumed with bounded framing buffers before connection reuse.
- Request work is distributed across the existing data-plane rings; no measured
  throughput improvement is claimed. Compare CPU, LOAD throughput, memory, tail latency and subsequent cache
  reads before promoting it to a default.

## Validation

The native tests are explicitly ignored by the portable default suite because
io_uring can be denied by container seccomp. Run them explicitly on a supported
Linux kernel with `io_uring_setup`, `io_uring_enter` and `io_uring_register`
permitted. A denied ring initialization fails these tests; it is never counted
as a passing fallback run.

```sh
cargo test -p talon-uring --tests --locked -- --ignored --nocapture
cargo test -p talon-worker --test uring_origin --locked -- --ignored --nocapture
```

Tests cover alignment and pointer preservation through Bytes ownership/slices,
direct-write page-cache residency versus buffered files, exact file lengths,
fixed/chunked HTTP and HTTPS payloads, and ring-local cancellation/draining, concurrent directory creation,
staging collision preservation, failed rename cleanup, HTTP/HTTPS and certificate rejection, connection reuse,
chunked/truncated responses, cancelled HTTP attempts, S3 If-Match Range LOAD,
whole and paged commits, dynamic namespaces, cache hits and reopen recovery.
A wire-level LOAD/BatchLoad/READ test leaves the Worker's Tokio scheduler
undriven and blocks its only blocking worker, with no fallback origin thread.
It verifies HTTPS download, 503 retry/backoff, whole/paged cache publication,
cold/warm descriptors, L1 promotion, cache-only reads, full/partial misses,
reply and ring shutdown using
only the native data-plane rings. The fixture uses a preconfigured namespace,
an IP-literal TLS endpoint and sufficient cache capacity to isolate this path.
A concurrency regression test requires 129 HTTP requests to reach the origin
before any can complete, detecting an accidental 128-operation cap.
They use a local TLS origin and real cache files, not a live cloud account or a
performance benchmark. Ordinary orderly reopen does not establish power-loss
durability.

On the local development machine use the prescribed `wt-build` container and
data-disk mounts. Localhost tests require `NO_PROXY=127.0.0.1,localhost` and
`no_proxy=127.0.0.1,localhost` when the shell has an HTTP proxy configured.
