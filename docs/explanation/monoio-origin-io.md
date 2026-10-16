# Experimental Monoio origin and cache I/O

Worker origin HTTP(S) downloads and whole-block/page cache commits use native
Monoio/io_uring by default (`TALON_WORKER_ORIGIN_IO=uring`), with automatic
kTLS selection (`TALON_WORKER_ORIGIN_TLS=auto`). Set
`TALON_WORKER_ORIGIN_IO=tokio` to use the legacy origin/cache path.
Unknown values and io_uring initialization failures stop startup in `uring` mode;
the experiment never silently switches to an epoll or blocking implementation.

```sh
target/release/talon-worker
```

The existing Worker configuration, credentials, namespace policy and cache
directories apply. Cache sidecars retain logical lengths; native payload files
may include zero-filled tail padding. Choose the data-plane
runtime consistently: origin `uring` requires the native Monoio data plane.
`TALON_WORKER_FORCE_TOKIO_DATA_PLANE=1` with origin `uring`, or unavailable
io_uring, is rejected at startup. Non-ring native I/O callers receive an error.

## Automatic kTLS selection

When origin I/O is `uring`, `TALON_WORKER_ORIGIN_TLS=auto` is the default.
Before serving traffic, the worker runs six bounded loopback probes with dummy
keys: TLS 1.2/1.3 × AES-128-GCM/AES-256-GCM/ChaCha20-Poly1305. These use the
existing Tokio startup runtime, without creating a Monoio runtime or thread.
Each probe records TX/RX installation and traffic-key replacement separately.
TLS 1.3 installation also requires `TLS_RX_EXPECT_NO_PAD` support.
TLS 1.3 remains eligible even when replacement is unsupported. The immutable
snapshot is shared across all origin rings. No request performs capability probes. This detects backports without guessing
from `uname`; unsupported/failed probes disable the corresponding combination.

Before the TLS handshake, auto chooses the highest offloadable version and
restricts offered ciphers to those supported by the snapshot and caller config:

| Kernel capability and allowed versions | Preferred handshake/record layer |
| --- | --- |
| TLS 1.3 TX/RX with `TLS_RX_EXPECT_NO_PAD` | TLS 1.3 + kTLS |
| TLS 1.2 only | TLS 1.2 + kTLS |
| None, or policy excludes all offloadable combinations | Normal highest allowed version + rustls |

Rustls still authenticates the peer. Caller trust roots, client authentication,
explicit version/cipher restrictions, and other settings survive filtering.
The narrow `ClientConfig::with_restricted_cipher_suites` patch only removes
suites/versions; it cannot re-enable caller-disabled algorithms.

`TALON_WORKER_ORIGIN_TLS_MIN_VERSION=1.2` is the default global floor; set it to
`1.3` to require TLS 1.3. Specific origins may raise that floor with, for example:

```bash
export TALON_WORKER_ORIGIN_TLS_ENDPOINT_MIN_VERSIONS='{"https://my-bucket.s3.us-east-1.amazonaws.com":"1.3"}'
```

Rules match an exact HTTPS hostname and effective port, not paths or wildcards;
use the actual request host for virtual-hosted buckets. The global floor always
wins over a lower endpoint value. These settings govern native origin downloads,
not reqwest uploads; non-default rules require `ORIGIN_IO=uring`. A bucket policy
using `s3:TlsVersion` can reject HTTP after a successful handshake: configure its
minimum explicitly. HTTP 403 never causes automatic TLS renegotiation/replay.

If the peer explicitly rejects the preferred protocol/cipher negotiation, auto
reconnects once using the original allowed set and rustls. Certificate failures,
malformed records and connection resets do not trigger this retry. If actual
handoff fails after socket state has changed, auto also closes that socket and
reconnects with rustls before any HTTP bytes are sent. Both paths preserve the
minimum version; there is no in-place rollback or certificate-check bypass.
Once application traffic starts, protocol/authentication errors fail the request.
The specific exception is an authenticated KeyUpdate that cannot be installed:
close the socket and replay an incomplete GET/HEAD once on a fresh connection,
using kTLS again with newly negotiated keys. The incomplete body is discarded
before any cache commit. POST/PUT/DELETE are never replayed by this mechanism;
a second KeyUpdate failure returns an error. The existing outer retry policy
can make further bounded attempts. Idle KeyUpdates simply close the connection
when replacement is unavailable. KeyUpdate does not cause a rustls downgrade or
an endpoint blacklist. Newer kernels replace keys on the existing connection.

`TALON_WORKER_ORIGIN_TLS=rustls` skips probes and disables offload; `ktls` requires
it, fails startup if the capability snapshot is empty, and never falls back.
An endpoint whose allowed versions/ciphers cannot offload fails its request in
strict mode. Startup logs show the snapshot; debug logs show actual negotiation
and record-layer selection (`RUST_LOG=talon_backend=debug`). Standalone users of
`MonoioClient` must call `KernelTlsCapabilities::probe` during initialization and
install the result with `with_kernel_tls_capabilities`; an unprobed client uses
rustls in auto mode. Setting `ORIGIN_IO=tokio` uses the separate reqwest path.

In kTLS mode, owned io_uring `recvmsg` operations put application plaintext
straight into the final aligned HTTP body allocation. Owned ancillary storage,
iovecs and descriptors survive cancellation through completion. HTTP framing
uses small peeks followed by exact consumption; peeks can copy a body prefix.
Control records are bounded and processed separately: rustls retains ticket/key
scheduling, while the adapter installs derived keys and handles alerts and
KeyUpdate replies. TCP queue inspection prevents reuse with a partial encrypted
record that kTLS readiness has not exposed yet. A conservative record-sequence
budget closes long-lived connections before cipher confidentiality limits.
TLS 1.2 TX handoff separates Linux's incrementing explicit nonce range from
rustls's prior XOR-derived handshake nonces; it does not restart the old nonce.

This removes the remaining **user-space body copy** from the kTLS payload path,
not every kernel copy. Small receives and HTTP/TLS boundary misalignment can
still require kernel buffering. TLS 1.3 kTLS always enables the kernel's
`TLS_RX_EXPECT_NO_PAD` direct-decryption hint; there is no separate switch.
TLS 1.2 does not use this option. A kernel that cannot enable the hint is
excluded from TLS 1.3 offload by the startup probe; the existing auto/strict
selection and minimum-version policy still apply. Configured origins are
expected to send unpadded application records: padded or control records are
decrypted again into a kernel buffer, adding work rather than breaking TLS
correctness. See the [kernel TLS interface](https://docs.kernel.org/networking/tls.html).

Linux 6.2 has basic kTLS but cannot replace installed traffic keys. On the
validation machine, auto therefore uses TLS 1.3 kTLS and reconnects when a
KeyUpdate arrives. In-place key replacement still requires a newer kernel;
reconnect success must not be reported as in-place key-update support.

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
codecs and a native adapter for rustls 0.23's unbuffered API. TLS verifies certificate chains
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
Each data-plane ring cancels and awaits disposable idle-connection monitors,
and drains accepted mutation tasks, after stopping connections and before
dropping its runtime. No fallback executor is created.

When native origin I/O is enabled, each whole/page cache store probes a temporary
regular file in its directory during initialization, including on-demand
namespace initialization. `statx(STATX_DIOALIGN)` supplies separate memory-address
and write-offset/length alignments. Both are logged and retained by the store;
there is no per-write probe or hardcoded sector size. On older kernels without
this query, initialization searches power-of-two alignments using actual direct
writes to the temporary file, checking both offset/length and buffer addresses.
The search is bounded by the system page/filesystem block size; it verifies that
successful writes leave no resident file pages with `mincore`. A reported lack
of direct-I/O support, unsuccessful probe, or buffered fallback fails initialization
explicitly. This compatibility probe runs only during store initialization.
The probe assumes ordinary files beneath that store share its I/O requirements.

HTTP Range bodies are received into a single allocation whose start satisfies
the target store's memory alignment. Its capacity includes the write-aligned
length plus address-alignment slack. Receive windows expose only the logical
body length; the extra tail is zero-initialized in that same allocation.
Headers are consumed separately from borrowed TLS plaintext, leaving body
bytes for the final receive buffer. Plain HTTP uses nonblocking socket peeks to locate framing and
then consumes exactly that framing; peeks can include a small body prefix.
Known-length chunked Range responses fill successive windows of the same
allocation rather than assembling copied chunks. Non-Range chunked APIs read
into a growable allocation directly; resizing that allocation can still move
previous bytes when the unknown response size exceeds its capacity.

A request-scoped download context carries the target alignment through the
existing backend APIs. The HTTP client explicitly retains it across its native
ring task. After a complete body is received, the context retains its initialized
backing allocation while returning the logical `Bytes` view to the backend for
Range/version validation. Retries release the previous retained response before
receiving another. The context ends with the fetch, returning a payload owner
that keeps both views through page slicing and detached disk commits. No global
pointer registry, extra executor or payload copy is involved.

Native payload writes round their length up to the probed write alignment using
the same backing allocation. Tail padding stays on disk: no truncate or buffered
tail rewrite. Business `Bytes`, sidecars and block metadata retain logical
lengths. L1 page fills use their known logical page
length; range serving uses the requested window. Valid source ranges remain the
caller's responsibility; padding adds no source-EOF validation.

Storage accounting measures each data file's `st_blocks * 512` after sync and
before publication, independently of its logical or padded file length. The
index and LRU charge these allocated bytes; `talon_worker_resident_bytes`,
Worker status/heartbeat snapshots, foreground capacity enforcement and background
high/low watermarks use this same cost. Replacements update the cost, failed
unlinks retain it, and successful removal subtracts it. Startup scans rebuild
allocation costs from the actual data files; sidecars do not persist these costs.
Metric scrapes read in-memory counters without scanning or statting files.

This is allocated space attributed to indexed data files, including direct-I/O
padding and filesystem block rounding. It excludes sidecars, access checkpoints,
directories, staging/orphan files and deleted files still held by open readers.
It is not total filesystem usage. Capacity settings that previously fit many
small logical pages may now fit fewer pages; reserve space outside the configured
cache capacity for these excluded costs. L1 accounting remains payload bytes.

Aligned full pages and the final short page can use direct writes. An interior
slice cannot use following payload bytes as padding. External admissions without
a safe padded owner, or slices with unaligned addresses, retain buffered writes.
Empty files and metadata sidecars also use buffered I/O. Selected direct I/O
failures are reported, never silently retried as buffered writes.
File synchronization and atomic rename
retain the previous commit semantics. Writes use unique staging files with the
existing `.tmp.<pid>.<sequence>` naming convention. Published file descriptors
are invalidated after rename; residency is published by the existing runtime.
Both configured namespaces and namespaces created on demand use this execution
policy. Whole-block sidecar failures retain the existing best-effort behavior;
paged commits require a sidecar before publishing their page.

## Rustls fallback buffer lifetime

The adapter transfers its input allocation into Monoio socket operations. Rustls
is called only after completion. It borrows accepted plaintext from that same
allocation, and the adapter consumes the view before discarding/reusing input.
A cancelled future drops its socket/session, but the kernel operation retains
its input allocation until completion; no `unsafe_io` borrowed kernel buffer is
used. Failed or cancelled connections never return to the pool.

The rustls extension preserves the existing decrypt/authentication and protocol
state handlers. Only the opt-in client's accepted application-data delivery
changes from an internal Vec queue to an input-buffer range. Buffered clients
and the original unbuffered entry point retain their behavior. TLS 1.2/1.3,
session tickets, and key updates continue through rustls's original state
machine. This fallback does not extract keys or use kTLS.

Records are read with a header read followed by a payload read. This avoids
compacting application ciphertext across receives but adds completion overhead;
measure it against the prior adapter before claiming a throughput improvement.
Only fragmented handshake input may be compacted. Input is capped at 256 KiB,
with each ciphertext record capped at 18,432 bytes before rustls validation.

## Scope and limitations

- This changes the I/O engine, **not** the complete-Range buffering contract.
  It is not streaming download-to-disk or end-to-end zero-copy. The rustls adapter
  receives into an owned buffer, authenticates/decrypts in place, and borrows
  plaintext using the opt-in rustls patch. It removes the ciphertext adapter
  copy and internal plaintext-queue copy. One user-space payload copy remains:
  authenticated plaintext goes directly into the final aligned HTTP body
  allocation. This simultaneously removes framing and assembles records/chunks;
  there is no additional body-assembly or alignment copy. TLS record offsets,
  HTTP framing, contiguous `Bytes`, and direct-write alignment prevent simply
  handing arbitrary borrowed record slices to the existing cache interface.
  Handshake processing, request encryption, and small framing/metadata buffers
  still move bytes; this is a payload-path claim, not a process-wide copy count.
  Direct payload writes bypass the data page cache; buffered writes populate it.
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
cargo test -p talon-worker --test unbuffered_tls --locked
cargo test -p talon-uring --tests --locked -- --ignored --nocapture
cargo test -p talon-worker --test uring_origin --locked -- --ignored --nocapture \
  --skip uring_ktls_required --skip uring_ktls_idle --skip uring_ktls_tls13 --skip uring_live_s3
# Also requires kTLS TLS 1.2; no rustls fallback can satisfy these tests:
cargo test -p talon-worker --test uring_origin --locked uring_ktls_required -- --ignored
cargo test -p talon-worker --test uring_origin --locked uring_ktls_idle -- --ignored
# TLS 1.3 without key replacement: native reconnect, partial body discard,
# retry bound, no replay for POST (e.g. validation Linux 6.2):
cargo test -p talon-worker --test uring_origin --locked uring_ktls_tls13_reconnect_without_key_update -- --ignored
# Requires TLS 1.3 kernel key replacement; fails explicitly on older kernels:
cargo test -p talon-worker --test uring_origin --locked uring_ktls_tls13_key_update -- --ignored
```

The rustls regression checks input/decoded-payload pointer containment for six
TLS 1.2/1.3 AEAD suites, fragmented records, post-handshake key updates, and
rejection of tampered ciphertext before delivering plaintext. Native HTTPS
tests cover small TLS records, 64 KiB+ fixed/chunked bodies and repeated body-read
cancellation. Tests also cover alignment and pointer preservation through Bytes
ownership/slices, direct-write page-cache residency, padded file lengths versus
logical lengths,
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

The kTLS tests additionally cover AES-128/256-GCM and ChaCha20-Poly1305,
fixed/chunked aligned downloads, direct/buffered cache commits, truncated bodies,
cancellation, idle close/partial records, tampered ciphertext rejection, and
automatic fallback without bypassing certificate checks. Separate strict-kTLS
tests distinguish TLS 1.3 reconnection on old kernels from in-place key
replacement on newer kernels; rustls fallback cannot satisfy either test.
These checks do not establish a throughput improvement.

The startup-policy tests check deterministic capability matrices, preservation of
caller restrictions, endpoint minimums, pool isolation, TLS 1.3-only server
fallback, and no replay on HTTP 403. Set `TALON_TEST_EXPECT_AUTO_TLS=1.2` or `1.3`
on a known validation host to assert the actual preferred handshake version.
The separately ignored `uring_live_s3_tls_policy` test sends unsigned HEADs to
AWS S3 with default and TLS 1.3 minimum policies; it needs network access but no
credentials. It validates TLS/HTTP transport, not authenticated object caching.

Validated on the documented Linux 6.2 AWS host: 12 native tests passed, including
TLS 1.3 kTLS reconnect on idle/mid-body KeyUpdate, partial-response discard,
retry limits, no POST replay, and cache payload integrity. Unsigned HEADs to
`s3.us-east-1.amazonaws.com` negotiated TLS 1.3/AES-128-GCM with kTLS for both
default and TLS 1.3-minimum policies (HTTP 405 from the service root). In-place
kernel key replacement remains excluded on this host. Local validation passed
185 backend unit tests and 8 native tests without kernel TLS.
