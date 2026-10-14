# Talon explicit-offset splice extension

Source: the published monoio 0.2.4 crate from crates.io, with its original
Apache-2.0, MIT and third-party licenses retained. Only the diff is committed; the generated dependency tree is excluded from
the workspace and Git.

The public 0.2.4 splice API uses offset -1, which advances the shared open-file
position. Talon caches `Arc<OwnedFd>` descriptors and concurrently reads
different immutable ranges without duplicating or seeking them. Monoio's
operation submission API is private, so this extension lives inside that crate.

Local changes:

- src/io/splice.rs and src/driver/op/splice.rs: expose
  splice_file_to_pipe(`Arc<OwnedFd>`, offset, pipe, len). The operation owns the
  file Arc and pipe SharedFd until completion, including when its future is
  dropped. Legacy drivers return Unsupported instead of blocking the reactor.
- src/builder.rs and src/driver/uring/mod.rs: register bounded and unbounded
  io-wq worker limits before runtime startup. Registration failures fail
  construction. Remove the ineffective must_use attribute from the existing
  Default implementation to allow warnings-denied builds on Rust 1.96.
- src/net/unix/pipe.rs: create Linux pipes with O_CLOEXEC and expose AsRawFd
  for a synchronous nonblocking drain after the file operation completes.
- src/driver/op/send.rs and src/net/tcp/stream.rs: expose completion-owned
  Linux send_more so response headers retain MSG_MORE coalescing.

The Worker uses io_uring only for file-to-pipe reads, then drains ready pages to
the socket with nonblocking splice(2) on the same ring thread. Both
SPLICE_F_NONBLOCK and socket O_NONBLOCK are required; EAGAIN waits on the ring's
writable poll. The header send overlaps the first file read, but completes
before any payload is drained. Cached file contents remain immutable throughout
the transfer. This is not a general guarantee that arbitrary splice calls
cannot block.

Worker tests cover shared-FD offsets, concurrent ranges, multi-file transfers,
pipe reuse, short reads, disconnects, cancellation ownership, and progress with
six stalled readers and one io-wq worker per class. No Monoio blocking pool is
attached. The experimental linked-SQE API is not part of this patch.

Remove this local patch when an upstream release exposes equivalent
completion-owned explicit-offset splice, MSG_MORE, and io-wq budget support.
Linux 6.12.100 forces IORING_OP_SPLICE into io-wq: removing the user-space send
pool does not eliminate kernel worker activity.

## Applying the patch

Run `python3 scripts/prepare_monoio.py` before the first Cargo command and
after changing the patch. It downloads the pinned crates.io release, verifies
its SHA-256, and applies `patches/monoio-0.2.4.patch` with `git apply`.
The generated `.patched-deps/monoio` directory is ignored by Git and Docker;
Cargo selects it through the workspace's `[patch.crates-io]` entry.
CI and Docker prepare it automatically. `just prepare` runs the same script.

For an offline build, supply the original release archive with
`python3 scripts/prepare_monoio.py --archive /path/to/monoio-0.2.4.crate`.
An already prepared tree is reused without network access. The script never
modifies Cargo's shared registry sources. Regeneration is needed only when
the pinned archive, patch, or preparation script changes; do not edit generated
sources directly. Edit the patch instead, and do not regenerate dependencies
while Cargo is compiling them.

The original release checksum is
`3bd0f8bcde87b1949f95338b547543fcab187bc7e7a5024247e359a5e828ba6a`.
All upstream license files remain in the generated dependency.

## monoio-http 0.3.12 dependency cleanup

`monoio-http-0.3.12.patch` removes its unused, unconditional `monoio-rustls`
0.3 dependency, which brings in rustls 0.21 and webpki 0.101 with known security
advisories. No monoio-http source references that dependency. Talon's native
transport supplies TLS separately through monoio-rustls 0.4 and rustls 0.23.46
or newer. It also spells out elided lifetimes flagged by Rust 1.96, because
path dependencies are subject to CI's `-D warnings`; these annotations do not
change behavior.

The same preparation script verifies and patches the pinned monoio-http archive
into `.patched-deps/monoio-http`. Offline builds can additionally pass
`--monoio-http-archive /path/to/monoio-http-0.3.12.crate`.
Remove this patch when an upstream release drops or updates the unused dependency.

## Rustls borrowed plaintext delivery

The native origin transport patches rustls 0.23.46 with an opt-in client method,
`process_tls_records_in_place`. The upstream unbuffered API still copies accepted
application plaintext into `received_plaintext`. This extension records its
input-relative range instead, then returns a borrowed view through `ReadTraffic`.
It preserves the existing record authentication and protocol-state handlers;
`take_received_plaintext` is reached only after those checks. Range capture is
scoped to a single protocol processing call. No raw pointer is dereferenced by
the patch, and buffered clients and the original unbuffered method are unchanged.

Talon retains the input allocation until the plaintext view is consumed, and
transfers owned input/output to Monoio through I/O completion, including after
cancellation. It does not enable monoio-rustls `unsafe_io`. The previous
monoio-rustls delimiter extension is no longer needed. Payload copy and alignment
limits are described in `docs/explanation/monoio-origin-io.md`.

The preparation script verifies the rustls release SHA-256
`48e13bd8c0e9365c43cfa5c9e8f9ad49d3c8444926c9aac819e0e4dc503c8fdf`
and applies `patches/rustls-0.23.46.patch` to `.patched-deps/rustls`, preserving
upstream license files. Offline preparation accepts
`--rustls-archive /path/to/rustls-0.23.46.crate` alongside `--archive` for Monoio.
Remove this extension when upstream offers equivalent borrowed plaintext delivery.
`crates/talon-worker/tests/unbuffered_tls.rs` checks pointer containment, six
AEAD suites, key updates, fragmented messages, and ciphertext authentication.
