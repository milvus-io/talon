# Talon explicit-offset splice extension

Source: the published monoio 0.2.4 crate from crates.io, with its original
Apache-2.0, MIT and third-party licenses retained. Files not listed below are
unchanged from that release. This directory is excluded from the workspace.

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
