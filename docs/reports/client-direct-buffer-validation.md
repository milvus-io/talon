# Client direct-destination receive validation

> Historical development snapshot. Later ownership, scheduling and performance
> results are documented in [the final hot-path report](client-hotpath-validation.md).

Date: 2026-09-16. Local candidate: `perf/client-uring`, stacked above
`perf/ring-splice` (#591). This record covers the direct-destination change in
the working tree; earlier throughput measurements used a different implementation.

## Result and API migration

TCP object payloads for `read_into` receive directly into the supplied allocation.
The native first `readv` uses separate header and destination iovecs. Partial body
reads continue at the received offset. Cross-block reads use disjoint destination
regions; there is no intermediate payload Vec or block-assembly copy.

Rust `read_into` and lower-level `*_into` methods now transfer buffer ownership and
return `(Result<usize, Error>, buffer)`. `ReadDestination` requires stable backing
storage even when its owner moves. Vec, boxed slices/arrays and BytesMut are
supported; inline arrays must be boxed first. This is a Rust source API change.
The C pointer/callback ABI is unchanged. Python still returns bytes, receiving
into the final unpublished Python bytes allocation. CPython explicitly permits
filling a newly allocated bytes object created with a NULL source
([bytes C API](https://docs.python.org/3/c-api/bytes.html#c.PyBytes_AsString)).
C/Python destinations may be uninitialized; neither requires a staging buffer or
an initial clear. The Tokio fallback also receives directly into that storage.

SDK `read()` and `BlockReader::read()` allocate their final result once and receive
block regions directly. Lower-level Vec-returning RPCs without a supplied target
retain their existing bounded speculative receive path. Protocol metadata and
bounded error messages still have separate storage. Regular TCP still copies
from kernel socket buffers: this change does not implement hardware zero-copy RX.

## Ownership and cancellation

- An exclusive target can split into disjoint regions, but cannot clone itself.
- Each submitted receive retains an allocation owner through kernel completion.
- A dropped request locks its parent region until all descendant operations retire;
  replica retry cannot overwrite a region still owned by an earlier receive.
- Returning the buffer, including on errors or a failed sibling block, waits for
  all outstanding owners. Dropping that recovery future leaves storage with the
  completion owners. C callbacks run after this barrier.
- Errors may leave partially modified contents. C callers retain the existing
  obligation to keep the client and destination valid until callback completion.

## Evidence

| Validation | Result |
| --- | --- |
| Workspace tests, all features, excluding Python | Passed |
| Native receive and RPC tests | 16 passed |
| Native SDK integration tests | 10 passed |
| Native C entry test | 1 passed |
| Python tests, including native io_uring and explicit Tokio | 8 passed |
| Process-local seccomp denial: cache client, hosted SDK, C direct buffer | Passed |
| Workspace all-target/all-feature Clippy, warnings denied | Passed |
| Workspace no-dependency rustdoc, warnings denied | Passed |
| Formatting and whitespace checks | Passed |

The native receive probe inspects the actual iovec passed to Monoio: its payload
pointer must equal the supplied destination address. Continuation submissions
must point to that same allocation plus the received offset. Tests cover a
fragmented header, a partial body, EINTR, small error replies, malformed lengths,
unaligned destination regions and guard bytes. Returned Vec/Box pointers are
checked to ensure recovery does not move payload bytes.

Lifecycle tests exercise outstanding descendant leases, dropped recovery futures,
retry exclusion, real-ring cancellation and concurrent progress. C receives a
4 KiB range spanning five blocks into `MaybeUninit` storage, observing only the
successful byte count. Python tests verify both the bytes object's identity and
its storage address before and after a nonempty SDK read, then exercise the
public binding with EOF clamping. Both Python backend variants pass.

Local raw logs and a source SHA256 manifest are in
`target/client-direct-buffer-20260916/` in the development worktree. The first broad
`--ignored` invocation also selected an internal child-only test without its
required environment. Final native validation uses the dedicated integration
suite and the separate seccomp wrapper; those runs pass.

## Reproduction

Build/cache/temp paths are configured under `/data/yuruiz` by the local
`target/client-uring-env.sh`. Native tests require io_uring permission and loopback
sockets. Test parallelism uses `TALON_CLIENT_IO_THREADS=2`.

```sh
cargo test --workspace --exclude talon-python --all-features --locked
cargo test -p talon-cache-client --all-features --locked -- --ignored --skip falls_back_when_ring_setup_is_denied
cargo test -p talon-rust-client --test native_client --all-features --locked -- --ignored --skip falls_back_when_ring_setup_is_denied
cargo test -p talon-c --all-features --locked -- --ignored
cargo test -p talon-python --locked -- --include-ignored
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo doc --workspace --all-features --no-deps --locked
cargo fmt --all --check
```

Use `scripts/test_client_no_uring.py` with Cargo test-artifact JSON for the three
fallback cases: `client_io::tests::auto_falls_back_when_ring_setup_is_denied`,
`hosted_auto_falls_back_when_ring_setup_is_denied`, and
`tests::multi_block_read_receives_into_uninitialized_caller_storage`. The wrapper
changes only the test process's seccomp filter.

## Performance boundary

This candidate has not been redeployed to the dual-Worker benchmark. No new QPS,
CPU-per-request or latency improvement is claimed. The previous 4 KiB comparison
cannot be reused as evidence for this implementation.
