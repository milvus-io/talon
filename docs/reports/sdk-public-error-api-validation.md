# Rust SDK public error API validation

Validated on 2026-09-11 against Talon main `e7f40ea629b522cffa5549a437be0660d2bf9eeb`
plus this change, using rustc 1.96.1 and Cargo 1.96.1.

## Scope

The Rust SDK now re-exports `BlockReadError`, `CacheReadError`, `CoordinatorError`,
`WorkerError`, `DataErrorCode`, and `DataPlaneError`. These are the existing types;
`Client::stat`, `Client::read_into`, error variants, conversions, diagnostics, and
error sources retain their behavior. The [SDK documentation](../../clients/rust/src/lib.rs)
shows consumer-side classification while preserving the outer diagnostic.
Retry and origin fallback policy remain with the caller.

This is one complete local stack layer based on `main`: public exports, SDK
documentation, regression tests, and the standalone consumer CI check. The
milvus-storage dependency migration is subsequent cross-repository work, after
the Talon change merges; it is not part of this layer.

## Commands and results

Run from the repository root:

| Command | Result |
| --- | --- |
| `cargo fmt --all --check` | Passed |
| `git diff --check` | Passed |
| `NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost cargo test -p talon-rust-client --all-features --locked` | 19 SDK unit tests, 5 public API tests, and 1 doctest passed |
| `cargo clippy -p talon-rust-client --all-targets --all-features --locked -- -D warnings` | Passed |
| `RUSTDOCFLAGS='-D warnings' cargo doc -p talon-rust-client --all-features --no-deps --locked` | Passed |
| `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | Passed |
| `NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost cargo test --workspace --exclude talon-python --all-features --locked` | 1296 passed, 21 ignored; Python is tested separately as in CI |
| `NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost cargo test -p talon-python --locked` | 4 passed |
| `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --locked` | Passed |

The initial SDK test run could not bind local TCP listeners in the sandbox
(`Operation not permitted`). The same command passed with local socket access.

The independent consumer uses its own workspace and has exactly one direct
dependency, `talon-rust-client`. It compiles and runs the same public API tests:

```sh
cp Cargo.lock clients/rust/tests/consumer/Cargo.lock
cargo test --manifest-path clients/rust/tests/consumer/Cargo.toml --offline --target-dir target
```

All 5 consumer tests passed with default SDK features. Cargo metadata confirmed
the single direct dependency; comparison of the lockfiles confirmed that all
consumer dependency versions match the workspace lockfile. The copied lockfile
is generated and ignored. CI runs these same commands after workspace tests.

The regression matrix covers all ten remote error codes in direct worker and
replica-exhaustion paths, coordinator/worker timeouts and connection failures,
placement failures, protocol length errors, and invalid inputs. Tests preserve
typed sources, last-worker addresses, and outer diagnostics, and use misleading
`NotFound Timeout` text to verify that classification follows types and codes.

The workspace checks above were added before publication. The `typos` and
`lychee` binaries are not installed locally, so spelling and offline link checks
remain for CI. Live-cluster E2E and the later Storage migration were not run as
part of this change.
