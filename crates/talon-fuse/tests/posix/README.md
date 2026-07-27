# POSIX compatibility tests

This directory contains external filesystem compatibility suites that run
against a real Talon FUSE mount. Each suite has its own pinned version and
runner so coverage can be expanded without coupling the suites to Rust's test
harness.

The initial suite is [pjdfstest](https://github.com/pjd/pjdfstest), which checks
POSIX system call behavior, error codes, permissions, and metadata semantics.

## Run pjdfstest

The runner requires:

- Linux with a mounted Talon FUSE filesystem
- root privileges
- `git`, `autoconf`, `automake`, `make`, a C compiler, `perl`, `prove`,
  `openssl`, and `findmnt`

On Ubuntu, the required user-space packages can be installed with:

```sh
sudo apt-get update
sudo apt-get install -y autoconf automake build-essential git openssl perl util-linux
```

Run the full suite against a writable directory inside the Talon mount:

```sh
sudo crates/talon-fuse/tests/posix/pjdfstest/run.sh \
  --mountpoint /mnt/talon/s3/test-bucket
```

Run one group:

```sh
sudo crates/talon-fuse/tests/posix/pjdfstest/run.sh \
  --mountpoint /mnt/talon/s3/test-bucket \
  open
```

Run one test file:

```sh
sudo crates/talon-fuse/tests/posix/pjdfstest/run.sh \
  --mountpoint /mnt/talon/s3/test-bucket \
  open/00.t
```

The runner downloads and builds the pinned pjdfstest revision under
`target/posix/`. It rejects non-FUSE targets to reduce the risk of accidentally
running destructive filesystem tests against the host filesystem.

The complete suite is not expected to pass until Talon implements all covered
operations. Add operation-specific invocations and expected-result tracking
incrementally as support is added.

## Run through the kernel-mount fixture

The `mount_e2e` integration test can start mock coordinator and worker servers,
mount Talon through the kernel, and invoke this runner inside the synthesized
`s3/bucket` directory.

Run the complete suite:

```sh
sudo TALON_REQUIRE_FUSE=1 TALON_RUN_PJDFSTEST=1 \
  cargo test -p talon-fuse --features mount --test mount_e2e \
  mount_pjdfstest_compatibility_suite -- --ignored --nocapture
```

Run selected groups or files:

```sh
sudo TALON_REQUIRE_FUSE=1 \
  TALON_RUN_PJDFSTEST=1 \
  TALON_PJDFSTEST_TESTS=open/00.t,unlink \
  cargo test -p talon-fuse --features mount --test mount_e2e \
  mount_pjdfstest_compatibility_suite -- --ignored --nocapture
```

## Run the kernel I/O benchmark

The opt-in benchmark measures the Talon userspace and kernel FUSE path over
local protocol mocks. It does not include object-store or cross-region latency.

```sh
sudo TALON_REQUIRE_FUSE=1 TALON_RUN_FUSE_BENCH=1 \
  cargo test -p talon-fuse --features mount --test mount_e2e \
  mount_kernel_io_benchmark -- --ignored --nocapture
```

The workload size can be adjusted with:

- `TALON_FUSE_BENCH_READ_MIB`
- `TALON_FUSE_BENCH_WRITE_MIB`
- `TALON_FUSE_BENCH_RANDOM_OPS`
