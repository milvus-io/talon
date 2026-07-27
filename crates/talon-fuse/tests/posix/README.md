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
