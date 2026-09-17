#!/usr/bin/env python3
"""Run the SDK fallback test with io_uring_setup denied in this process only.

Pass the talon_cache_client unit-test executable produced by cargo test --no-run.
Requires Linux x86_64 or aarch64 with seccomp filters; does not change host settings.
"""
import argparse
import ctypes
import json
import os
import platform


class Filter(ctypes.Structure):
    _fields_ = [
        ("code", ctypes.c_ushort), ("jt", ctypes.c_ubyte),
        ("jf", ctypes.c_ubyte), ("k", ctypes.c_uint),
    ]


class Program(ctypes.Structure):
    _fields_ = [("len", ctypes.c_ushort), ("filter", ctypes.POINTER(Filter))]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("test_binary", nargs="?", help="Path to the SDK unit-test executable")
    parser.add_argument("--artifacts", help="cargo test --lib --no-run --message-format=json output")
    parser.add_argument("--target", default="talon_cache_client", help="Cargo test target name")
    parser.add_argument("--test", default="client_io::tests::auto_falls_back_when_ring_setup_is_denied", help="Exact test name")
    args = parser.parse_args()
    if platform.system() != "Linux" or platform.machine() not in ("x86_64", "aarch64"):
        parser.error("requires Linux x86_64 or aarch64")
    if bool(args.test_binary) == bool(args.artifacts):
        parser.error("provide either a test binary or --artifacts")
    binary = args.test_binary
    if args.artifacts:
        with open(args.artifacts, encoding="utf-8") as artifacts:
            for line in artifacts:
                item = json.loads(line)
                if (item.get("reason") == "compiler-artifact"
                        and item.get("executable")
                        and item["target"]["name"] == args.target):
                    binary = item["executable"]
                    break
        if not binary:
            parser.error("SDK unit-test executable missing from artifacts")
    binary = os.path.abspath(binary)
    # Load seccomp_data.nr; deny io_uring_setup (425 on these architectures)
    # with EPERM, allowing other syscalls including TCP and thread creation.
    filters = (Filter * 4)(
        Filter(0x20, 0, 0, 0), Filter(0x15, 0, 1, 425),
        Filter(0x06, 0, 0, 0x00050001), Filter(0x06, 0, 0, 0x7FFF0000),
    )
    program = Program(4, filters)
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(38, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "could not set no_new_privs")
    if libc.prctl(22, 2, ctypes.byref(program), 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "could not install process-local seccomp filter")
    os.execv(binary, [binary, "--exact",
                     args.test,
                     "--include-ignored", "--nocapture"])


if __name__ == "__main__":
    main()
