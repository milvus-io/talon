"""End-to-end tests for the Python client against a real distributed Talon
cluster backed by MinIO.

Unlike clients/python/tests/test_client.py — which starts a local coordinator,
worker, and a Python stub origin itself — this suite targets an already-deployed
stack (see test/stack/deploy.sh): 3 HA coordinators, workers, and a real MinIO
origin seeded with a deterministic (i % 251) ramp object. The client resolves
placement through the coordinator and reads bytes straight from a worker, so a
byte-exact result proves the full SDK -> worker -> MinIO chain.

Run: pytest test/sdk/python/ -v
Env: TALON_E2E_COORDINATOR (default 127.0.0.1:17000), TALON_E2E_BLOCK_SIZE,
     TALON_E2E_BUCKET, TALON_E2E_KEY
"""

import os
import threading

import pytest

# Hard import, not importorskip: the wheel is a required prerequisite installed
# by the CI workflow, so a missing/unimportable module must fail red instead of
# silently skipping every case in this file.
import talon

COORDINATOR = os.environ.get("TALON_E2E_COORDINATOR", "127.0.0.1:17000")
BLOCK_SIZE = int(os.environ.get("TALON_E2E_BLOCK_SIZE", "8388608"))
BUCKET = os.environ.get("TALON_E2E_BUCKET", "talon-e2e")
KEY = os.environ.get("TALON_E2E_KEY", "bench")
URI = f"s3://{BUCKET}/{KEY}"


def ramp(start: int, length: int) -> bytes:
    """The deterministic bytes the MinIO seed object contains (i % 251)."""
    return bytes((i % 251) for i in range(start, start + length))


@pytest.fixture(scope="module")
def client():
    with talon.Client(COORDINATOR, block_size=BLOCK_SIZE) as c:
        yield c


def test_stat_returns_size_and_version(client):
    info = client.stat(URI)
    assert info.version, "MinIO ETag should be reported as the object version"
    assert info.size > 0


def test_read_resolves_version_automatically(client):
    """The common case: no version supplied, resolved via stat (#318)."""
    assert client.read(URI, offset=0, length=4096) == ramp(0, 4096)


def test_read_returns_exact_bytes(client):
    assert client.read(URI, offset=0, length=4096) == ramp(0, 4096)


def test_read_at_offset(client):
    assert client.read(URI, offset=1000, length=8192) == ramp(1000, 8192)


def test_read_spanning_block_boundaries(client):
    """A range wider than one block splits into per-block fetches and
    reassembles in order. Getting the order or the boundary wrong here yields
    plausible-looking but corrupt data, so assert the bytes, not the length."""
    length = BLOCK_SIZE + (4 << 20)
    got = client.read(URI, offset=0, length=length)
    assert len(got) == length
    assert got == ramp(0, length)


def test_read_crossing_a_single_boundary(client):
    """The narrow case: a small read straddling exactly one block edge."""
    offset = BLOCK_SIZE - 2048
    got = client.read(URI, offset=offset, length=4096)
    assert got == ramp(offset, 4096)


def test_zero_length_read_is_empty(client):
    assert client.read(URI, offset=0, length=0) == b""


def test_concurrent_reads_from_threads(client):
    """The GIL is released around I/O, so threads overlap rather than
    serialising. Correctness is the assertion; the timing is informational."""
    results = {}
    errors = []

    def read(i):
        try:
            results[i] = client.read(URI, offset=i * 65536, length=65536)
        except Exception as e:  # surfaced below so a failure names itself
            errors.append(e)

    threads = [threading.Thread(target=read, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"threaded reads failed: {errors}"
    for i in range(8):
        assert results[i] == ramp(i * 65536, 65536)


def test_bad_uri_raises_value_error(client):
    for bad in ["no-scheme", "ftp://bucket/key", "s3://bucket", "s3:///key"]:
        with pytest.raises(ValueError):
            client.read(bad, offset=0, length=1)
