"""End-to-end tests for the Python client against a real cluster.

Deliberately not mocks. The value of this binding is that it drives the real
protocol path — placement lookup, replica resolution, block splitting — and a
mock would assert that the mock behaves as written.

Run with the cluster fixtures below, which start a coordinator, a worker, and a
local blob origin. Skipped when the binaries are not built.
"""

import os
import subprocess
import tempfile
import threading
import time
import urllib.request

import pytest

talon = pytest.importorskip("talon")

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
BIN = os.path.join(REPO, "target", "release")
COORD_PORT, WORKER_PORT, ADMIN_PORT, ORIGIN_PORT = 17610, 17611, 17661, 17710
BLOCK_SIZE = 8 << 20
VERSION = "0x8LOADTEST"
URI = "az://container/bench"


def ramp(start: int, length: int) -> bytes:
    """The deterministic bytes the test origin serves."""
    return bytes((i % 251) for i in range(start, start + length))


def _wait_ready(url: str, timeout: float = 30.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as r:
                if r.status == 200:
                    return True
        except Exception:
            time.sleep(0.5)
    return False


@pytest.fixture(scope="module")
def cluster():
    """A coordinator, a worker, and a blob origin, torn down afterwards."""
    for exe in ("talon-worker", "talon-coordinator"):
        if not os.path.exists(os.path.join(BIN, exe)):
            pytest.skip(f"{exe} not built; run `cargo build --release`")

    cache = tempfile.mkdtemp(prefix="talon-pytest-")
    env = dict(
        os.environ,
        TALON_WORKER_CACHE_DIRS=cache,
        TALON_WORKER_AZURE_ACCOUNT="test",
        TALON_WORKER_AZURE_SAS="test",
        TALON_WORKER_AZURE_ENDPOINT=f"http://127.0.0.1:{ORIGIN_PORT}",
        RUST_LOG="warn",
    )
    procs = [
        subprocess.Popen(
            ["python3", os.path.join(REPO, "scripts", "loadtest_origin.py"), str(ORIGIN_PORT)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    ]
    time.sleep(1)
    procs.append(subprocess.Popen(
        [os.path.join(BIN, "talon-coordinator"),
         "--listen", f"127.0.0.1:{COORD_PORT}",
         "--admin-listen", f"127.0.0.1:{COORD_PORT + 50}"],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    ))
    time.sleep(2)
    procs.append(subprocess.Popen(
        [os.path.join(BIN, "talon-worker"),
         "--listen", f"127.0.0.1:{WORKER_PORT}",
         "--admin-listen", f"127.0.0.1:{ADMIN_PORT}",
         "--coordinator", f"127.0.0.1:{COORD_PORT}"],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    ))

    # An unready worker error-frames every read, so wait rather than sleeping.
    if not _wait_ready(f"http://127.0.0.1:{ADMIN_PORT}/readyz"):
        for p in procs:
            p.kill()
        pytest.fail("worker never became ready")

    yield f"127.0.0.1:{COORD_PORT}"

    for p in procs:
        p.kill()
        p.wait(timeout=5)


@pytest.fixture(scope="module")
def client(cluster):
    with talon.Client(cluster, block_size=BLOCK_SIZE) as c:
        yield c


def test_read_returns_exact_bytes(client):
    assert client.read(URI, version=VERSION, offset=0, length=4096) == ramp(0, 4096)


def test_read_at_offset(client):
    assert client.read(URI, version=VERSION, offset=1000, length=8192) == ramp(1000, 8192)


def test_read_spanning_block_boundaries(client):
    """A range wider than one block splits into per-block fetches and
    reassembles in order. Getting the order or the boundary wrong here yields
    plausible-looking but corrupt data, so assert the bytes, not the length."""
    length = BLOCK_SIZE + (4 << 20)
    got = client.read(URI, version=VERSION, offset=0, length=length)
    assert len(got) == length
    assert got == ramp(0, length)


def test_read_crossing_a_single_boundary(client):
    """The narrow case: a small read straddling exactly one block edge."""
    offset = BLOCK_SIZE - 2048
    got = client.read(URI, version=VERSION, offset=offset, length=4096)
    assert got == ramp(offset, 4096)


def test_zero_length_read_is_empty(client):
    assert client.read(URI, version=VERSION, offset=0, length=0) == b""


def test_concurrent_reads_from_threads(client):
    """The GIL is released around I/O, so threads overlap rather than
    serialising. Correctness is the assertion; the timing is informational."""
    results = {}
    errors = []

    def read(i):
        try:
            results[i] = client.read(URI, version=VERSION, offset=i * 65536, length=65536)
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
    for bad in ["no-scheme", "ftp://bucket/key", "az://bucket", "az:///key"]:
        with pytest.raises(ValueError):
            client.read(bad, version=VERSION, offset=0, length=1)


def test_client_repr_and_coordinator(client, cluster):
    assert client.coordinator == cluster
    assert "Client(" in repr(client)
