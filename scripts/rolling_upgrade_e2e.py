#!/usr/bin/env python3
"""Live etcd + two Coordinators + persistent SDK/Worker restart acceptance.

Build the worker/coordinator (with etcd) and rolling_upgrade_probe first. Run in
its development container with an explicit data-backed --work-dir. Logs and
report.json are retained. No Kubernetes resources or external services are used.
"""
import argparse
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def http(port, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data,
                                     method="GET" if body is None else "PUT",
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=3) as response:
        payload = response.read()
        return json.loads(payload) if payload else None


def until(predicate, seconds=15):
    deadline = time.monotonic() + seconds
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except urllib.error.HTTPError as error:
            # Shared-runner etcd latency can exceed the deliberately short
            # backend timeout. Retry observations, never mutations or SDK calls.
            if error.code != 503:
                raise
            last_error = RuntimeError(f"{error}: {error.read().decode(errors='replace')}")
            error.close()
        except OSError as error:
            last_error = error
        time.sleep(0.05)
    raise AssertionError("condition did not converge within deadline") from last_error


def observe_discovery(read, state=None, token=None):
    """Return the same snapshot whose state and topology were checked."""
    def ready():
        view = read()
        if token is not None:
            assert view["topology_token"] == token, "logical topology changed"
        if state is not None and not (
            len(view["workers"]) == 1 and state in view["workers"][0]["state"]
        ):
            return None
        return view

    return until(ready)


class Origin(BaseHTTPRequestHandler):
    gets = 0
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def serve(self, head=False):
        start, end = 0, (1 << 20) - 1
        requested_range = self.headers.get("x-ms-range", self.headers.get("Range"))
        if requested_range:
            start, end = map(int, requested_range.removeprefix("bytes=").split("-"))
        self.send_response(200 if head else 206)
        self.send_header("Content-Length", str((1 << 20) if head else end - start + 1))
        self.send_header("Content-Range", f"bytes {start}-{end}/{1 << 20}")
        self.send_header("ETag", '"v1"')
        self.send_header("Last-Modified", "Mon, 01 Jan 2024 00:00:00 GMT")
        self.end_headers()
        if not head:
            type(self).gets += 1
            self.wfile.write(bytes(i % 251 for i in range(start, end + 1)))

    def do_HEAD(self):
        self.serve(head=True)

    def do_GET(self):
        self.serve()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--bin-dir", type=Path, default=Path("target/debug"))
    parser.add_argument("--etcd", required=True)
    parser.add_argument("--page-size", type=int, choices=[0, 4096], default=4096)
    parser.add_argument("--runtime", choices=["tokio", "uring"], default="tokio")
    parser.add_argument("--backend-contract", action="store_true")
    args = parser.parse_args()
    # Every service in this harness is loopback-local, including the origin.
    os.environ["NO_PROXY"] = os.environ["no_proxy"] = "127.0.0.1,localhost"
    args.work_dir.mkdir(parents=True, exist_ok=False)
    root = args.work_dir.resolve()
    bins = args.bin_dir.resolve()
    processes, logs, checks = [], [], []
    ports = {name: free_port() for name in ["etcd", "peer", "c1", "a1", "c2", "a2", "w", "wa", "w2", "dup", "dupa"]}
    assert len(set(ports.values())) == len(ports), "ephemeral port collision; rerun"
    origin = ThreadingHTTPServer(("127.0.0.1", 0), Origin)
    threading.Thread(target=origin.serve_forever, daemon=True).start()

    def start(name, binary, argv=(), env=None, probe=False):
        log = (root / f"{len(processes):02d}-{name}.log").open("wb")
        logs.append(log)
        child = subprocess.Popen([str(binary), *argv], stdin=subprocess.PIPE if probe else subprocess.DEVNULL,
                                 stdout=subprocess.PIPE if probe else log, stderr=log, text=probe,
                                 env={**os.environ, "RUST_LOG": "info", **(env or {})})
        processes.append(child)
        return child

    def stop(child, sig=signal.SIGTERM, clean=True):
        if child.poll() is None:
            child.send_signal(sig)
            child.wait(timeout=23)
        if clean and sig == signal.SIGTERM:
            assert child.returncode == 0, f"unclean shutdown: {child.returncode}"

    def command(probe, command, expected=None):
        probe.stdin.write(command + "\n")
        probe.stdin.flush()
        assert select.select([probe.stdout], [], [], 12)[0], "SDK request timed out"
        result = probe.stdout.readline().strip()
        if expected is not None:
            assert result == expected, (command, result, expected)
        return result

    def check(name):
        checks.append(name)
        print(f"PASS {name}", flush=True)

    def start_etcd():
        endpoint = f"http://127.0.0.1:{ports['etcd']}"
        peer = f"http://127.0.0.1:{ports['peer']}"
        child = start("etcd", args.etcd, ["--data-dir", str(root / "etcd"), "--listen-client-urls", endpoint,
                      "--advertise-client-urls", endpoint, "--listen-peer-urls", peer,
                      "--initial-advertise-peer-urls", peer, "--initial-cluster", f"default={peer}"])
        until(lambda: http(ports["etcd"], "/health"))
        return child

    def coordinator(number):
        return start(f"coordinator-{number}", bins / "talon-coordinator", env={
            "TALON_COORDINATOR_LISTEN": f"127.0.0.1:{ports[f'c{number}']}",
            "TALON_COORDINATOR_ADMIN_LISTEN": f"127.0.0.1:{ports[f'a{number}']}",
            "TALON_COORDINATOR_NODE_ID": f"coordinator-{number}", "TALON_COORDINATOR_CLUSTER_ID": "rolling-test",
            "TALON_COORDINATOR_STATE_BACKEND": "etcd", "TALON_COORDINATOR_ETCD_ENDPOINTS": f"http://127.0.0.1:{ports['etcd']}",
            "TALON_COORDINATOR_HEARTBEAT_INTERVAL_MS": "100", "TALON_COORDINATOR_LEASE_TTL_MS": "1000",
            "TALON_COORDINATOR_REQUEST_TIMEOUT_MS": "300", "TALON_COORDINATOR_UNHEALTHY_AFTER_MS": "500"})

    def worker(port="w", cache="cache", identity=None, admin="wa"):
        env = {"TALON_WORKER_LISTEN": f"127.0.0.1:{ports[port]}",
               "TALON_WORKER_ADMIN_LISTEN": f"127.0.0.1:{ports[admin]}",
               "TALON_WORKER_COORDINATOR": f"127.0.0.1:{ports['c2']}",
               "TALON_WORKER_CLUSTER_ID": "rolling-test", "TALON_WORKER_CACHE_DIRS": str(root / cache),
               "TALON_WORKER_BLOCK_SIZE": "65536", "TALON_WORKER_CAPACITY_BYTES": str(8 << 20),
               "TALON_WORKER_L2_PAGE_SIZE_BYTES": str(args.page_size), "TALON_WORKER_HEARTBEAT_INTERVAL_MS": "100",
               "TALON_WORKER_AZURE_ACCOUNT": "test", "TALON_WORKER_AZURE_SAS": "test",
               "TALON_WORKER_AZURE_ENDPOINT": f"http://127.0.0.1:{origin.server_port}",
               "TALON_WORKER_FORCE_TOKIO_DATA_PLANE": "1" if args.runtime == "tokio" else "0"}
        if identity is not None:
            env["TALON_WORKER_NODE_ID"] = identity
        return start("worker", bins / "talon-worker", env=env)

    def discovery(number=1, state=None, token=None):
        return observe_discovery(
            lambda: http(ports[f"a{number}"], "/api/v1/worker-discovery"), state, token)

    def update(retired):
        current = until(lambda: http(ports["a1"], "/api/v1/worker-membership"))
        registry = current["registry"]
        registry["members"][0]["retired"] = retired
        http(ports["a1"], "/api/v1/worker-membership", {"expected_registry_revision": current["registry_revision"],
             "registry": registry})

    try:
        etcd = start_etcd()
        if args.backend_contract:
            with (root / "etcd-contract.log").open("wb") as log:
                subprocess.run(["cargo", "test", "--locked", "-p", "talon-coordinator", "--features", "etcd,state-store-testkit",
                                "--test", "etcd_contract"], check=True, stdout=log, stderr=log,
                               env={**os.environ, "TALON_ETCD_TEST_ENDPOINT": f"http://127.0.0.1:{ports['etcd']}"})
            check("live etcd backend contract")
        c1, c2 = coordinator(1), coordinator(2)
        for number in [1, 2]:
            until(lambda: http(ports[f"a{number}"], "/readyz"))
        w = worker()
        until(lambda: http(ports["wa"], "/readyz"))
        if args.runtime == "uring":
            assert any("serving data plane on io_uring rings" in p.read_text() for p in root.glob("*-worker.log")), "native io_uring unavailable (fallback is not native acceptance)"
        identity = (root / "cache/worker_identity").read_bytes()
        worker_id = json.loads(identity)["worker_id"]
        p1 = start("sdk-1", bins / "examples/rolling_upgrade_probe", [f"127.0.0.1:{ports['c1']}"], probe=True)
        p2 = start("sdk-2", bins / "examples/rolling_upgrade_probe", [f"127.0.0.1:{ports['c2']}"], probe=True)
        command(p1, "membership", "MEMBERSHIP_OK")
        duplicate_lock = worker("dup", admin="dupa")
        duplicate_lock.wait(timeout=5)
        assert duplicate_lock.returncode != 0
        check("same directory excludes a second process")
        token = discovery(state="Serving")["topology_token"]
        discovery(2, state="Serving", token=token)
        command(p1, "read", "OK 8192")
        command(p2, "read", "OK 8192")
        origin_gets = Origin.gets
        assert origin_gets > 0
        check("persistent membership, two-coordinator agreement and exact cache fill")
        for sig, endpoint in [(signal.SIGTERM, "w2"), (signal.SIGKILL, "w2")]:
            before = discovery(state="Serving", token=token)["workers"][0]["state"]["Serving"]["instance_id"]
            stop(w, sig)
            for number in [1, 2]:
                discovery(number, state="Offline", token=token)
            time.sleep(0.6)
            command(p1, "read", "ERR Unavailable")
            command(p1, "stat", "ERR Unavailable")
            w = worker(endpoint)
            until(lambda: http(ports["wa"], "/readyz"))
            assert (root / "cache/worker_identity").read_bytes() == identity
            view = discovery(state="Serving", token=token)
            assert view["workers"][0]["state"]["Serving"]["instance_id"] != before
            time.sleep(0.6)
            command(p1, "read", "OK 8192")
            assert Origin.gets == origin_gets, "restart refetched resident data"
            check(f"{sig.name}: stable identity/owner, unavailable gap, recovered cache hit")
        duplicate = worker("dup", cache="duplicate", identity=worker_id, admin="dupa")
        discovery(state="Conflict", token=token)
        time.sleep(0.6)
        command(p1, "read", "ERR Unavailable")
        stop(duplicate)
        discovery(state="Serving", token=token)
        check("duplicate identity conflicts without changing topology")
        stop(c1)
        time.sleep(0.6)
        command(p2, "read", "OK 8192")
        c1 = coordinator(1)
        until(lambda: http(ports["a1"], "/readyz"))
        assert discovery()["topology_token"] == token
        command(p1, "read", "OK 8192")
        check("SIGTERM coordinator replacement preserves members and peer service")
        stop(etcd, clean=False)
        time.sleep(0.7)
        assert command(p1, "read") in ["ERR Unavailable", "ERR Timeout"]
        etcd = start_etcd()
        for number in [1, 2]:
            discovery(number, state="Serving", token=token)
        time.sleep(0.6)
        command(p1, "read", "OK 8192")
        assert discovery()["topology_token"] == token
        check("backend outage fails closed and durable registry recovers")
        stop(w)
        discovery(state="Offline", token=token)
        update(retired=True)
        assert discovery()["topology_token"] != token
        rejected = worker("w2")
        time.sleep(1)
        assert not discovery()["workers"], "heartbeat resurrected a retired member"
        stop(rejected)
        update(retired=False)
        w = worker("w2")
        until(lambda: http(ports["wa"], "/readyz"))
        discovery(state="Serving", token=token)
        check("retirement persists until explicit administrative reactivation")
        report = {"runtime": args.runtime, "page_size": args.page_size, "checks": checks,
                  "origin_gets": Origin.gets, "worker_id": worker_id, "topology_token": token}
        (root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    finally:
        for child in reversed(processes):
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=23)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
        origin.shutdown()
        for log in logs:
            log.close()
        print(f"Artifacts: {root}", flush=True)


if __name__ == "__main__":
    main()
