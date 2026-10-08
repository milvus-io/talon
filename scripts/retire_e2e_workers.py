#!/usr/bin/env python3
"""Retire pod-name identities deliberately replaced by an ephemeral E2E stack.

Use --snapshot before replacement to capture every worker pod, including unready
pods. Read those names from stdin after replacement to retire them explicitly.
This is test orchestration, not a production policy: missing/offline workers must
not be retired implicitly.
"""

import argparse
import json
import subprocess
import sys
import time


def kubectl(namespace, *args, body=None):
    return subprocess.run(
        ["kubectl", "-n", namespace, *args],
        input=body, text=True, capture_output=True, check=True, timeout=15,
    ).stdout


def worker_pods(namespace, release):
    return json.loads(kubectl(
        namespace, "get", "pods", "-l",
        f"app.kubernetes.io/instance={release},app.kubernetes.io/component=worker",
        "-o", "json",
    ))["items"]


def snapshot_workers(namespace, release):
    # Readiness can change after Helm's wait. Every replaced pod may already
    # have registered a persistent identity, regardless of its current readiness.
    return {pod["metadata"]["name"] for pod in worker_pods(namespace, release)}


def retire_once(namespace, release, replaced):
    pods = worker_pods(namespace, release)
    # Include unready and terminating pods: loss of readiness is not retirement.
    present = {pod["metadata"]["name"] for pod in pods}
    if replaced & present:
        return False
    runner = next((pod["metadata"]["name"] for pod in pods
                   if not pod["metadata"].get("deletionTimestamp")
                   and any(c["type"] == "Ready" and c["status"] == "True"
                           for c in pod.get("status", {}).get("conditions", []))), None)
    if runner is None:
        return False

    def request(method, body=None):
        args = ["exec", "-i", runner, "-c", "worker", "--", "curl",
                "--silent", "--show-error", "--max-time", "10",
                "--write-out", "\n%{http_code}", "--request", method]
        if body is not None:
            args += ["--header", "Content-Type: application/json", "--data-binary", "@-"]
        args += [f"http://{release}-coordinator:8000/api/v1/worker-membership"]
        output = kubectl(namespace, *args, body=body)
        payload, status = output.rsplit("\n", 1)
        if status in {"409", "503"}:
            # CAS race, backend outage, or an old instance lease not yet expired.
            return None
        if status not in {"200", "204"}:
            raise RuntimeError(f"membership {method}: HTTP {status}: {payload}")
        return payload

    payload = request("GET")
    if payload is None:
        return False
    snapshot = json.loads(payload)
    changed = []
    for member in snapshot["registry"]["members"]:
        if member["worker_id"] in replaced and not member["retired"]:
            member["retired"] = True
            changed.append(member["worker_id"])
    if not changed:
        return True
    update = json.dumps({
        "expected_registry_revision": snapshot["registry_revision"],
        "registry": snapshot["registry"],
    })
    if request("PUT", update) is None:
        return False
    print("retired replaced E2E workers: " + ", ".join(sorted(changed)), flush=True)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--release", required=True)
    parser.add_argument("--snapshot", action="store_true",
                        help="print all current worker pod names before replacement")
    args = parser.parse_args()
    if args.snapshot:
        for name in sorted(snapshot_workers(args.namespace, args.release)):
            print(name)
        return
    replaced = set(sys.stdin.read().split())
    if not replaced:
        return
    deadline = time.monotonic() + 120
    last_error = "old pods or instance leases still present"
    while time.monotonic() < deadline:
        try:
            if retire_once(args.namespace, args.release, replaced):
                return
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
            last_error = str(error)
        time.sleep(1)
    raise SystemExit(f"could not retire replaced E2E workers: {last_error}")


if __name__ == "__main__":
    main()
