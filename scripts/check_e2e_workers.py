#!/usr/bin/env python3
"""Verify StatefulSet E2E replacements retain each ordinal's disk identity.

Capture --snapshot before replacement, then pipe that JSON into a second call.
By default every Pod must have a new UID; --replaced selects a single ordinal.
All Workers must be ready and retain their identity. This helper never retires
members or writes to the registry.
"""

import argparse
import json
import subprocess
import sys
import time


def kubectl(namespace, *args):
    return subprocess.run(
        ["kubectl", "-n", namespace, *args],
        text=True, capture_output=True, check=True, timeout=15,
    ).stdout


def snapshot_workers(namespace, release):
    pods = json.loads(kubectl(
        namespace, "get", "pods", "-l",
        f"app.kubernetes.io/instance={release},app.kubernetes.io/component=worker",
        "-o", "json",
    ))["items"]
    if not pods:
        raise RuntimeError("no Worker Pods found")
    result = {}
    for pod in pods:
        metadata = pod["metadata"]
        name = metadata["name"]
        if metadata.get("deletionTimestamp") or not any(
            c["type"] == "Ready" and c["status"] == "True"
            for c in pod.get("status", {}).get("conditions", [])
        ):
            raise RuntimeError(f"Worker Pod {name} is not ready or is terminating")
        identity = json.loads(kubectl(
            namespace, "exec", name, "-c", "worker", "--",
            "cat", "/var/cache/talon/worker_identity",
        ))
        result[name] = {"uid": metadata["uid"], "identity": identity}
    return result


def replacement_complete(before, after, replaced):
    if before.keys() != after.keys():
        return False
    for name, previous in before.items():
        if after[name]["identity"] != previous["identity"]:
            raise ValueError(f"Worker identity changed for retained ordinal {name}")
    return all(after[name]["uid"] != before[name]["uid"] for name in replaced)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--release", required=True)
    parser.add_argument("--snapshot", action="store_true")
    parser.add_argument("--replaced", action="append",
                        help="ordinal whose Pod UID must change; default: all")
    args = parser.parse_args()
    before = None if args.snapshot else json.load(sys.stdin)
    replaced = set(args.replaced or (before or {}))
    if before is not None and (not before or not replaced <= before.keys()):
        parser.error("replacement requires a nonempty snapshot containing every selected ordinal")

    deadline = time.monotonic() + 120
    last_error = "replacement Pod UIDs or ordinal inventory have not converged"
    while time.monotonic() < deadline:
        try:
            current = snapshot_workers(args.namespace, args.release)
            if args.snapshot:
                print(json.dumps(current, sort_keys=True))
                return
            if replacement_complete(before, current, replaced):
                print("Worker replacement retained all identities: " + ", ".join(sorted(replaced)))
                return
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, RuntimeError) as error:
            last_error = str(error)
        time.sleep(1)
    raise SystemExit(f"Worker replacement check timed out: {last_error}")


if __name__ == "__main__":
    main()
