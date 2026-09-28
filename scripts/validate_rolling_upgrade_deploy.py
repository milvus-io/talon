#!/usr/bin/env python3
"""Check rollout/storage invariants in YAML files or rendered Helm YAML (stdin)."""
from pathlib import Path
import sys
import yaml

objects = ([obj for path in sys.argv[1:] for obj in yaml.safe_load_all(Path(path).read_text())]
           if len(sys.argv) > 1 else list(yaml.safe_load_all(sys.stdin)))
workers = [o for o in objects if o and o.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component") == "worker"
           and o.get("kind") in {"Deployment", "StatefulSet"}]
assert len(workers) == 1
worker = workers[0]
assert worker["kind"] == "StatefulSet"
spec = worker["spec"]
assert spec["podManagementPolicy"] == "OrderedReady"
assert spec["minReadySeconds"] >= 1
assert spec["persistentVolumeClaimRetentionPolicy"] == {"whenDeleted": "Retain", "whenScaled": "Retain"}
assert spec["updateStrategy"]["rollingUpdate"]["partition"] >= 0
assert spec["volumeClaimTemplates"][0]["metadata"]["name"] == "cache"
pod = spec["template"]["spec"]
assert pod["terminationGracePeriodSeconds"] > 20
assert not any("ephemeral" in v or "emptyDir" in v for v in pod.get("volumes", []))
container = pod["containers"][0]
assert not any(e["name"] == "TALON_WORKER_NODE_ID" for e in container["env"])
assert container["startupProbe"]["failureThreshold"] * container["startupProbe"]["periodSeconds"] >= 300
assert any(o and o.get("kind") == "Service" and o["metadata"]["name"] == spec["serviceName"]
           and o["spec"]["clusterIP"] == "None" for o in objects)
coordinator = next(o for o in objects if o and o.get("kind") == "Deployment")
assert coordinator["spec"]["template"]["spec"]["terminationGracePeriodSeconds"] > 20
assert coordinator["spec"]["minReadySeconds"] >= 1
if coordinator["spec"]["replicas"] > 1:
    assert coordinator["spec"]["strategy"]["rollingUpdate"] == {"maxUnavailable": 0, "maxSurge": 1}
print("rolling upgrade deployment invariants passed")
