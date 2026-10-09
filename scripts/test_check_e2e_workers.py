#!/usr/bin/env python3
"""Regression checks for retained identities across same-name Pod replacements."""

import copy
import io
import json
import unittest
from unittest.mock import patch

from check_e2e_workers import main, replacement_complete, snapshot_workers


def pod(name="talon-worker-0", uid="new-pod", ready=True, deleting=False):
    metadata = {"name": name, "uid": uid}
    if deleting:
        metadata["deletionTimestamp"] = "2026-10-09T00:00:00Z"
    return {"metadata": metadata, "status": {"conditions": [
        {"type": "Ready", "status": "True" if ready else "False"},
    ]}}


def snapshot():
    return {
        f"talon-worker-{i}": {
            "uid": f"pod-{i}",
            "identity": {"format_version": 1, "cluster_id": "e2e",
                         "worker_id": f"disk-id-{i}", "block_size": 65536, "page_size": 0},
        }
        for i in range(3)
    }


class WorkerReplacementTests(unittest.TestCase):
    @patch("check_e2e_workers.kubectl")
    def test_snapshot_reads_disk_identity_instead_of_using_pod_name(self, kubectl):
        identity = snapshot()["talon-worker-0"]["identity"]
        kubectl.side_effect = [json.dumps({"items": [pod()]}), json.dumps(identity)]
        self.assertEqual(snapshot_workers("ns", "talon"), {
            "talon-worker-0": {"uid": "new-pod", "identity": identity},
        })
        kubectl.assert_called_with("ns", "exec", "talon-worker-0", "-c", "worker", "--",
                                   "cat", "/var/cache/talon/worker_identity")

    @patch("check_e2e_workers.kubectl")
    def test_snapshot_rejects_missing_unready_and_terminating_pods(self, kubectl):
        for pods in [[], [pod(ready=False)], [pod(deleting=True)]]:
            kubectl.reset_mock()
            kubectl.return_value = json.dumps({"items": pods})
            with self.assertRaises(RuntimeError):
                snapshot_workers("ns", "talon")
            kubectl.assert_called_once()

    def test_same_names_with_new_uids_and_retained_identities_succeed(self):
        before = snapshot()
        after = copy.deepcopy(before)
        for worker in after.values():
            worker["uid"] += "-replacement"
        self.assertTrue(replacement_complete(before, after, set(before)))

    def test_old_ready_pods_and_partial_rollout_do_not_pass(self):
        before = snapshot()
        self.assertFalse(replacement_complete(before, before, set(before)))
        after = copy.deepcopy(before)
        after["talon-worker-0"]["uid"] = "replacement"
        self.assertFalse(replacement_complete(before, after, set(before)))

    def test_single_ordinal_replacement_keeps_other_pods(self):
        before = snapshot()
        after = copy.deepcopy(before)
        after["talon-worker-0"]["uid"] = "replacement"
        self.assertTrue(replacement_complete(before, after, {"talon-worker-0"}))

    def test_missing_or_extra_ordinals_do_not_pass(self):
        before = snapshot()
        after = copy.deepcopy(before)
        del after["talon-worker-2"]
        self.assertFalse(replacement_complete(before, after, {"talon-worker-0"}))
        after = copy.deepcopy(before)
        after["unexpected"] = after["talon-worker-0"]
        self.assertFalse(replacement_complete(before, after, {"talon-worker-0"}))

    def test_identity_loss_or_geometry_change_fails_even_with_new_pods(self):
        before = snapshot()
        for field, value in [("worker_id", "new-owner"), ("cluster_id", "other"),
                             ("block_size", 131072), ("page_size", 4096)]:
            after = copy.deepcopy(before)
            after["talon-worker-0"]["uid"] = "replacement"
            after["talon-worker-0"]["identity"][field] = value
            with self.assertRaisesRegex(ValueError, "identity changed"):
                replacement_complete(before, after, {"talon-worker-0"})

    @patch("check_e2e_workers.time.sleep")
    @patch("check_e2e_workers.snapshot_workers")
    def test_cli_waits_for_same_name_replacement_then_succeeds(self, workers, sleep):
        before = snapshot()
        after = copy.deepcopy(before)
        after["talon-worker-0"]["uid"] = "replacement"
        workers.side_effect = [RuntimeError("Pod terminating"), before, after]
        with patch("sys.argv", ["check_e2e_workers.py", "--namespace", "ns",
                                "--release", "talon", "--replaced", "talon-worker-0"]), \
                patch("sys.stdin", io.StringIO(json.dumps(before))), \
                patch("sys.stdout", new_callable=io.StringIO) as output:
            main()
        self.assertIn("retained all identities: talon-worker-0", output.getvalue())
        self.assertEqual(workers.call_count, 3)
        self.assertEqual(sleep.call_count, 2)


if __name__ == "__main__":
    unittest.main()
