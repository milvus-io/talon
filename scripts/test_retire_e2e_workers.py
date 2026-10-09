#!/usr/bin/env python3
"""Regression checks for explicit retirement in ephemeral E2E deployments."""

import json
import unittest
from unittest.mock import patch

from retire_e2e_workers import retire_once, snapshot_workers


def pod(name, ready=True, deleting=False):
    metadata = {"name": name}
    if deleting:
        metadata["deletionTimestamp"] = "2026-10-08T00:00:00Z"
    return {"metadata": metadata, "status": {"conditions": [
        {"type": "Ready", "status": "True" if ready else "False"},
    ]}}


def snapshot(revision="r1", extra=()):
    return {"registry_revision": revision, "registry": {
        "format_version": 1,
        "members": [
            {"worker_id": name, "zone": "zone-a", "retired": name == "tombstone"}
            for name in ("old", "new", "unrelated-offline", "tombstone", *extra)
        ],
    }}


class RetirementTests(unittest.TestCase):
    @patch("retire_e2e_workers.kubectl")
    def test_snapshot_includes_unready_and_terminating_workers(self, kubectl):
        kubectl.return_value = json.dumps({"items": [
            pod("ready"), pod("unready", ready=False), pod("terminating", deleting=True),
        ]})
        self.assertEqual(snapshot_workers("ns", "talon"),
                         {"ready", "unready", "terminating"})
        kubectl.assert_called_once_with(
            "ns", "get", "pods", "-l",
            "app.kubernetes.io/instance=talon,app.kubernetes.io/component=worker",
            "-o", "json",
        )

    @patch("retire_e2e_workers.kubectl")
    def test_retire_only_explicitly_replaced_ids_and_preserve_full_registry(self, kubectl):
        before = snapshot()
        kubectl.side_effect = [
            json.dumps({"items": [pod("new")]}), json.dumps(before) + "\n200", "\n204",
        ]
        self.assertTrue(retire_once("ns", "talon", {"old"}))
        sent = json.loads(kubectl.call_args.kwargs["body"])
        before["registry"]["members"][0]["retired"] = True
        self.assertEqual(sent, {"expected_registry_revision": "r1", "registry": before["registry"]})

    @patch("retire_e2e_workers.kubectl")
    def test_unready_or_terminating_old_pods_are_not_retired(self, kubectl):
        for old in [pod("old", ready=False), pod("old", deleting=True)]:
            kubectl.reset_mock()
            kubectl.return_value = json.dumps({"items": [old, pod("new")]})
            self.assertFalse(retire_once("ns", "talon", {"old"}))
            kubectl.assert_called_once()

    @patch("retire_e2e_workers.kubectl")
    def test_retry_cas_conflict_reloads_revision_and_keeps_concurrent_registration(self, kubectl):
        kubectl.side_effect = [
            json.dumps({"items": [pod("new")]}), json.dumps(snapshot()) + "\n200",
            "membership changed\n409",
            json.dumps({"items": [pod("new")]}),
            json.dumps(snapshot("r2", ("concurrent",))) + "\n200", "\n204",
        ]
        self.assertFalse(retire_once("ns", "talon", {"old"}))
        self.assertTrue(retire_once("ns", "talon", {"old"}))
        sent = json.loads(kubectl.call_args.kwargs["body"])
        self.assertEqual(sent["expected_registry_revision"], "r2")
        self.assertEqual(sent["registry"]["members"][-1]["worker_id"], "concurrent")
        self.assertFalse(sent["registry"]["members"][-1]["retired"])

    @patch("retire_e2e_workers.kubectl")
    def test_live_old_instance_lease_is_retried(self, kubectl):
        kubectl.side_effect = [
            json.dumps({"items": [pod("new")]}), json.dumps(snapshot()) + "\n200",
            "stop worker and wait for instance withdrawal/expiration before retirement\n503",
        ]
        self.assertFalse(retire_once("ns", "talon", {"old"}))

    @patch("retire_e2e_workers.kubectl")
    def test_already_retired_is_idempotent(self, kubectl):
        kubectl.side_effect = [
            json.dumps({"items": [pod("new")]}), json.dumps(snapshot()) + "\n200",
        ]
        self.assertTrue(retire_once("ns", "talon", {"tombstone"}))
        self.assertEqual(kubectl.call_count, 2)

    @patch("retire_e2e_workers.kubectl")
    def test_unexpected_http_error_is_not_ignored(self, kubectl):
        kubectl.side_effect = [json.dumps({"items": [pod("new")]}), "forbidden\n403"]
        with self.assertRaisesRegex(RuntimeError, "HTTP 403"):
            retire_once("ns", "talon", {"old"})


if __name__ == "__main__":
    unittest.main()
