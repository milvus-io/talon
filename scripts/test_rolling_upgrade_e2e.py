#!/usr/bin/env python3
"""Regression checks for bounded, read-only acceptance observations."""
import io
import json
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from rolling_upgrade_e2e import observe_discovery, until


def unavailable():
    return HTTPError("http://localhost/discovery", 503, "Unavailable", {},
                     io.BytesIO(b"etcd state backend operation timed out"))


def snapshot(state="Offline", token="stable"):
    return {"workers": [{"state": state}], "topology_token": token}


class ObservationTests(unittest.TestCase):
    @patch("rolling_upgrade_e2e.time.sleep")
    def test_transient_503_returns_the_observed_snapshot(self, _sleep):
        view = snapshot()
        read = Mock(side_effect=[unavailable(), view])
        self.assertIs(observe_discovery(read, "Offline", "stable"), view)
        self.assertEqual(read.call_count, 2)

    @patch("rolling_upgrade_e2e.time.sleep")
    def test_waits_for_state_without_rereading_successful_snapshot(self, _sleep):
        view = snapshot()
        read = Mock(side_effect=[snapshot("Serving"), view])
        self.assertIs(observe_discovery(read, "Offline", "stable"), view)
        self.assertEqual(read.call_count, 2)

    def test_topology_violation_is_fatal_even_before_state_converges(self):
        read = Mock(return_value=snapshot("Serving", "changed"))
        with self.assertRaisesRegex(AssertionError, "logical topology changed"):
            observe_discovery(read, "Offline", "stable")
        read.assert_called_once()

    def test_nontransient_http_error_is_not_retried(self):
        read = Mock(side_effect=HTTPError("http://localhost", 403, "Forbidden", {}, None))
        with self.assertRaises(HTTPError):
            observe_discovery(read)
        read.assert_called_once()

    def test_invalid_response_is_not_retried(self):
        read = Mock(side_effect=json.JSONDecodeError("invalid", "?", 0))
        with self.assertRaises(json.JSONDecodeError):
            observe_discovery(read)
        read.assert_called_once()

    @patch("rolling_upgrade_e2e.time.sleep")
    @patch("rolling_upgrade_e2e.time.monotonic", side_effect=[0, 0, 16])
    def test_persistent_503_fails_with_backend_diagnostic(self, _clock, _sleep):
        read = Mock(side_effect=unavailable())
        with self.assertRaisesRegex(AssertionError, "deadline") as failure:
            observe_discovery(read)
        self.assertIn("etcd state backend operation timed out", str(failure.exception.__cause__))
        read.assert_called_once()

    @patch("rolling_upgrade_e2e.time.sleep")
    def test_startup_connection_failure_can_recover(self, _sleep):
        predicate = Mock(side_effect=[ConnectionRefusedError(), {"ready": True}])
        self.assertEqual(until(predicate), {"ready": True})


if __name__ == "__main__":
    unittest.main()
