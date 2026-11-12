import json
import os
import sys
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

# Permite execucao tanto no host sem libs compiladas quanto em containers
sys.modules.setdefault("confluent_kafka", MagicMock())
sys.modules.setdefault("prometheus_client", MagicMock())

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "apps", "consumer")))

from main import validate_event, MAX_EVENT_BYTES


class TestConsumerValidation(unittest.TestCase):
    def setUp(self):
        self.valid_event = {
            "event_id": str(uuid4()),
            "event_type": "order.payment_completed",
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "payload": {"amount": 150.00, "currency": "BRL"},
        }

    def test_valid_event(self):
        raw = json.dumps(self.valid_event).encode("utf-8")
        parsed = validate_event(raw)
        self.assertEqual(parsed["event_id"], self.valid_event["event_id"])
        self.assertEqual(parsed["event_type"], "order.payment_completed")
        self.assertTrue(parsed["occurred_at"].endswith("+00:00") or "Z" in parsed["occurred_at"])

    def test_none_payload_raises(self):
        with self.assertRaises(ValueError):
            validate_event(None)

    def test_oversized_payload_raises(self):
        huge_payload = dict(self.valid_event)
        huge_payload["payload"] = {"data": "x" * (MAX_EVENT_BYTES + 10)}
        raw = json.dumps(huge_payload).encode("utf-8")
        with self.assertRaises(ValueError) as ctx:
            validate_event(raw)
        self.assertIn("64 KiB", str(ctx.exception))

    def test_missing_timezone_raises(self):
        event = dict(self.valid_event)
        event["occurred_at"] = "2026-11-02T20:00:00"  # Sem fuso horario
        raw = json.dumps(event).encode("utf-8")
        with self.assertRaises(ValueError) as ctx:
            validate_event(raw)
        self.assertIn("fuso horario", str(ctx.exception))

    def test_invalid_uuid_raises(self):
        event = dict(self.valid_event)
        event["event_id"] = "not-a-valid-uuid"
        raw = json.dumps(event).encode("utf-8")
        with self.assertRaises(ValueError):
            validate_event(raw)

    def test_invalid_event_type_raises(self):
        event = dict(self.valid_event)
        event["event_type"] = "INVALID_EVENT"  # Precisa ter formato com pontos
        raw = json.dumps(event).encode("utf-8")
        with self.assertRaises(ValueError):
            validate_event(raw)

    def test_unexpected_fields_rejected(self):
        event = dict(self.valid_event)
        event["extra_field"] = "malicious"
        raw = json.dumps(event).encode("utf-8")
        with self.assertRaises(ValueError):
            validate_event(raw)


if __name__ == "__main__":
    unittest.main()
