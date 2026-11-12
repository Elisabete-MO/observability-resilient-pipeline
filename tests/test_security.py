import json
import os
import sys
import unittest
from unittest.mock import MagicMock

# Permite execucao sem bibliotecas de terceiros no host (FastAPI, confluent_kafka, etc.)
sys.modules.setdefault("confluent_kafka", MagicMock())
sys.modules.setdefault("prometheus_client", MagicMock())
sys.modules.setdefault("fastapi", MagicMock())
sys.modules.setdefault("fastapi.middleware.cors", MagicMock())
sys.modules.setdefault("fastapi.responses", MagicMock())
sys.modules.setdefault("pydantic", MagicMock())

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "apps", "producer")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "apps", "consumer")))

import apps.producer.main as producer_module
import apps.consumer.main as consumer_module


class TestSecurityHardening(unittest.TestCase):
    def test_token_bucket_rate_limiter(self):
        # Limiter com 5 RPS
        limiter = producer_module.TokenBucketLimiter(5)
        # Deve permitir 5 tokens iniciais
        for _ in range(5):
            self.assertTrue(limiter.allow())
        # O 6o token consecutivo imediato deve ser bloqueado
        self.assertFalse(limiter.allow())

    def test_token_bucket_disabled_when_zero(self):
        limiter = producer_module.TokenBucketLimiter(0)
        # Sempre permite quando rps <= 0
        for _ in range(100):
            self.assertTrue(limiter.allow())

    def _build_message(self, value=b'{"corrupted": true}'):
        mock_message = MagicMock()
        mock_message.topic.return_value = "raw-events"
        mock_message.partition.return_value = 1
        mock_message.offset.return_value = 42
        mock_message.key.return_value = b"test-key"
        mock_message.value.return_value = value
        return mock_message

    def test_dlq_message_routing(self):
        # Mock do producer da DLQ confirmando a entrega no callback
        mock_dlq_producer = MagicMock()

        def produce_side_effect(**kwargs):
            callback = kwargs.get("on_delivery")
            if callback is not None:
                callback(None, MagicMock())

        mock_dlq_producer.produce.side_effect = produce_side_effect
        mock_message = self._build_message()

        # Processar mensagem com erro
        result = consumer_module.process_message(mock_message, dlq_producer=mock_dlq_producer)

        self.assertTrue(result)
        # Verifica se o producer da DLQ foi chamado com os headers corretos
        self.assertTrue(mock_dlq_producer.produce.called)
        call_kwargs = mock_dlq_producer.produce.call_args[1]
        self.assertEqual(call_kwargs["topic"], consumer_module.DLQ_TOPIC)
        self.assertEqual(call_kwargs["key"], b"test-key")

        headers_dict = dict(call_kwargs["headers"])
        self.assertEqual(headers_dict["x-error-reason"], b"validation_error")
        self.assertEqual(headers_dict["x-original-partition"], b"1")
        self.assertEqual(headers_dict["x-original-offset"], b"42")

    def test_dlq_delivery_error_prevents_commit(self):
        mock_dlq_producer = MagicMock()
        mock_dlq_producer.produce.side_effect = lambda **kwargs: kwargs[
            "on_delivery"
        ](RuntimeError("broker indisponivel"), None)
        mock_consumer = MagicMock()
        mock_message = self._build_message()

        result = consumer_module.process_and_commit(
            mock_consumer,
            mock_message,
            dlq_producer=mock_dlq_producer,
        )

        self.assertFalse(result)
        mock_consumer.commit.assert_not_called()

    def test_dlq_timeout_prevents_commit(self):
        mock_dlq_producer = MagicMock()
        mock_dlq_producer.produce.side_effect = lambda **kwargs: None
        mock_consumer = MagicMock()
        mock_message = self._build_message()

        original_timeout = consumer_module.DLQ_PUBLISH_TIMEOUT_SECONDS
        try:
            consumer_module.DLQ_PUBLISH_TIMEOUT_SECONDS = 0.05
            result = consumer_module.process_and_commit(
                mock_consumer,
                mock_message,
                dlq_producer=mock_dlq_producer,
            )
        finally:
            consumer_module.DLQ_PUBLISH_TIMEOUT_SECONDS = original_timeout

        self.assertFalse(result)
        mock_consumer.commit.assert_not_called()

    def test_missing_dlq_producer_prevents_commit(self):
        mock_consumer = MagicMock()
        mock_message = self._build_message()

        result = consumer_module.process_and_commit(
            mock_consumer,
            mock_message,
            dlq_producer=None,
        )

        self.assertFalse(result)
        mock_consumer.commit.assert_not_called()

    def test_dlq_ack_allows_commit(self):
        mock_dlq_producer = MagicMock()

        def produce_side_effect(**kwargs):
            kwargs["on_delivery"](None, MagicMock())

        mock_dlq_producer.produce.side_effect = produce_side_effect
        mock_consumer = MagicMock()
        mock_consumer.commit.return_value = []
        mock_message = self._build_message()

        result = consumer_module.process_and_commit(
            mock_consumer,
            mock_message,
            dlq_producer=mock_dlq_producer,
        )

        self.assertTrue(result)
        mock_consumer.commit.assert_called_once_with(
            message=mock_message,
            asynchronous=False,
        )


if __name__ == "__main__":
    unittest.main()
