import asyncio
import os
import sys
import unittest
from unittest.mock import MagicMock
from uuid import uuid4

# Permite execucao sem bibliotecas de terceiros no host
sys.modules.setdefault("confluent_kafka", MagicMock())
sys.modules.setdefault("prometheus_client", MagicMock())

# Cria mock para HTTPException se fastapi nao estiver instalado
class MockHTTPException(Exception):
    def __init__(self, status_code, detail=None):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"HTTP {status_code}: {detail}")

fastapi_mock = MagicMock()
fastapi_mock.HTTPException = MockHTTPException
sys.modules.setdefault("fastapi", fastapi_mock)
sys.modules.setdefault("fastapi.middleware.cors", MagicMock())
sys.modules.setdefault("fastapi.responses", MagicMock())
sys.modules.setdefault("pydantic", MagicMock())

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "apps", "producer")))

import apps.producer.main as producer_module
# Garante que usamos a mesma classe MockHTTPException
producer_module.HTTPException = MockHTTPException


class TestProducerDurability(unittest.TestCase):
    def setUp(self):
        self.mock_request = MagicMock()
        self.mock_producer = MagicMock()
        self.mock_request.app.state.producer = self.mock_producer
        self.mock_request.app.state.produce_semaphore = asyncio.Semaphore(10)
        
        # Evento mock valido
        self.event_id = uuid4()
        self.mock_event = MagicMock()
        self.mock_event.event_id = self.event_id
        self.mock_event.model_dump.return_value = {
            "event_id": str(self.event_id),
            "event_type": "order.payment.confirmed",
            "occurred_at": "2026-11-03T02:00:00Z",
            "payload": {"amount": 150.0},
        }

    def test_successful_delivery_returns_partition_and_offset(self):
        async def run_test():
            def produce_side_effect(topic, key, value, on_delivery):
                mock_msg = MagicMock()
                mock_msg.partition.return_value = 2
                mock_msg.offset.return_value = 105
                # Executa o callback de sucesso
                on_delivery(None, mock_msg)

            self.mock_producer.produce.side_effect = produce_side_effect

            orig_wait = producer_module.WAIT_FOR_ACK
            try:
                producer_module.WAIT_FOR_ACK = True
                result = await producer_module.handle_event_ingestion(self.mock_event, self.mock_request)
                self.assertEqual(result["status"], "published")
                self.assertEqual(result["event_id"], str(self.event_id))
                self.assertEqual(result["partition"], 2)
                self.assertEqual(result["offset"], 105)
            finally:
                producer_module.WAIT_FOR_ACK = orig_wait

        asyncio.run(run_test())

    def test_broker_error_raises_503(self):
        async def run_test():
            def produce_side_effect(topic, key, value, on_delivery):
                on_delivery("broker transport failure", None)

            self.mock_producer.produce.side_effect = produce_side_effect

            orig_wait = producer_module.WAIT_FOR_ACK
            try:
                producer_module.WAIT_FOR_ACK = True
                with self.assertRaises(MockHTTPException) as ctx:
                    await producer_module.handle_event_ingestion(self.mock_event, self.mock_request)
                self.assertEqual(ctx.exception.status_code, 503)
            finally:
                producer_module.WAIT_FOR_ACK = orig_wait

        asyncio.run(run_test())

    def test_broker_timeout_raises_504(self):
        async def run_test():
            # Nao chama on_delivery para simular broker sem resposta
            self.mock_producer.produce.side_effect = lambda **kwargs: None

            orig_wait = producer_module.WAIT_FOR_ACK
            orig_timeout = producer_module.ACK_TIMEOUT_SECONDS
            try:
                producer_module.WAIT_FOR_ACK = True
                producer_module.ACK_TIMEOUT_SECONDS = 0.05  # Timeout rapido para o teste
                with self.assertRaises(MockHTTPException) as ctx:
                    await producer_module.handle_event_ingestion(self.mock_event, self.mock_request)
                self.assertEqual(ctx.exception.status_code, 504)
            finally:
                producer_module.WAIT_FOR_ACK = orig_wait
                producer_module.ACK_TIMEOUT_SECONDS = orig_timeout

        asyncio.run(run_test())

    def test_semaphore_locked_raises_503_backpressure(self):
        async def run_test():
            sem = asyncio.Semaphore(1)
            await sem.acquire()  # Ocupa o semaforo completamente
            self.mock_request.app.state.produce_semaphore = sem

            with self.assertRaises(MockHTTPException) as ctx:
                await producer_module.handle_event_ingestion(self.mock_event, self.mock_request)
            self.assertEqual(ctx.exception.status_code, 503)
            self.assertIn("Producer sobrecarregado", ctx.exception.detail)

        asyncio.run(run_test())

    def test_wait_for_ack_disabled_returns_immediately(self):
        async def run_test():
            # Nao chama on_delivery
            self.mock_producer.produce.side_effect = lambda **kwargs: None

            orig_wait = producer_module.WAIT_FOR_ACK
            try:
                producer_module.WAIT_FOR_ACK = False
                result = await producer_module.handle_event_ingestion(self.mock_event, self.mock_request)
                self.assertEqual(result["status"], "published")
                self.assertEqual(result["event_id"], str(self.event_id))
                self.assertNotIn("partition", result)
            finally:
                producer_module.WAIT_FOR_ACK = orig_wait

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
