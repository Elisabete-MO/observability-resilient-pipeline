import json
import logging
import os
import random
import signal
import threading
import time
from uuid import UUID

from confluent_kafka import Consumer, KafkaError, KafkaException
from prometheus_client import Counter, Histogram, start_http_server


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("consumer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "raw-events")
GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "events-processor")
METRICS_PORT = int(os.getenv("CONSUMER_METRICS_PORT", "8001"))

MIN_DELAY_MS = int(os.getenv("PROCESSING_MIN_MS", "50"))
MAX_DELAY_MS = int(os.getenv("PROCESSING_MAX_MS", "200"))
ERROR_RATE = float(os.getenv("PROCESSING_ERROR_RATE", "0"))

if not 0 <= MIN_DELAY_MS <= MAX_DELAY_MS <= 30000:
    raise ValueError("Os tempos devem respeitar 0 <= minimo <= maximo <= 30000.")

if not 0 <= ERROR_RATE <= 1:
    raise ValueError("PROCESSING_ERROR_RATE deve estar entre 0 e 1.")

RECEIVED = Counter(
    "consumer_events_received_total",
    "Eventos recebidos do Kafka.",
)
PROCESSED = Counter(
    "consumer_events_processed_total",
    "Resultados do processamento dos eventos.",
    ["result"],
)
DURATION = Histogram(
    "consumer_processing_duration_seconds",
    "Duracao das tentativas de processamento.",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
KAFKA_ERRORS = Counter(
    "consumer_kafka_errors_total",
    "Erros de leitura ou confirmacao no Kafka.",
    ["operation"],
)

stop = threading.Event()


def handle_shutdown(signum, frame):
    logger.info("Encerramento solicitado")
    stop.set()


def validate_event(raw: bytes) -> dict:
    event = json.loads(raw)

    if not isinstance(event, dict):
        raise ValueError("O evento deve ser um objeto JSON.")

    UUID(event["event_id"])

    if not isinstance(event["event_type"], str):
        raise ValueError("event_type deve ser texto.")

    if not isinstance(event["occurred_at"], str):
        raise ValueError("occurred_at deve ser texto.")

    if not isinstance(event["payload"], dict):
        raise ValueError("payload deve ser um objeto.")

    return event


def process_message(message) -> bool:
    """Retorna False quando interrompido, deixando o offset sem commit."""
    started = time.perf_counter()

    try:
        event = validate_event(message.value())

        delay = random.uniform(MIN_DELAY_MS, MAX_DELAY_MS) / 1000

        # Permite encerrar durante a espera sem confirmar o evento.
        if stop.wait(delay):
            return False

        if random.random() < ERROR_RATE:
            PROCESSED.labels("simulated_error").inc()
            logger.warning(
                "Erro simulado tratado event_id=%s partition=%s offset=%s",
                event["event_id"],
                message.partition(),
                message.offset(),
            )
        else:
            PROCESSED.labels("success").inc()
            logger.info(
                "Processado event_id=%s partition=%s offset=%s",
                event["event_id"],
                message.partition(),
                message.offset(),
            )

    except (ValueError, TypeError, KeyError, UnicodeError):
        PROCESSED.labels("invalid").inc()
        logger.warning(
            "Evento invalido tratado partition=%s offset=%s",
            message.partition(),
            message.offset(),
            exc_info=True,
        )
    finally:
        DURATION.observe(time.perf_counter() - started)

    return True


def main():
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )

    try:
        start_http_server(METRICS_PORT, addr="0.0.0.0")
        consumer.subscribe([TOPIC])

        logger.info(
            "Consumer iniciado topic=%s group=%s metrics_port=%s",
            TOPIC,
            GROUP,
            METRICS_PORT,
        )

        while not stop.is_set():
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue

                KAFKA_ERRORS.labels("poll").inc()
                logger.error("Erro de leitura Kafka: %s", message.error())
                continue

            RECEIVED.inc()

            if not process_message(message):
                break

            # Confirma apenas este evento apos seu tratamento.
            try:
                partitions = consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                if partitions and any(p.error for p in partitions):
                    raise RuntimeError(
                        "Kafka retornou erro na confirmacao do offset."
                    )

            except (KafkaException, RuntimeError):
                KAFKA_ERRORS.labels("commit").inc()
                logger.exception("Falha ao confirmar offset")
                raise

    finally:
        consumer.close()
        logger.info("Consumer encerrado")


if __name__ == "__main__":
    main()
