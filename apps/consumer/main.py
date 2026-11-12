import json
import logging
import os
import random
import re
import signal
import threading
import time
from datetime import datetime, timezone
from uuid import UUID

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)
from prometheus_client import Counter, Gauge, Histogram, start_http_server


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("consumer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "raw-events")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "raw-events-dlq")
GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "events-processor")
METRICS_PORT = int(os.getenv("CONSUMER_METRICS_PORT", "8001"))

MIN_DELAY_MS = int(os.getenv("PROCESSING_MIN_MS", "50"))
MAX_DELAY_MS = int(os.getenv("PROCESSING_MAX_MS", "200"))
ERROR_RATE = float(os.getenv("PROCESSING_ERROR_RATE", "0"))
DLQ_PUBLISH_TIMEOUT_SECONDS = float(
    os.getenv("DLQ_PUBLISH_TIMEOUT_SECONDS", "5")
)

MAX_EVENT_BYTES = 64 * 1024

if not 0 <= MIN_DELAY_MS <= MAX_DELAY_MS <= 30000:
    raise ValueError(
        "Os tempos devem respeitar 0 <= minimo <= maximo <= 30000."
    )

if not 0 <= ERROR_RATE <= 1:
    raise ValueError("PROCESSING_ERROR_RATE deve estar entre 0 e 1.")

if DLQ_PUBLISH_TIMEOUT_SECONDS <= 0:
    raise ValueError("DLQ_PUBLISH_TIMEOUT_SECONDS deve ser maior que zero.")

if not 1 <= METRICS_PORT <= 65535:
    raise ValueError("Porta de metricas invalida.")

# Metricas obrigatorias conforme Escopo.pdf (Requisito 2)
PROCESSED = Counter(
    "kafka_consumer_processed_events_total",
    "Eventos processados pelo worker.",
    ["status"],
)
DURATION = Histogram(
    "kafka_consumer_processing_duration_seconds",
    "Tempo total de processamento individual.",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
LAG_RECORDS = Gauge(
    "kafka_consumer_lag_records",
    "Diferenca entre a ultima mensagem do topico e o offset atual.",
    ["partition"],
)
KAFKA_ERRORS = Counter(
    "kafka_consumer_errors_total",
    "Erros de leitura ou confirmacao no Kafka.",
    ["operation"],
)
DLQ_EVENTS = Counter(
    "kafka_consumer_dlq_events_total",
    "Mensagens isoladas na Dead Letter Queue.",
    ["reason"],
)

# Inicializa as series obrigatorias
for status in ("success", "error"):
    PROCESSED.labels(status=status)

for p in ("0", "1", "2"):
    LAG_RECORDS.labels(partition=p).set(0)

stop = threading.Event()


def handle_shutdown(signum, frame):
    logger.info("Encerramento solicitado")
    stop.set()


def reject_constant(value):
    raise ValueError(f"Constante JSON invalida: {value}")


def validate_event(raw: bytes | None) -> dict:
    if raw is None:
        raise ValueError("Evento sem conteudo.")

    if len(raw) > MAX_EVENT_BYTES:
        raise ValueError("Evento acima de 64 KiB.")

    event = json.loads(raw, parse_constant=reject_constant)

    if not isinstance(event, dict):
        raise ValueError("O evento deve ser um objeto JSON.")

    expected_fields = {
        "event_id",
        "event_type",
        "occurred_at",
        "payload",
    }

    if set(event) != expected_fields:
        raise ValueError("Campos ausentes ou desconhecidos no evento.")

    if not isinstance(event["event_id"], str):
        raise ValueError("event_id deve ser um UUID em texto.")

    UUID(event["event_id"])

    event_type = event["event_type"]

    if (
        not isinstance(event_type, str)
        or len(event_type) > 80
        or re.fullmatch(
            r"[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+",
            event_type,
        ) is None
    ):
        raise ValueError("event_type possui formato invalido.")

    occurred_at = event["occurred_at"]

    if not isinstance(occurred_at, str):
        raise ValueError("occurred_at deve ser texto ISO 8601.")

    timestamp = datetime.fromisoformat(occurred_at)

    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("occurred_at deve informar o fuso horario.")

    event["occurred_at"] = timestamp.astimezone(timezone.utc).isoformat()

    if not isinstance(event["payload"], dict):
        raise ValueError("payload deve ser um objeto JSON.")

    return event


def update_consumer_lag(consumer: Consumer) -> None:
    """Calcula o lag entre o high watermark do Kafka e o offset atual por particao."""
    try:
        assigned = consumer.assignment()
        if not assigned:
            return
        for tp in assigned:
            low, high = consumer.get_watermark_offsets(tp, timeout=0.2)
            positions = consumer.position([tp])
            current = (
                positions[0].offset
                if positions and positions[0].offset >= 0
                else 0
            )
            lag = max(0, high - current)
            LAG_RECORDS.labels(partition=str(tp.partition)).set(lag)
    except Exception as exc:
        logger.debug("Nao foi possivel calcular lag da particao: %s", exc)


def send_to_dlq(dlq_producer: Producer | None, message, reason: str) -> bool:
    """Envia para a DLQ e so retorna True apos a confirmacao do broker.

    O offset original so pode ser confirmado quando esta funcao retornar True.
    """
    if dlq_producer is None:
        KAFKA_ERRORS.labels("dlq").inc()
        logger.error(
            "DLQ indisponivel; offset nao sera confirmado partition=%s offset=%s",
            message.partition(),
            message.offset(),
        )
        return False

    delivered = threading.Event()
    delivery_result = {"ok": False}

    def on_delivery(error, _dlq_message):
        if error is not None:
            logger.error(
                "Falha na confirmacao de entrega na DLQ partition=%s offset=%s: %s",
                message.partition(),
                message.offset(),
                error,
            )
        else:
            delivery_result["ok"] = True
        delivered.set()

    try:
        headers = [
            ("x-error-reason", reason.encode("utf-8")),
            ("x-original-topic", (message.topic() or "").encode("utf-8")),
            ("x-original-partition", str(message.partition()).encode("utf-8")),
            ("x-original-offset", str(message.offset()).encode("utf-8")),
            ("x-failed-at", datetime.now(timezone.utc).isoformat().encode("utf-8")),
        ]
        key = message.key()
        value = message.value() or b""
        dlq_producer.produce(
            topic=DLQ_TOPIC,
            key=key,
            value=value,
            headers=headers,
            on_delivery=on_delivery,
        )

        # poll() serve os callbacks de entrega enquanto o broker confirma.
        deadline = time.monotonic() + DLQ_PUBLISH_TIMEOUT_SECONDS
        while not delivered.is_set() and time.monotonic() < deadline:
            dlq_producer.poll(0.1)

        if not delivered.is_set():
            KAFKA_ERRORS.labels("dlq").inc()
            logger.error(
                "Timeout aguardando confirmacao da DLQ (%.2fs) partition=%s offset=%s; "
                "offset nao sera confirmado",
                DLQ_PUBLISH_TIMEOUT_SECONDS,
                message.partition(),
                message.offset(),
            )
            return False

        if not delivery_result["ok"]:
            KAFKA_ERRORS.labels("dlq").inc()
            logger.error(
                "Mensagem nao confirmada na DLQ partition=%s offset=%s; "
                "offset nao sera confirmado",
                message.partition(),
                message.offset(),
            )
            return False

        DLQ_EVENTS.labels(reason=reason).inc()
        logger.warning(
            "Mensagem isolada na DLQ topic=%s partition=%s offset=%s reason=%s",
            DLQ_TOPIC,
            message.partition(),
            message.offset(),
            reason,
        )
        return True
    except Exception as exc:
        KAFKA_ERRORS.labels("dlq").inc()
        logger.error("Falha ao publicar mensagem na DLQ: %s", exc)
        return False


def process_message(message, dlq_producer: Producer | None = None) -> bool:
    """False indica interrupcao: o offset deve permanecer sem commit."""
    started = time.perf_counter()

    try:
        event = validate_event(message.value())
        delay = random.uniform(MIN_DELAY_MS, MAX_DELAY_MS) / 1000

        if stop.wait(delay):
            return False

        if random.random() < ERROR_RATE:
            PROCESSED.labels(status="error").inc()

            if not send_to_dlq(dlq_producer, message, "simulated_error"):
                logger.error(
                    "Erro simulado nao isolado na DLQ; offset nao sera confirmado "
                    "event_id=%s partition=%s offset=%s",
                    event["event_id"],
                    message.partition(),
                    message.offset(),
                )
                return False

            logger.warning(
                "Erro simulado tratado event_id=%s partition=%s offset=%s",
                event["event_id"],
                message.partition(),
                message.offset(),
            )
        else:
            PROCESSED.labels(status="success").inc()
            logger.info(
                "Processado event_id=%s partition=%s offset=%s",
                event["event_id"],
                message.partition(),
                message.offset(),
            )

    except (
        ValueError,
        TypeError,
        KeyError,
        UnicodeError,
        OverflowError,
        RecursionError,
    ) as exc:
        PROCESSED.labels(status="error").inc()

        if not send_to_dlq(dlq_producer, message, "validation_error"):
            logger.error(
                "Evento invalido nao isolado na DLQ; offset nao sera confirmado "
                "partition=%s offset=%s: %s",
                message.partition(),
                message.offset(),
                exc,
            )
            return False

        logger.warning(
            "Evento invalido tratado e encaminhado para DLQ partition=%s offset=%s: %s",
            message.partition(),
            message.offset(),
            exc,
        )
    finally:
        DURATION.observe(time.perf_counter() - started)

    return True


def commit_offset(consumer: Consumer, message) -> None:
    """Confirma o offset somente apos o processamento ou o ACK da DLQ."""
    try:
        partitions = consumer.commit(
            message=message,
            asynchronous=False,
        )

        if partitions and any(
            partition.error for partition in partitions
        ):
            raise RuntimeError(
                "Kafka retornou erro na confirmacao do offset."
            )

    except (KafkaException, RuntimeError):
        KAFKA_ERRORS.labels("commit").inc()
        logger.exception("Falha ao confirmar offset")
        raise


def process_and_commit(
    consumer: Consumer,
    message,
    dlq_producer: Producer | None,
) -> bool:
    """Processa e so confirma o offset quando process_message autoriza."""
    if not process_message(message, dlq_producer=dlq_producer):
        logger.error(
            "Processamento interrompido sem commit partition=%s offset=%s",
            message.partition(),
            message.offset(),
        )
        return False

    commit_offset(consumer, message)
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

    dlq_producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": "consumer-dlq-producer",
            "acks": "all",
        }
    )

    last_lag_update = 0.0

    try:
        start_http_server(METRICS_PORT, addr="0.0.0.0")
        consumer.subscribe([TOPIC])

        logger.info(
            "Consumer iniciado topic=%s group=%s dlq=%s metrics_port=%s",
            TOPIC,
            GROUP,
            DLQ_TOPIC,
            METRICS_PORT,
        )

        while not stop.is_set():
            now = time.monotonic()
            if now - last_lag_update >= 1.0:
                update_consumer_lag(consumer)
                last_lag_update = now

            message = consumer.poll(0.5)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue

                KAFKA_ERRORS.labels("poll").inc()
                logger.error("Erro de leitura Kafka: %s", message.error())
                continue

            # O commit so acontece apos sucesso ou ACK efetivo da DLQ.
            if not process_and_commit(consumer, message, dlq_producer):
                break

    finally:
        consumer.close()
        dlq_producer.flush(5)
        logger.info("Consumer encerrado")


if __name__ == "__main__":
    main()