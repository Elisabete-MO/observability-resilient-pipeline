import asyncio
import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated, Any
from uuid import UUID

from confluent_kafka import KafkaException, Producer
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Histogram,
    generate_latest,
)
from pydantic import BaseModel, ConfigDict, Field, field_validator


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("producer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "raw-events")
MAX_EVENT_BYTES = 64 * 1024
CONFIRMATION_TIMEOUT_SECONDS = 12

HTTP_REQUESTS = Counter(
    "producer_http_requests_total",
    "Requisicoes HTTP por rota e status.",
    ["route", "status"],
)
HTTP_DURATION = Histogram(
    "producer_http_request_duration_seconds",
    "Duracao das requisicoes HTTP.",
    ["route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15),
)
PUBLISH_RESULTS = Counter(
    "producer_publish_results_total",
    "Resultados das tentativas de publicacao.",
    ["result"],
)
DELIVERY_DURATION = Histogram(
    "producer_kafka_delivery_duration_seconds",
    "Tempo ate a confirmacao ou falha de entrega.",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15),
)


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    event_type: Annotated[
        str,
        Field(
            min_length=1,
            max_length=80,
            pattern=r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$",
        ),
    ]
    occurred_at: datetime
    payload: dict[str, Any]

    @field_validator("occurred_at")
    @classmethod
    def validate_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Informe o fuso horario, por exemplo Z ou +00:00.")
        return value.astimezone(timezone.utc)


def poll_kafka(producer: Producer, stop: threading.Event) -> None:
    # Os callbacks de entrega sao executados durante poll().
    while not stop.is_set():
        producer.poll(0.1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": "producer-api",
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": 10000,
            "request.timeout.ms": 5000,
        }
    )
    stop = threading.Event()
    worker = threading.Thread(
        target=poll_kafka,
        args=(producer, stop),
        daemon=True,
    )

    app.state.producer = producer
    worker.start()

    try:
        yield
    finally:
        stop.set()
        await asyncio.to_thread(worker.join)
        remaining = await asyncio.to_thread(producer.flush, 10)
        if remaining:
            logger.error("Encerramento com %s mensagens pendentes", remaining)


app = FastAPI(
    title="Pipeline Producer API",
    version="0.1.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def measure_requests(request: Request, call_next):
    started = time.perf_counter()
    status = 500

    try:
        response = await call_next(request)
        status = response.status_code
        return response
    finally:
        route = request.scope.get("route")
        route_name = route.path if route else "unmatched"

        # Evita contabilizar as coletas do proprio Prometheus.
        if route_name != "/metrics":
            HTTP_REQUESTS.labels(route_name, str(status)).inc()
            HTTP_DURATION.labels(route_name).observe(
                time.perf_counter() - started
            )


@app.get("/health")
def health():
    # Confirma que o processo HTTP esta respondendo.
    return {"status": "ok"}


@app.get("/ready")
def ready(request: Request):
    # Consulta os metadados para verificar acesso ao topico.
    try:
        metadata = request.app.state.producer.list_topics(
            topic=TOPIC,
            timeout=3,
        )
        topic = metadata.topics.get(TOPIC)

        if (
            topic is None
            or topic.error is not None
            or not topic.partitions
            or any(partition.leader < 0 for partition in topic.partitions.values())
        ):
            raise HTTPException(503, "Topico Kafka indisponivel.")

    except KafkaException:
        raise HTTPException(503, "Kafka indisponivel.") from None

    return {"status": "ready", "topic": TOPIC}


@app.get("/metrics")
def metrics():
    return Response(
        content=generate_latest(),
        headers={"Content-Type": CONTENT_TYPE_LATEST},
    )


@app.post("/events", status_code=202)
def publish_event(event: Event, request: Request):
    encoded = json.dumps(
        event.model_dump(mode="json"),
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")

    if len(encoded) > MAX_EVENT_BYTES:
        raise HTTPException(413, "O evento deve ter no maximo 64 KiB.")

    completed = threading.Event()
    outcome: dict[str, Any] = {}
    started = time.perf_counter()

    def on_delivery(error, message):
        DELIVERY_DURATION.observe(time.perf_counter() - started)

        if error is not None:
            PUBLISH_RESULTS.labels("failed").inc()
            outcome["error"] = str(error)
        else:
            PUBLISH_RESULTS.labels("confirmed").inc()
            outcome["partition"] = message.partition()
            outcome["offset"] = message.offset()

        completed.set()

    try:
        request.app.state.producer.produce(
            topic=TOPIC,
            key=str(event.event_id),
            value=encoded,
            on_delivery=on_delivery,
        )
    except (BufferError, KafkaException):
        PUBLISH_RESULTS.labels("enqueue_failed").inc()
        logger.exception("Falha ao enfileirar event_id=%s", event.event_id)
        raise HTTPException(503, "Nao foi possivel publicar no Kafka.") from None

    if not completed.wait(CONFIRMATION_TIMEOUT_SECONDS):
        logger.error("Confirmacao expirada event_id=%s", event.event_id)
        return JSONResponse(
            status_code=503,
            content={
                "detail": "Confirmacao nao recebida no prazo.",
                "event_id": str(event.event_id),
                "status": "unknown",
            },
        )

    if "error" in outcome:
        logger.error(
            "Falha de entrega event_id=%s error=%s",
            event.event_id,
            outcome["error"],
        )
        raise HTTPException(503, "Kafka nao confirmou a publicacao.")

    logger.info(
        "Publicado event_id=%s partition=%s offset=%s",
        event.event_id,
        outcome["partition"],
        outcome["offset"],
    )

    return {
        "status": "published",
        "event_id": str(event.event_id),
        "topic": TOPIC,
        "partition": outcome["partition"],
        "offset": outcome["offset"],
    }
