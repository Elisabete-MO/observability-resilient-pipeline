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

KAFKA_EXCEPTION_TYPES = (
    (KafkaException, RuntimeError)
    if isinstance(KafkaException, type) and issubclass(KafkaException, BaseException)
    else (RuntimeError,)
)
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
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
RATE_LIMIT_RPS = int(os.getenv("PRODUCER_RATE_LIMIT_RPS", "0"))
WAIT_FOR_ACK = os.getenv("PRODUCER_WAIT_FOR_ACK", "true").lower() in ("true", "1", "yes")
ACK_TIMEOUT_SECONDS = float(os.getenv("PRODUCER_ACK_TIMEOUT_SECONDS", "2.0"))
MAX_CONCURRENT_PRODUCES = int(os.getenv("PRODUCER_MAX_CONCURRENT_PRODUCES", "500"))


class TokenBucketLimiter:
    """Limitador de taxa em memoria (Token Bucket) defensivo."""

    def __init__(self, rps: int):
        self.capacity = float(rps)
        self.tokens = float(rps)
        self.rate = float(rps)
        self.last_update = time.monotonic()
        self.lock = threading.Lock()

    def allow(self) -> bool:
        if self.rate <= 0:
            return True
        with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.last_update = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            return False


limiter = TokenBucketLimiter(RATE_LIMIT_RPS)

# Metricas obrigatorias conforme Escopo.pdf (Requisito 1)
HTTP_REQUESTS = Counter(
    "http_requests_total",
    "Total de requisicoes recebidas.",
    ["status_code", "method"],
)
HTTP_DURATION = Histogram(
    "http_request_duration_seconds",
    "Latencia do processamento da requisicao HTTP.",
    buckets=(0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
KAFKA_PUBLISHED_EVENTS = Counter(
    "kafka_producer_published_events_total",
    "Mensagens enviadas com sucesso ao Kafka.",
)
KAFKA_PRODUCER_ERRORS = Counter(
    "kafka_producer_errors_total",
    "Falhas ao tentar enviar ao Kafka.",
)
DELIVERY_DURATION = Histogram(
    "kafka_producer_delivery_duration_seconds",
    "Tempo ate a confirmacao ou falha de entrega.",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
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
            raise ValueError(
                "Informe o fuso horario, por exemplo Z ou +00:00."
            )
        return value.astimezone(timezone.utc)


def poll_kafka(producer: Producer, stop: threading.Event) -> None:
    # Executa os callbacks de confirmacao de entrega.
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
    app.state.produce_semaphore = asyncio.Semaphore(MAX_CONCURRENT_PRODUCES)
    worker.start()

    try:
        yield
    finally:
        stop.set()
        await asyncio.to_thread(worker.join)
        remaining = await asyncio.to_thread(producer.flush, 10)

        if remaining:
            logger.error(
                "Encerramento com %s mensagens pendentes",
                remaining,
            )


app = FastAPI(
    title="Pipeline Producer API",
    version="1.0.0",
    lifespan=lifespan,
)

# Protecao contra requisicoes forjadas de origens cruzadas (CORS / CSRF)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
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
        # Registra metricas HTTP sem poluir com endpoints operacionais de infraestrutura
        if request.url.path not in ("/metrics", "/health", "/ready"):
            HTTP_REQUESTS.labels(
                status_code=str(status),
                method=request.method,
            ).inc()
            HTTP_DURATION.observe(time.perf_counter() - started)


@app.middleware("http")
async def security_and_limits_middleware(request: Request, call_next):
    # Protege contra requisicoes gigantes com Content-Length abusivo antes do parse (CWE-400)
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > MAX_EVENT_BYTES:
                return JSONResponse(
                    status_code=413,
                    content={"detail": "Payload excede o limite maximo de 64 KiB."},
                    headers={
                        "X-Content-Type-Options": "nosniff",
                        "X-Frame-Options": "DENY",
                    },
                )
        except ValueError:
            return JSONResponse(
                status_code=400,
                content={"detail": "Content-Length invalido."},
            )

    # Rate limiting defensivo (quando configurado > 0)
    if RATE_LIMIT_RPS > 0 and request.url.path in ("/api/v1/events", "/events"):
        if not limiter.allow():
            return JSONResponse(
                status_code=429,
                content={"detail": "Taxa de requisicoes excedida."},
                headers={"Retry-After": "1"},
            )

    response = await call_next(request)

    # Adiciona headers de seguranca HTTP defensivos (OWASP)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    return response


@app.get("/health")
def health():
    # Indica apenas que o processo HTTP responde.
    return {"status": "ok"}


@app.get("/ready")
def ready(request: Request):
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
            or any(
                partition.leader < 0 or partition.error is not None
                for partition in topic.partitions.values()
            )
        ):
            raise HTTPException(503, "Topico Kafka indisponivel.")

    except KAFKA_EXCEPTION_TYPES:
        raise HTTPException(503, "Kafka indisponivel.") from None

    return {"status": "ready", "topic": TOPIC}


@app.get("/metrics")
def metrics():
    return Response(
        content=generate_latest(),
        headers={"Content-Type": CONTENT_TYPE_LATEST},
    )


async def handle_event_ingestion(event: Event, request: Request):
    try:
        encoded = json.dumps(
            event.model_dump(mode="json"),
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise HTTPException(
            422,
            "O evento deve conter valores JSON validos.",
        ) from None

    # Limite do evento serializado que sera publicado no Kafka
    if len(encoded) > MAX_EVENT_BYTES:
        raise HTTPException(
            413,
            "O evento deve ter no maximo 64 KiB.",
        )

    # Obter semaforo de concorrencia para controle de backpressure
    produce_semaphore = getattr(request.app.state, "produce_semaphore", None)
    if produce_semaphore is None:
        request.app.state.produce_semaphore = asyncio.Semaphore(MAX_CONCURRENT_PRODUCES)
        produce_semaphore = request.app.state.produce_semaphore

    # Se a fila de espera do semaforo estiver cheia, falha rapido (backpressure / anti-DoS)
    if produce_semaphore.locked():
        raise HTTPException(
            503,
            "Producer sobrecarregado (concorrencia maxima de publicacao atingida). Tente novamente.",
        )

    started = time.perf_counter()
    loop = asyncio.get_running_loop()
    delivery_future = loop.create_future() if WAIT_FOR_ACK else None

    def on_delivery(error, message):
        DELIVERY_DURATION.observe(time.perf_counter() - started)
        if error is not None:
            KAFKA_PRODUCER_ERRORS.inc()
            logger.error("Falha na entrega Kafka para event_id=%s: %s", event.event_id, error)
            if delivery_future is not None and not delivery_future.done():
                exc = error if isinstance(error, BaseException) else RuntimeError(str(error))
                loop.call_soon_threadsafe(
                    delivery_future.set_exception,
                    exc,
                )
        else:
            KAFKA_PUBLISHED_EVENTS.inc()
            if delivery_future is not None and not delivery_future.done():
                loop.call_soon_threadsafe(
                    delivery_future.set_result,
                    (message.partition(), message.offset()),
                )

    async with produce_semaphore:
        try:
            request.app.state.producer.produce(
                topic=TOPIC,
                key=str(event.event_id),
                value=encoded,
                on_delivery=on_delivery,
            )
            request.app.state.producer.poll(0)
        except BufferError:
            KAFKA_PRODUCER_ERRORS.inc()
            logger.error("Buffer do Producer cheio para event_id=%s", event.event_id)
            raise HTTPException(
                503,
                "Buffer do Producer cheio.",
            ) from None
        except KAFKA_EXCEPTION_TYPES as exc:
            KAFKA_PRODUCER_ERRORS.inc()
            logger.exception("Falha ao enfileirar no Kafka event_id=%s: %s", event.event_id, exc)
            raise HTTPException(
                503,
                "Nao foi possivel publicar no Kafka.",
            ) from None

        if not WAIT_FOR_ACK:
            return {
                "status": "published",
                "event_id": str(event.event_id),
                "topic": TOPIC,
            }

        try:
            partition, offset = await asyncio.wait_for(
                delivery_future,
                timeout=ACK_TIMEOUT_SECONDS,
            )
            return {
                "status": "published",
                "event_id": str(event.event_id),
                "topic": TOPIC,
                "partition": partition,
                "offset": offset,
            }
        except asyncio.TimeoutError:
            KAFKA_PRODUCER_ERRORS.inc()
            logger.error("Timeout aguardando ACK do Kafka para event_id=%s apos %.2fs", event.event_id, ACK_TIMEOUT_SECONDS)
            raise HTTPException(
                504,
                f"Timeout aguardando persistencia do evento no Kafka ({ACK_TIMEOUT_SECONDS}s).",
            ) from None
        except KAFKA_EXCEPTION_TYPES as exc:
            raise HTTPException(
                503,
                f"Falha de persistencia no Kafka: {exc}",
            ) from None


@app.post("/api/v1/events", status_code=202)
async def publish_event_v1(event: Event, request: Request):
    return await handle_event_ingestion(event, request)


@app.post("/events", status_code=202)
async def publish_event(event: Event, request: Request):
    return await handle_event_ingestion(event, request)