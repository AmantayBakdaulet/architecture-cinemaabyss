import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from fastapi import FastAPI
from pydantic import BaseModel

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
TOPICS = ["movie-events", "user-events", "payment-events"]

log = logging.getLogger("uvicorn.error")
producer = None


class MovieEvent(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: int | None = None
    rating: float | None = None
    genres: list[str] | None = None
    description: str | None = None


class UserEvent(BaseModel):
    user_id: int
    username: str | None = None
    email: str | None = None
    action: str
    timestamp: datetime


class PaymentEvent(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str
    timestamp: datetime
    method_type: str | None = None


async def start(client):
    while True:
        try:
            await client.start()
            return
        except KafkaConnectionError:
            log.info("kafka is not ready, retry in 5s")
            await asyncio.sleep(5)


async def consume():
    consumer = AIOKafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BROKERS,
        group_id="events-service",
        auto_offset_reset="earliest",
    )
    await start(consumer)
    async for msg in consumer:
        log.info("consumed %s partition=%s offset=%s: %s", msg.topic, msg.partition, msg.offset, msg.value.decode())


@asynccontextmanager
async def lifespan(app):
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKERS)
    await start(producer)
    task = asyncio.create_task(consume())
    yield
    task.cancel()
    await producer.stop()


app = FastAPI(lifespan=lifespan)


async def publish(event_type, event_id, data):
    event = {
        "id": event_id,
        "type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": data.model_dump(mode="json"),
    }
    meta = await producer.send_and_wait(f"{event_type}-events", json.dumps(event).encode())
    log.info("produced %s partition=%s offset=%s", event_id, meta.partition, meta.offset)
    return {"status": "success", "partition": meta.partition, "offset": meta.offset, "event": event}


@app.get("/api/events/health")
def health():
    return {"status": True}


@app.post("/api/events/movie", status_code=201)
async def movie_event(data: MovieEvent):
    return await publish("movie", f"movie-{data.movie_id}-{data.action}", data)


@app.post("/api/events/user", status_code=201)
async def user_event(data: UserEvent):
    return await publish("user", f"user-{data.user_id}-{data.action}", data)


@app.post("/api/events/payment", status_code=201)
async def payment_event(data: PaymentEvent):
    return await publish("payment", f"payment-{data.payment_id}-{data.status}", data)
