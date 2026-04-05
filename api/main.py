from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from datetime import datetime, timezone
from kafka import KafkaProducer
import json
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="IoT Ingestion API")

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka.data-plane.svc.cluster.local:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

VALID_API_KEYS = set(os.getenv("API_KEYS", "dev-key-001").split(","))


class TelemetryPayload(BaseModel):
    device_id: str
    temperature: float
    humidity: float


@app.post("/telemetry")
async def ingest_telemetry(
    payload: TelemetryPayload,
    x_api_key: str = Header(...),
):
    if x_api_key not in VALID_API_KEYS:
        raise HTTPException(status_code=401, detail="Invalid API key")

    message = {
        "device_id": payload.device_id,
        "temperature": payload.temperature,
        "humidity": payload.humidity,
        "received_at": datetime.now(timezone.utc).isoformat(),
    }

    producer.send(
        topic="telemetry",
        value=message,
        key=payload.device_id.encode("utf-8"),
    )
    producer.flush()

    logger.info(f"Received telemetry from {payload.device_id}")

    return {"status": "ok", "received_at": message["received_at"]}


@app.get("/health")
async def health():
    return {"status": "ok"}