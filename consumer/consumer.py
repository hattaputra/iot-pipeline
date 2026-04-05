from kafka import KafkaConsumer
from clickhouse_driver import Client
from datetime import datetime
import json
import os
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka.data-plane.svc.cluster.local:9092")
CLICKHOUSE_HOST    = os.getenv("CLICKHOUSE_HOST", "clickhouse.data-plane.svc.cluster.local")
CLICKHOUSE_USER    = os.getenv("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "iot123")
CLICKHOUSE_DB      = os.getenv("CLICKHOUSE_DB", "iot")


def get_clickhouse_client():
    return Client(
        host=CLICKHOUSE_HOST,
        database=CLICKHOUSE_DB,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def get_kafka_consumer():
    # Retry loop -- consumer mungkin start sebelum Kafka ready
    while True:
        try:
            consumer = KafkaConsumer(
                "telemetry",
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="telemetry-consumer-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            logger.info("Connected to Kafka")
            return consumer
        except Exception as e:
            logger.warning(f"Kafka not ready, retrying in 5s: {e}")
            time.sleep(5)


def main():
    ch = get_clickhouse_client()
    consumer = get_kafka_consumer()

    logger.info("Consumer started, waiting for messages...")

    for message in consumer:
        try:
            data = message.value

            ch.execute(
                "INSERT INTO telemetry (device_id, temperature, humidity, received_at) VALUES",
                [{
                    "device_id":   data["device_id"],
                    "temperature": float(data["temperature"]),
                    "humidity":    float(data["humidity"]),
                    "received_at": datetime.fromisoformat(data["received_at"]),
                }]
            )

            logger.info(f"Inserted: {data['device_id']} temp={data['temperature']} hum={data['humidity']}")

        except Exception as e:
            # Log dan lanjut -- jangan crash consumer karena satu bad message
            logger.error(f"Failed to insert: {e} | raw: {message.value}")


if __name__ == "__main__":
    main()