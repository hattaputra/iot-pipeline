"""
event_detector.py
-----------------
Kafka consumer (separate consumer group) yang membaca topic 'telemetry',
mendeteksi event Too Hot (27 < temp <= 30), dan mempersist ke ClickHouse.

State machine per device_id disimpan di Redis untuk:
  - Survival saat pod restart (zero data loss)
  - Recovery otomatis dari open events yang belum di-close

Flow:
  Kafka message
    -> parse JSON
    -> check threshold
    -> update Redis state machine
    -> kalau event close: flush ke ClickHouse, hapus state dari Redis

Gap timeout:
  Kalau last_seen > 60 detik yang lalu, event dianggap closed.
  Di-check setiap kali ada message masuk (lazy evaluation per device).
  Background thread juga sweep semua open events tiap 30 detik
  untuk handle device yang tiba-tiba berhenti kirim data.
"""

import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone, timedelta

import redis
from clickhouse_driver import Client as ClickHouseClient
from kafka import KafkaConsumer

# ── Config (dari env var, dengan fallback untuk development) ──────────────────

KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP", "kafka.data-plane.svc.cluster.local:9092")
KAFKA_TOPIC        = os.getenv("KAFKA_TOPIC", "telemetry")
KAFKA_GROUP_ID     = os.getenv("KAFKA_GROUP_ID", "event-detector")

REDIS_HOST         = os.getenv("REDIS_HOST", "redis.data-plane.svc.cluster.local")
REDIS_PORT         = int(os.getenv("REDIS_PORT", "6379"))

CH_HOST            = os.getenv("CH_HOST", "clickhouse.data-plane.svc.cluster.local")
CH_PORT            = int(os.getenv("CH_PORT", "9000"))
CH_USER            = os.getenv("CH_USER", "admin")
CH_PASSWORD        = os.getenv("CH_PASSWORD", "iot123")
CH_DATABASE        = os.getenv("CH_DATABASE", "iot")

TOO_HOT_MIN        = float(os.getenv("TOO_HOT_MIN", "27.0"))  # exclusive
TOO_HOT_MAX        = float(os.getenv("TOO_HOT_MAX", "30.0"))  # inclusive
GAP_TIMEOUT_SEC    = int(os.getenv("GAP_TIMEOUT_SEC", "60"))
SWEEP_INTERVAL_SEC = int(os.getenv("SWEEP_INTERVAL_SEC", "30"))

# Redis key prefix untuk isolasi namespace
REDIS_KEY_PREFIX   = "hot_event"

# ── Datetime helpers ──────────────────────────────────────────────────────────

def utcnow() -> datetime:
    """
    Return current UTC time sebagai naive datetime.
    Naive UTC dipakai konsisten di seluruh codebase supaya tidak ada
    mismatch saat subtract dua datetime (aware vs naive error).
    ClickHouse DateTime column juga expect naive UTC.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def parse_ts(ts: str) -> datetime:
    """
    Parse ISO timestamp string menjadi naive UTC datetime.
    Handle format dengan atau tanpa timezone suffix (Z, +00:00, dll).

    Catatan: pakai rfind('+') bukan split('-') supaya tidak salah potong
    karakter '-' yang ada di bagian tanggal (contoh: 2026-04-08).
    """
    ts_clean = ts.replace("Z", "")
    # Strip timezone offset (+HH:MM) dari kanan
    if "+" in ts_clean:
        ts_clean = ts_clean[:ts_clean.rfind("+")]
    dt = datetime.fromisoformat(ts_clean)
    return dt.replace(tzinfo=None)

# ── Logger ────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("event-detector")

# ── Graceful shutdown ─────────────────────────────────────────────────────────

shutdown_event = threading.Event()

def handle_sigterm(signum, frame):
    log.info("SIGTERM received. Initiating graceful shutdown...")
    shutdown_event.set()

signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)


# ── Redis helpers ─────────────────────────────────────────────────────────────

def redis_key(device_id: str) -> str:
    return f"{REDIS_KEY_PREFIX}:{device_id}"


def load_state(r: redis.Redis, device_id: str) -> dict | None:
    """Load open event state untuk device dari Redis. Return None kalau tidak ada."""
    raw = r.get(redis_key(device_id))
    if raw is None:
        return None
    return json.loads(raw)


def save_state(r: redis.Redis, device_id: str, state: dict) -> None:
    """Persist state ke Redis. Tidak ada TTL -- state hidup sampai event di-close."""
    r.set(redis_key(device_id), json.dumps(state))


def delete_state(r: redis.Redis, device_id: str) -> None:
    """Hapus state setelah event di-close dan di-flush ke ClickHouse."""
    r.delete(redis_key(device_id))


def list_open_events(r: redis.Redis) -> list[str]:
    """Return semua device_id yang punya open event di Redis."""
    keys = r.keys(f"{REDIS_KEY_PREFIX}:*")
    prefix_len = len(REDIS_KEY_PREFIX) + 1  # +1 untuk ':'
    return [k.decode()[prefix_len:] for k in keys]


# ── ClickHouse flush ──────────────────────────────────────────────────────────

def flush_event(ch: ClickHouseClient, device_id: str, state: dict) -> None:
    """
    Tulis satu closed event ke iot.too_hot_events.
    Idempotent: ReplacingMergeTree deduplikasi by (device_id, start_at).
    """
    start_at   = parse_ts(state["start_at"])
    end_at     = parse_ts(state["end_at"])
    duration   = int((end_at - start_at).total_seconds())
    readings   = state["readings"]
    avg_temp   = sum(readings) / len(readings) if readings else 0.0
    max_temp   = max(readings) if readings else 0.0
    version    = int(time.time())

    row = {
        "date":          start_at.date(),
        "device_id":     device_id,
        "start_at":      start_at,
        "end_at":        end_at,
        "duration_sec":  duration,
        "max_temp":      round(max_temp, 2),
        "avg_temp":      round(avg_temp, 2),
        "reading_count": len(readings),
        "_version":      version,
    }

    ch.execute(
        """
        INSERT INTO iot.too_hot_events
        (date, device_id, start_at, end_at, duration_sec,
         max_temp, avg_temp, reading_count, _version)
        VALUES
        """,
        [row],
    )

    log.info(
        "Event flushed | device=%s | start=%s | end=%s | duration=%ds | max=%.1f | avg=%.1f",
        device_id, state["start_at"], state["end_at"],
        duration, max_temp, avg_temp,
    )


# ── State machine ─────────────────────────────────────────────────────────────

def is_too_hot(temp: float) -> bool:
    return TOO_HOT_MIN < temp <= TOO_HOT_MAX


def process_reading(
    r: redis.Redis,
    ch: ClickHouseClient,
    device_id: str,
    temperature: float,
    received_at: datetime,
) -> None:
    """
    Core state machine. Dipanggil per message dari Kafka.

    State transitions:
      NORMAL + not hot  -> stay NORMAL (no-op)
      NORMAL + hot      -> OPEN event, simpan state ke Redis
      HOT    + hot      -> update state (last_seen, readings)
      HOT    + not hot  -> CLOSE event, flush ke ClickHouse, hapus state
      HOT    + gap > 60s -> CLOSE event (timeout), lalu proses reading ini
    """
    ts = received_at.isoformat()
    state = load_state(r, device_id)

    if state is not None:
        # Cek gap timeout sebelum proses reading ini
        last_seen = parse_ts(state["last_seen"])
        gap = (received_at - last_seen).total_seconds()

        if gap > GAP_TIMEOUT_SEC:
            log.info(
                "Gap timeout (%.0fs) | device=%s | closing event started at %s",
                gap, device_id, state["start_at"],
            )
            # Close event dengan last_seen sebagai end_at (bukan received_at)
            state["end_at"] = state["last_seen"]
            flush_event(ch, device_id, state)
            delete_state(r, device_id)
            state = None  # treat current reading sebagai fresh start

    if is_too_hot(temperature):
        if state is None:
            # Open new event
            state = {
                "start_at":  ts,
                "last_seen": ts,
                "end_at":    ts,
                "readings":  [temperature],
            }
            log.info("Event OPENED | device=%s | temp=%.1f | at=%s", device_id, temperature, ts)
        else:
            # Extend existing event
            state["last_seen"] = ts
            state["end_at"]    = ts
            state["readings"].append(temperature)

        save_state(r, device_id, state)

    else:
        if state is not None:
            # Temperature keluar range -> close event
            state["end_at"] = state["last_seen"]  # end_at = last hot reading
            flush_event(ch, device_id, state)
            delete_state(r, device_id)
            log.info("Event CLOSED | device=%s | temp back to %.1f", device_id, temperature)
        # else: normal reading, no open event -> no-op


# ── Background sweep ──────────────────────────────────────────────────────────

def gap_sweep_worker(r: redis.Redis, ch: ClickHouseClient) -> None:
    """
    Background thread yang sweep semua open events tiap SWEEP_INTERVAL_SEC.
    Menangani kasus device mati tiba-tiba tanpa ada reading berikutnya
    yang men-trigger timeout di process_reading().
    """
    log.info("Gap sweep worker started (interval=%ds)", SWEEP_INTERVAL_SEC)
    while not shutdown_event.is_set():
        shutdown_event.wait(SWEEP_INTERVAL_SEC)
        if shutdown_event.is_set():
            break

        open_devices = list_open_events(r)
        if not open_devices:
            continue

        now = utcnow()
        for device_id in open_devices:
            state = load_state(r, device_id)
            if state is None:
                continue

            last_seen = parse_ts(state["last_seen"])
            gap = (now - last_seen).total_seconds()

            if gap > GAP_TIMEOUT_SEC:
                log.info(
                    "Sweep timeout (%.0fs) | device=%s | closing event started at %s",
                    gap, device_id, state["start_at"],
                )
                state["end_at"] = state["last_seen"]
                flush_event(ch, device_id, state)
                delete_state(r, device_id)

    log.info("Gap sweep worker stopped.")


# ── Main consumer loop ────────────────────────────────────────────────────────

def main() -> None:
    log.info("Starting event-detector")
    log.info("Threshold: %.1f < temp <= %.1f | gap_timeout: %ds",
             TOO_HOT_MIN, TOO_HOT_MAX, GAP_TIMEOUT_SEC)

    # Init connections
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=False)
    r.ping()
    log.info("Redis connected: %s:%d", REDIS_HOST, REDIS_PORT)

    ch = ClickHouseClient(
        host=CH_HOST, port=CH_PORT,
        user=CH_USER, password=CH_PASSWORD,
        database=CH_DATABASE,
    )
    log.info("ClickHouse connected: %s:%d", CH_HOST, CH_PORT)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",   # event detector tidak perlu reprocess history
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    log.info("Kafka consumer connected | topic=%s | group=%s", KAFKA_TOPIC, KAFKA_GROUP_ID)

    # Recover open events dari Redis (pod restart recovery)
    open_devices = list_open_events(r)
    if open_devices:
        log.info("Recovered %d open event(s) from Redis: %s", len(open_devices), open_devices)
    else:
        log.info("No open events in Redis. Clean start.")

    # Start background sweep thread
    sweep_thread = threading.Thread(
        target=gap_sweep_worker,
        args=(r, ch),
        daemon=True,
        name="gap-sweep",
    )
    sweep_thread.start()

    # Main consume loop
    log.info("Consuming messages...")
    try:
        for message in consumer:
            if shutdown_event.is_set():
                break

            try:
                payload    = message.value
                device_id  = payload["device_id"]
                temperature = float(payload["temperature"])
                # Gunakan received_at dari payload kalau ada, fallback ke now
                if "received_at" in payload:
                    received_at = parse_ts(payload["received_at"])
                else:
                    received_at = utcnow()

                process_reading(r, ch, device_id, temperature, received_at)

            except (KeyError, ValueError) as e:
                # Malformed message -- log dan skip, jangan crash consumer
                log.warning("Malformed message skipped | error=%s | payload=%s", e, message.value)
            except Exception as e:
                log.error("Unexpected error processing message: %s", e, exc_info=True)

    finally:
        log.info("Shutting down consumer...")
        consumer.close()
        shutdown_event.set()
        sweep_thread.join(timeout=5)
        log.info("Event detector stopped.")


if __name__ == "__main__":
    main()