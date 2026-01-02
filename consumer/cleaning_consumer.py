"""
Cleaning Consumer
-----------------
Lee de Kafka (3 tópicos), limpia registros y guarda en MongoDB.
Mongo está puesto en localhost:27018 = URI: mongodb://localhost:27018
"""

import json
import signal
import sys
from datetime import datetime
from typing import Optional

from confluent_kafka import Consumer, KafkaException
from dateutil import parser as dtparser
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

KAFKA_BOOTSTRAP = "localhost:9092"
TOPICS = ["dirty_events", "dirty_metadata", "dirty_reference"]


MONGO_URI = "mongodb://localhost:27018"
DB_NAME = "streaming_demo"

def parse_timestamp(x) -> Optional[str]:
    """Convierte varios formatos (ISO, dd-mm-YYYY, YYYY/mm/dd, epoch ms) a ISO 8601."""
    if x in (None, "", "null"):
        return None
    try:
        # epoch en ms como string o número
        if isinstance(x, (int, float)) or (isinstance(x, str) and x.isdigit()):
            ms = int(x)
            dt = datetime.fromtimestamp(ms / 1000.0)
            return dt.isoformat()
        # dateutil maneja ISO, YYYY/mm/dd HH:MM:SS, dd-mm-YYYY HH:MM:SS, etc.
        dt = dtparser.parse(str(x))
        return dt.isoformat()
    except Exception:
        return None

def to_float(x) -> Optional[float]:
    """Convierte enteros/strings a float. Devuelve None si no se puede."""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    # remover posibles sufijos " units"
    if s.endswith(" units"):
        s = s.replace(" units", "")
    # "N/A", texto, etc.
    try:
        return float(s)
    except Exception:
        return None

def normalize_text(x) -> Optional[str]:
    if x is None:
        return None
    return str(x).strip().lower()

def main():
    # --- Kafka consumer ---
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "cleaning-consumer-1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)
    consumer.subscribe(TOPICS)

    # --- Mongo ---
    mclient = MongoClient(MONGO_URI)
    db = mclient[DB_NAME]

    raw_events = db["raw_dirty.events"]
    raw_meta = db["raw_dirty.metadata"]
    raw_ref = db["raw_dirty.reference"]
    clean_events = db["cleaned.events"]
    clean_meta = db["cleaned.metadata"]
    clean_ref = db["cleaned.reference"]

    # índices (dedupe por event_id en cleaned.events)
    clean_events.create_index([("event_id", ASCENDING)], unique=True)
    # (si quisiera índices para entity_id en todas)
    for col in [raw_events, raw_meta, raw_ref, clean_events, clean_meta, clean_ref]:
        col.create_index([("entity_id", ASCENDING)])

    print(" Cleaning Consumer escuchando tópicos:", TOPICS)
    running = True

    def handle_sig(sig, frame):
        nonlocal running
        print("\nDeteniendo consumidor…")
        running = False

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            topic = msg.topic()
            key = msg.key().decode("utf-8") if msg.key() else None
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception:
                print(f" Mensaje no-JSON en {topic}: {msg.value()[:100]!r}")
                continue

            # --- Guardado RAW (siempre) ---
            if topic == "dirty_events":
                raw_events.insert_one({**payload, "_topic": topic, "_k": key, "_ingested_at": datetime.now()})
                # Limpieza de events
                event_id = payload.get("event_id")
                entity_id = payload.get("entity_id")
                ts = parse_timestamp(payload.get("timestamp"))
                value = to_float(payload.get("value"))
                cleaned = {
                    "event_id": event_id,
                    "entity_id": entity_id,
                    "timestamp_iso": ts,
                    "value": value,
                    # flags de calidad
                    "is_null_ts": ts is None,
                    "is_null_value": value is None,
                }
                try:
                    clean_events.insert_one(cleaned)
                except DuplicateKeyError:
                    # Duplicado por event_id
                    pass

            elif topic == "dirty_metadata":
                raw_meta.insert_one({**payload, "_topic": topic, "_k": key, "_ingested_at": datetime.now()})
                # Limpieza de metadata
                cleaned = {
                    "entity_id": payload.get("entity_id"),
                    "category": normalize_text(payload.get("category")),
                    "geo": normalize_text(payload.get("geo")),
                    "platform": normalize_text(payload.get("platform")),
                }
                clean_meta.insert_one(cleaned)

            elif topic == "dirty_reference":
                raw_ref.insert_one({**payload, "_topic": topic, "_k": key, "_ingested_at": datetime.now()})
                # Limpieza de reference
                cleaned = {
                    "entity_id": payload.get("entity_id"),
                    "baseline_mean": to_float(payload.get("baseline_mean")),
                    "baseline_std": to_float(payload.get("baseline_std")),
                    "label": normalize_text(payload.get("label")),
                    "is_null_mean": to_float(payload.get("baseline_mean")) is None,
                    "is_null_std": to_float(payload.get("baseline_std")) is None,
                }
                clean_ref.insert_one(cleaned)

            # commit manual por lote (simple)
            consumer.commit(asynchronous=True)

    finally:
        consumer.close()
        mclient.close()
        print(" Consumer cerrado.")

if __name__ == "__main__":
    main()
