
'''
Dirty Data Producer
-------------------
Genera mensajes con errores intencionales hacia tres tópicos de Kafka:
  - dirty_events
  - dirty_metadata
  - dirty_reference

Ejemplo de uso (con Kafka arriba):
    python producer/dirty_producer.py --bootstrap localhost:9092 --rate 5
'''
import json, random, time, uuid
from datetime import datetime
from confluent_kafka import Producer
import argparse

TOPICS = {
    "events": "dirty_events",
    "metadata": "dirty_metadata",
    "reference": "dirty_reference",
}

CATEGORIES = ["office", "school", "tech", "art"]
GEO = ["mx-cdmx", "mx-ytz", "mx-jal", "mx-nl"]
PLATFORMS = ["android", "ios", "web"]

def ts_mixed(dt: datetime) -> str:
    # Formatos mezclados para provocar errores de timestamp
    choices = [
        dt.isoformat(),
        dt.strftime("%Y/%m/%d %H:%M:%S"),
        dt.strftime("%d-%m-%Y %H:%M:%S"),
        str(int(dt.timestamp() * 1000)),   # epoch ms
    ]
    return random.choice(choices)

def maybe_none(value, p=0.05):
    return None if random.random() < p else value

def maybe_corrupt_value(v):
    r = random.random()
    if r < 0.07:
        return "twenty"      # tipo string no numérico
    if r < 0.14:
        return str(v)        # número como texto
    return v

def build_event(entity_id: str, duplicate=False):
    now = datetime.utcnow()
    event_id = str(uuid.uuid4()) if not duplicate else entity_id + "-dup"
    value = round(random.uniform(0, 100), 2)
    payload = {
        "event_id": event_id,
        "entity_id": entity_id,
        "timestamp": ts_mixed(now),   # intencionalmente mixto
        "value": maybe_corrupt_value(value),
    }
    # Nulos intencionales
    if random.random() < 0.05: payload["timestamp"] = None
    if random.random() < 0.03: payload["value"] = None
    return payload

def build_metadata(entity_id: str):
    return {
        "entity_id": entity_id,
        "category": maybe_none(random.choice(CATEGORIES), 0.1),
        "geo": maybe_none(random.choice(GEO), 0.06),
        "platform": random.choice(PLATFORMS).upper() if random.random() < 0.2 else random.choice(PLATFORMS)
    }

def build_reference(entity_id: str):
    mean = random.uniform(30, 70)
    std = random.uniform(5, 15)
    if random.random() < 0.1:
        mean = f"{mean:.2f} units"    # unidad pegada
    if random.random() < 0.1:
        std = "N/A"                   # no numérico
    return {
        "entity_id": entity_id,
        "baseline_mean": mean,
        "baseline_std": std,
        "label": random.choice(["normal", "alert", "critical"]),
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")

def main(bootstrap: str, rate: int):
    producer = Producer({"bootstrap.servers": bootstrap})
    print(f"Sending to Kafka ({bootstrap}). Rate: {rate} msg/s per topic")
    i = 0
    entity_ids = [f"e-{n:04d}" for n in range(1, 51)]

    try:
        while True:
            i += 1
            ent = random.choice(entity_ids)
            ev = build_event(ent, duplicate=(random.random() < 0.10))  # 10% duplicados
            md = build_metadata(ent)
            rf = build_reference(ent)

            producer.produce(TOPICS["events"], key=ent, value=json.dumps(ev), callback=delivery_report)
            producer.produce(TOPICS["metadata"], key=ent, value=json.dumps(md), callback=delivery_report)
            producer.produce(TOPICS["reference"], key=ent, value=json.dumps(rf), callback=delivery_report)

            if i % 50 == 0:
                producer.flush(2.0)

            time.sleep(max(0.01, 1.0 / max(1, rate)))
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--rate", type=int, default=5)
    args = parser.parse_args()
    main(args.bootstrap, args.rate)
