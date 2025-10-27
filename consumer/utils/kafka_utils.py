from confluent_kafka import Producer
import socket

def get_producer(bootstrap_servers: str = "kafka:9092") -> Producer:
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": socket.gethostname(),
        "enable.idempotence": False,
        "linger.ms": 10,
        "batch.num.messages": 10000,
    }
    return Producer(conf)
