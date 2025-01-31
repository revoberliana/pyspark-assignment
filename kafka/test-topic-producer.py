import json
import random
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

while True:
    event = {
        "user_id": random.randint(1, 100),
        "amount": round(random.uniform(10, 500), 2),
        "timestamp": time.time(),
    }
    producer.send("test-topic", event)
    print(f"Sent: {event}")
    time.sleep(5)
