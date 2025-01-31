import time
import random
from kafka import KafkaProducer
import event_pb2  # File hasil generate Protobuf

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: v.SerializeToString(),
)

while True:
    event = event_pb2.Event(
        user_id=random.randint(1, 100),
        amount=round(random.uniform(10, 500), 2),
        timestamp=int(time.time()),
    )
    producer.send("test-topic", event)
    print(f"Sent Protobuf Event: {event}")
    time.sleep(5)
