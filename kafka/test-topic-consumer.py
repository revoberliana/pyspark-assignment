import json
from collections import defaultdict
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "test-topic",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    group_id="consumer-group-1",
    auto_offset_reset="earliest",
)

user_totals = defaultdict(float)

for message in consumer:
    event = message.value
    user_id = event["user_id"]
    amount = event["amount"]

    user_totals[user_id] += amount
    print(f"User {user_id} Total Amount: {user_totals[user_id]:.2f}")
