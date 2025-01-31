from kafka import KafkaConsumer
import event_pb2  # Import file hasil Protobuf

consumer = KafkaConsumer(
    "test-topic",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: event_pb2.Event.FromString(x),
    group_id="consumer-group-1",
    auto_offset_reset="earliest",
)

for message in consumer:
    event = message.value
    print(f"User {event.user_id} | Amount: {event.amount} | Timestamp: {event.timestamp}")
