import os, json
from kafka import KafkaConsumer

broker = os.getenv("KAFKA_BROKER", "kafka:9092")
topic  = os.getenv("KAFKA_TOPIC", "events")
group  = os.getenv("GROUP_ID", "demo-consumer")

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=broker,
    group_id=group,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    key_deserializer=lambda b: json.loads(b.decode("utf-8")) if b else None,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

print(f"[consumer] listening on {broker} topic={topic} group={group}")
for msg in consumer:
    print("[consumer]", msg.value)
