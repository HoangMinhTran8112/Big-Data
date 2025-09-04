import os, json
from kafka import KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("TOPIC", "weather.raw")
GROUP_ID = os.getenv("GROUP_ID", "weather-consumer")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,
    enable_auto_commit=True,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"[consumer] consuming from {BOOTSTRAP} topic={TOPIC}")
for msg in consumer:
    print("[consumer]", msg.topic, msg.partition, msg.offset, "=>", msg.value.get("name"), msg.value.get("dt"))
