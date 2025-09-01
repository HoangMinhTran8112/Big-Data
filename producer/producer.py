import json, os, time, random, datetime
from kafka import KafkaProducer

broker = os.getenv("KAFKA_BROKER", "kafka:9092")
topic  = os.getenv("KAFKA_TOPIC", "events")

producer = KafkaProducer(
    bootstrap_servers=broker,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: json.dumps(k).encode("utf-8") if k else None,
    retries=5,
)

print(f"[producer] sending to {broker} topic={topic}")
i = 0
while True:
    i += 1
    event = {
        "event_id": i,
        "ts": datetime.datetime.utcnow().isoformat() + "Z",
        "device_id": f"d-{random.randint(1,5)}",
        "reading": round(random.uniform(10, 50), 2),
        "status": random.choice(["ok", "warn", "alert"]),
    }
    producer.send(topic, value=event, key={"device_id": event["device_id"]})
    if i % 50 == 0:
        producer.flush()
        print(f"[producer] sent {i} messages")
    time.sleep(1)
