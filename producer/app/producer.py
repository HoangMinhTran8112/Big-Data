import json, os, time, requests
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("TOPIC", "weather.raw")
API_KEY = os.getenv("OPENWEATHERMAP_API_KEY")
CITY = os.getenv("CITY", "Ho Chi Minh City")
INTERVAL = int(os.getenv("INTERVAL_SECONDS", "15"))

if not API_KEY:
    raise SystemExit("OPENWEATHERMAP_API_KEY is required")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: v.encode("utf-8") if v else None,
)

def fetch_weather():
    # simple current weather by city name
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": CITY, "appid": API_KEY, "units": "metric"}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json()

print(f"[producer] sending to {BOOTSTRAP} topic={TOPIC} city='{CITY}'")
while True:
    try:
        payload = fetch_weather()
        key = str(payload.get("id") or "")
        producer.send(TOPIC, value=payload, key=key)
        producer.flush()
        print("[producer] sent", payload.get("name"), payload.get("dt"))
    except Exception as e:
        print("[producer] error:", e)
    time.sleep(INTERVAL)
