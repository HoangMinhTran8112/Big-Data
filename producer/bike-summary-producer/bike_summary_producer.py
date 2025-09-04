import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from kafka import KafkaProducer
from bike_utils import get_network_data, aggregate_summary

# -------------------- Config --------------------
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "kafka:9092") 
TOPIC_NAME       = os.getenv("TOPIC_NAME", "bike_summary")
SLEEP_TIME       = max(5, int(os.getenv("SLEEP_TIME", "60")))
CITY             = os.getenv("CITY", "London")
API_URL          = os.getenv("API_URL", "https://api.citybik.es/v2/networks/santander-cycles")
LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO")

# -------------------- Logging --------------------
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bike-summary-producer")

def build_payload() -> Optional[Dict[str, Any]]:
    """Fetch network JSON and build an aggregate summary payload."""
    data = get_network_data(API_URL)
    if not data:
        return None

    net = data.get("network") or {}
    stations = net.get("stations") or []
    return aggregate_summary(
        stations=stations,
        city=CITY,
        producer_ts=datetime.now(timezone.utc).isoformat(),
    )

def main():
    log.info(f"Starting Bike Summary Producer | Broker={KAFKA_BROKER_URL} | Topic={TOPIC_NAME} | City={CITY}")

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER_URL],
        acks="all",
        retries=3,
        linger_ms=50,
        compression_type="gzip",
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
    )

    try:
        while True:
            payload = build_payload()
            if payload:
                producer.send(TOPIC_NAME, value=payload)
                log.info(
                    "sent city=%s free=%s empty=%s empty_ratio=%.4f",
                    payload["city"],
                    payload["total_free_bikes"],
                    payload["total_empty_docks"],
                    payload["dock_empty_ratio"],
                )
            else:
                log.warning("skip cycle (no payload)")
            time.sleep(SLEEP_TIME)
    finally:
        try:
            producer.flush(timeout=10)
        except Exception:
            pass
        log.info("Producer closed.")

if __name__ == "__main__":
    main()
