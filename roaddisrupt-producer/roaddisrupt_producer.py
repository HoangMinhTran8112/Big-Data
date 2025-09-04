import configparser
import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from kafka import KafkaProducer
from roaddisrupt_utils import get_road_disruption_data, aggregate_disruptions

# -------------------- Config --------------------
# Load configuration from environment variables or use defaults
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
TOPIC_NAME       = os.getenv("TOPIC_NAME", "road disruption")
SLEEP_TIME       = max(5, int(os.getenv("SLEEP_TIME", "60")))
API_URL          = os.getenv("API_URL", "https://api.tfl.gov.uk/Road/all/Disruption")
LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO")

# -------------------- Logging --------------------
# Set up logging configuration
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("roaddisrupt-producer")

def _load_api_key(cfg_path: str = "roaddisrupt_service.cfg") -> Optional[str]:
    """
    Load TFL API key from .cfg file.
    Returns the API key as a string, or None if not found.
    """
    try:
        cfg = configparser.ConfigParser()
        if not cfg.read(cfg_path):
            log.error("Config file '%s' not found.", cfg_path)
            return None
        token = cfg["tfl_api_credential"]["access_token"]
        if not token:
            log.error("Missing 'access_token' in config.")
            return None
        return token
    except Exception as e:
        log.error("Failed to read API key: %s", e)
        return None

def build_payload(api_key: str) -> Optional[Dict[str, Any]]:
    """
    Fetch network JSON and build an aggregate summary payload.
    Returns a list of payloads or None if data is unavailable.
    """
    data = get_road_disruption_data(API_URL, api_key)
    if not data:
        return None

    return aggregate_disruptions(
        data=data,
        producer_ts=datetime.now(timezone.utc).isoformat(),
    )

def main():
    # Load API key from config file
    api_key = _load_api_key("roaddisrupt_service.cfg")
    if not api_key:
        log.error("No API key available. Exiting.")
        return
    log.info(f"Starting Road Disruption Producer | Broker={KAFKA_BROKER_URL} | Topic={TOPIC_NAME}")

    # Initialize Kafka producer with configuration
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
            # Build payloads from API data
            payloads = build_payload(api_key)
            if payloads:
                for payload in payloads:
                    # Send each payload to Kafka topic
                    producer.send(TOPIC_NAME, value=payload)
                    log.info(
                        "sent lat=%.4f lon=%.4f severity=%s timestamp=%s",
                        payload["lat"], payload["lon"],
                        payload["severity"], payload["timestamp"]
                    )
            else:
                log.warning("skip cycle (no payload)")
            # Wait before next cycle
            time.sleep(SLEEP_TIME)
    finally:
        try:
            # Ensure all messages are sent before closing
            producer.flush(timeout=10)
        except Exception:
            pass
        log.info("Producer closed.")

if __name__ == "__main__":
    main()
