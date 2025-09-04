import os
import json
import time
import logging
import configparser
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from kafka import KafkaProducer
from roaddisrupt_utils import get_road_disruption_data, aggregate_disruptions

# -------------------- Config --------------------
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "kafka:9092")
TOPIC_NAME       = os.getenv("TOPIC_NAME", "road_disruption")
SLEEP_TIME       = max(5, int(os.getenv("SLEEP_TIME", "60")))
CITY             = os.getenv("CITY", "London")
API_URL          = os.getenv("API_URL", "https://api.tfl.gov.uk/Road/all/Disruption")
LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO")

# -------------------- Logging --------------------
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("roaddisrupt-producer")

# -------------------- Helpers --------------------
def _load_api_key(cfg_path: str = "roaddisrupt_service.cfg") -> Optional[str]:
    """Load TfL API key from .cfg file."""
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

def _build_key(payload: Dict[str, Any]) -> bytes:
    """Stable Kafka key (useful if you switch sink to FullKeyStrategy)."""
    key_str = f"{payload.get('id') or 'unknown'}|{payload.get('provider_ts')}"
    return key_str.encode("utf-8")

def build_payload(api_key: str) -> Optional[List[Dict[str, Any]]]:
    """Fetch TfL JSON and build a list of flat disruption records."""
    data = get_road_disruption_data(api_url=API_URL, api_key=api_key)
    if not data:
        return None
    raw_list = data if isinstance(data, list) else (data.get("disruptions") or data.get("roads") or [])
    return aggregate_disruptions(
        data=raw_list,
        city=CITY,
        producer_ts=datetime.now(timezone.utc).isoformat(),
    )

# -------------------- Main loop --------------------
def main():
    api_key = _load_api_key()
    if not api_key:
        log.error("No API key available. Exiting.")
        return

    log.info("Starting Road Disruption Producer | Broker=%s | Topic=%s | City=%s",
             KAFKA_BROKER_URL, TOPIC_NAME, CITY)

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
            payloads = build_payload(api_key)
            if payloads:
                for payload in payloads:
                    lat = payload.get("lat", payload.get("latitude"))
                    lon = payload.get("lon", payload.get("longitude"))

                    key = _build_key(payload)
                    producer.send(TOPIC_NAME, key=key, value=payload)

                    log.info(
                        "sent id=%s sev=%s lat=%s lon=%s provider_ts=%s",
                        payload.get("id"),
                        payload.get("severity"),
                        lat,
                        lon,
                        payload.get("provider_ts"),
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
