"""Produce air quality data to 'air_quality' Kafka topic from OpenWeatherMap API."""
import os
import json
import time
import logging
import configparser
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from kafka import KafkaProducer
from airquality_utils import get_air_quality_data, aggregate_summary

# -------------------- Config --------------------
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
TOPIC_NAME       = os.getenv("TOPIC_NAME", "air_quality")
SLEEP_TIME       = max(5, int(os.getenv("SLEEP_TIME", "600")))
LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO")

CITY = os.getenv("CITY", "London")
LAT  = float(os.getenv("LAT", "51.5073219"))
LON  = float(os.getenv("LON", "-0.1276474"))

# -------------------- Logging --------------------
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("airquality-producer")

def _load_api_key(cfg_path: str = "airquality_service.cfg") -> Optional[str]:
    """Load OWM API key from the same cfg the original script used."""
    try:
        cfg = configparser.ConfigParser()
        if not cfg.read(cfg_path):
            log.error("Config file '%s' not found.", cfg_path)
            return None
        token = cfg["openweathermap_api_credential"]["access_token"]
        if not token:
            log.error("Missing 'access_token' in config.")
            return None
        return token
    except Exception as e:
        log.error("Failed to read API key: %s", e)
        return None

def build_payload(api_key: str) -> Optional[Dict[str, Any]]:
    """Fetch OWM JSON and build a flat payload (aligned with bike producer)."""
    raw = get_air_quality_data(LAT, LON, api_key)  # returns None on error
    if not raw:
        return None
    return aggregate_summary(
        raw_json=raw,
        city=CITY,
        producer_ts=datetime.now(timezone.utc).isoformat(),
    )

def main():
    api_key = _load_api_key()
    if not api_key:
        log.error("No API key available. Exiting.")
        return

    log.info("Starting Air Quality Producer | Broker=%s | Topic=%s | City=%s",
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
            payload = build_payload(api_key)
            if payload:
                producer.send(TOPIC_NAME, value=payload)
                log.info(
                    "sent city=%s aqi=%s pm2_5=%.3f pm10=%.3f o3=%.3f no2=%.3f",
                    payload["city"], payload["aqi"],
                    payload["pm2_5"], payload["pm10"],
                    payload["o3"], payload["no2"]
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
