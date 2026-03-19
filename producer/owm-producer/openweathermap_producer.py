"""Produce current weather data to 'weather' Kafka topic from OpenWeatherMap API."""
import os
import json
import time
import logging
import configparser
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from kafka import KafkaProducer
from owm_utils import get_weather_data, aggregate_weather_summary

# -------------------- Config --------------------
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "kafka:9092")
TOPIC_NAME       = os.getenv("TOPIC_NAME", "weather")
SLEEP_TIME       = max(5, int(os.getenv("SLEEP_TIME", "60")))
LOG_LEVEL        = os.getenv("LOG_LEVEL", "INFO")

# Weather-specific config (aligns with your CITY-based approach)
CITY    = os.getenv("CITY", "London")
UNITS   = os.getenv("UNITS", "metric")  # metric | imperial | standard
API_URL = os.getenv("API_URL", "https://api.openweathermap.org/data/2.5/weather")

# -------------------- Logging --------------------
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("owm-producer")

# -------------------- API key loader --------------------
def _load_api_key(cfg_path: str = "openweathermap_service.cfg") -> Optional[str]:
    """Load OpenWeatherMap API key from .cfg file."""
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

# -------------------- Payload builder --------------------
def build_payload(api_key: str) -> Optional[Dict[str, Any]]:
    """Fetch OWM JSON and build a flat payload."""
    data = get_weather_data(api_url=API_URL, city=CITY, api_key=api_key, units=UNITS)
    if not data:
        return None
    return aggregate_weather_summary(
        data=data,
        city=CITY,
        producer_ts=datetime.now(timezone.utc).isoformat(),
        units=UNITS,
    )

# -------------------- Main loop --------------------
def main():
    api_key = _load_api_key()
    if not api_key:
        log.error("No API key available. Exiting.")
        return

    log.info(
        "Starting OWM Weather Producer | Broker=%s | Topic=%s | City=%s | Units=%s",
        KAFKA_BROKER_URL, TOPIC_NAME, CITY, UNITS
    )

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

                #  Log numeric values directly
                log.info(
                    "sent city=%s temp=%s feels_like=%s humidity=%s wind=%s weather=%s",
                    payload.get("city"),
                    payload.get("temp"),
                    payload.get("feels_like"),
                    payload.get("humidity"),
                    payload.get("wind_speed"),
                    payload.get("weather_main"),
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
