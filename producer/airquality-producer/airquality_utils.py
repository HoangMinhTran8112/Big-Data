import json
import logging
from typing import Any, Dict, Optional, Tuple

import requests

log = logging.getLogger("airquality-utils")

# --------- HTTP fetch (aligned with bike utils) ----------
def get_air_quality_data(lat: float, lon: float, api_key: str,
                         timeout: Tuple[int, int] = (4, 8)) -> Optional[Dict[str, Any]]:
    """Call OpenWeatherMap Air Pollution API. Return dict or None on failure."""
    try:
        url = "http://api.openweathermap.org/data/2.5/air_pollution"
        params = {"lat": lat, "lon": lon, "appid": api_key}
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        log.warning("OWM API/JSON error: %s", e)
        return None

# --------- safe converters ----------
def _to_float(x, default=0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except (ValueError, TypeError):
        return default

def _to_int(x, default=0) -> int:
    try:
        if x is None:
            return default
        return int(float(x))
    except (ValueError, TypeError):
        return default

# --------- Shape raw JSON to aligned payload ----------
def aggregate_summary(
    raw_json: Dict[str, Any],
    city: str,
    producer_ts: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Transform OWM air quality JSON into a flat Kafka payload.
    Aligned with bike producer: includes 'city' and ISO UTC 'timestamp'.
    """
    try:
        items = raw_json.get("list") or []
        if not items:
            return None
        first = items[0]
        main = first.get("main") or {}
        comps = first.get("components") or {}

        # NEW: provider timestamp from OWM (seconds since epoch)
        provider_ts = first.get("dt")

        return {
            "city": city,
            "timestamp": producer_ts,     # producer-side ISO timestamp
            "provider_ts": provider_ts,   # ✅ add this for Mongo sink id
            "aqi": _to_int(main.get("aqi")),
            "co": _to_float(comps.get("co")),
            "no": _to_float(comps.get("no")),
            "no2": _to_float(comps.get("no2")),
            "o3": _to_float(comps.get("o3")),
            "so2": _to_float(comps.get("so2")),
            "pm2_5": _to_float(comps.get("pm2_5")),
            "pm10": _to_float(comps.get("pm10")),
            "nh3": _to_float(comps.get("nh3")),
        }
    except Exception as e:
        log.warning("Failed to shape payload: %s", e)
        return None
