import json
import logging
from typing import Any, Dict, Optional, Tuple

import requests

log = logging.getLogger("owm-utils")


def get_weather_data(
    api_url: str,
    city: str,
    api_key: str,
    units: str = "metric",
    timeout: Tuple[int, int] = (4, 8),
) -> Optional[Dict[str, Any]]:
    """
    Call OpenWeatherMap Current Weather endpoint.
    Returns parsed JSON dict or None on failure.
    """
    params = {"q": city, "appid": api_key, "units": units}
    try:
        resp = requests.get(api_url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        log.warning("API/JSON error for %s (city=%s): %s", api_url, city, e)
        return None


def _get(d: Dict[str, Any], path: str, default=None):
    """
    Safe nested getter with 'a.b.c' path.
    """
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def aggregate_weather_summary(
    data: Dict[str, Any],
    city: str,
    producer_ts: Optional[str] = None,
    units: str = "metric",
) -> Dict[str, Any]:
    """
    Build a compact, analysis-friendly weather snapshot.
    Mirrors the bike summary style: scalar fields, minimal nesting.
    """
    main = data.get("main", {}) or {}
    wind = data.get("wind", {}) or {}
    clouds = data.get("clouds", {}) or {}
    weather0 = (data.get("weather") or [{}])[0] or {}
    coord = data.get("coord", {}) or {}

    # Optional precipitation fields (may be missing)
    rain_1h = _get(data, "rain.1h", 0.0)
    snow_1h = _get(data, "snow.1h", 0.0)

    # Visibility may be absent; ensure number
    visibility = data.get("visibility", None)
    if isinstance(visibility, str):
        try:
            visibility = int(float(visibility))
        except Exception:
            visibility = None

    payload = {
        "city": city,
        "timestamp": producer_ts,
        "units": units,  # for downstream reference
        # Coordinates
        "lat": coord.get("lat"),
        "lon": coord.get("lon"),
        # Core readings
        "temp": main.get("temp"),
        "feels_like": main.get("feels_like"),
        "temp_min": main.get("temp_min"),
        "temp_max": main.get("temp_max"),
        "pressure": main.get("pressure"),
        "humidity": main.get("humidity"),
        "visibility": visibility,
        # Wind / Clouds
        "wind_speed": wind.get("speed"),
        "wind_deg": wind.get("deg"),
        "wind_gust": wind.get("gust"),
        "clouds_pct": clouds.get("all"),
        # Weather description
        "weather_main": weather0.get("main"),
        "weather_desc": weather0.get("description"),
        # Precip (optional)
        "rain_1h": rain_1h,
        "snow_1h": snow_1h,
        # Provider metadata
        "provider_ts": _get(data, "dt", None),  
        "provider_id": data.get("id"),
    }
    return payload
