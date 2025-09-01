import json
import logging
from typing import Any, Dict, Optional, Tuple, List

import requests

log = logging.getLogger("bike-utils")

def get_network_data(api_url: str, timeout: Tuple[int, int] = (4, 8)) -> Optional[Dict[str, Any]]:
    """Fetch network JSON from CityBikes API. Return dict or None on failure."""
    try:
        resp = requests.get(api_url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        log.warning("API/JSON error for %s: %s", api_url, e)
        return None

def _to_int(x, default=0):
    try:
        if x is None:
            return default
        v = int(float(x))
        return max(0, v)
    except (ValueError, TypeError):
        return default

def aggregate_summary(
    stations: List[Dict[str, Any]],
    city: str,
    producer_ts: Optional[str] = None,
) -> Dict[str, Any]:
    safe_stations = [st for st in stations if isinstance(st, dict)]

    total_stations    = len(safe_stations)
    total_free_bikes  = sum(_to_int(st.get("free_bikes"))  for st in safe_stations)
    total_empty_docks = sum(_to_int(st.get("empty_slots")) for st in safe_stations)

    total_docks = total_free_bikes + total_empty_docks
    dock_empty_ratio = round(total_empty_docks / total_docks, 4) if total_docks else 0.0

    return {
        "city": city,
        "timestamp": producer_ts,
        "total_stations": total_stations,
        "total_docks": total_docks,
        "total_free_bikes": total_free_bikes,
        "total_empty_docks": total_empty_docks,
        "dock_empty_ratio": dock_empty_ratio,
    }
