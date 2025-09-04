import json
import logging
from typing import Any, Dict, Optional, Tuple, List

import requests

# Set up logger for this module
log = logging.getLogger("roaddisrupt-utils")

def get_road_disruption_data(api_url: str, api_key: str, timeout: Tuple[int, int] = (4, 8)) -> Optional[Dict[str, Any]]:
    """
    Fetch network JSON from TFL API.
    Returns a dictionary with the API response, or None on failure.
    """
    try:
        params = {"app_key": api_key}
        resp = requests.get(api_url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        log.warning("API/JSON error for %s: %s", api_url, e)
        return None

def _to_float(x, default=0.0) -> float:
    """
    Safely convert a value to float, returning a default on failure.
    """
    try:
        if x is None:
            return default
        return float(x)
    except (ValueError, TypeError):
        return default

def aggregate_disruptions(
    data: List[Dict[str, Any]],
    producer_ts: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Transform TfL road disruption JSON into flat Kafka payloads.
    Each disruption includes lat/lon, severity, and timestamp.
    Only active disruptions are included.
    """
    disruptions = []

    try:
        for item in data:
            # Only process active disruptions
            if "active" not in (item.get("status") or "").lower():
                continue

            severity = item.get("severity")
            # Use current update time or last modified time as timestamp
            timestamp = item.get("currentUpdateDateTime") or item.get("lastModifiedTime")

            # Extract affected street coordinates
            streets = item.get("streets") or []
            for street in streets:
                segments = street.get("segments") or []
                for seg in segments:
                    line = seg.get("lineString")
                    if not line:
                        continue

                    # Parse the lineString JSON to get coordinates
                    coords = json.loads(line) 

                    for lon, lat in coords:
                        disruptions.append({
                            "lat": _to_float(lat),
                            "lon": _to_float(lon),
                            "severity": severity,
                            "timestamp": timestamp or producer_ts,
                        })

        return disruptions
    except Exception as e:
        log.warning("Failed to shape payload: %s", e)
        return []
