import json
import logging
from typing import Any, Dict, List, Optional, Tuple

import requests

log = logging.getLogger("roaddisrupt-utils")

def get_road_disruption_data(api_url: str, api_key: str, timeout: Tuple[int, int] = (4, 8)) -> Optional[Dict[str, Any]]:
    """Fetch JSON from TfL API. Return dict or None on failure."""
    try:
        params = {"app_key": api_key}
        resp = requests.get(api_url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        log.warning("API/JSON error for %s: %s", api_url, e)
        return None

def _to_float(x, default=0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except (ValueError, TypeError):
        return default

def _parse_line_string(line: Any) -> List[Tuple[float, float]]:
    """
    Parse TfL 'lineString' which may be:
      - JSON list: [[lon,lat],[lon,lat],...]
      - already-parsed list of pairs
      - space-separated 'lon,lat lon,lat ...'
    Return list of (lon, lat).
    """
    coords: List[Tuple[float, float]] = []

    if isinstance(line, list):
        for pair in line:
            if isinstance(pair, (list, tuple)) and len(pair) >= 2:
                coords.append((_to_float(pair[0]), _to_float(pair[1])))
        return coords

    if not isinstance(line, str):
        return coords

    # Try JSON first
    try:
        parsed = json.loads(line)
        if isinstance(parsed, list):
            for pair in parsed:
                if isinstance(pair, (list, tuple)) and len(pair) >= 2:
                    coords.append((_to_float(pair[0]), _to_float(pair[1])))
            if coords:
                return coords
    except Exception:
        pass

    # Fallback: "lon,lat lon,lat ..."
    try:
        for p in [p.strip() for p in line.split() if p.strip()]:
            if "," in p:
                lon_s, lat_s = p.split(",", 1)
                coords.append((_to_float(lon_s), _to_float(lat_s)))
    except Exception:
        pass

    return coords

def aggregate_disruptions(
    data: List[Dict[str, Any]],
    city: str,
    producer_ts: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Flatten TfL road disruptions into per-point records with:
      city, provider_ts, timestamp, severity, lat, lon, road_name, id
    Only 'active' disruptions are emitted.
    """
    out: List[Dict[str, Any]] = []

    try:
        for item in data:
            if "active" not in (item.get("status") or "").lower():
                continue

            severity = item.get("severity")
            provider_ts = item.get("currentUpdateDateTime") or item.get("lastModifiedTime")
            road_name = item.get("roadName") or item.get("location") or None
            disrupt_id = item.get("id") or item.get("linkId") or item.get("incidentId")

            for street in (item.get("streets") or []):
                for seg in (street.get("segments") or []):
                    line = seg.get("lineString")
                    if not line:
                        continue

                    for lon, lat in _parse_line_string(line):
                        out.append({
                            "city": city,
                            "provider_ts": provider_ts,   # for Mongo sink projection
                            "timestamp": producer_ts,     # producer ISO time
                            "severity": severity,
                            "lat": _to_float(lat),
                            "lon": _to_float(lon),
                            "road_name": road_name,
                            "id": disrupt_id,
                        })
        return out
    except Exception as e:
        log.warning("Failed to shape payload: %s", e)
        return []
