# ─────────────────────────────────────────────────────────────────────────────
# data_layer.py — Data Reading, Normalization, Caching (Local File Mode)
# ─────────────────────────────────────────────────────────────────────────────
# MySQL is DISABLED for local testing. Anomaly history stored in JSON file.
# All data is read from dummy_data/ folder.
# ─────────────────────────────────────────────────────────────────────────────

import json
import os
import logging
from datetime import datetime, timedelta
from typing import Any, Optional

from config import DUMMY_DATA_DIR, CACHE_TTL_HOURS, ANOMALY_LOG_FILE

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# LOCAL FILE READING
# ─────────────────────────────────────────────────────────────────────────────

def read_local_json(filename: str) -> dict:
    """Read a JSON file from the dummy_data directory."""
    filepath = os.path.join(DUMMY_DATA_DIR, filename)
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        return {"error": f"File not found: {filename}"}
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error in {filename}: {e}")
        return {"error": f"Invalid JSON in {filename}: {e}"}


def get_onlimo_data(station_id: str = "", das: str = "") -> list[dict]:
    """Read Onlimo station data from local file."""
    raw = read_local_json("onlimo_stations.json")
    if "error" in raw:
        return [raw]

    data = raw.get("data", [])

    # Filter by DAS if specified
    if das:
        data = [s for s in data if s.get("das", "").lower() == das.lower()]

    # Filter by station_id if specified
    if station_id:
        data = [s for s in data if s.get("station_id", "").upper() == station_id.upper()]

    return data if data else [{"info": "No stations found matching criteria"}]


def get_rainfall_data(location: str = "", lat: float = 0.0, lon: float = 0.0) -> dict:
    """Read rainfall data from local file, match by location or coordinates."""
    raw = read_local_json("bmkg_rainfall.json")
    if "error" in raw:
        return raw

    data = raw.get("data", [])

    # Match by location name
    if location:
        for entry in data:
            if location.lower() in entry.get("kelurahan", "").lower() or \
               location.lower() in entry.get("kecamatan", "").lower():
                return {
                    "source": "local_dummy",
                    "location": entry.get("kelurahan"),
                    "lat": entry.get("latitude"),
                    "lon": entry.get("longitude"),
                    "total_rainfall_mm": entry["summary_24h"]["total_rainfall_mm"],
                    "max_hourly_mm": entry["summary_24h"]["max_hourly_mm"],
                    "window_hours": 24,
                    "is_high_rainfall": entry["summary_24h"]["total_rainfall_mm"] > 50.0,
                    "hourly": entry.get("hourly", [])
                }

    # Match by closest coordinates
    if lat and lon:
        closest = None
        min_dist = float("inf")
        for entry in data:
            dist = abs(entry.get("latitude", 0) - lat) + abs(entry.get("longitude", 0) - lon)
            if dist < min_dist:
                min_dist = dist
                closest = entry
        if closest:
            return {
                "source": "local_dummy",
                "location": closest.get("kelurahan"),
                "lat": closest.get("latitude"),
                "lon": closest.get("longitude"),
                "total_rainfall_mm": closest["summary_24h"]["total_rainfall_mm"],
                "max_hourly_mm": closest["summary_24h"]["max_hourly_mm"],
                "window_hours": 24,
                "is_high_rainfall": closest["summary_24h"]["total_rainfall_mm"] > 50.0,
                "hourly": closest.get("hourly", [])
            }

    # Return first entry as default
    if data:
        entry = data[0]
        return {
            "source": "local_dummy",
            "location": entry.get("kelurahan"),
            "total_rainfall_mm": entry["summary_24h"]["total_rainfall_mm"],
            "window_hours": 24,
            "is_high_rainfall": entry["summary_24h"]["total_rainfall_mm"] > 50.0
        }

    return {"error": "No rainfall data available"}


def get_sparing_logger_data(das: str = "", district: str = "") -> list[dict]:
    """Read Sparing Logger (outlet IPAL) data from local file."""
    raw = read_local_json("sparing_logger.json")
    if "error" in raw:
        return [raw]

    data = raw.get("data", [])

    if district:
        data = [d for d in data if district.lower() in d.get("kabkot", "").lower() or
                district.lower() in d.get("kecamatan", "").lower()]

    return data if data else [{"info": "No Sparing Logger entries found"}]


def get_sparing_monitoring_data(company_id: str = "", days: int = 3) -> list[dict]:
    """Read Sparing Monitoring data from local file, filter by company."""
    raw = read_local_json("sparing_monitoring.json")
    if "error" in raw:
        return [raw]

    data = raw.get("data", [])

    if company_id:
        data = [d for d in data if d.get("company_id", "").upper() == company_id.upper()]

    return data if data else [{"info": f"No monitoring data found for {company_id}"}]


def get_sitala_data(district: str = "") -> list[dict]:
    """Read SITALA IKA data from local file."""
    raw = read_local_json("sitala_ika.json")
    if "error" in raw:
        return [raw]

    data = raw.get("data", [])

    if district:
        data = [d for d in data if district.lower() in d.get("kabkot", "").lower()]

    return data if data else [{"info": "No IKA data found for district"}]


# ─────────────────────────────────────────────────────────────────────────────
# UTILITY FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert a value to float."""
    if value is None or value == "" or value == "-" or value == "N/A":
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    """Safely convert a value to int."""
    if value is None or value == "" or value == "-":
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


# ─────────────────────────────────────────────────────────────────────────────
# SHORT-TERM MEMORY CACHE (72-hour)
# ─────────────────────────────────────────────────────────────────────────────

class MemoryCache:
    """In-memory cache for short-term data."""

    def __init__(self, ttl_hours: int = CACHE_TTL_HOURS):
        self._store: dict[str, dict] = {}
        self._ttl = timedelta(hours=ttl_hours)

    def set(self, key: str, value: Any) -> None:
        self._store[key] = {"value": value, "timestamp": datetime.now()}

    def get(self, key: str) -> Optional[Any]:
        if key not in self._store:
            return None
        entry = self._store[key]
        if datetime.now() - entry["timestamp"] > self._ttl:
            del self._store[key]
            return None
        return entry["value"]

    def clear(self) -> None:
        self._store.clear()


cache = MemoryCache()


# ─────────────────────────────────────────────────────────────────────────────
# ANOMALY LOG (replaces MySQL in local mode)
# ─────────────────────────────────────────────────────────────────────────────

def log_anomaly(anomaly_data: dict) -> str:
    """Save anomaly to local JSON file (replaces MySQL)."""
    try:
        # Read existing log
        if os.path.exists(ANOMALY_LOG_FILE):
            with open(ANOMALY_LOG_FILE, "r", encoding="utf-8") as f:
                log = json.load(f)
        else:
            log = []

        # Add timestamp and append
        anomaly_data["logged_at"] = datetime.now().isoformat()
        log.append(anomaly_data)

        # Write back
        with open(ANOMALY_LOG_FILE, "w", encoding="utf-8") as f:
            json.dump(log, f, ensure_ascii=False, indent=2)

        station = anomaly_data.get("station_name", anomaly_data.get("station_id", "unknown"))
        return f"Anomaly logged: {station} (total entries: {len(log)})"
    except Exception as e:
        logger.error(f"Failed to log anomaly: {e}")
        return f"ERROR: Failed to log anomaly: {e}"


def get_station_history(station_id: str, days: int = 30) -> list[dict]:
    """Get anomaly history from local JSON file."""
    try:
        if not os.path.exists(ANOMALY_LOG_FILE):
            return []
        with open(ANOMALY_LOG_FILE, "r", encoding="utf-8") as f:
            log = json.load(f)
        return [e for e in log if e.get("station_id") == station_id]
    except Exception:
        return []
