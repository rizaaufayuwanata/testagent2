# =============================================================================
# response_schema.py — Guaranteed Output Schema for WQSA Dashboard
# =============================================================================
# Normalizes chain.py output so dashboard ALWAYS receives consistent structure.
#
# USAGE in chain.py:
#   from response_schema import normalize_chain_output
#   ...
#   return normalize_chain_output(raw_result_dict)
#
# USAGE in agents.py (_parse_json fallback):
#   from response_schema import normalize_agent_output
#   ...
#   return normalize_agent_output(raw, label, expected_schema="analyst")
# =============================================================================

from datetime import datetime
from typing import Any, Optional
import json
import re
import logging

logger = logging.getLogger(__name__)


# =============================================================================
# 1. SCHEMA DEFINITIONS — Field-level contracts
# =============================================================================
# Setiap field punya: type, default, required (untuk logging/warning).
# Dashboard JavaScript bisa assume semua field ini SELALU ada.
# =============================================================================

# ── Station-level schema (per anomalous station) ────────────────────────────

STATION_SCHEMA = {
    "station_id":        {"type": str,   "default": "UNKNOWN"},
    "station_name":      {"type": str,   "default": ""},
    "indeks_mutu":       {"type": float, "default": 0.0},
    "status":            {"type": str,   "default": "TIDAK DIKETAHUI"},
    "is_limpasan":       {"type": bool,  "default": False},
    "rainfall_mm":       {"type": float, "default": 0.0},
    "pollution_profile": {"type": str,   "default": "TIDAK_DIKETAHUI"},
    "cod_bod_ratio":     {"type": float, "default": 0.0},
    "urgency":           {"type": str,   "default": "PANTAU"},
    "anomaly_reasons":   {"type": list,  "default": []},
    "top_suspect":       {"type": str,   "default": ""},
    "causal_evidence":   {"type": list,  "default": []},
}

# ── Analysis highlight schema (dashboard cards/KPI) ─────────────────────────

HIGHLIGHT_SCHEMA = {
    "overall_urgency":  {"type": str,  "default": "PANTAU"},
    "total_anomalous":  {"type": int,  "default": 0},
    "total_normal":     {"type": int,  "default": 0},
    "top_stations":     {"type": list, "default": []},
    "priority_actions": {"type": list, "default": []},
}

# ── Top station schema (inside highlight.top_stations[]) ─────────────────────

TOP_STATION_SCHEMA = {
    "station_id":        {"type": str,   "default": "UNKNOWN"},
    "station_name":      {"type": str,   "default": ""},
    "status":            {"type": str,   "default": "TIDAK DIKETAHUI"},
    "indeks_mutu":       {"type": float, "default": 0.0},
    "urgency":           {"type": str,   "default": "PANTAU"},
    "pollution_profile": {"type": str,   "default": "TIDAK_DIKETAHUI"},
    "top_suspect":       {"type": str,   "default": ""},
    "causal_confidence": {"type": str,   "default": "RENDAH"},
    "key_finding":       {"type": str,   "default": "Data tidak tersedia"},
}

# ── Priority action schema ───────────────────────────────────────────────────

ACTION_SCHEMA = {
    "priority": {"type": int,  "default": 0},
    "action":   {"type": str,  "default": ""},
    "target":   {"type": str,  "default": ""},
    "deadline": {"type": str,  "default": ""},
}

# ── Detail reasoning per station ─────────────────────────────────────────────

STEP_SCHEMA = {
    "step":   {"type": int, "default": 0},
    "name":   {"type": str, "default": ""},
    "result": {"type": str, "default": "Data tidak tersedia"},
}

DETAIL_STATION_SCHEMA = {
    "station_id":   {"type": str,  "default": "UNKNOWN"},
    "station_name": {"type": str,  "default": ""},
    "steps":        {"type": list, "default": []},
    "causal_chain": {"type": list, "default": []},
}

DETAIL_REASONING_SCHEMA = {
    "per_station":      {"type": list, "default": []},
    "data_quality_note": {"type": str, "default": ""},
}

# ── KPI update schema ────────────────────────────────────────────────────────

KPI_SCHEMA = {
    "total_stations":  {"type": int,   "default": 0},
    "critical":        {"type": int,   "default": 0},
    "warning":         {"type": int,   "default": 0},
    "good":            {"type": int,   "default": 0},
    "max_rain_mm":     {"type": float, "default": 0.0},
    "langgar_count":   {"type": int,   "default": 0},
}

# ── IKA summary schema ───────────────────────────────────────────────────────

IKA_SCHEMA = {
    "actual":  {"type": float, "default": 0.0},
    "target":  {"type": float, "default": 0.0},
    "gap":     {"type": float, "default": 0.0},
    "urgency": {"type": str,   "default": "DATA_TIDAK_TERSEDIA"},
}

# ── Data quality schema ──────────────────────────────────────────────────────

DATA_QUALITY_SCHEMA = {
    "overall_score":     {"type": int,  "default": 0},
    "issues_found":      {"type": int,  "default": 0},
    "outliers":          {"type": list, "default": []},
    "null_warnings":     {"type": list, "default": []},
    "excluded_stations": {"type": list, "default": []},
}

# ── Data reliability schema ──────────────────────────────────────────────────

DATA_RELIABILITY_SCHEMA = {
    "is_reliable":        {"type": bool, "default": False},
    "level":              {"type": str,  "default": "UNKNOWN"},
    "score":              {"type": int,  "default": 0},
    "warning":            {"type": str,  "default": None},  # nullable
    "sensor_null_params": {"type": list, "default": []},
    "dead_stations":      {"type": list, "default": []},
    "excluded_stations":  {"type": list, "default": []},
}

# ── Top-level chain output schema ────────────────────────────────────────────

CHAIN_OUTPUT_SCHEMA = {
    "status":              {"type": str,  "default": "error"},
    "session_id":          {"type": str,  "default": ""},
    "timestamp":           {"type": str,  "default": ""},
    "feedback_loops":      {"type": int,  "default": 0},
    "message":             {"type": str,  "default": None},  # nullable — only for no_data/error
    "data_quality":        {"type": dict, "default": {},   "schema": DATA_QUALITY_SCHEMA},
    "data_reliability":    {"type": dict, "default": {},   "schema": DATA_RELIABILITY_SCHEMA},
    "analysis_highlight":  {"type": dict, "default": {},   "schema": HIGHLIGHT_SCHEMA},
    "detail_reasoning":    {"type": dict, "default": {},   "schema": DETAIL_REASONING_SCHEMA},
    "kpi_update":          {"type": dict, "default": {},   "schema": KPI_SCHEMA},
    "ika_summary":         {"type": dict, "default": {},   "schema": IKA_SCHEMA},
    "telegram_message":    {"type": str,  "default": ""},
    "analyst_narrative":   {"type": str,  "default": ""},
    "unresolved_issues":   {"type": list, "default": []},
}


# =============================================================================
# 2. NORMALIZER ENGINE
# =============================================================================

def _coerce(value: Any, spec: dict) -> Any:
    """Coerce a value to match the spec type, fallback to default."""
    expected_type = spec["type"]
    default = spec.get("default")

    # Nullable field — None is allowed
    if default is None and value is None:
        return None

    if value is None:
        return _deep_copy_default(default)

    # Type match
    if isinstance(value, expected_type):
        return value

    # Coercion attempts
    try:
        if expected_type == float and isinstance(value, (int, str)):
            return float(value)
        if expected_type == int and isinstance(value, (float, str)):
            return int(float(value))
        if expected_type == str:
            return str(value)
        if expected_type == bool:
            return bool(value)
    except (ValueError, TypeError):
        pass

    return _deep_copy_default(default)


def _deep_copy_default(default: Any) -> Any:
    """Return a fresh copy of default to avoid mutation."""
    if isinstance(default, list):
        return []
    if isinstance(default, dict):
        return {}
    return default


def normalize_dict(data: Any, schema: dict, label: str = "") -> dict:
    """
    Normalize a dict against a schema.
    Guarantees every field in schema exists with correct type.
    Extra fields in data are preserved (forward compatibility).
    """
    if not isinstance(data, dict):
        data = {}

    result = {}
    missing = []

    for field_name, spec in schema.items():
        raw_value = data.get(field_name)

        if raw_value is None and spec.get("default") is not None:
            missing.append(field_name)

        coerced = _coerce(raw_value, spec)

        # Recursive normalization for nested dicts with sub-schema
        sub_schema = spec.get("schema")
        if sub_schema and isinstance(coerced, dict):
            coerced = normalize_dict(coerced, sub_schema, label=f"{label}.{field_name}")

        result[field_name] = coerced

    if missing and label:
        logger.warning(f"[Schema] {label}: missing fields filled with defaults: {missing}")

    # Preserve extra fields not in schema (forward compatibility)
    for key, value in data.items():
        if key not in result:
            result[key] = value

    return result


def normalize_list(data: Any, item_schema: dict, label: str = "") -> list:
    """Normalize a list of dicts against an item schema."""
    if not isinstance(data, list):
        return []
    return [normalize_dict(item, item_schema, label=f"{label}[{i}]") for i, item in enumerate(data)]


# =============================================================================
# 3. MAIN NORMALIZER — call this from chain.py
# =============================================================================

def normalize_chain_output(raw: dict) -> dict:
    """
    Normalize the full chain output dict.
    Guarantees every field the dashboard needs is present with correct types.

    Call this as the LAST step in run_chain() before returning:

        return normalize_chain_output({
            "status": "done",
            "analysis_highlight": report.get("analysis_highlight"),
            ...
        })
    """
    if not isinstance(raw, dict):
        raw = {}

    # ── Top-level normalization ──────────────────────────────────────────
    result = normalize_dict(raw, CHAIN_OUTPUT_SCHEMA, label="chain_output")

    # ── Nested list normalization ────────────────────────────────────────

    # analysis_highlight.top_stations[]
    hl = result.get("analysis_highlight", {})
    if isinstance(hl, dict):
        hl["top_stations"] = normalize_list(
            hl.get("top_stations"), TOP_STATION_SCHEMA, "highlight.top_stations"
        )
        hl["priority_actions"] = normalize_list(
            hl.get("priority_actions"), ACTION_SCHEMA, "highlight.priority_actions"
        )

    # detail_reasoning.per_station[].steps[]
    dr = result.get("detail_reasoning", {})
    if isinstance(dr, dict):
        per_station = dr.get("per_station", [])
        if isinstance(per_station, list):
            normalized_stations = []
            for i, ps in enumerate(per_station):
                ns = normalize_dict(ps, DETAIL_STATION_SCHEMA, f"detail.per_station[{i}]")
                ns["steps"] = normalize_list(
                    ns.get("steps"), STEP_SCHEMA, f"detail.per_station[{i}].steps"
                )
                normalized_stations.append(ns)
            dr["per_station"] = normalized_stations

    # ── Ensure timestamp is valid ────────────────────────────────────────
    if not result.get("timestamp"):
        result["timestamp"] = datetime.now().isoformat()

    # ── Build fallback telegram_message if empty ─────────────────────────
    if not result.get("telegram_message") and result.get("status") == "done":
        result["telegram_message"] = _build_fallback_telegram(result)

    return result


def _build_fallback_telegram(result: dict) -> str:
    """Build a minimal telegram message from whatever data is available."""
    hl = result.get("analysis_highlight", {})
    rel = result.get("data_reliability", {})

    lines = [
        f"🌊 WQSA Auto-Report",
        f"━━━━━━━━━━━━━━━━━━",
    ]

    # Reliability warning first
    if rel.get("warning"):
        lines.append(f"⚠️ {rel['warning']}")
        lines.append("")

    urgency = hl.get("overall_urgency", "?")
    anomalous = hl.get("total_anomalous", 0)
    normal = hl.get("total_normal", 0)

    lines.append(f"Urgensi: {urgency}")
    lines.append(f"Anomali: {anomalous} stasiun | Normal: {normal} stasiun")

    for s in hl.get("top_stations", [])[:5]:
        sid = s.get("station_id", "?")
        finding = s.get("key_finding", "")
        lines.append(f"\n📍 {sid} — {finding}")

    for a in hl.get("priority_actions", [])[:5]:
        pri = a.get("priority", "")
        act = a.get("action", "")
        lines.append(f"\n{pri}. {act}")

    return "\n".join(lines)


# =============================================================================
# 4. IMPROVED JSON PARSER — replaces _parse_json in agents.py
# =============================================================================

def parse_agent_json(raw: str, label: str = "Agent") -> dict:
    """
    More robust JSON parser that handles common LLM output issues:
    1. Pure JSON
    2. ```json ... ``` fenced
    3. Preamble text + ```json ... ``` (the case that currently breaks)
    4. JSON embedded in markdown/narrative
    5. Truncated JSON (partial recovery)
    """
    if not raw or not raw.strip():
        logger.error(f"[{label}] Empty output")
        return {"_parse_error": "empty_output"}

    clean = raw.strip()

    # ── Strategy 1: Direct JSON parse ────────────────────────────────────
    if clean.startswith("{"):
        try:
            return json.loads(clean)
        except json.JSONDecodeError:
            pass

    # ── Strategy 2: Extract from markdown fence ──────────────────────────
    # Handles: "some text\n```json\n{...}\n```\nmore text"
    fence_pattern = r'```(?:json)?\s*\n?\s*(\{[\s\S]*?\})\s*\n?\s*```'
    matches = re.findall(fence_pattern, clean)
    if matches:
        # Try the longest match (most likely the full JSON)
        for match in sorted(matches, key=len, reverse=True):
            try:
                return json.loads(match)
            except json.JSONDecodeError:
                continue

    # ── Strategy 3: Find the first { and last } ─────────────────────────
    first_brace = clean.find("{")
    last_brace = clean.rfind("}")
    if first_brace >= 0 and last_brace > first_brace:
        candidate = clean[first_brace:last_brace + 1]
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass

    # ── Strategy 4: Truncated JSON — try to recover ──────────────────────
    # Add closing braces/brackets to see if it parses
    if first_brace >= 0:
        candidate = clean[first_brace:]
        for suffix in ["}", "}}", "}}}", "]}", "]}}", "]}}}",
                        '"}', '"}}', '"]}'  ]:
            try:
                result = json.loads(candidate + suffix)
                logger.warning(f"[{label}] Recovered truncated JSON with suffix '{suffix}'")
                result["_truncated"] = True
                return result
            except json.JSONDecodeError:
                continue

    # ── All strategies failed ────────────────────────────────────────────
    logger.error(f"[{label}] JSON parse failed after all strategies. Raw: {raw[:300]}")
    return {"_parse_error": f"all_strategies_failed", "_raw_preview": raw[:500]}


# =============================================================================
# 5. AGENT OUTPUT NORMALIZER — call from agents.py after parse
# =============================================================================

# Minimal schemas per agent — just enough to not crash downstream
EVALUATOR_OUTPUT_SCHEMA = {
    "data_quality":              {"type": dict, "default": {}, "schema": DATA_QUALITY_SCHEMA},
    "cleaned_candidate_stations": {"type": list, "default": []},
    "cleaned_rainfall":          {"type": dict, "default": {}},
    "cleaned_sparing":           {"type": dict, "default": {}},
    "cleaned_sitala":            {"type": list, "default": []},
}

ANALYST_OUTPUT_SCHEMA = {
    "anomalous_stations":   {"type": list,  "default": []},
    "normal_station_count": {"type": int,   "default": 0},
    "ika_gap":              {"type": float, "default": None},  # nullable
    "ika_urgency":          {"type": str,   "default": "DATA_TIDAK_TERSEDIA"},
}

REPORTER_OUTPUT_SCHEMA = {
    "action":             {"type": str,  "default": "accept"},
    "analysis_highlight": {"type": dict, "default": {}, "schema": HIGHLIGHT_SCHEMA},
    "detail_reasoning":   {"type": dict, "default": {}, "schema": DETAIL_REASONING_SCHEMA},
    "kpi_update":         {"type": dict, "default": {}, "schema": KPI_SCHEMA},
    "ika_summary":        {"type": dict, "default": {}, "schema": IKA_SCHEMA},
    "telegram_message":   {"type": str,  "default": ""},
}


def normalize_agent_output(parsed: dict, agent: str) -> dict:
    """
    Normalize a parsed agent output against its expected schema.
    agent: "evaluator" | "analyst" | "reporter"
    """
    schemas = {
        "evaluator": EVALUATOR_OUTPUT_SCHEMA,
        "analyst":   ANALYST_OUTPUT_SCHEMA,
        "reporter":  REPORTER_OUTPUT_SCHEMA,
    }
    schema = schemas.get(agent)
    if not schema:
        return parsed

    return normalize_dict(parsed, schema, label=f"agent.{agent}")
