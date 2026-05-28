# ─────────────────────────────────────────────────────────────────────────────
# tools.py — WQSA Tools (MySQL-backed Mode)
# ─────────────────────────────────────────────────────────────────────────────
# All data tools query MySQL via data_layer.py.
# generate_rec() uses OpenRouter.
# Analysis tools are pure logic — no external dependencies.
# ─────────────────────────────────────────────────────────────────────────────

import json
import logging
from openai import OpenAI

from config import (
    OPENROUTER_API_KEY, AGENT_MODEL,
    ANOMALY_INDEX_THRESHOLD, ANOMALY_CHANGE_PERCENT,
    RATIO_INDUSTRY_THRESHOLD, RATIO_MIXED_LOW,
    RAINFALL_HIGH_MM, RAINFALL_WINDOW_HOURS,
    IKA_GAP_WARNING, IKA_GAP_CRITICAL,
    TARGET_DAS, TARGET_REGION,
)
from data_layer import (
    get_onlimo_data, get_rainfall_data,
    get_sparing_logger_data, get_sparing_monitoring_data,
    get_sitala_data,
    safe_float, cache, log_anomaly, get_station_history,
)

logger = logging.getLogger(__name__)

# ── OpenRouter client — lazy init to avoid crash when key not yet loaded ─────
_openrouter = None


def _get_openrouter() -> OpenAI:
    global _openrouter
    if _openrouter is None:
        key = OPENROUTER_API_KEY
        if not key:
            raise RuntimeError(
                "OPENROUTER_API_KEY is not set. "
                "Make sure wqsa.env exists and contains OPENROUTER_API_KEY=sk-or-..."
            )
        _openrouter = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=key,
        )
    return _openrouter


# ─────────────────────────────────────────────────────────────────────────────
# DATA TOOLS (query MySQL via data_layer)
# ─────────────────────────────────────────────────────────────────────────────

def query_onlimo(station_id: str = "", das: str = TARGET_DAS) -> str:
    """Read Onlimo station data from database."""
    data = get_onlimo_data(station_id, das)
    return json.dumps(data, ensure_ascii=False, indent=2)


def get_bmkg_rain(location: str = "", lat: float = 0.0, lon: float = 0.0) -> str:
    """Read rainfall data from database."""
    data = get_rainfall_data(location, lat, lon)
    return json.dumps(data, ensure_ascii=False, indent=2)


def query_sparing_logger(das: str = TARGET_DAS, district: str = "") -> str:
    """Read Sparing Logger data from database."""
    data = get_sparing_logger_data(das, district)
    return json.dumps(data, ensure_ascii=False, indent=2)


def query_sparing_monitoring(company_id: str = "", days: int = 3) -> str:
    """Read Sparing Monitoring data from database."""
    data = get_sparing_monitoring_data(company_id, days)
    return json.dumps(data, ensure_ascii=False, indent=2)


def query_sitala(district: str = TARGET_REGION) -> str:
    """Read SITALA IKA data from database."""
    data = get_sitala_data(district)
    return json.dumps(data, ensure_ascii=False, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
# GENERATE RECOMMENDATION (via OpenRouter)
# ─────────────────────────────────────────────────────────────────────────────

def generate_rec(context: str) -> str:
    """Generate recommendation using OpenRouter."""
    try:
        response = _get_openrouter().chat.completions.create(
            model=AGENT_MODEL,
            messages=[
                {"role": "system", "content": f"""Kamu adalah ahli lingkungan hidup dan analis kualitas air sungai Indonesia.
Berdasarkan data yang diberikan, buat laporan rekomendasi dalam Bahasa Indonesia dengan format:

📍 STASIUN: [nama stasiun dan lokasi]
📊 STATUS: [status mutu air dan indeks]
🔬 PROFIL PENCEMAR: [hasil analisis COD/BOD ratio]
🌧️ CURAH HUJAN: [data curah hujan dan klasifikasi]
🏭 KETAATAN SPARING: [status taat/langgar per industri]
📈 BENCHMARK IKA: [IKA aktual vs target, gap]

⚠️ LEVEL URGENSI: [PANTAU / WASPADA / TINDAK]
🎯 CONFIDENCE: [0-100%]

📋 REKOMENDASI:
1. [Tindakan spesifik dan terlokalisasi]
2. [Tindakan lanjutan jika diperlukan]

💡 REASONING: [Penjelasan singkat mengapa kesimpulan ini diambil]

DAS target: {TARGET_DAS} | Region: {TARGET_REGION}"""},
                {"role": "user", "content": context}
            ],
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"generate_rec failed: {e}")
        return f"ERROR: Recommendation generation failed: {e}"


# ─────────────────────────────────────────────────────────────────────────────
# ANALYSIS TOOLS (pure logic, no external dependencies)
# ─────────────────────────────────────────────────────────────────────────────

def think(thought: str) -> str:
    """Internal reasoning tool for planning and self-reflection."""
    logger.info(f"[THINK] {thought}")
    return f"Thought recorded: {thought}"


def calculate_pollution_profile(cod: float, bod: float) -> str:
    """COD/BOD ratio → pollution source profile."""
    if bod <= 0:
        return json.dumps({"error": "BOD is zero or negative", "cod": cod, "bod": bod})

    ratio = round(cod / bod, 2)
    if ratio > RATIO_INDUSTRY_THRESHOLD:
        profile, desc = "INDUSTRI", "Kuat mengindikasikan limbah industri"
    elif ratio > RATIO_MIXED_LOW:
        profile, desc = "CAMPURAN", "Campuran limbah industri dan domestik"
    else:
        profile, desc = "DOMESTIK", "Dominan limbah domestik/organik"

    return json.dumps({
        "cod": cod, "bod": bod, "cod_bod_ratio": ratio,
        "pollution_profile": profile, "description": desc,
        "threshold_industry": RATIO_INDUSTRY_THRESHOLD,
        "threshold_mixed": RATIO_MIXED_LOW,
    }, ensure_ascii=False)


def detect_anomaly(station_data: str) -> str:
    """Detect anomaly in station data."""
    try:
        data = json.loads(station_data) if isinstance(station_data, str) else station_data
        if isinstance(data, list):
            data = data[0] if data else {}

        index = safe_float(data.get("indeks_mutu", data.get("water_quality_index")))
        status = data.get("status", "").upper()
        station_id = data.get("station_id", "unknown")
        params = data.get("parameter", {})
        cod = safe_float(params.get("cod", data.get("cod")))
        bod = safe_float(params.get("bod", data.get("bod")))
        tss = safe_float(params.get("tss", data.get("tss")))

        is_anomaly = False
        reasons = []

        if index >= ANOMALY_INDEX_THRESHOLD:
            is_anomaly = True
            reasons.append(f"Indeks mutu {index} >= threshold {ANOMALY_INDEX_THRESHOLD}")
        if "CEMAR SEDANG" in status or "CEMAR BERAT" in status:
            is_anomaly = True
            reasons.append(f"Status: {status}")

        history = get_station_history(station_id, days=30)
        if history:
            avg = sum(safe_float(h.get("water_quality_index", h.get("indeks_mutu"))) for h in history) / len(history)
            if avg > 0:
                change = ((index - avg) / avg) * 100
                if abs(change) > ANOMALY_CHANGE_PERCENT:
                    is_anomaly = True
                    reasons.append(f"Perubahan {change:.1f}% dari baseline ({avg:.2f})")

        param_values = {"COD": cod, "BOD": bod, "TSS": tss}
        critical = max(param_values, key=lambda k: param_values[k]) if any(param_values.values()) else "N/A"

        return json.dumps({
            "station_id": station_id,
            "station_name": data.get("station_name", "unknown"),
            "indeks_mutu": index, "status": status,
            "is_anomaly": is_anomaly, "reasons": reasons,
            "critical_parameter": critical,
            "critical_value": param_values.get(critical, 0),
            "cod": cod, "bod": bod, "tss": tss,
            "history_count": len(history),
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Anomaly detection failed: {e}"})


def evaluate_rainfall_branching(rainfall_data: str) -> str:
    """Step 3 branching: is anomaly caused by rainfall?"""
    try:
        data = json.loads(rainfall_data) if isinstance(rainfall_data, str) else rainfall_data
        total_mm = safe_float(data.get("total_rainfall_mm"))
        is_runoff = total_mm > RAINFALL_HIGH_MM

        return json.dumps({
            "total_rainfall_mm": total_mm,
            "threshold_mm": RAINFALL_HIGH_MM,
            "classification": "LIMPASAN" if is_runoff else "BUKAN_LIMPASAN",
            "is_runoff": is_runoff,
            "should_continue_to_step4": not is_runoff,
            "reasoning": (
                f"Curah hujan {total_mm}mm/24h "
                f"{'melebihi' if is_runoff else 'di bawah'} threshold {RAINFALL_HIGH_MM}mm. "
                f"{'Anomali disebabkan limpasan hujan.' if is_runoff else 'Bukan limpasan — lanjut investigasi industri.'}"
            )
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Rainfall evaluation failed: {e}"})


def check_sparing_compliance(monitoring_data: str) -> str:
    """Check Sparing monitoring data against baku mutu.

    Supports both:
    - Pre-computed status_taat from ETL (preferred)
    - Manual value vs baku_mutu comparison (fallback)
    """
    try:
        data = json.loads(monitoring_data) if isinstance(monitoring_data, str) else monitoring_data
        if not isinstance(data, list):
            data = [data]

        results, violations = [], []
        for record in data:
            value = safe_float(record.get("value"))
            bm = safe_float(record.get("baku_mutu"))
            bm_min = safe_float(record.get("baku_mutu_min"))
            param = record.get("parameter", "unknown")
            company = record.get("company_name", record.get("company_id", "unknown"))

            # Use pre-computed status from ETL if available
            pre_status = record.get("status")
            if pre_status in ("TAAT", "LANGGAR"):
                is_ok = pre_status == "TAAT"
            elif bm > 0:
                # pH uses range check (min <= value <= max)
                if param.lower() == "ph" and bm_min > 0:
                    is_ok = bm_min <= value <= bm
                else:
                    is_ok = value <= bm
            else:
                continue  # No baku mutu to compare against

            entry = {
                "company": company, "parameter": param,
                "value": value, "baku_mutu": bm,
                "unit": record.get("unit", "mg/L"),
                "status": "TAAT" if is_ok else "LANGGAR",
                "pct_of_bm": round((value / bm) * 100, 1) if bm > 0 else 0,
                "date": record.get("date", "N/A"),
            }
            results.append(entry)
            if not is_ok:
                violations.append(entry)

        return json.dumps({
            "total_checked": len(results),
            "total_violations": len(violations),
            "overall_status": "LANGGAR" if violations else "TAAT",
            "details": results, "violations": violations,
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"Compliance check failed: {e}"})


def calculate_ika_gap(sitala_data: str) -> str:
    """Calculate IKA actual vs target gap."""
    try:
        data = json.loads(sitala_data) if isinstance(sitala_data, str) else sitala_data
        if isinstance(data, list):
            data = data[0] if data else {}

        actual = safe_float(data.get("ika_actual"))
        target = safe_float(data.get("ika_target"))
        gap = round(actual - target, 2) if actual and target else None

        if gap is not None:
            if gap <= IKA_GAP_CRITICAL:
                urgency = "TINDAK"
            elif gap <= IKA_GAP_WARNING:
                urgency = "WASPADA"
            else:
                urgency = "PANTAU"
        else:
            urgency = "DATA_TIDAK_TERSEDIA"

        return json.dumps({
            "district": data.get("kabkot", TARGET_REGION),
            "ika_actual": actual, "ika_target": target,
            "ika_gap": gap, "urgency_from_ika": urgency,
            "trend_yoy": data.get("trend_yoy"),
            "year": data.get("year", "N/A"),
        }, ensure_ascii=False, indent=2)
    except Exception as e:
        return json.dumps({"error": f"IKA gap failed: {e}"})


def log_anomaly_to_db(anomaly_json: str) -> str:
    """Save anomaly to MySQL anomaly_log table."""
    try:
        data = json.loads(anomaly_json) if isinstance(anomaly_json, str) else anomaly_json
        return log_anomaly(data)
    except Exception as e:
        return f"ERROR: {e}"


# ─────────────────────────────────────────────────────────────────────────────
# CROSS-CORRELATION TOOL (used exclusively by DataEvaluatorAgent)
# ─────────────────────────────────────────────────────────────────────────────

import math

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km between two lat/lon points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


# Industry types that match INDUSTRI pollution profile
_INDUSTRIAL_TYPES = {
    "tekstil", "textile", "pewarnaan", "finishing", "printing",
    "kimia", "farmasi", "chemical", "pulp", "kertas", "paper",
    "electroplating", "logam", "metal", "penyamakan", "kulit",
}

def cross_correlate_evidence(station_json: str, step4_json: str, step2_json: str) -> str:
    """
    Cross-correlate anomalous station with Sparing findings.

    For each violating industry, scores three dimensions:
      1. Spatial  — distance from logger outlet to station (km)
      2. Temporal — was the violation dated on/before the anomaly?
      3. Profile  — does the industry type match the COD/BOD pollution profile?

    Returns a causal evidence list ranked by confidence (TINGGI → SEDANG → RENDAH).
    """
    try:
        station = json.loads(station_json) if isinstance(station_json, str) else station_json
        step4   = json.loads(step4_json)   if isinstance(step4_json, str)   else step4_json
        step2   = json.loads(step2_json)   if isinstance(step2_json, str)   else step2_json

        if isinstance(station, list):
            station = station[0] if station else {}

        s_lat  = safe_float(station.get("latitude"))
        s_lon  = safe_float(station.get("longitude"))
        s_ts   = station.get("timestamp", "")[:10]  # YYYY-MM-DD
        profile = (step2.get("profile") or "").upper()

        industries = step4.get("industries", [])
        results = []

        for ind in industries:
            company   = ind.get("company_name", "unknown")
            id_logger = ind.get("id_logger", "")
            ind_lat   = safe_float(ind.get("latitude",  step4.get("lat",  0)))
            ind_lon   = safe_float(ind.get("longitude", step4.get("lon",  0)))
            ind_type  = (ind.get("industry_type") or "").lower()
            violations = ind.get("violations", [])

            if not violations:
                continue

            # ── 1. Spatial score ──────────────────────────────────────────
            if s_lat and s_lon and ind_lat and ind_lon:
                dist_km = round(_haversine_km(s_lat, s_lon, ind_lat, ind_lon), 2)
                spatial_ok = dist_km <= 10.0   # within 10 km
                spatial_note = f"{dist_km}km dari stasiun"
            else:
                dist_km = None
                spatial_ok = True              # no coordinates → can't rule out
                spatial_note = "koordinat tidak tersedia"

            # ── 2. Temporal score ─────────────────────────────────────────
            violation_dates = [v.get("date", "")[:10] for v in violations if v.get("date")]
            most_recent_violation = max(violation_dates) if violation_dates else ""
            temporal_ok = bool(most_recent_violation and most_recent_violation <= s_ts) if s_ts else True
            temporal_note = (
                f"pelanggaran {most_recent_violation}, anomali {s_ts}"
                if most_recent_violation else "tanggal pelanggaran tidak tersedia"
            )

            # ── 3. Profile score ──────────────────────────────────────────
            profile_ok = any(t in ind_type for t in _INDUSTRIAL_TYPES) and profile == "INDUSTRI"
            profile_note = (
                f"industri '{ind_type}' sesuai profil {profile}"
                if profile_ok else f"industri '{ind_type}' kurang sesuai profil {profile}"
            )

            # ── Composite confidence ──────────────────────────────────────
            score = sum([spatial_ok, temporal_ok, profile_ok])
            if score == 3:
                confidence = "TINGGI"
            elif score == 2:
                confidence = "SEDANG"
            else:
                confidence = "RENDAH"

            results.append({
                "company_name":          company,
                "id_logger":             id_logger,
                "distance_km":           dist_km,
                "spatial_ok":            spatial_ok,
                "temporal_ok":           temporal_ok,
                "profile_ok":            profile_ok,
                "causal_confidence":     confidence,
                "confidence_score":      score,
                "violated_params":       [v.get("parameter") for v in violations],
                "most_recent_violation": most_recent_violation,
                "evidence_notes": {
                    "spatial":  spatial_note,
                    "temporal": temporal_note,
                    "profile":  profile_note,
                },
            })

        # Sort by confidence score descending
        results.sort(key=lambda r: r["confidence_score"], reverse=True)

        top = results[0]["company_name"] if results else "tidak ada"
        verdict = results[0]["causal_confidence"] if results else "TIDAK_CUKUP_DATA"

        return json.dumps({
            "station_id":       station.get("station_id"),
            "candidates_found": len(results),
            "top_suspect":      top,
            "verdict":          verdict,
            "causal_chain":     results,
        }, ensure_ascii=False, indent=2)

    except Exception as e:
        return json.dumps({"error": f"cross_correlate_evidence failed: {e}"})


# ─────────────────────────────────────────────────────────────────────────────
# TOOL DEFINITIONS (declared to the LLM)
# ─────────────────────────────────────────────────────────────────────────────

TOOLS = [
    {"type": "function", "function": {
        "name": "think",
        "description": "Internal reasoning tool. Plan, evaluate, and self-reflect before acting. Always start complex analyses with think().",
        "parameters": {"type": "object", "properties": {
            "thought": {"type": "string", "description": "Your reasoning or plan"}
        }, "required": ["thought"]}
    }},
    {"type": "function", "function": {
        "name": "query_onlimo",
        "description": "Query Onlimo KLHK station data from database: water quality index, status, COD/BOD/TSS/DO/pH. Call this first in Step 1. Leave station_id empty to get all stations.",
        "parameters": {"type": "object", "properties": {
            "station_id": {"type": "string", "description": "Station ID (e.g. 'KLHK02'). Empty = all stations."},
            "das": {"type": "string", "description": f"DAS name. Default: {TARGET_DAS}"}
        }}
    }},
    {"type": "function", "function": {
        "name": "get_bmkg_rain",
        "description": "Get BMKG rainfall data from database. Used in Step 3 branching: if total > 50mm/24h → LIMPASAN → STOP. If low → continue Step 4.",
        "parameters": {"type": "object", "properties": {
            "location": {"type": "string", "description": "Location name (kecamatan/desa/kotkab)"},
            "lat": {"type": "number", "description": "Latitude"},
            "lon": {"type": "number", "description": "Longitude"}
        }}
    }},
    {"type": "function", "function": {
        "name": "query_sparing_logger",
        "description": (
            "Get Sparing Logger data from database: outlet IPAL locations and industries. "
            "Used in Step 4. Results contain 'id_logger' field — use this value when calling "
            "query_sparing_monitoring() to get monitoring data for a specific logger."
        ),
        "parameters": {"type": "object", "properties": {
            "das": {"type": "string", "description": f"DAS name. Default: {TARGET_DAS}"},
            "district": {"type": "string", "description": "Kabupaten/kota or kecamatan name to filter (matched against industry address)"}
        }}
    }},
    {"type": "function", "function": {
        "name": "query_sparing_monitoring",
        "description": (
            "Get Sparing Monitoring data from database: daily parameter values vs baku mutu for a logger. "
            "Used in Step 4. Pass the 'id_logger' value from query_sparing_logger() results as company_id."
        ),
        "parameters": {"type": "object", "properties": {
            "company_id": {"type": "string", "description": "Logger ID ('id_logger' field from query_sparing_logger results)"},
            "days": {"type": "integer", "description": "Days of data. Default: 3"}
        }}
    }},
    {"type": "function", "function": {
        "name": "query_sitala",
        "description": "Get SITALA IKA data from database: water quality index per kabupaten/kota vs RPJMN target. Used in Step 5.",
        "parameters": {"type": "object", "properties": {
            "district": {"type": "string", "description": f"Kabupaten/kota name. Default: {TARGET_REGION}"}
        }}
    }},
    {"type": "function", "function": {
        "name": "generate_rec",
        "description": "Generate final recommendation report. Called in Step 5 with FULL context from Steps 1-4.",
        "parameters": {"type": "object", "properties": {
            "context": {"type": "string", "description": "Full reasoning context from all previous steps"}
        }, "required": ["context"]}
    }},
    {"type": "function", "function": {
        "name": "calculate_pollution_profile",
        "description": "COD/BOD ratio analysis. >4.0=INDUSTRI, 2-4=CAMPURAN, <2=DOMESTIK. Used in Step 2.",
        "parameters": {"type": "object", "properties": {
            "cod": {"type": "number", "description": "COD value"},
            "bod": {"type": "number", "description": "BOD value"}
        }, "required": ["cod", "bod"]}
    }},
    {"type": "function", "function": {
        "name": "detect_anomaly",
        "description": "Detect anomaly in station data: index >= 3.0, CEMAR status, vs historical baseline.",
        "parameters": {"type": "object", "properties": {
            "station_data": {"type": "string", "description": "JSON string of station data"}
        }, "required": ["station_data"]}
    }},
    {"type": "function", "function": {
        "name": "evaluate_rainfall_branching",
        "description": "Step 3 branching: total_rainfall > 50mm/24h → LIMPASAN → STOP. Else continue to Step 4.",
        "parameters": {"type": "object", "properties": {
            "rainfall_data": {"type": "string", "description": "JSON string of rainfall data"}
        }, "required": ["rainfall_data"]}
    }},
    {"type": "function", "function": {
        "name": "check_sparing_compliance",
        "description": "Check monitoring values vs baku mutu. Returns TAAT or LANGGAR per parameter. Supports pre-computed status_taat from ETL.",
        "parameters": {"type": "object", "properties": {
            "monitoring_data": {"type": "string", "description": "JSON string of monitoring data"}
        }, "required": ["monitoring_data"]}
    }},
    {"type": "function", "function": {
        "name": "calculate_ika_gap",
        "description": "IKA actual vs target gap. gap < -7 = TINDAK, < -3 = WASPADA, else PANTAU.",
        "parameters": {"type": "object", "properties": {
            "sitala_data": {"type": "string", "description": "JSON string of SITALA data"}
        }, "required": ["sitala_data"]}
    }},
    {"type": "function", "function": {
        "name": "log_anomaly_to_db",
        "description": "Save anomaly results to MySQL anomaly_log table for historical tracking.",
        "parameters": {"type": "object", "properties": {
            "anomaly_json": {"type": "string", "description": "JSON string of anomaly details"}
        }, "required": ["anomaly_json"]}
    }},
    {"type": "function", "function": {
        "name": "cross_correlate_evidence",
        "description": (
            "Cross-correlate an anomalous station with Sparing violation findings. "
            "Scores each violating industry across three dimensions: "
            "(1) spatial — distance from logger to station, "
            "(2) temporal — was the violation dated before the anomaly? "
            "(3) profile — does the industry type match the COD/BOD pollution profile? "
            "Returns ranked causal evidence with confidence: TINGGI / SEDANG / RENDAH. "
            "Call this after check_sparing_compliance(), before finalising your evaluation."
        ),
        "parameters": {"type": "object", "properties": {
            "station_json": {"type": "string", "description": "JSON string of the anomalous station data (single station dict)"},
            "step4_json":   {"type": "string", "description": "JSON string of step4 Sparing results (output of check_sparing_compliance or query_sparing)"},
            "step2_json":   {"type": "string", "description": "JSON string of step2 pollution profile result"},
        }, "required": ["station_json", "step4_json", "step2_json"]}
    }},
]

# ── Tool executor ────────────────────────────────────────────────────────────
TOOL_FUNCTIONS = {
    "think":                        lambda args: think(args["thought"]),
    "query_onlimo":                 lambda args: query_onlimo(args.get("station_id", ""), args.get("das", TARGET_DAS)),
    "get_bmkg_rain":                lambda args: get_bmkg_rain(args.get("location", ""), args.get("lat", 0.0), args.get("lon", 0.0)),
    "query_sparing_logger":         lambda args: query_sparing_logger(args.get("das", TARGET_DAS), args.get("district", "")),
    "query_sparing_monitoring":     lambda args: query_sparing_monitoring(args.get("company_id", ""), args.get("days", 3)),
    "query_sitala":                 lambda args: query_sitala(args.get("district", TARGET_REGION)),
    "generate_rec":                 lambda args: generate_rec(args["context"]),
    "calculate_pollution_profile":  lambda args: calculate_pollution_profile(args["cod"], args["bod"]),
    "detect_anomaly":               lambda args: detect_anomaly(args["station_data"]),
    "evaluate_rainfall_branching":  lambda args: evaluate_rainfall_branching(args["rainfall_data"]),
    "check_sparing_compliance":     lambda args: check_sparing_compliance(args["monitoring_data"]),
    "calculate_ika_gap":            lambda args: calculate_ika_gap(args["sitala_data"]),
    "log_anomaly_to_db":            lambda args: log_anomaly_to_db(args["anomaly_json"]),
    "cross_correlate_evidence":     lambda args: cross_correlate_evidence(
                                        args["station_json"], args["step4_json"], args["step2_json"]
                                    ),
}