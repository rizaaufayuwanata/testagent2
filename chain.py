# ─────────────────────────────────────────────────────────────────────────────
# chain.py — Multi-Agent Chain Orchestrator for WQSA
# ─────────────────────────────────────────────────────────────────────────────
#
# Three phases:
#
#   Phase 1 (Python, deterministic)
#     Fetch ALL raw data from MySQL — stations, rainfall, sparing, SITALA.
#     No LLM involved. Build a raw_data_bundle dict.
#
#   Phase 2 (DataEvaluatorAgent — agentic loop)
#     Receives raw_data_bundle. Calls analysis tools iteratively.
#     Cross-correlates evidence. Outputs structured EvaluationResult JSON.
#
#   Phase 3 (AnalyticalAgent — single LLM call)
#     Receives EvaluationResult. Writes the final Indonesian report.
#
# ─────────────────────────────────────────────────────────────────────────────

import json
import re
import logging
from datetime import datetime
from typing import Optional

from config import (
    TARGET_DAS, TARGET_REGION,
    ANOMALY_INDEX_THRESHOLD,
)
from data_layer import (
    get_onlimo_data, get_rainfall_data,
    get_sparing_logger_data, get_sparing_monitoring_data,
    get_sitala_data, log_anomaly,
    safe_float,
)
from agents import DataEvaluatorAgent, AnalyticalAgent

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# INPUT PARSER
# ─────────────────────────────────────────────────────────────────────────────

def parse_target(text: str) -> dict:
    """Parse user message to determine analysis scope."""
    text_lower = text.lower().strip()

    station_match = re.search(r'klhk\s*(\d+)', text_lower)
    if station_match:
        return {"type": "station", "station_id": f"KLHK{station_match.group(1)}"}

    loc_match = re.search(
        r'(?:di|area|wilayah|sekitar|lokasi|daerah)\s+([a-zA-Z]+(?:\s+[a-zA-Z]+)?)',
        text_lower
    )
    if loc_match:
        return {"type": "location", "location": loc_match.group(1).strip().title()}

    return {"type": "all"}


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 1 — Deterministic data fetch
# ─────────────────────────────────────────────────────────────────────────────

def fetch_raw_data(target: dict) -> dict:
    """
    Fetch all data needed for evaluation.
    Returns a raw_data_bundle dict passed to DataEvaluatorAgent.
    """
    logger.info(f"[Phase 1] Fetching raw data — target: {target}")

    # ── Fetch stations ────────────────────────────────────────────────────────
    if target["type"] == "station":
        stations = get_onlimo_data(station_id=target["station_id"], das=TARGET_DAS)
    elif target["type"] == "location":
        all_stations = get_onlimo_data(das=TARGET_DAS)
        loc = target["location"].lower()
        stations = [
            s for s in all_stations
            if loc in (s.get("kecamatan") or "").lower()
            or loc in (s.get("kabkot") or "").lower()
            or loc in (s.get("station_name") or "").lower()
        ] or all_stations  # fallback to all if no match
    else:
        stations = get_onlimo_data(das=TARGET_DAS)

    valid_stations = [s for s in stations if "error" not in s and "info" not in s]
    logger.info(f"[Phase 1] Fetched {len(valid_stations)} stations")

    # Quick pre-filter: only fetch rain/sparing for anomalous-looking stations
    # to avoid N×M queries when scanning all 500+ stations nationally
    candidates = [
        s for s in valid_stations
        if safe_float(s.get("indeks_mutu")) >= ANOMALY_INDEX_THRESHOLD
        or "CEMAR" in (s.get("status") or "").upper()
    ]
    if not candidates:
        # If nothing is above threshold, still include all (evaluator will confirm)
        candidates = valid_stations[:10]

    logger.info(f"[Phase 1] {len(candidates)} candidate stations for detail fetch")

    # ── Fetch rainfall for each candidate ────────────────────────────────────
    rainfall_by_station = {}
    for s in candidates:
        sid = s.get("station_id", "")
        rain = get_rainfall_data(
            location=s.get("kecamatan", ""),
            lat=safe_float(s.get("latitude")),
            lon=safe_float(s.get("longitude")),
        )
        rainfall_by_station[sid] = rain

    # ── Fetch sparing for each candidate ─────────────────────────────────────
    sparing_by_station = {}
    seen_districts = set()

    for s in candidates:
        sid = s.get("station_id", "")
        district = s.get("kecamatan") or s.get("kabkot") or ""

        if district in seen_districts:
            # Re-use already-fetched data for same district
            for other_sid, data in sparing_by_station.items():
                if data.get("_district") == district:
                    sparing_by_station[sid] = data
                    break
            continue

        seen_districts.add(district)
        loggers = get_sparing_logger_data(das=TARGET_DAS, district=district)
        valid_loggers = [l for l in loggers if "error" not in l and "info" not in l]

        monitoring_by_logger = {}
        for lgr in valid_loggers:
            id_logger = lgr.get("id_logger") or lgr.get("company_id")
            if id_logger:
                mon = get_sparing_monitoring_data(company_id=id_logger, days=3)
                monitoring_by_logger[id_logger] = [
                    m for m in mon if "error" not in m and "info" not in m
                ]

        sparing_by_station[sid] = {
            "_district": district,
            "loggers": valid_loggers,
            "monitoring": monitoring_by_logger,
        }

    # ── Fetch SITALA for the region ───────────────────────────────────────────
    sitala = get_sitala_data(district=TARGET_REGION)
    valid_sitala = [s for s in sitala if "error" not in s and "info" not in s]

    logger.info("[Phase 1] Raw data fetch complete")

    return {
        "target":              target,
        "fetch_timestamp":     datetime.now().isoformat(),
        "all_stations":        valid_stations,
        "candidate_stations":  candidates,
        "rainfall":            rainfall_by_station,
        "sparing":             sparing_by_station,
        "sitala":              valid_sitala,
        "region":              TARGET_REGION,
        "das":                 TARGET_DAS,
    }


# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT FORMATTER
# ─────────────────────────────────────────────────────────────────────────────

def format_output(evaluation: dict, report: str) -> str:
    """Combine evaluation trace with analytical report for Telegram."""
    lines = []
    lines.append("🔗 *WQSA ANALYSIS COMPLETE*")
    lines.append(f"⏱️ {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("━" * 30)

    # Evaluation summary header
    anomalous = evaluation.get("anomalous_stations", [])
    normal_count = evaluation.get("normal_station_count", 0)
    lines.append(f"\n📡 *Evaluation Summary*")
    lines.append(f"  Anomali terdeteksi: {len(anomalous)} stasiun ⚠️")
    lines.append(f"  Kondisi normal: {normal_count} stasiun ✅")

    if anomalous:
        lines.append(f"\n  Top suspects:")
        for s in anomalous:
            top = s.get("top_suspect") or "tidak teridentifikasi"
            urgency = s.get("urgency", "")
            emoji = {"TINDAK": "🔴", "WASPADA": "🟡", "PANTAU": "🟢"}.get(urgency, "⚪")
            confidence = ""
            causal = s.get("causal_evidence", [])
            if causal:
                confidence = f" [{causal[0].get('causal_confidence', '')}]"
            lines.append(f"  {emoji} {s.get('station_id')} → {top}{confidence}")

    lines.append(f"\n{'━' * 30}")
    lines.append("\n" + report)
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# DB LOGGER
# ─────────────────────────────────────────────────────────────────────────────

def _log_evaluation_to_db(evaluation: dict, report: str,
                           user_id: Optional[int], session_id: Optional[str]):
    """Persist each anomalous station from evaluation to anomaly_log."""
    for s in evaluation.get("anomalous_stations", []):
        causal = s.get("causal_evidence", [])
        top = causal[0] if causal else {}
        try:
            log_anomaly({
                "station_id":        s.get("station_id"),
                "station_name":      s.get("station_name"),
                "indeks_mutu":       s.get("indeks_mutu"),
                "status_mutu":       s.get("status"),
                "is_anomaly":        True,
                "reasons":           s.get("anomaly_reasons", []),
                "critical_parameter": (s.get("causal_evidence") or [{}])[0].get("violated_params", [""])[0],
                "pollution_profile": s.get("pollution_profile"),
                "cod_bod_ratio":     s.get("cod_bod_ratio"),
                "rainfall_mm_24h":   s.get("rainfall_mm"),
                "is_runoff":         s.get("is_limpasan", False),
                "urgency_level":     s.get("urgency"),
                "ika_gap":           evaluation.get("ika_gap"),
                "recommendation":    report[:2000],
                "telegram_user_id":  user_id,
                "session_id":        session_id,
            })
        except Exception as e:
            logger.error(f"Failed to log {s.get('station_id')}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN CHAIN RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def run_chain(user_text: str, user_id: Optional[int] = None,
              session_id: Optional[str] = None) -> str:
    """
    Run the full multi-agent chain:
      Phase 1 — fetch raw data (Python)
      Phase 2 — DataEvaluatorAgent evaluates + cross-correlates
      Phase 3 — AnalyticalAgent writes the report
    """
    print(f"\n{'━'*50}")
    print(f"🔗 WQSA CHAIN — {user_text[:50]}")
    print(f"{'━'*50}")

    # ── Phase 1: Fetch ────────────────────────────────────────────────────────
    target = parse_target(user_text)
    print(f"\n📡 [Phase 1] Fetching raw data — target: {target}")
    raw_bundle = fetch_raw_data(target)

    if not raw_bundle["candidate_stations"]:
        print("  ⚠️  No candidate stations found")
        return "ℹ️ Tidak ada data stasiun ditemukan di database. Pastikan ETL sudah dijalankan."

    print(f"  ✅ {len(raw_bundle['all_stations'])} total stations, "
          f"{len(raw_bundle['candidate_stations'])} candidates for analysis")

    # ── Phase 2: Evaluator agent ──────────────────────────────────────────────
    print(f"\n🤖 [Phase 2] Handing off to DataEvaluatorAgent")
    logger.info("[Chain] Handing off to DataEvaluatorAgent")
    evaluator = DataEvaluatorAgent()
    evaluation = evaluator.run(raw_bundle)

    anomalous_count = len(evaluation.get("anomalous_stations", []))
    print(f"\n  📊 Evaluator result: {anomalous_count} anomalous station(s) found")

    # ── Phase 3: Analytical agent ─────────────────────────────────────────────
    print(f"\n🧠 [Phase 3] Handing off to AnalyticalAgent")
    logger.info("[Chain] Handing off to AnalyticalAgent")
    analytical = AnalyticalAgent()
    report = analytical.run(evaluation)

    # ── Persist + format ──────────────────────────────────────────────────────
    print(f"\n💾 Logging results to DB...")
    try:
        _log_evaluation_to_db(evaluation, report, user_id, session_id)
        print(f"  ✅ Logged {anomalous_count} anomaly record(s)")
    except Exception as e:
        print(f"  ⚠️  DB log failed: {e}")
        logger.error(f"[Chain] DB log failed: {e}")

    print(f"\n✅ Chain complete — sending reply to Telegram\n{'━'*50}\n")
    return format_output(evaluation, report)