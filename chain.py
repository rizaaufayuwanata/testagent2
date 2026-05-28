# =============================================================================
# chain.py — Multi-Agent Pipeline Orchestrator (Hardcoded)
# =============================================================================
# Phase 1: Python fetch raw data from MySQL (deterministic)
# Phase 2: DataEvaluatorAgent → validate, clean, flag issues
# Phase 3: DataAnalystAgent → reasoning chain → AnalysisResult
# Phase 4: ReportEvaluatorAgent → validate → accept OR feedback to Phase 3
#           Max 2 feedback loops, then force accept
# =============================================================================

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
from agents import DataEvaluatorAgent, DataAnalystAgent, ReportEvaluatorAgent

logger = logging.getLogger(__name__)

MAX_FEEDBACK_LOOPS = 2


# =============================================================================
# INPUT PARSER
# =============================================================================

def parse_target(analysis_type: str, target_value: str = "") -> dict:
    """Parse dashboard input into target dict."""
    if analysis_type == "station" and target_value:
        # Normalize station ID
        match = re.search(r'KLHK\s*(\d+)', target_value, re.IGNORECASE)
        sid = f"KLHK{match.group(1)}" if match else target_value.upper()
        return {"type": "station", "station_id": sid}
    elif analysis_type == "region" and target_value:
        return {"type": "location", "location": target_value.strip().title()}
    else:
        return {"type": "all"}


# =============================================================================
# PHASE 1 — Deterministic data fetch from MySQL
# =============================================================================

def fetch_raw_data(target: dict) -> dict:
    print(f"\n📡 [Phase 1] Fetching raw data — target: {target}")

    # Fetch stations
    if target["type"] == "station":
        stations = get_onlimo_data(station_id=target["station_id"], das=TARGET_DAS)
    elif target["type"] == "location":
        all_st = get_onlimo_data(das=TARGET_DAS)
        loc = target["location"].lower()
        stations = [
            s for s in all_st
            if loc in (s.get("kecamatan") or "").lower()
            or loc in (s.get("kabkot") or "").lower()
            or loc in (s.get("station_name") or "").lower()
        ] or all_st
    else:
        stations = get_onlimo_data(das=TARGET_DAS)

    valid = [s for s in stations if "error" not in s and "info" not in s]
    print(f"  Stations fetched: {len(valid)}")

    # Pre-filter candidates
    candidates = [
        s for s in valid
        if safe_float(s.get("indeks_mutu")) >= ANOMALY_INDEX_THRESHOLD
        or "CEMAR" in (s.get("status") or "").upper()
    ] or valid[:10]

    print(f"  Candidates for analysis: {len(candidates)}")

    # Fetch rainfall per candidate
    rainfall = {}
    for s in candidates:
        sid = s.get("station_id", "")
        rainfall[sid] = get_rainfall_data(
            location=s.get("kecamatan", ""),
            lat=safe_float(s.get("latitude")),
            lon=safe_float(s.get("longitude")),
        )

    # Fetch sparing per district
    sparing = {}
    seen = set()
    for s in candidates:
        sid = s.get("station_id", "")
        district = s.get("kecamatan") or s.get("kabkot") or ""
        if district in seen:
            for k, v in sparing.items():
                if v.get("_district") == district:
                    sparing[sid] = v
                    break
            continue
        seen.add(district)
        loggers = get_sparing_logger_data(das=TARGET_DAS, district=district)
        valid_l = [l for l in loggers if "error" not in l and "info" not in l]
        mon = {}
        for lgr in valid_l:
            lid = lgr.get("id_logger") or lgr.get("company_id")
            if lid:
                mon[lid] = [m for m in get_sparing_monitoring_data(company_id=lid, days=3) if "error" not in m and "info" not in m]
        sparing[sid] = {"_district": district, "loggers": valid_l, "monitoring": mon}

    # Fetch SITALA
    sitala = [s for s in get_sitala_data(district=TARGET_REGION) if "error" not in s and "info" not in s]

    print(f"  ✅ Phase 1 complete\n")
    return {
        "target": target,
        "fetch_timestamp": datetime.now().isoformat(),
        "all_stations": valid,
        "candidate_stations": candidates,
        "rainfall": rainfall,
        "sparing": sparing,
        "sitala": sitala,
        "region": TARGET_REGION,
        "das": TARGET_DAS,
    }


# =============================================================================
# DB LOGGER
# =============================================================================

def _log_results(analysis: dict, report: dict, session_id: str):
    for s in analysis.get("anomalous_stations", []):
        causal = s.get("causal_evidence", [])
        top = causal[0] if causal else {}
        try:
            log_anomaly({
                "station_id":       s.get("station_id"),
                "indeks_mutu":      s.get("indeks_mutu"),
                "status_mutu":      s.get("status"),
                "is_anomaly":       True,
                "reasons":          s.get("anomaly_reasons", []),
                "critical_parameter": (top.get("violated_params") or [""])[0] if top else "",
                "pollution_profile": s.get("pollution_profile"),
                "cod_bod_ratio":    s.get("cod_bod_ratio"),
                "rainfall_mm_24h":  s.get("rainfall_mm"),
                "is_runoff":        s.get("is_limpasan", False),
                "urgency_level":    s.get("urgency"),
                "ika_gap":          analysis.get("ika_gap"),
                "recommendation":   json.dumps(report.get("analysis_highlight", {}).get("priority_actions", []), ensure_ascii=False)[:2000],
                "session_id":       session_id,
            })
        except Exception as e:
            logger.error(f"Log failed {s.get('station_id')}: {e}")


# =============================================================================
# MAIN CHAIN RUNNER
# =============================================================================

def run_chain(analysis_type: str = "full_scan", target_value: str = "",
              session_id: Optional[str] = None) -> dict:
    """
    Run the full 4-phase multi-agent chain.
    Returns structured dict for dashboard consumption.
    """
    ts = datetime.now()
    session_id = session_id or f"dash-{int(ts.timestamp())}"

    print(f"\n{'━'*50}")
    print(f"🔗 WQSA CHAIN — {analysis_type}: {target_value or 'all'}")
    print(f"{'━'*50}")

    # ── Phase 1: Fetch ─────────────────────────────────────────────────────
    target = parse_target(analysis_type, target_value)
    raw_bundle = fetch_raw_data(target)

    if not raw_bundle["candidate_stations"]:
        return {
            "status": "no_data",
            "message": "Tidak ada data stasiun ditemukan. Pastikan ETL sudah dijalankan.",
            "analysis_highlight": None,
            "detail_reasoning": None,
        }

    # ── Phase 2: DataEvaluator ─────────────────────────────────────────────
    print(f"🤖 [Phase 2] DataEvaluator")
    evaluator = DataEvaluatorAgent()
    cleaned = evaluator.run(raw_bundle)
    data_quality = cleaned.get("data_quality", {})

    # ── Phase 3: DataAnalyst ───────────────────────────────────────────────
    print(f"\n🤖 [Phase 3] DataAnalyst")
    analyst = DataAnalystAgent()
    analysis = analyst.run(cleaned)

    # ── Phase 4: ReportEvaluator (with feedback loop) ──────────────────────
    print(f"\n🤖 [Phase 4] ReportEvaluator")
    reporter = ReportEvaluatorAgent()
    report = reporter.run(analysis, data_quality)

    loops = 0
    while report.get("action") == "feedback" and loops < MAX_FEEDBACK_LOOPS:
        loops += 1
        feedback_text = json.dumps({
            "issues": report.get("issues", []),
            "questions": report.get("questions", []),
        }, ensure_ascii=False)
        print(f"\n🔄 Feedback loop {loops}/{MAX_FEEDBACK_LOOPS}")

        # DataAnalyst revises
        analysis = analyst.run(cleaned, feedback=feedback_text)
        # ReportEvaluator re-validates
        report = reporter.run(analysis, data_quality)

    if report.get("action") == "feedback":
        print(f"  ⚠️ Max feedback loops reached — forcing accept")
        report["action"] = "accept"
        report["unresolved_issues"] = report.get("issues", [])

    # ── Log to DB ──────────────────────────────────────────────────────────
    print(f"\n💾 Logging results...")
    _log_results(analysis, report, session_id)

    # ── Build final output ─────────────────────────────────────────────────
    anomalous = analysis.get("anomalous_stations", [])
    normal_ct = analysis.get("normal_station_count", 0)
    print(f"\n✅ Chain complete — {len(anomalous)} anomalous, {normal_ct} normal")
    print(f"{'━'*50}\n")

    return {
        "status":             "done",
        "session_id":         session_id,
        "timestamp":          ts.isoformat(),
        "feedback_loops":     loops,
        "data_quality":       data_quality,
        "analysis_raw":       analysis,
        "analysis_highlight": report.get("analysis_highlight"),
        "detail_reasoning":   report.get("detail_reasoning"),
        "kpi_update":         report.get("kpi_update"),
        "ika_summary":        report.get("ika_summary"),
        "telegram_message":   report.get("telegram_message"),
        "unresolved_issues":  report.get("unresolved_issues", []),
    }