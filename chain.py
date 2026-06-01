# =============================================================================
# chain.py — Multi-Agent Pipeline Orchestrator
# =============================================================================
# Phase 1: Python fetch raw data from MySQL (deterministic)
# Phase 2: DataEvaluatorAgent → validate, clean via workspace tools
# Phase 3: DataAnalystAgent → reasoning chain → AnalysisResult
# Phase 4: ReportEvaluatorAgent → validate → accept OR feedback to Phase 3
#           Max 2 feedback loops, then force accept
# =============================================================================

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import json
import re
import logging
import time
from datetime import datetime
from typing import Optional


def _log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    logger.info(msg)

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
from response_schema import normalize_chain_output

logger = logging.getLogger(__name__)

MAX_FEEDBACK_LOOPS = 2


# =============================================================================
# INPUT PARSER
# =============================================================================

def parse_target(analysis_type: str, target_value: str = "") -> dict:
    """Parse dashboard input into target dict."""
    if analysis_type == "station" and target_value:
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
    _log(f"[Phase 1] Fetching raw data — target: {target}")

    if target["type"] == "station":
        # Jangan filter DAS saat analisis stasiun spesifik
        # — stasiun bisa berasal dari DAS mana saja
        stations = get_onlimo_data(station_id=target["station_id"])
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
    _log(f"[Phase 1] Stations fetched: {len(valid)}")

    candidates = [
        s for s in valid
        if safe_float(s.get("indeks_mutu")) >= ANOMALY_INDEX_THRESHOLD
        or "CEMAR" in (s.get("status") or "").upper()
    ] or valid[:10]

    _log(f"[Phase 1] Candidates for analysis: {len(candidates)}")

    rainfall = {}
    for s in candidates:
        sid = s.get("station_id", "")
        rainfall[sid] = get_rainfall_data(
            location=s.get("kecamatan", ""),
            lat=safe_float(s.get("latitude")),
            lon=safe_float(s.get("longitude")),
        )

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

    sitala = [s for s in get_sitala_data(district=TARGET_REGION) if "error" not in s and "info" not in s]

    _log(f"[Phase 1] Complete — {len(candidates)} candidates, {len(sitala)} sitala records")
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
# RELIABILITY ASSESSMENT
# =============================================================================

def _assess_reliability(data_quality: dict, raw_bundle: dict) -> dict:
    """Assess data reliability independent of agent output."""
    dq_score    = data_quality.get("overall_score", 100) if data_quality else 100
    excluded    = data_quality.get("excluded_stations", []) if data_quality else []
    high_null   = data_quality.get("high_null_fields", {}) if data_quality else {}
    outliers    = data_quality.get("outliers", []) if data_quality else []

    SENSOR_PARAMS = {"cod", "bod", "tss", "do", "ph", "amonia", "nitrat", "turbidity"}
    sensor_nulls = [k for k in high_null if any(p in k.lower() for p in SENSOR_PARAMS)]
    is_sensor_failure = len(sensor_nulls) >= 3

    dead_stations = []
    for s in raw_bundle.get("candidate_stations", []):
        params = s.get("parameter", {})
        non_zero = [v for v in params.values() if isinstance(v, (int, float)) and v > 0]
        if len(non_zero) == 0:
            sid = s.get("station_id", "")
            if sid:
                dead_stations.append(sid)

    is_reliable = (
        dq_score >= 60
        and not is_sensor_failure
        and len(excluded) == 0
        and len(dead_stations) == 0
    )

    if is_sensor_failure:
        level = "SENSOR_FAILURE"
        warning = (
            f"SENSOR/TELEMETRY CLUSTER FAILURE — {len(sensor_nulls)} parameter utama "
            f"tidak memiliki data valid ({', '.join(sensor_nulls[:4])}). "
            "Hasil PANTAU ini adalah FALSE POSITIVE — bukan berarti kondisi baik."
        )
    elif len(dead_stations) > 0:
        level = "NO_DATA"
        warning = (
            f"Stasiun {', '.join(dead_stations[:3])} tidak mengirim data sensor "
            "(semua nilai 0). Tidak dapat menilai kondisi sebenarnya."
        )
    elif dq_score < 60:
        level = "LOW_QUALITY"
        warning = (
            f"Kualitas data rendah (score: {dq_score}/100). "
            "Hasil analisis mungkin tidak mencerminkan kondisi sebenarnya."
        )
    else:
        level = "OK"
        warning = None

    return {
        "is_reliable":        is_reliable,
        "level":              level,
        "score":              dq_score,
        "warning":            warning,
        "sensor_null_params": sensor_nulls,
        "dead_stations":      dead_stations,
        "excluded_stations":  [e.get("station_id", "") for e in excluded] if isinstance(excluded, list) and excluded and isinstance(excluded[0], dict) else excluded,
    }


# =============================================================================
# MAIN CHAIN RUNNER
# =============================================================================

def run_chain(analysis_type: str = "full_scan", target_value: str = "",
              session_id: Optional[str] = None) -> dict:
    """
    Run the full 4-phase multi-agent chain.
    Returns normalized dict for dashboard consumption.
    """
    ts = datetime.now()
    session_id = session_id or f"dash-{int(ts.timestamp())}"

    _log(f"CHAIN START — {analysis_type}: {target_value or 'all'} | session={session_id}")
    chain_start = time.time()

    # ── Phase 1: Fetch ─────────────────────────────────────────────────────
    target = parse_target(analysis_type, target_value)
    raw_bundle = fetch_raw_data(target)

    if not raw_bundle["candidate_stations"]:
        # Beri pesan spesifik berdasarkan target
        all_stations_count = len(raw_bundle.get("all_stations", []))
        if analysis_type == "station" and target_value:
            msg = (
                f"Stasiun <b>{target_value}</b> tidak memiliki data monitoring (sensor). "
                f"ETL yang perlu dijalankan: <b>Onlimo → Monitoring</b>, "
                f"pilih stasiun <b>{target_value}</b> di Admin ETL.<br>"
                f"<small>Stasiun ada di database master, tapi belum ada pembacaan sensor yang di-sync.</small>"
            )
        elif all_stations_count == 0:
            msg = (
                "Tidak ada data stasiun sama sekali. "
                "Jalankan <b>ETL → Onlimo → Stasiun</b> terlebih dahulu."
            )
        else:
            msg = (
                f"Ditemukan {all_stations_count} stasiun master, tapi tidak ada yang memiliki "
                f"data monitoring aktif. Jalankan <b>ETL → Onlimo → Monitoring</b>."
            )
        _log(f"[Phase 1] No candidates: {msg}")
        return normalize_chain_output({
            "status": "no_data",
            "session_id": session_id,
            "timestamp": ts.isoformat(),
            "message": msg,
            "missing_etl": "onlimo_monitoring",
            "all_stations_count": all_stations_count,
        })

    # ── Phase 2: DataEvaluator ─────────────────────────────────────────────
    _log(f"[Phase 2] Starting DataEvaluator...")
    evaluator = DataEvaluatorAgent()
    cleaned = evaluator.run(raw_bundle)
    data_quality = cleaned.get("data_quality", {})
    _log(f"[Phase 2] Done — score={data_quality.get('overall_score', '?')} | {time.time()-chain_start:.1f}s")

    # Cek apakah masih ada stasiun setelah cleaning
    remaining_stations = cleaned.get("cleaned_candidate_stations", [])
    if not remaining_stations:
        _log(f"[Phase 2] All stations excluded by evaluator — aborting analysis")
        reliability = _assess_reliability(data_quality, raw_bundle)
        return normalize_chain_output({
            "status": "no_data",
            "session_id": session_id,
            "timestamp": ts.isoformat(),
            "message": "Semua stasiun di-exclude oleh evaluator karena data tidak reliable.",
            "data_quality": data_quality,
            "data_reliability": reliability,
        })

    # ── Phase 3: DataAnalyst ───────────────────────────────────────────────
    _log(f"[Phase 3] Starting DataAnalyst...")
    analyst = DataAnalystAgent()
    analysis = analyst.run(cleaned)
    _log(f"[Phase 3] Done in {time.time()-chain_start:.1f}s total")

    # ── Phase 4: ReportEvaluator (with feedback loop) ──────────────────────
    _log(f"[Phase 4] Starting ReportEvaluator...")
    reporter = ReportEvaluatorAgent()
    report = reporter.run(analysis, data_quality)

    loops = 0
    while report.get("action") == "feedback" and loops < MAX_FEEDBACK_LOOPS:
        loops += 1
        feedback_text = json.dumps({
            "issues": report.get("issues", []),
            "questions": report.get("questions", []),
        }, ensure_ascii=False)
        _log(f"[Phase 4] Feedback loop {loops}/{MAX_FEEDBACK_LOOPS}")

        analysis = analyst.run(cleaned, feedback=feedback_text)
        report = reporter.run(analysis, data_quality)

    if report.get("action") == "feedback":
        _log(f"[Phase 4] Max feedback loops reached — forcing accept")
        report["action"] = "accept"
        report["unresolved_issues"] = report.get("issues", [])

    # ── Log to DB ──────────────────────────────────────────────────────────
    _log(f"[Phase 5] Logging results to DB...")
    _log_results(analysis, report, session_id)

    # ── Reliability Assessment ─────────────────────────────────────────────
    data_reliability = _assess_reliability(data_quality, raw_bundle)
    _log(f"[Reliability] level={data_reliability['level']} score={data_reliability['score']}")

    # ── Build & normalize final output ─────────────────────────────────────
    anomalous = analysis.get("anomalous_stations", [])
    normal_ct = analysis.get("normal_station_count", 0)
    elapsed = time.time() - chain_start
    _log(f"CHAIN COMPLETE — {len(anomalous)} anomalous, {normal_ct} normal | total={elapsed:.1f}s")

    return normalize_chain_output({
        "status":             "done",
        "session_id":         session_id,
        "timestamp":          ts.isoformat(),
        "feedback_loops":     loops,
        "data_quality":       data_quality,
        "data_reliability":   data_reliability,
        "analysis_raw":       analysis,
        "analyst_narrative":  analysis.get("_raw_narrative", ""),
        "analysis_highlight": report.get("analysis_highlight"),
        "detail_reasoning":   report.get("detail_reasoning"),
        "kpi_update":         report.get("kpi_update"),
        "ika_summary":        report.get("ika_summary"),
        "telegram_message":   report.get("telegram_message"),
        "unresolved_issues":  report.get("unresolved_issues", []),
    })