# =============================================================================
# agents.py — Multi-Agent Pipeline for WQSA (3 agents)
# =============================================================================
# Agent 1: DataEvaluatorAgent  — validate, clean, flag data quality issues
# Agent 2: DataAnalystAgent    — reasoning chain (detect → profile → rain → sparing → correlate → ika)
# Agent 3: ReportEvaluatorAgent — validate analysis consistency, format for dashboard (or send feedback)
# =============================================================================

import json
import logging
from typing import Optional
from datetime import datetime

from openai import OpenAI
from config import (
    OPENROUTER_API_KEY,
    EVALUATOR_MODEL, ANALYST_MODEL, REPORTER_MODEL,
    TARGET_DAS, TARGET_REGION,
    ANOMALY_INDEX_THRESHOLD, RAINFALL_HIGH_MM,
    IKA_GAP_WARNING, IKA_GAP_CRITICAL,
)
from tools import (
    think, detect_anomaly, calculate_pollution_profile,
    evaluate_rainfall_branching, check_sparing_compliance,
    calculate_ika_gap, cross_correlate_evidence,
    safe_float,
)

logger = logging.getLogger(__name__)

# ── Lazy OpenRouter clients per model ─────────────────────────────────────
_clients = {}

def _get_client(model: str) -> OpenAI:
    if model not in _clients:
        if not OPENROUTER_API_KEY:
            raise RuntimeError("OPENROUTER_API_KEY not set")
        _clients[model] = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=OPENROUTER_API_KEY)
    return _clients[model]


def _run_loop(model, system_prompt, user_content, tools_list, tool_funcs, max_steps=30, label="Agent"):
    """Generic agentic loop shared by DataEvaluator and DataAnalyst."""
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
    ]
    step = 0
    for _ in range(max_steps):
        response = _get_client(model).chat.completions.create(
            model=model, messages=messages, tools=tools_list, tool_choice="auto",
        )
        msg = response.choices[0].message

        if not msg.tool_calls:
            raw = (msg.content or "").strip()
            print(f"  ✅ [{label}] Done — {step} tool calls")
            return raw

        messages.append({
            "role": "assistant", "content": msg.content,
            "tool_calls": [
                {"id": tc.id, "type": "function",
                 "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
                for tc in msg.tool_calls
            ],
        })

        for tc in msg.tool_calls:
            step += 1
            name = tc.function.name
            args = json.loads(tc.function.arguments)
            summary = ", ".join(f"{k}={str(v)[:50]}" for k, v in args.items())
            print(f"  [{step:02d}] → {name}({summary})")

            if name in tool_funcs:
                try:
                    result = tool_funcs[name](args)
                    preview = str(result)[:100].replace("\n", " ")
                    print(f"       ↳ {preview}{'...' if len(str(result)) > 100 else ''}")
                except Exception as e:
                    result = json.dumps({"error": str(e)})
                    print(f"       ↳ ERROR: {e}")
            else:
                result = json.dumps({"error": f"Unknown tool: {name}"})

            messages.append({"role": "tool", "tool_call_id": tc.id, "content": str(result)})

    print(f"  ⚠️ [{label}] Hit MAX_STEPS={max_steps}")
    return json.dumps({"error": f"Exceeded {max_steps} steps", "partial": True})


def _parse_json(raw: str, label: str) -> dict:
    """Parse LLM JSON output, stripping markdown fences."""
    try:
        clean = raw.strip()
        if clean.startswith("```"):
            clean = clean.split("```", 2)[1]
            if clean.startswith("json"):
                clean = clean[4:]
            clean = clean.rsplit("```", 1)[0].strip()
        return json.loads(clean)
    except json.JSONDecodeError as e:
        logger.error(f"[{label}] JSON parse failed: {e}\nRaw: {raw[:500]}")
        return {"error": f"JSON parse failed: {e}", "raw_output": raw[:2000]}


# =============================================================================
# AGENT 1 — DataEvaluatorAgent (data quality validation)
# =============================================================================

EVALUATOR_TOOLS = [
    {"type": "function", "function": {
        "name": "think",
        "description": "Plan your data evaluation strategy.",
        "parameters": {"type": "object", "properties": {
            "thought": {"type": "string"}
        }, "required": ["thought"]}
    }},
    {"type": "function", "function": {
        "name": "check_data_quality",
        "description": "Check a dataset for nulls, zeros, missing fields, and outliers. Returns quality report.",
        "parameters": {"type": "object", "properties": {
            "dataset_name": {"type": "string", "description": "Which dataset: 'onlimo', 'rainfall', 'sparing', 'sitala'"},
            "data_json": {"type": "string", "description": "JSON string of the dataset to check"}
        }, "required": ["dataset_name", "data_json"]}
    }},
    {"type": "function", "function": {
        "name": "flag_outlier",
        "description": "Flag a specific field value as an outlier with reasoning.",
        "parameters": {"type": "object", "properties": {
            "station_id": {"type": "string"},
            "field_name": {"type": "string", "description": "e.g. 'amonia', 'cod', 'indeks_mutu'"},
            "value": {"type": "number"},
            "reason": {"type": "string", "description": "Why this value is suspicious"}
        }, "required": ["station_id", "field_name", "value", "reason"]}
    }},
]


def _check_data_quality(args: dict) -> str:
    """Deterministic data quality checks."""
    name = args.get("dataset_name", "")
    try:
        data = json.loads(args.get("data_json", "[]"))
    except json.JSONDecodeError:
        return json.dumps({"error": "Invalid JSON"})

    if not isinstance(data, list):
        data = [data]

    issues = []
    total = len(data)
    null_counts = {}

    for i, row in enumerate(data):
        if not isinstance(row, dict):
            continue
        for key, val in row.items():
            if val is None or val == "" or val == 0.0:
                null_counts[key] = null_counts.get(key, 0) + 1
            # Outlier check for known parameters
            fv = safe_float(val) if isinstance(val, (int, float, str)) else 0
            if name == "onlimo":
                params = row.get("parameter", {})
                if isinstance(params, dict):
                    for pk, pv in params.items():
                        fval = safe_float(pv)
                        if pk == "amonia" and fval > 100:
                            issues.append({"station": row.get("station_id"), "field": f"parameter.{pk}", "value": fval, "issue": f"Amonia {fval} mg/L sangat tinggi (normal <10)"})
                        if pk == "ph" and fval > 0 and (fval < 3 or fval > 12):
                            issues.append({"station": row.get("station_id"), "field": f"parameter.{pk}", "value": fval, "issue": f"pH {fval} di luar rentang fisik (3-12)"})
                        if pk == "do" and fval > 20:
                            issues.append({"station": row.get("station_id"), "field": f"parameter.{pk}", "value": fval, "issue": f"DO {fval} mg/L terlalu tinggi (normal <15)"})
                        if pk == "cod" and fval > 1000:
                            issues.append({"station": row.get("station_id"), "field": f"parameter.{pk}", "value": fval, "issue": f"COD {fval} mg/L ekstrem"})

    # Fields with >50% null
    high_null = {k: v for k, v in null_counts.items() if total > 0 and v / total > 0.5}

    return json.dumps({
        "dataset": name,
        "total_records": total,
        "outliers_found": len(issues),
        "outliers": issues[:20],
        "high_null_fields": high_null,
        "quality_score": max(0, 100 - len(issues) * 5 - len(high_null) * 10),
    }, ensure_ascii=False, indent=2)


def _flag_outlier(args: dict) -> str:
    return json.dumps({
        "flagged": True,
        "station_id": args.get("station_id"),
        "field": args.get("field_name"),
        "value": args.get("value"),
        "reason": args.get("reason"),
    })


EVALUATOR_TOOL_FUNCS = {
    "think":              lambda a: think(a["thought"]),
    "check_data_quality": lambda a: _check_data_quality(a),
    "flag_outlier":       lambda a: _flag_outlier(a),
}

EVALUATOR_SYSTEM = f"""You are the DATA EVALUATOR for WQSA — DAS {TARGET_DAS}, {TARGET_REGION}.

YOUR ONLY JOB: Validate data quality of the raw data bundle. Do NOT analyse pollution or generate recommendations.

STEPS:
1. think() — plan which datasets to check
2. check_data_quality() for each dataset: onlimo, rainfall, sparing, sitala
3. flag_outlier() for any extreme values that need attention
4. Output JSON with cleaned data and quality report

OUTPUT FORMAT (JSON only, no markdown):
{{
  "data_quality": {{
    "overall_score": 0-100,
    "issues_found": int,
    "outliers": [{{ "station_id", "field", "value", "reason" }}],
    "null_warnings": [{{ "dataset", "field", "null_pct" }}],
    "excluded_stations": ["station_ids with critically bad data"]
  }},
  "cleaned_candidate_stations": [... stations suitable for analysis],
  "cleaned_rainfall": {{ ... }},
  "cleaned_sparing": {{ ... }},
  "cleaned_sitala": [...]
}}"""


class DataEvaluatorAgent:
    def run(self, raw_bundle: dict) -> dict:
        print("\n🔍 [DataEvaluator] Starting data quality check...")
        print(f"   Model    : {EVALUATOR_MODEL}")
        print(f"   API      : OpenRouter (single key, model routed by string)")
        print(f"   Stations : {len(raw_bundle.get('candidate_stations', []))}")
        raw = _run_loop(
            EVALUATOR_MODEL, EVALUATOR_SYSTEM,
            "Raw data bundle:\n\n" + json.dumps(raw_bundle, ensure_ascii=False, default=str),
            EVALUATOR_TOOLS, EVALUATOR_TOOL_FUNCS,
            max_steps=15, label="DataEvaluator",
        )
        result = _parse_json(raw, "DataEvaluator")
        if "error" in result:
            # Fallback: pass data through uncleaned
            result["cleaned_candidate_stations"] = raw_bundle.get("candidate_stations", [])
            result["cleaned_rainfall"] = raw_bundle.get("rainfall", {})
            result["cleaned_sparing"] = raw_bundle.get("sparing", {})
            result["cleaned_sitala"] = raw_bundle.get("sitala", [])
            result["data_quality"] = {"overall_score": 0, "issues_found": 0, "outliers": [], "note": "Evaluator failed, data passed through uncleaned"}
        return result


# =============================================================================
# AGENT 2 — DataAnalystAgent (reasoning chain)
# =============================================================================

ANALYST_TOOLS = [
    {"type": "function", "function": {
        "name": "think", "description": "Plan and reason about analysis steps.",
        "parameters": {"type": "object", "properties": {"thought": {"type": "string"}}, "required": ["thought"]}
    }},
    {"type": "function", "function": {
        "name": "detect_anomaly", "description": "Detect anomaly in station data: index >= 3.0, CEMAR status.",
        "parameters": {"type": "object", "properties": {"station_data": {"type": "string"}}, "required": ["station_data"]}
    }},
    {"type": "function", "function": {
        "name": "calculate_pollution_profile", "description": "COD/BOD ratio: >4=INDUSTRI, 2-4=CAMPURAN, <2=DOMESTIK.",
        "parameters": {"type": "object", "properties": {"cod": {"type": "number"}, "bod": {"type": "number"}}, "required": ["cod", "bod"]}
    }},
    {"type": "function", "function": {
        "name": "evaluate_rainfall_branching", "description": "Rainfall >50mm/24h → LIMPASAN → skip sparing investigation.",
        "parameters": {"type": "object", "properties": {"rainfall_data": {"type": "string"}}, "required": ["rainfall_data"]}
    }},
    {"type": "function", "function": {
        "name": "check_sparing_compliance", "description": "Check monitoring values vs baku mutu → TAAT/LANGGAR per parameter.",
        "parameters": {"type": "object", "properties": {"monitoring_data": {"type": "string"}}, "required": ["monitoring_data"]}
    }},
    {"type": "function", "function": {
        "name": "cross_correlate_evidence",
        "description": "Cross-correlate station anomaly with Sparing violations: spatial + temporal + profile match. Call ONLY if violations exist.",
        "parameters": {"type": "object", "properties": {
            "station_json": {"type": "string"}, "step4_json": {"type": "string"}, "step2_json": {"type": "string"},
        }, "required": ["station_json", "step4_json", "step2_json"]}
    }},
    {"type": "function", "function": {
        "name": "calculate_ika_gap", "description": "IKA actual vs target → TINDAK/WASPADA/PANTAU.",
        "parameters": {"type": "object", "properties": {"sitala_data": {"type": "string"}}, "required": ["sitala_data"]}
    }},
]

ANALYST_TOOL_FUNCS = {
    "think":                       lambda a: think(a["thought"]),
    "detect_anomaly":              lambda a: detect_anomaly(a["station_data"]),
    "calculate_pollution_profile": lambda a: calculate_pollution_profile(a["cod"], a["bod"]),
    "evaluate_rainfall_branching": lambda a: evaluate_rainfall_branching(a["rainfall_data"]),
    "check_sparing_compliance":    lambda a: check_sparing_compliance(a["monitoring_data"]),
    "cross_correlate_evidence":    lambda a: cross_correlate_evidence(a["station_json"], a["step4_json"], a["step2_json"]),
    "calculate_ika_gap":           lambda a: calculate_ika_gap(a["sitala_data"]),
}

ANALYST_SYSTEM = f"""You are the DATA ANALYST for WQSA — DAS {TARGET_DAS}, {TARGET_REGION}.

YOUR JOB: Analyse cleaned data from the DataEvaluator. Follow the reasoning chain strictly.

CHAIN:
1. detect_anomaly() per candidate station
2. calculate_pollution_profile() for anomalous stations (use COD/BOD from parameter field)
3. evaluate_rainfall_branching() using the rainfall data for the station's area
4. check_sparing_compliance() using sparing monitoring data
5. cross_correlate_evidence() ONLY if violations found (total_violations > 0)
6. calculate_ika_gap() once per region

If you receive FEEDBACK from the ReportEvaluator, address each question and revise your analysis.

OUTPUT (JSON only):
{{
  "analysis_id": "string",
  "timestamp": "ISO",
  "anomalous_stations": [{{
    "station_id": "...", "station_name": "...",
    "indeks_mutu": 0.0, "status": "...",
    "is_limpasan": false, "rainfall_mm": 0.0,
    "pollution_profile": "INDUSTRI|CAMPURAN|DOMESTIK",
    "cod_bod_ratio": 0.0,
    "causal_evidence": [{{
      "company_name": "...", "causal_confidence": "TINGGI|SEDANG|RENDAH",
      "distance_km": 0.0, "violated_params": []
    }}],
    "top_suspect": "...", "urgency": "TINDAK|WASPADA|PANTAU",
    "anomaly_reasons": []
  }}],
  "normal_station_count": 0,
  "ika_gap": null, "ika_urgency": "..."
}}"""


class DataAnalystAgent:
    def run(self, cleaned_data: dict, feedback: Optional[str] = None) -> dict:
        label = "DataAnalyst"
        if feedback:
            print(f"\n🔄 [{label}] Re-analysing with feedback from ReportEvaluator...")
            print(f"   Model    : {ANALYST_MODEL}")
            print(f"   API      : OpenRouter (single key, model routed by string)")
            content = (
                "Cleaned data bundle:\n\n" + json.dumps(cleaned_data, ensure_ascii=False, default=str)
                + "\n\n--- FEEDBACK FROM REPORT EVALUATOR ---\n" + feedback
            )
        else:
            print(f"\n📊 [{label}] Starting analysis...")
            print(f"   Model      : {ANALYST_MODEL}")
            print(f"   API        : OpenRouter (single key, model routed by string)")
            print(f"   Candidates : {len(cleaned_data.get('cleaned_candidate_stations', []))}")
            content = "Cleaned data bundle:\n\n" + json.dumps(cleaned_data, ensure_ascii=False, default=str)

        raw = _run_loop(
            ANALYST_MODEL, ANALYST_SYSTEM, content,
            ANALYST_TOOLS, ANALYST_TOOL_FUNCS,
            max_steps=30, label=label,
        )
        return _parse_json(raw, label)


# =============================================================================
# AGENT 3 — ReportEvaluatorAgent (validate + format for dashboard)
# =============================================================================

REPORTER_SYSTEM = f"""You are the REPORT EVALUATOR for WQSA — DAS {TARGET_DAS}, {TARGET_REGION}.

You receive an AnalysisResult JSON from the DataAnalyst.
You have TWO possible actions:

ACTION A — SEND FEEDBACK (if inconsistencies found):
Return JSON with "action": "feedback" and questions for the analyst.

ACTION B — FORMAT OUTPUT (if analysis is valid):
Return JSON with "action": "accept" and dashboard-ready payload.

CHECK FOR THESE INCONSISTENCIES:
1. Confidence TINGGI but evidence is weak (only 1 of 3 dimensions true)
2. Urgency TINDAK but no violating companies identified
3. Pollution profile INDUSTRI but no Sparing loggers found
4. Station CEMAR BERAT but all parameters below normal thresholds
5. IKA gap positive but urgency is TINDAK
6. Rainfall >50mm but is_limpasan=false
7. Cross-correlation TINGGI but distance >10km
8. Recommendations mention companies not in sparing data
9. Anomalous station count in summary doesn't match detail entries
10. Violation date newer than anomaly date (temporal inversion)

OUTPUT FORMAT — Feedback:
{{
  "action": "feedback",
  "issues": ["description of each inconsistency found"],
  "questions": ["specific question for the analyst to address"]
}}

OUTPUT FORMAT — Accept:
{{
  "action": "accept",
  "analysis_highlight": {{
    "overall_urgency": "PANTAU|WASPADA|TINDAK",
    "total_anomalous": int,
    "total_normal": int,
    "top_stations": [{{
      "station_id": "...", "station_name": "...",
      "status": "...", "indeks_mutu": 0.0,
      "urgency": "...", "pollution_profile": "...",
      "top_suspect": "...", "causal_confidence": "...",
      "key_finding": "1-sentence summary"
    }}],
    "priority_actions": [{{
      "priority": 1, "action": "...", "target": "...", "deadline": "..."
    }}]
  }},
  "detail_reasoning": {{
    "per_station": [{{
      "station_id": "...", "station_name": "...",
      "steps": [
        {{"step": 1, "name": "Scan", "result": "..."}},
        {{"step": 2, "name": "Profil Pencemar", "result": "..."}},
        {{"step": 3, "name": "Curah Hujan", "result": "..."}},
        {{"step": 4, "name": "Korelasi Sparing", "result": "..."}},
        {{"step": 5, "name": "IKA & Urgensi", "result": "..."}}
      ],
      "causal_chain": [{{...}}]
    }}],
    "data_quality_note": "..."
  }},
  "kpi_update": {{
    "total_stations": int, "critical": int, "warning": int, "good": int,
    "max_rain_mm": float, "langgar_count": int
  }},
  "telegram_message": "formatted summary for auto-send to Telegram",
  "ika_summary": {{"actual": float, "target": float, "gap": float, "urgency": "..."}}
}}

RULES:
- Output ONLY valid JSON
- Bahasa Indonesia for telegram_message and key_finding
- Be strict about inconsistencies — if something doesn't add up, use Action A"""


class ReportEvaluatorAgent:
    def run(self, analysis_result: dict, data_quality: Optional[dict] = None) -> dict:
        print(f"\n🧠 [ReportEvaluator] Validating analysis...")
        print(f"   Model : {REPORTER_MODEL}")
        print(f"   API   : OpenRouter (single key, model routed by string)")

        context = "AnalysisResult:\n\n" + json.dumps(analysis_result, ensure_ascii=False, indent=2, default=str)
        if data_quality:
            context += "\n\nData Quality Report:\n" + json.dumps(data_quality, ensure_ascii=False, indent=2)

        try:
            response = _get_client(REPORTER_MODEL).chat.completions.create(
                model=REPORTER_MODEL,
                messages=[
                    {"role": "system", "content": REPORTER_SYSTEM},
                    {"role": "user", "content": context},
                ],
            )
            raw = response.choices[0].message.content.strip()
            result = _parse_json(raw, "ReportEvaluator")

            action = result.get("action", "accept")
            if action == "feedback":
                print(f"  🔄 [ReportEvaluator] Found {len(result.get('issues', []))} issues — sending feedback")
            else:
                print(f"  ✅ [ReportEvaluator] Analysis accepted — formatting for dashboard")
            return result

        except Exception as e:
            logger.error(f"[ReportEvaluator] Failed: {e}")
            return {"action": "accept", "error": str(e), "analysis_highlight": {}, "detail_reasoning": {}}