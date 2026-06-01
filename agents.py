# =============================================================================
# agents.py — Multi-Agent Pipeline for WQSA (3 agents)
# =============================================================================
# Agent 1: DataEvaluatorAgent  — validate, clean, normalize via workspace tools
# Agent 2: DataAnalystAgent    — reasoning chain (detect→profile→rain→sparing→correlate→ika)
# Agent 3: ReportEvaluatorAgent — validate analysis consistency, format for dashboard
# =============================================================================

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import copy
import json
import logging
import time
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
from response_schema import parse_agent_json, normalize_agent_output

logger = logging.getLogger(__name__)

# ── Lazy OpenRouter clients per model ─────────────────────────────────────
_clients = {}

def _get_client(model: str) -> OpenAI:
    if model not in _clients:
        if not OPENROUTER_API_KEY:
            raise RuntimeError("OPENROUTER_API_KEY not set")
        _clients[model] = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=OPENROUTER_API_KEY,
            timeout=120.0,
        )
    return _clients[model]


def _log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    logger.info(msg)


# Kumpulkan teks dan think() calls dari satu loop
_loop_collected_texts: list[str] = []
_loop_think_notes: list[dict] = []   # {"step": n, "label": "...", "thought": "..."}


def _run_loop(model, system_prompt, user_content, tools_list, tool_funcs, max_steps=30, label="Agent"):
    """Generic agentic loop shared by DataEvaluator and DataAnalyst."""
    global _loop_collected_texts, _loop_think_notes
    _loop_collected_texts = []
    _loop_think_notes     = []

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
    ]
    step = 0
    loop_start = time.time()

    for iteration in range(max_steps):
        _log(f"[{label}] LLM call #{iteration+1}/{max_steps} (total elapsed: {time.time()-loop_start:.1f}s)...")
        try:
            t0 = time.time()
            response = _get_client(model).chat.completions.create(
                model=model, messages=messages, tools=tools_list, tool_choice="auto",
            )
            _log(f"[{label}] LLM responded in {time.time()-t0:.1f}s")
        except Exception as e:
            _log(f"[{label}] LLM call FAILED: {e}")
            raise

        msg = response.choices[0].message

        if msg.content and msg.content.strip():
            _loop_collected_texts.append(msg.content.strip())

        if not msg.tool_calls:
            raw = (msg.content or "").strip()
            _log(f"[{label}] Done — {step} tool calls, {time.time()-loop_start:.1f}s total")
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
            _log(f"[{label}] Step {step:02d} → {name}({summary})")

            # Tangkap konten think() — inilah narasi analisis terbaik
            if name == "think":
                thought = args.get("thought", "").strip()
                if thought:
                    _loop_think_notes.append({
                        "step": step,
                        "label": label,
                        "thought": thought,
                    })

            if name in tool_funcs:
                try:
                    result = tool_funcs[name](args)
                    preview = str(result)[:120].replace("\n", " ")
                    print(f"           ↳ {preview}{'...' if len(str(result)) > 120 else ''}", flush=True)
                except Exception as e:
                    result = json.dumps({"error": str(e)})
                    _log(f"           ↳ ERROR: {e}")
            else:
                result = json.dumps({"error": f"Unknown tool: {name}"})

            messages.append({"role": "tool", "tool_call_id": tc.id, "content": str(result)})

    _log(f"[{label}] Hit MAX_STEPS={max_steps} after {time.time()-loop_start:.1f}s")
    return json.dumps({"error": f"Exceeded {max_steps} steps", "partial": True})


# =============================================================================
# CLEANING WORKSPACE — In-memory data store modified by evaluator tools
# =============================================================================
# Data hidup di sini, bukan di JSON output LLM.
# Bahkan kalau LLM JSON-nya rusak, data sudah bersih di workspace.
# =============================================================================

# Batas domain fisik untuk parameter kualitas air
# (param_name, min, max, zero_is_sensor_failure, typical_max_natural)
DOMAIN_BOUNDS = {
    "ph":        {"min": 0,   "max": 14,    "zero_fail": True,  "suspect_above": 12},
    "do":        {"min": 0,   "max": 20,    "zero_fail": False, "suspect_above": 18},
    "cod":       {"min": 0,   "max": 500,   "zero_fail": False, "suspect_above": 300},
    "bod":       {"min": 0,   "max": 200,   "zero_fail": False, "suspect_above": 100},
    "tss":       {"min": 0,   "max": 1000,  "zero_fail": False, "suspect_above": 500},
    "amonia":    {"min": 0,   "max": 50,    "zero_fail": False, "suspect_above": 30},
    "nitrat":    {"min": 0,   "max": 100,   "zero_fail": False, "suspect_above": 50},
    "nitrit":    {"min": 0,   "max": 10,    "zero_fail": False, "suspect_above": 5},
    "suhu":      {"min": 10,  "max": 50,    "zero_fail": True,  "suspect_above": 45},
    "turbidity": {"min": 0,   "max": 4000,  "zero_fail": False, "suspect_above": 2000},
    "dhl":       {"min": 0.5, "max": 50000, "zero_fail": True,  "suspect_above": 5000},
    "ews_per":   {"min": 0,   "max": 100,   "zero_fail": False, "suspect_above": 100},
}

# Parameter kunci — kalau mayoritas ini 0/null, stasiun dianggap mati
KEY_PARAMS = {"cod", "bod", "do", "ph", "tss", "amonia"}


class CleaningWorkspace:
    """
    In-memory workspace untuk data cleaning.
    Dimodifikasi oleh evaluator tools secara deterministik.
    Setelah evaluator selesai, to_cleaned_bundle() menghasilkan data bersih.
    """

    def __init__(self, raw_bundle: dict):
        self.stations = copy.deepcopy(raw_bundle.get("candidate_stations", []))
        self.all_stations = copy.deepcopy(raw_bundle.get("all_stations", []))
        self.rainfall = copy.deepcopy(raw_bundle.get("rainfall", {}))
        self.sparing = copy.deepcopy(raw_bundle.get("sparing", {}))
        self.sitala = copy.deepcopy(raw_bundle.get("sitala", []))

        self.target = raw_bundle.get("target", {})
        self.region = raw_bundle.get("region", TARGET_REGION)
        self.das = raw_bundle.get("das", TARGET_DAS)
        self.fetch_timestamp = raw_bundle.get("fetch_timestamp", "")

        # Tracking
        self.excluded_stations: list[dict] = []
        self.flags: list[dict] = []
        self.modifications: list[dict] = []
        self.null_warnings: list[dict] = []
        self._quality_deductions = 0

    # ── Tool: Apply domain bounds ────────────────────────────────────────

    def apply_domain_bounds(self) -> str:
        """
        Cek semua parameter di semua stasiun terhadap batas fisik.
        - None/null dari DB → flag sebagai "sensor tidak kirim data"
        - Nilai di luar batas fisik → set null + flag
        - Nilai 0 pada parameter yang tidak mungkin 0 → set null + flag
        Returns summary JSON.
        """
        violations = []
        null_from_db = []

        for station in self.stations:
            sid = station.get("station_id", "?")
            params = station.get("parameter", {})
            if not isinstance(params, dict):
                continue

            for param_name, bounds in DOMAIN_BOUNDS.items():
                raw_val = params.get(param_name)

                # None dari DB = sensor tidak kirim data
                if raw_val is None:
                    null_from_db.append({"station_id": sid, "param": param_name})
                    self.flags.append({
                        "station_id": sid,
                        "field": param_name,
                        "value": None,
                        "new_value": None,
                        "reason": f"{param_name} null — sensor tidak mengirim data",
                        "severity": "warning",
                        "action": "already_null",
                    })
                    self._quality_deductions += 1
                    continue

                val = safe_float(raw_val)
                violated = False
                reason = ""

                # Cek batas fisik
                if val < bounds["min"] or val > bounds["max"]:
                    reason = (
                        f"{param_name}={val} di luar batas fisik "
                        f"[{bounds['min']}, {bounds['max']}]"
                    )
                    violated = True

                # Cek zero = sensor failure
                elif val == 0 and bounds["zero_fail"]:
                    reason = (
                        f"{param_name}=0 — sensor kemungkinan mati "
                        f"(parameter ini tidak mungkin 0 di perairan alami)"
                    )
                    violated = True

                # Cek suspect (di atas threshold wajar)
                elif val > bounds["suspect_above"]:
                    reason = (
                        f"{param_name}={val} di atas batas wajar "
                        f"({bounds['suspect_above']}), kemungkinan sensor error"
                    )
                    violated = True

                if violated:
                    params[param_name] = None  # Set null
                    self.flags.append({
                        "station_id": sid,
                        "field": param_name,
                        "value": val,
                        "new_value": None,
                        "reason": reason,
                        "severity": "critical",
                        "action": "set_null",
                    })
                    self._quality_deductions += 3
                    violations.append({
                        "station_id": sid,
                        "param": param_name,
                        "value": val,
                        "reason": reason,
                    })

        return json.dumps({
            "total_checked": sum(
                len([p for p in s.get("parameter", {}) if s.get("parameter", {}).get(p) is not None])
                for s in self.stations
            ),
            "null_from_db": len(null_from_db),
            "null_params": null_from_db[:20],
            "violations_found": len(violations),
            "violations": violations[:30],
        }, ensure_ascii=False, indent=2)

    # ── Tool: Detect dead sensors ────────────────────────────────────────

    def detect_dead_sensors(self) -> str:
        """
        Deteksi stasiun dengan sensor mati:
        - Semua parameter 0/null → DEAD
        - Mayoritas key params 0/null → SENSOR_CLUSTER_FAILURE
        Returns summary dan auto-exclude dead stations.
        """
        results = []
        to_exclude = []

        for station in self.stations:
            sid = station.get("station_id", "?")
            params = station.get("parameter", {})
            if not isinstance(params, dict):
                continue

            # Hitung parameter non-null non-zero
            total_params = 0
            nonzero_params = 0
            key_nonzero = 0
            key_total = 0

            for p, v in params.items():
                fv = safe_float(v) if v is not None else None
                total_params += 1
                if fv is not None and fv > 0:
                    nonzero_params += 1
                if p in KEY_PARAMS:
                    key_total += 1
                    if fv is not None and fv > 0:
                        key_nonzero += 1

            if total_params == 0:
                status = "NO_DATA"
                to_exclude.append((sid, "Tidak ada data parameter"))
            elif nonzero_params == 0:
                status = "DEAD"
                to_exclude.append((sid, "Semua parameter 0/null — sensor mati"))
            elif key_total > 0 and key_nonzero / key_total < 0.5:
                status = "SENSOR_CLUSTER_FAILURE"
                missing = [p for p in KEY_PARAMS if safe_float(params.get(p)) in (None, 0)]
                self.flags.append({
                    "station_id": sid,
                    "field": "key_params",
                    "value": f"{key_nonzero}/{key_total} active",
                    "reason": f"Sensor cluster failure — parameter kunci mati: {', '.join(missing)}",
                    "severity": "critical",
                    "action": "flag",
                })
                self._quality_deductions += 10
            else:
                status = "OK"

            results.append({
                "station_id": sid,
                "status": status,
                "total_params": total_params,
                "nonzero_params": nonzero_params,
                "key_params_active": f"{key_nonzero}/{key_total}",
            })

        # Auto-exclude dead stations
        for sid, reason in to_exclude:
            self._exclude(sid, reason)
            self._quality_deductions += 15

        return json.dumps({
            "stations_checked": len(results),
            "dead_count": len(to_exclude),
            "results": results,
        }, ensure_ascii=False, indent=2)

    # ── Tool: Exclude station ────────────────────────────────────────────

    def exclude_station_tool(self, station_id: str, reason: str) -> str:
        """Remove a station from cleaned output."""
        return json.dumps(self._exclude(station_id, reason), ensure_ascii=False)

    def _exclude(self, station_id: str, reason: str) -> dict:
        before = len(self.stations)
        self.stations = [s for s in self.stations if s.get("station_id") != station_id]
        after = len(self.stations)
        removed = before - after
        entry = {"station_id": station_id, "reason": reason, "removed": removed > 0}
        self.excluded_stations.append(entry)
        return entry

    # ── Tool: Replace parameter value ────────────────────────────────────

    def replace_parameter_tool(self, station_id: str, param: str,
                                new_value, reason: str) -> str:
        """Replace a parameter value in a station."""
        for s in self.stations:
            if s.get("station_id") == station_id:
                params = s.get("parameter", {})
                old_val = params.get(param)
                params[param] = new_value
                mod = {
                    "station_id": station_id, "param": param,
                    "old_value": old_val, "new_value": new_value,
                    "reason": reason,
                }
                self.modifications.append(mod)
                return json.dumps(mod, ensure_ascii=False)
        return json.dumps({"error": f"Station {station_id} not found"})

    # ── Tool: Set parameter null ─────────────────────────────────────────

    def set_parameter_null_tool(self, station_id: str, param: str,
                                 reason: str) -> str:
        """Mark a parameter as null (sensor mati/unreliable)."""
        return self.replace_parameter_tool(station_id, param, None, reason)

    # ── Output methods ───────────────────────────────────────────────────

    def to_cleaned_bundle(self) -> dict:
        """Return the full cleaned data bundle for the analyst."""
        return {
            "target": self.target,
            "fetch_timestamp": self.fetch_timestamp,
            "all_stations": self.all_stations,
            "cleaned_candidate_stations": self.stations,
            "cleaned_rainfall": self.rainfall,
            "cleaned_sparing": self.sparing,
            "cleaned_sitala": self.sitala,
            "region": self.region,
            "das": self.das,
            "data_quality": self.get_quality_report(),
        }

    def get_quality_report(self) -> dict:
        """Return the quality assessment."""
        score = max(0, 100 - self._quality_deductions)
        high_null = {}

        # Hitung null percentage per parameter di semua stasiun
        if self.stations:
            param_nulls: dict[str, int] = {}
            param_totals: dict[str, int] = {}
            for s in self.stations:
                for p, v in s.get("parameter", {}).items():
                    param_totals[p] = param_totals.get(p, 0) + 1
                    if v is None or safe_float(v) == 0:
                        param_nulls[p] = param_nulls.get(p, 0) + 1
            for p, count in param_nulls.items():
                total = param_totals.get(p, 1)
                if count / total > 0.5:
                    pct = round(count / total * 100, 1)
                    high_null[p] = f"{pct}%"
                    self.null_warnings.append({
                        "dataset": "onlimo",
                        "field": p,
                        "null_pct": pct,
                    })

        return {
            "overall_score": score,
            "issues_found": len(self.flags),
            "outliers": [f for f in self.flags if f.get("severity") == "critical"],
            "null_warnings": self.null_warnings,
            "high_null_fields": high_null,
            "excluded_stations": self.excluded_stations,
            "modifications": self.modifications[:20],
        }


# =============================================================================
# AGENT 1 — DataEvaluatorAgent
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
        "name": "apply_domain_bounds",
        "description": (
            "Check ALL parameters on ALL stations against physical bounds. "
            "Auto-sets values outside physical range to null. "
            "Bounds: pH [0,14], DO [0,20], suhu [10,50], DHL [>0.5], "
            "amonia [0,50], nitrat [0,100], COD [0,500], BOD [0,200]. "
            "Also flags zero values for parameters that cannot be zero in natural water "
            "(pH, DHL, suhu). Call this FIRST."
        ),
        "parameters": {"type": "object", "properties": {}, "required": []}
    }},
    {"type": "function", "function": {
        "name": "detect_dead_sensors",
        "description": (
            "Detect stations with dead sensors. "
            "All params 0/null = DEAD (auto-excluded). "
            ">50% key params (COD,BOD,DO,pH,TSS,amonia) zero = SENSOR_CLUSTER_FAILURE. "
            "Call this AFTER apply_domain_bounds."
        ),
        "parameters": {"type": "object", "properties": {}, "required": []}
    }},
    {"type": "function", "function": {
        "name": "check_data_quality",
        "description": "Check a specific dataset for nulls, zeros, missing fields, and outliers.",
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
            "field_name": {"type": "string"},
            "value": {"type": "number"},
            "reason": {"type": "string"}
        }, "required": ["station_id", "field_name", "value", "reason"]}
    }},
    {"type": "function", "function": {
        "name": "exclude_station",
        "description": "Remove a station from cleaned output entirely. Use when data is too unreliable for analysis.",
        "parameters": {"type": "object", "properties": {
            "station_id": {"type": "string"},
            "reason": {"type": "string"}
        }, "required": ["station_id", "reason"]}
    }},
    {"type": "function", "function": {
        "name": "set_parameter_null",
        "description": "Mark a specific parameter as null (sensor mati/unreliable). Keeps the station but removes the bad value.",
        "parameters": {"type": "object", "properties": {
            "station_id": {"type": "string"},
            "param": {"type": "string", "description": "Parameter name: cod, bod, tss, do, ph, nitrat, nitrit, amonia, suhu, turbidity, dhl"},
            "reason": {"type": "string"}
        }, "required": ["station_id", "param", "reason"]}
    }},
    {"type": "function", "function": {
        "name": "replace_parameter",
        "description": "Replace a parameter value with a corrected value (e.g. unit conversion, clamping).",
        "parameters": {"type": "object", "properties": {
            "station_id": {"type": "string"},
            "param": {"type": "string"},
            "new_value": {"type": "number"},
            "reason": {"type": "string"}
        }, "required": ["station_id", "param", "new_value", "reason"]}
    }},
]


def _check_data_quality(args: dict) -> str:
    """Deterministic data quality checks (unchanged from original)."""
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
            if name == "onlimo":
                params = row.get("parameter", {})
                if isinstance(params, dict):
                    for pk, pv in params.items():
                        fval = safe_float(pv)
                        if pk == "amonia" and fval > 100:
                            issues.append({"station": row.get("station_id"), "field": f"parameter.{pk}", "value": fval, "issue": f"Amonia {fval} mg/L sangat tinggi (normal <10)"})
                        if pk == "ph" and fval > 0 and (fval < 3 or fval > 12):
                            issues.append({"station": row.get("station_id"), "field": f"parameter.{pk}", "value": fval, "issue": f"pH {fval} di luar rentang wajar (3-12)"})
                        if pk == "do" and fval > 20:
                            issues.append({"station": row.get("station_id"), "field": f"parameter.{pk}", "value": fval, "issue": f"DO {fval} mg/L terlalu tinggi (normal <15)"})
                        if pk == "cod" and fval > 1000:
                            issues.append({"station": row.get("station_id"), "field": f"parameter.{pk}", "value": fval, "issue": f"COD {fval} mg/L ekstrem"})

    high_null = {k: v for k, v in null_counts.items() if total > 0 and v / total > 0.5}

    return json.dumps({
        "dataset": name,
        "total_records": total,
        "outliers_found": len(issues),
        "outliers": issues[:20],
        "high_null_fields": high_null,
        "quality_score": max(0, 100 - len(issues) * 5 - len(high_null) * 10),
    }, ensure_ascii=False, indent=2)


EVALUATOR_SYSTEM = f"""You are the DATA EVALUATOR for WQSA — DAS {TARGET_DAS}, {TARGET_REGION}.

YOUR ONLY JOB: Validate and clean the raw data bundle. Do NOT analyse pollution or generate recommendations.

NORMALIZATION METHODOLOGY:
1. Call apply_domain_bounds() FIRST — auto-detects range violations, sets impossible values to null.
2. Call detect_dead_sensors() — finds stations with all-zero readings (auto-excludes them).
3. Call check_data_quality() for each dataset to find remaining issues.
4. Use flag_outlier() for domain-specific outliers you notice.
5. Use exclude_station() to remove stations too unreliable for analysis.
6. Use set_parameter_null() for individual bad values (keeps station, removes value).
7. Use replace_parameter() ONLY if you can justify the correction (e.g. unit conversion).

RULES:
- NEVER impute missing sensor readings with mean/median — this could mask real pollution.
- If a value is suspicious but not physically impossible, FLAG it, don't delete it.
- If >50% of a station's key parameters are null/zero, consider excluding it.
- Null/0 on DHL, pH, or suhu means sensor failure in natural Indonesian water.
- Values like amonia > 50 mg/L or nitrat > 100 mg/L are almost certainly sensor errors.

After all tools, output a short JSON summary (the actual cleaned data is already saved by the tools):
{{
  "evaluation_summary": "brief text summary",
  "actions_taken": ["list of key actions"],
  "remaining_concerns": ["issues that could not be resolved"]
}}"""


class DataEvaluatorAgent:
    def run(self, raw_bundle: dict) -> dict:
        _log(f"[DataEvaluator] START — model={EVALUATOR_MODEL} stations={len(raw_bundle.get('candidate_stations', []))}")

        # Buat workspace — tools akan memodifikasi ini
        workspace = CleaningWorkspace(raw_bundle)

        # Tool functions sebagai closures — capture workspace
        tool_funcs = {
            "think":               lambda a: think(a["thought"]),
            "apply_domain_bounds": lambda a: workspace.apply_domain_bounds(),
            "detect_dead_sensors": lambda a: workspace.detect_dead_sensors(),
            "check_data_quality":  lambda a: _check_data_quality(a),
            "flag_outlier":        lambda a: _flag_outlier_ws(workspace, a),
            "exclude_station":     lambda a: workspace.exclude_station_tool(a["station_id"], a["reason"]),
            "set_parameter_null":  lambda a: workspace.set_parameter_null_tool(a["station_id"], a["param"], a["reason"]),
            "replace_parameter":   lambda a: workspace.replace_parameter_tool(a["station_id"], a["param"], a.get("new_value"), a["reason"]),
        }

        content = "Raw data bundle:\n\n" + json.dumps(raw_bundle, ensure_ascii=False, default=str)

        raw = _run_loop(
            EVALUATOR_MODEL, EVALUATOR_SYSTEM, content,
            EVALUATOR_TOOLS, tool_funcs,
            max_steps=15, label="DataEvaluator",
        )

        # Parse LLM output untuk summary (optional — workspace sudah punya data)
        parsed = parse_agent_json(raw, "DataEvaluator")

        # Data bersih SELALU dari workspace, bukan dari LLM JSON
        result = workspace.to_cleaned_bundle()

        # Merge LLM summary jika ada
        if "evaluation_summary" in parsed:
            result["data_quality"]["evaluator_summary"] = parsed.get("evaluation_summary", "")
        if "remaining_concerns" in parsed:
            result["data_quality"]["remaining_concerns"] = parsed.get("remaining_concerns", [])

        _log(f"[DataEvaluator] Done — score={result['data_quality']['overall_score']}, "
             f"excluded={len(result['data_quality']['excluded_stations'])}, "
             f"flags={result['data_quality']['issues_found']}")

        return result


def _flag_outlier_ws(workspace: CleaningWorkspace, args: dict) -> str:
    """Flag outlier and record in workspace."""
    workspace.flags.append({
        "station_id": args.get("station_id"),
        "field": args.get("field_name"),
        "value": args.get("value"),
        "reason": args.get("reason"),
        "severity": "critical",
        "action": "flag",
    })
    workspace._quality_deductions += 3
    return json.dumps({
        "flagged": True,
        "station_id": args.get("station_id"),
        "field": args.get("field_name"),
        "value": args.get("value"),
        "reason": args.get("reason"),
    })


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
        "description": "Cross-correlate station anomaly with Sparing violations. Call ONLY if violations exist.",
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

# Daftar tools analyst — untuk referensi ReportEvaluator
ANALYST_TOOL_NAMES = [
    "think", "detect_anomaly", "calculate_pollution_profile",
    "evaluate_rainfall_branching", "check_sparing_compliance",
    "cross_correlate_evidence", "calculate_ika_gap",
]

ANALYST_SYSTEM = f"""You are the DATA ANALYST for WQSA — DAS {TARGET_DAS}, {TARGET_REGION}.

YOUR JOB: Analyse cleaned data from the DataEvaluator. Follow the reasoning chain strictly.

CHAIN:
1. detect_anomaly() per candidate station
2. calculate_pollution_profile() for anomalous stations (use COD/BOD from parameter field)
3. evaluate_rainfall_branching() using the rainfall data for the station's area
4. check_sparing_compliance() using sparing monitoring data
5. cross_correlate_evidence() ONLY if violations found (total_violations > 0)
6. calculate_ika_gap() once per region

HANDLING MISSING DATA:
- If a parameter is null (set to null by the evaluator), it means sensor failure. Skip that parameter.
- If COD or BOD is null, you CANNOT calculate pollution profile → skip step 2 for that station.
- If sparing monitoring arrays are empty, report "no monitoring data available" — do NOT invent data.
- If sitala is empty, report ika_gap as null and ika_urgency as "DATA_TIDAK_TERSEDIA".
- Work with what you have. Partial analysis is better than no analysis.

If you receive FEEDBACK from the ReportEvaluator, address each question.
IMPORTANT: You can ONLY answer questions using your available tools: {', '.join(ANALYST_TOOL_NAMES)}.
If a question requires data retrieval, sensor inspection, or administrative action,
respond that it is outside your analytical scope and recommend escalation.

OUTPUT (JSON only — no markdown, no preamble text, no ```json fences):
{{
  "analysis_id": "string",
  "timestamp": "ISO",
  "anomalous_stations": [{{
    "station_id": "...", "station_name": "...",
    "indeks_mutu": 0.0, "status": "...",
    "is_limpasan": false, "rainfall_mm": 0.0,
    "pollution_profile": "INDUSTRI|CAMPURAN|DOMESTIK|TIDAK_BISA_DIHITUNG",
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
            _log(f"[DataAnalyst] RE-ANALYSE with feedback — model={ANALYST_MODEL}")
            content = (
                "Cleaned data bundle:\n\n" + json.dumps(cleaned_data, ensure_ascii=False, default=str)
                + "\n\n--- FEEDBACK FROM REPORT EVALUATOR ---\n" + feedback
            )
        else:
            _log(f"[DataAnalyst] START — model={ANALYST_MODEL} candidates={len(cleaned_data.get('cleaned_candidate_stations', []))}")
            content = "Cleaned data bundle:\n\n" + json.dumps(cleaned_data, ensure_ascii=False, default=str)

        raw = _run_loop(
            ANALYST_MODEL, ANALYST_SYSTEM, content,
            ANALYST_TOOLS, ANALYST_TOOL_FUNCS,
            max_steps=30, label=label,
        )
        result = parse_agent_json(raw, label)
        result = normalize_agent_output(result, "analyst")

        # Bangun narasi dari think() calls — ini adalah reasoning chain terbaik
        narrative = _build_narrative_from_thinks(_loop_think_notes, _loop_collected_texts, raw)
        result["_raw_narrative"] = narrative
        _log(f"[DataAnalyst] Narrative: {len(narrative)} chars dari {len(_loop_think_notes)} think() calls")
        return result


def _build_narrative_from_thinks(
    think_notes: list,
    collected_texts: list,
    raw_fallback: str
) -> str:
    """
    Bangun narasi human-readable dari think() tool calls.
    think() berisi reasoning chain lengkap yang lebih informatif dari output JSON.
    """
    if think_notes:
        parts = []
        for i, note in enumerate(think_notes, 1):
            thought = note["thought"].strip()
            # Hapus tag <thinking> jika ada
            thought = thought.replace("<thinking>", "").replace("</thinking>", "").strip()
            if thought:
                parts.append(thought)

        if parts:
            return "\n\n---\n\n".join(parts)

    # Fallback: cari teks non-JSON dari collected_texts
    AVOID = ["summary of failures", "## failure", "```json", "```"]
    candidates = []
    for text in collected_texts:
        low = text.lower()
        # Skip teks yang pure JSON atau error summary
        if text.strip().startswith('{') or text.strip().startswith('```'):
            continue
        if any(kw in low for kw in AVOID):
            continue
        if len(text) > 100:
            candidates.append(text)

    if candidates:
        return max(candidates, key=len)

    return raw_fallback or ""


# =============================================================================
# AGENT 3 — ReportEvaluatorAgent
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

IMPORTANT — SCOPE AWARENESS:
The DataAnalyst has ONLY these tools: {', '.join(ANALYST_TOOL_NAMES)}.
The analyst CANNOT:
- Retrieve new data from the database
- Inspect sensor hardware
- Contact external offices (KLHK, SIMPEL, SITALA)
- Fix data quality issues (that was the DataEvaluator's job)

So DO NOT ask the analyst to:
- "retrieve monitoring data" — it can't
- "confirm sensor status" — it can't
- "provide IKA targets" — it can't

If data is missing, note it in your output as a data gap, not as a question for the analyst.
Only ask questions the analyst CAN answer with its tools (re-analyze, recalculate, re-check).

OUTPUT FORMAT — Feedback:
{{
  "action": "feedback",
  "issues": ["description of each inconsistency found"],
  "questions": ["specific question the analyst CAN answer with its tools"]
}}

OUTPUT FORMAT — Accept:
{{
  "action": "accept",
  "analysis_highlight": {{
    "overall_urgency": "PANTAU|WASPADA|TINDAK",
    "total_anomalous": 0,
    "total_normal": 0,
    "top_stations": [{{
      "station_id": "...", "station_name": "...",
      "status": "...", "indeks_mutu": 0.0,
      "urgency": "...", "pollution_profile": "...",
      "top_suspect": "...", "causal_confidence": "...",
      "key_finding": "1-sentence summary in Bahasa Indonesia"
    }}],
    "priority_actions": [{{
      "priority": 1, "action": "...", "target": "...", "deadline": "..."
    }}],
    "data_gaps": ["list of missing data that could not be resolved"]
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
    "total_stations": 0, "critical": 0, "warning": 0, "good": 0,
    "max_rain_mm": 0.0, "langgar_count": 0
  }},
  "telegram_message": "formatted summary in Bahasa Indonesia",
  "ika_summary": {{"actual": 0.0, "target": 0.0, "gap": 0.0, "urgency": "..."}}
}}

RULES:
- Output ONLY valid JSON — no markdown, no preamble, no ```json fences
- Bahasa Indonesia for telegram_message and key_finding
- Be strict about inconsistencies — if something doesn't add up, use Action A
- But only ask questions the analyst CAN answer"""


class ReportEvaluatorAgent:
    def run(self, analysis_result: dict, data_quality: Optional[dict] = None) -> dict:
        _log(f"[ReportEvaluator] START — model={REPORTER_MODEL}")

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
            result = parse_agent_json(raw, "ReportEvaluator")
            result = normalize_agent_output(result, "reporter")

            action = result.get("action", "accept")
            if action == "feedback":
                _log(f"[ReportEvaluator] FEEDBACK — {len(result.get('issues', []))} issues found")
            else:
                _log(f"[ReportEvaluator] ACCEPTED — analysis passed validation")
            return result

        except Exception as e:
            logger.error(f"[ReportEvaluator] Failed: {e}")
            return normalize_agent_output(
                {"action": "accept", "error": str(e)}, "reporter"
            )