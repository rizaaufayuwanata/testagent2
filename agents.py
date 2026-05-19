# ─────────────────────────────────────────────────────────────────────────────
# agents.py — Multi-Agent Pipeline for WQSA
# ─────────────────────────────────────────────────────────────────────────────
#
# Two agents, two jobs:
#
#   DataEvaluatorAgent   — receives pre-fetched raw data bundle, runs an
#                          agentic loop with analysis tools, outputs a
#                          structured EvaluationResult JSON.
#
#   AnalyticalAgent      — receives EvaluationResult, makes a single LLM
#                          call, writes the final Indonesian report.
#
# Neither agent queries the database directly — data fetching stays in
# chain.py (deterministic Python). Agents only reason about data.
# ─────────────────────────────────────────────────────────────────────────────

import json
import logging
from typing import Optional

from openai import OpenAI

from config import (
    OPENROUTER_API_KEY, AGENT_MODEL,
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

# ── Lazy OpenRouter client ────────────────────────────────────────────────────
_client: Optional[OpenAI] = None

def _get_client() -> OpenAI:
    global _client
    if _client is None:
        if not OPENROUTER_API_KEY:
            raise RuntimeError("OPENROUTER_API_KEY is not set in wqsa.env")
        _client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=OPENROUTER_API_KEY)
    return _client


# ─────────────────────────────────────────────────────────────────────────────
# EVALUATOR TOOLS — analysis only, no data fetch, no report writing
# ─────────────────────────────────────────────────────────────────────────────

EVALUATOR_TOOLS = [
    {"type": "function", "function": {
        "name": "think",
        "description": "Plan your evaluation strategy. Call first, and whenever you need to reconsider.",
        "parameters": {"type": "object", "properties": {
            "thought": {"type": "string"}
        }, "required": ["thought"]}
    }},
    {"type": "function", "function": {
        "name": "detect_anomaly",
        "description": "Detect anomaly in a single station's data. Returns is_anomaly, reasons, COD/BOD/TSS.",
        "parameters": {"type": "object", "properties": {
            "station_data": {"type": "string", "description": "JSON string of one station dict"}
        }, "required": ["station_data"]}
    }},
    {"type": "function", "function": {
        "name": "calculate_pollution_profile",
        "description": "COD/BOD ratio → INDUSTRI (>4.0) / CAMPURAN (2-4) / DOMESTIK (<2).",
        "parameters": {"type": "object", "properties": {
            "cod": {"type": "number"}, "bod": {"type": "number"}
        }, "required": ["cod", "bod"]}
    }},
    {"type": "function", "function": {
        "name": "evaluate_rainfall_branching",
        "description": "Check if anomaly is caused by rainfall runoff. total > 50mm/24h → LIMPASAN → stop Sparing investigation.",
        "parameters": {"type": "object", "properties": {
            "rainfall_data": {"type": "string", "description": "JSON string of rainfall dict for this station's location"}
        }, "required": ["rainfall_data"]}
    }},
    {"type": "function", "function": {
        "name": "check_sparing_compliance",
        "description": "Evaluate TAAT/LANGGAR status for monitoring records.",
        "parameters": {"type": "object", "properties": {
            "monitoring_data": {"type": "string", "description": "JSON string list of monitoring records"}
        }, "required": ["monitoring_data"]}
    }},
    {"type": "function", "function": {
        "name": "cross_correlate_evidence",
        "description": (
            "Cross-correlate a station anomaly with Sparing violations. "
            "Scores each violating industry on spatial distance, temporal consistency, "
            "and pollution profile match. Returns ranked causal evidence (TINGGI/SEDANG/RENDAH). "
            "ALWAYS call this after check_sparing_compliance — it is required for a valid evaluation."
        ),
        "parameters": {"type": "object", "properties": {
            "station_json": {"type": "string", "description": "JSON string of the anomalous station"},
            "step4_json":   {"type": "string", "description": "JSON string with industries list and violations"},
            "step2_json":   {"type": "string", "description": "JSON string of pollution profile result"},
        }, "required": ["station_json", "step4_json", "step2_json"]}
    }},
    {"type": "function", "function": {
        "name": "calculate_ika_gap",
        "description": "IKA actual vs target gap → urgency (TINDAK / WASPADA / PANTAU).",
        "parameters": {"type": "object", "properties": {
            "sitala_data": {"type": "string", "description": "JSON string of SITALA dict"}
        }, "required": ["sitala_data"]}
    }},
]

EVALUATOR_TOOL_FUNCTIONS = {
    "think":                    lambda args: think(args["thought"]),
    "detect_anomaly":           lambda args: detect_anomaly(args["station_data"]),
    "calculate_pollution_profile": lambda args: calculate_pollution_profile(args["cod"], args["bod"]),
    "evaluate_rainfall_branching": lambda args: evaluate_rainfall_branching(args["rainfall_data"]),
    "check_sparing_compliance": lambda args: check_sparing_compliance(args["monitoring_data"]),
    "cross_correlate_evidence": lambda args: cross_correlate_evidence(
                                    args["station_json"], args["step4_json"], args["step2_json"]
                                ),
    "calculate_ika_gap":        lambda args: calculate_ika_gap(args["sitala_data"]),
}

EVALUATOR_SYSTEM_PROMPT = f"""You are a water quality DATA EVALUATOR for DAS {TARGET_DAS}, {TARGET_REGION}.

== YOUR ONLY JOB ==
Analyse the raw data bundle provided by the user. Use your tools to reason through the evidence.
Output a single structured JSON object as your FINAL response. Nothing else.

== EVALUATION SEQUENCE ==
For EACH station in the data bundle:

1. ANOMALY CHECK
   - Call detect_anomaly(station_data) for each station
   - Skip stations with is_anomaly=false

2. POLLUTION PROFILE (anomalous stations only)
   - Call calculate_pollution_profile(cod, bod)
   - Extract COD and BOD from station's parameter field

3. RAINFALL BRANCH (anomalous stations only)
   - Call evaluate_rainfall_branching(rainfall_data)
   - Use the rainfall entry from the bundle matching this station's kecamatan/coordinates
   - If is_runoff=true → mark as LIMPASAN, skip steps 4-5 for this station

4. COMPLIANCE CHECK (non-limpasan anomalous stations only)
   - Call check_sparing_compliance(monitoring_data)
   - Use the sparing monitoring data from the bundle for this station's district

5. CAUSAL CROSS-CORRELATION (REQUIRED if violations found)
   - Call cross_correlate_evidence(station_json, step4_json, step2_json)
   - This is MANDATORY — never skip it when there are violations
   - Use the compliance results as step4_json

6. IKA GAP (once per region, not per station)
   - Call calculate_ika_gap(sitala_data) using the SITALA entry from the bundle

== REQUIRED OUTPUT FORMAT ==
After all tool calls, output ONLY this JSON (no markdown, no prose):

{{
  "evaluation_id": "<station_id_or_'full_scan'>_<YYYYMMDD>",
  "target": "<description of what was analysed>",
  "timestamp": "<ISO timestamp>",
  "anomalous_stations": [
    {{
      "station_id": "...",
      "station_name": "...",
      "indeks_mutu": 0.0,
      "status": "...",
      "is_limpasan": false,
      "rainfall_mm": 0.0,
      "pollution_profile": "INDUSTRI|CAMPURAN|DOMESTIK|DATA_TIDAK_VALID",
      "cod_bod_ratio": 0.0,
      "causal_evidence": [
        {{
          "company_name": "...",
          "causal_confidence": "TINGGI|SEDANG|RENDAH",
          "distance_km": 0.0,
          "temporal_ok": true,
          "profile_ok": true,
          "violated_params": ["COD", "BOD"],
          "evidence_notes": {{}}
        }}
      ],
      "top_suspect": "company name or null",
      "urgency": "TINDAK|WASPADA|PANTAU",
      "anomaly_reasons": []
    }}
  ],
  "normal_station_count": 0,
  "ika_gap": 0.0,
  "ika_urgency": "TINDAK|WASPADA|PANTAU",
  "region_summary": "<2-3 sentence factual summary in English, no recommendations>"
}}

== RULES ==
- Always call think() first to plan
- ALWAYS call cross_correlate_evidence after finding violations
- Output ONLY valid JSON as your final message — no preamble, no markdown fences
- Do not write recommendations — that is the Analytical Agent's job
- If data for a step is missing from the bundle, note it in the relevant field and continue"""


# ─────────────────────────────────────────────────────────────────────────────
# DataEvaluatorAgent
# ─────────────────────────────────────────────────────────────────────────────

class DataEvaluatorAgent:
    """
    Agentic evaluator that reasons over a pre-fetched data bundle.
    Calls analysis tools iteratively, outputs structured EvaluationResult JSON.
    """

    MAX_STEPS = 30  # Evaluator is focused — 30 steps should be plenty

    def run(self, raw_data_bundle: dict) -> dict:
        """
        Run the evaluator on a raw_data_bundle dict.
        Returns parsed EvaluationResult dict (or error dict).
        """
        logger.info("[EvaluatorAgent] Starting evaluation")
        print("\n🔍 [DataEvaluatorAgent] Starting — analysing raw data bundle...")
        print(f"   Stations in bundle: {len(raw_data_bundle.get('candidate_stations', []))}")

        messages = [
            {"role": "system",  "content": EVALUATOR_SYSTEM_PROMPT},
            {"role": "user",    "content": (
                "Here is the raw data bundle for your evaluation:\n\n"
                + json.dumps(raw_data_bundle, ensure_ascii=False, default=str)
            )},
        ]

        step = 0
        for _ in range(self.MAX_STEPS):
            response = _get_client().chat.completions.create(
                model=AGENT_MODEL,
                messages=messages,
                tools=EVALUATOR_TOOLS,
                tool_choice="auto",
            )
            msg = response.choices[0].message

            # No tool calls → LLM is done, this should be the JSON output
            if not msg.tool_calls:
                raw_text = (msg.content or "").strip()
                print(f"  ✅ [EvaluatorAgent] Done — {step} tool calls")
                logger.info(f"[EvaluatorAgent] Finished after {step} tool calls")
                return self._parse_output(raw_text)

            # Execute tool calls
            messages.append({
                "role": "assistant",
                "content": msg.content,
                "tool_calls": [
                    {"id": tc.id, "type": "function",
                     "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
                    for tc in msg.tool_calls
                ],
            })

            for tc in msg.tool_calls:
                step += 1
                tool_name = tc.function.name
                tool_args = json.loads(tc.function.arguments)

                # Summarise args for readable terminal output
                arg_summary = ", ".join(
                    f"{k}={str(v)[:60]}" for k, v in tool_args.items()
                )
                print(f"  [{step:02d}] → {tool_name}({arg_summary})")
                logger.info(f"[EvaluatorAgent] Step {step}: {tool_name}({list(tool_args.keys())})")

                if tool_name in EVALUATOR_TOOL_FUNCTIONS:
                    try:
                        result = EVALUATOR_TOOL_FUNCTIONS[tool_name](tool_args)
                        # Print a brief result preview
                        preview = str(result)[:120].replace("\n", " ")
                        print(f"       ↳ {preview}{'...' if len(str(result)) > 120 else ''}")
                    except Exception as e:
                        result = json.dumps({"error": str(e)})
                        print(f"       ↳ ERROR: {e}")
                        logger.error(f"[EvaluatorAgent] {tool_name} failed: {e}")
                else:
                    result = json.dumps({"error": f"Unknown tool: {tool_name}"})
                    print(f"       ↳ ERROR: unknown tool '{tool_name}'")

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": str(result),
                })

        logger.warning("[EvaluatorAgent] Reached MAX_STEPS without finishing")
        return {"error": f"Evaluator exceeded {self.MAX_STEPS} steps", "partial": True}

    def _parse_output(self, raw: str) -> dict:
        """Parse the LLM's JSON output, strip markdown fences if present."""
        try:
            # Strip ```json ... ``` or ``` ... ``` wrappers
            clean = raw.strip()
            if clean.startswith("```"):
                clean = clean.split("```", 2)[1]
                if clean.startswith("json"):
                    clean = clean[4:]
                clean = clean.rsplit("```", 1)[0].strip()
            return json.loads(clean)
        except json.JSONDecodeError as e:
            logger.error(f"[EvaluatorAgent] JSON parse failed: {e}\nRaw: {raw[:500]}")
            return {"error": f"Could not parse evaluator output: {e}", "raw": raw[:1000]}


# ─────────────────────────────────────────────────────────────────────────────
# AnalyticalAgent
# ─────────────────────────────────────────────────────────────────────────────

ANALYTICAL_SYSTEM_PROMPT = f"""Kamu adalah analis kualitas air sungai Indonesia untuk DAS {TARGET_DAS}.

Kamu menerima hasil evaluasi terstruktur dari Data Evaluator Agent. Tugasmu adalah
menginterpretasikan temuan tersebut dan menulis laporan rekomendasi yang jelas dan actionable.

Format laporan:

📍 RINGKASAN SITUASI
[Gambaran umum: berapa stasiun anomali, lokasi kritis, kondisi umum DAS]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[Untuk setiap stasiun anomali:]

📊 [STATION_ID] — [STATION_NAME]
Status: [indeks mutu + status]
Profil Pencemar: [INDUSTRI/CAMPURAN/DOMESTIK, COD/BOD ratio]
Curah Hujan: [mm/24h, LIMPASAN atau tidak]
Tersangka Utama: [nama perusahaan, confidence TINGGI/SEDANG/RENDAH]
  • Bukti: [jarak km, tanggal pelanggaran, parameter yang dilanggar]
Urgensi: [emoji] [PANTAU/WASPADA/TINDAK]

📋 REKOMENDASI TINDAKAN:
1. [Tindakan spesifik, sebutkan nama perusahaan dan parameter]
2. [Tindakan lanjutan]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📈 BENCHMARK IKA WILAYAH
IKA aktual vs target: [nilai + gap]
Trend: [naik/turun/stabil]

⚠️ LEVEL URGENSI KESELURUHAN: [emoji] [PANTAU/WASPADA/TINDAK]
🎯 CONFIDENCE ANALISIS: [persentase, berdasarkan kelengkapan data dan kekuatan bukti]

== ATURAN ==
- Gunakan Bahasa Indonesia
- Jika evaluasi menemukan LIMPASAN, jelaskan mengapa atribusi ke industri tidak dapat dilakukan
- Jika confidence RENDAH, sebutkan keterbatasan data
- Rekomendasikan tindakan spesifik (sebut nama perusahaan, parameter, tenggat waktu)
- Jangan membuat asumsi di luar data yang diberikan"""


class AnalyticalAgent:
    """
    Single-call analytical agent.
    Receives EvaluationResult from DataEvaluatorAgent, writes the final report.
    No tools — pure LLM reasoning on structured input.
    """

    def run(self, evaluation: dict) -> str:
        """
        Generate a final report from an EvaluationResult dict.
        Returns formatted report string.
        """
        logger.info("[AnalyticalAgent] Generating report")
        print("\n📝 [AnalyticalAgent] Generating recommendation report...")

        if "error" in evaluation:
            return (
                f"⚠️ Evaluator agent encountered an error: {evaluation['error']}\n"
                f"Partial results may be incomplete. Please retry or check the logs."
            )

        anomalous = evaluation.get("anomalous_stations", [])
        if not anomalous:
            return (
                f"✅ *Tidak ada anomali terdeteksi*\n\n"
                f"Seluruh stasiun dalam kondisi normal. "
                f"Monitoring rutin dapat dilanjutkan sesuai jadwal.\n\n"
                f"IKA Gap: {evaluation.get('ika_gap', 'N/A')} | "
                f"Urgensi IKA: {evaluation.get('ika_urgency', 'N/A')}"
            )

        try:
            response = _get_client().chat.completions.create(
                model=AGENT_MODEL,
                messages=[
                    {"role": "system", "content": ANALYTICAL_SYSTEM_PROMPT},
                    {"role": "user",   "content": (
                        "Berikut hasil evaluasi dari Data Evaluator Agent:\n\n"
                        + json.dumps(evaluation, ensure_ascii=False, indent=2, default=str)
                    )},
                ],
            )
            report = response.choices[0].message.content.strip()
            logger.info("[AnalyticalAgent] Report generated")
            print("  ✅ [AnalyticalAgent] Report complete")
            return report
        except Exception as e:
            logger.error(f"[AnalyticalAgent] Failed: {e}")
            return f"❌ Analytical agent failed: {e}"