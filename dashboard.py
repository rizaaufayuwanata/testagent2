# =============================================================================
# dashboard.py — WQSA Web Dashboard (Flask)
# =============================================================================
# Data from MySQL via data_layer.py. Agent via chain.py (3-agent pipeline).
# Defined inputs only (no free-text prompts).
# =============================================================================

import logging
import threading
import uuid
from datetime import datetime

import requests
from flask import Flask, jsonify, render_template, request

from config import TARGET_DAS, TARGET_REGION, TELEGRAM_BOT_TOKEN
from chain import run_chain
from data_layer import (
    get_all_onlimo_flat,
    get_all_rainfall_data,
    get_sparing_summary,
    get_sparing_violations,
    get_sparing_company_summary,
    get_all_sitala_with_urgency,
    get_all_anomaly_log,
    get_dashboard_summary,
)

app = Flask(__name__, template_folder="templates")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Job store (in-memory, keyed by job_id) ────────────────────────────────
_jobs = {}


# =============================================================================
# PAGE ROUTE
# =============================================================================

@app.route("/")
def index():
    return render_template("dashboard.html")


# =============================================================================
# DIAGNOSTIC ENDPOINT — check DB connection + table row counts
# =============================================================================

@app.route("/api/diag")
def api_diag():
    import pymysql
    from config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
    result = {
        "host": MYSQL_HOST, "port": MYSQL_PORT,
        "database": MYSQL_DATABASE, "user": MYSQL_USER,
        "connected": False, "tables": {}, "errors": [],
    }
    try:
        conn = pymysql.connect(
            host=MYSQL_HOST, port=MYSQL_PORT,
            user=MYSQL_USER, password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE, charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor, connect_timeout=5,
        )
        result["connected"] = True
        checks = [
            "onlimo_stasiun","onlimo_pembacaan","onlimo_status",
            "bmkg_lokasi","bmkg_prakiraan","bmkg_summary_harian",
            "sparing_industri","sparing_logger","sparing_monitoring",
            "sitala_ika","anomaly_log",
            "v_onlimo_terbaru","v_bmkg_terbaru",
            "v_sitala_terbaru","v_sparing_kepatuhan_terkini",
        ]
        with conn.cursor() as cur:
            for tbl in checks:
                try:
                    cur.execute(f"SELECT COUNT(*) AS n FROM `{tbl}`")
                    row = cur.fetchone()
                    result["tables"][tbl] = row["n"] if row else 0
                except Exception as e:
                    result["tables"][tbl] = f"ERROR: {e}"
                    result["errors"].append(f"{tbl}: {e}")
        conn.close()
    except Exception as e:
        result["errors"].append(f"Connection failed: {e}")
    return jsonify(result)



# =============================================================================
# API — DASHBOARD DATA ENDPOINTS (direct from DB, no agent)
# =============================================================================

@app.route("/api/summary")
def api_summary():
    try:
        return jsonify(get_dashboard_summary())
    except Exception as e:
        logger.exception("api_summary failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/onlimo")
def api_onlimo():
    try:
        das = request.args.get("das", "")
        return jsonify(get_all_onlimo_flat(das=das))
    except Exception as e:
        logger.exception("api_onlimo failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/bmkg")
def api_bmkg():
    try:
        data = get_all_rainfall_data()
        return jsonify(data)
    except Exception as e:
        logger.exception("api_bmkg failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/sparing")
def api_sparing():
    try:
        return jsonify({
            "summary":    get_sparing_summary(),
            "violations": get_sparing_violations(),
            "companies":  get_sparing_company_summary(),
        })
    except Exception as e:
        logger.exception("api_sparing failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/sitala")
def api_sitala():
    try:
        return jsonify(get_all_sitala_with_urgency())
    except Exception as e:
        logger.exception("api_sitala failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/anomaly_log")
def api_anomaly_log():
    try:
        limit = int(request.args.get("limit", 30))
        return jsonify(get_all_anomaly_log(limit=limit))
    except Exception as e:
        logger.exception("api_anomaly_log failed")
        return jsonify({"error": str(e)}), 500


# =============================================================================
# API — AGENT ANALYSIS (defined inputs only)
# =============================================================================

def _run_agent_job(job_id: str, analysis_type: str, target_value: str):
    """Background worker for agent analysis."""
    _jobs[job_id]["status"] = "running"
    _jobs[job_id]["started_at"] = datetime.now().isoformat()
    try:
        result = run_chain(
            analysis_type=analysis_type,
            target_value=target_value,
            session_id=job_id,
        )
        _jobs[job_id]["status"] = "done"
        _jobs[job_id]["result"] = result
    except Exception as e:
        logger.exception(f"Agent job {job_id} failed")
        _jobs[job_id]["status"] = "error"
        _jobs[job_id]["error"] = str(e)
    _jobs[job_id]["finished_at"] = datetime.now().isoformat()


@app.route("/api/analyze", methods=["POST"])
def api_analyze():
    """
    Start analysis job. Defined inputs only:
    {
      "type": "full_scan" | "station" | "region",
      "target": ""  (station_id or region name, empty for full_scan)
    }
    """
    try:
        data = request.get_json(force=True, silent=True) or {}
        analysis_type = data.get("type", "full_scan")
        target_value = data.get("target", "")

        if analysis_type not in ("full_scan", "station", "region"):
            return jsonify({"error": "Invalid type. Use: full_scan, station, region"}), 400

        job_id = f"job-{uuid.uuid4().hex[:12]}"
        _jobs[job_id] = {
            "status": "queued",
            "type": analysis_type,
            "target": target_value,
            "created_at": datetime.now().isoformat(),
            "result": None,
            "error": None,
        }

        thread = threading.Thread(target=_run_agent_job, args=(job_id, analysis_type, target_value), daemon=True)
        thread.start()

        logger.info(f"Job {job_id}: {analysis_type} / {target_value or 'all'}")
        return jsonify({"job_id": job_id, "status": "queued"}), 202

    except Exception as e:
        logger.exception("api_analyze POST failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/analyze/<job_id>")
def api_analyze_status(job_id: str):
    """Poll job status. Returns full result when done."""
    job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify({"job_id": job_id, **job})


@app.route("/api/analyze/<job_id>/detail")
def api_analyze_detail(job_id: str):
    """Get detail reasoning for the popup."""
    job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    if job["status"] != "done" or not job.get("result"):
        return jsonify({"error": "Analysis not complete yet"}), 400
    return jsonify({
        "detail_reasoning": job["result"].get("detail_reasoning"),
        "data_quality": job["result"].get("data_quality"),
        "feedback_loops": job["result"].get("feedback_loops", 0),
        "unresolved_issues": job["result"].get("unresolved_issues", []),
    })


# =============================================================================
# API — AUTO-SEND TO TELEGRAM
# =============================================================================

@app.route("/api/analyze/<job_id>/telegram", methods=["POST"])
def api_send_telegram(job_id: str):
    """Send analysis highlight to Telegram."""
    job = _jobs.get(job_id)
    if not job or job["status"] != "done":
        return jsonify({"error": "Job not done"}), 400

    result = job.get("result", {})
    message = result.get("telegram_message", "")
    if not message:
        # Fallback: build from highlight
        hl = result.get("analysis_highlight", {})
        if hl:
            message = (
                f"🌊 WQSA Auto-Report\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"Urgensi: {hl.get('overall_urgency', '?')}\n"
                f"Anomali: {hl.get('total_anomalous', 0)} stasiun\n"
                f"Normal: {hl.get('total_normal', 0)} stasiun\n"
            )
            for s in hl.get("top_stations", []):
                message += f"\n📍 {s.get('station_id')} — {s.get('key_finding', '')}"
            for a in hl.get("priority_actions", []):
                message += f"\n{a.get('priority', '')}. {a.get('action', '')}"
        else:
            message = "Analysis complete — no highlight data available."

    data = request.get_json(force=True, silent=True) or {}
    chat_id = data.get("chat_id")

    if not chat_id or not TELEGRAM_BOT_TOKEN:
        return jsonify({"error": "chat_id or TELEGRAM_BOT_TOKEN missing"}), 400

    try:
        # Chunk if too long
        chunks = [message[i:i+4000] for i in range(0, len(message), 4000)]
        for chunk in chunks:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": chunk},
                timeout=10,
            )
        return jsonify({"sent": True, "chunks": len(chunks)})
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return jsonify({"error": str(e)}), 500


# =============================================================================
# API — STATION LIST (for dropdown)
# =============================================================================

@app.route("/api/stations")
def api_stations():
    """Lightweight station list for dropdown."""
    try:
        rows = get_all_onlimo_flat(das=TARGET_DAS)
        return jsonify([
            {"station_id": r["station_id"], "station_name": r["station_name"]}
            for r in rows
        ])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print(f"  WQSA Dashboard — {TARGET_DAS} / {TARGET_REGION}")
    print(f"  Open: http://127.0.0.1:5000")
    print("=" * 60)
    app.run(debug=True, host="0.0.0.0", port=5000)