# =============================================================================
# dashboard.py — WQSA Web Dashboard (Flask)
# =============================================================================
# Data from MySQL via data_layer.py. Agent via chain.py (3-agent pipeline).
# Defined inputs only (no free-text prompts).
# =============================================================================

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import logging
import os
import threading
import uuid
from datetime import datetime

import requests
from flask import Flask, jsonify, render_template, request

from config import TARGET_DAS, TARGET_REGION, TELEGRAM_BOT_TOKEN
from chain import run_chain
from etl import (
    get_db, make_session,
    sync_onlimo_stasiun, sync_onlimo_monitoring, sync_onlimo_status,
    sync_bmkg, sync_sparing_logger, sync_sparing_monitoring, sync_sitala,
    run_all,
)
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

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_TEMPLATE_DIR = os.path.join(_BASE_DIR, "templates")
app = Flask(__name__, template_folder=_TEMPLATE_DIR)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Job store (in-memory, keyed by job_id) ────────────────────────────────
_jobs = {}

# ── ETL Job store ─────────────────────────────────────────────────────────
_etl_jobs = {}


# =============================================================================
# PAGE ROUTE
# =============================================================================

@app.route("/test")
def test_raw():
    """Serve dashboard.html directly bypassing Jinja2 cache."""
    from flask import send_file
    tpl_path = os.path.join(_TEMPLATE_DIR, "dashboard.html")
    return send_file(tpl_path, mimetype="text/html")


@app.route("/")
def index():
    tpl_path = os.path.join(_TEMPLATE_DIR, "dashboard.html")
    print(f"[TEMPLATE] Serving from: {tpl_path}", flush=True)
    print(f"[TEMPLATE] File exists: {os.path.exists(tpl_path)}", flush=True)
    print(f"[TEMPLATE] File size: {os.path.getsize(tpl_path) if os.path.exists(tpl_path) else 'N/A'}", flush=True)
    resp = render_template("dashboard.html")
    from flask import make_response
    r = make_response(resp)
    r.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    return r


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
# API — LIVE LOG STREAMING
# =============================================================================

@app.route("/api/log")
def api_log():
    """Return new lines from chain.log starting from a given line offset."""
    try:
        offset = int(request.args.get("offset", 0))
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "chain.log")
        if not os.path.exists(log_path):
            return jsonify({"lines": [], "total": 0})
        with open(log_path, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
        total = len(all_lines)
        new_lines = [l.rstrip() for l in all_lines[offset:]]
        return jsonify({"lines": new_lines, "total": total})
    except Exception as e:
        return jsonify({"error": str(e), "lines": [], "total": 0})


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
# ADMIN — ETL PAGE & ROUTES
# =============================================================================

@app.route("/admin/etl")
def admin_etl():
    return render_template("etl.html")


def _run_etl_job(job_id: str, source: str, days_back: int, station_ids: list = None):
    """Background worker for ETL jobs."""
    _etl_jobs[job_id]["status"] = "running"
    _etl_jobs[job_id]["started_at"] = datetime.now().isoformat()
    results = {}
    try:
        db = get_db()
        session = make_session()
        try:
            if source in ("all", "onlimo_stasiun"):
                results["onlimo_stasiun"] = sync_onlimo_stasiun(db, session)
            if source in ("all", "onlimo_monitoring"):
                results["onlimo_monitoring"] = sync_onlimo_monitoring(db, session, days_back, station_ids)
            if source in ("all", "onlimo_status"):
                results["onlimo_status"] = sync_onlimo_status(db, session, station_ids)
            if source == "onlimo":
                results["onlimo_stasiun"]    = sync_onlimo_stasiun(db, session)
                results["onlimo_monitoring"]  = sync_onlimo_monitoring(db, session, days_back, station_ids)
                results["onlimo_status"]      = sync_onlimo_status(db, session, station_ids)
            if source in ("all", "bmkg"):
                results["bmkg"] = sync_bmkg(db, session)
            if source in ("all", "sparing"):
                results["sparing_logger"]     = sync_sparing_logger(db, session)
                results["sparing_monitoring"] = sync_sparing_monitoring(db, session, days_back)
            if source == "sparing_logger":
                results["sparing_logger"] = sync_sparing_logger(db, session)
            if source == "sparing_monitoring":
                results["sparing_monitoring"] = sync_sparing_monitoring(db, session, days_back)
            if source in ("all", "sitala"):
                results["sitala"] = sync_sitala(db, session)
        finally:
            db.close()
            session.close()

        total = sum(v for v in results.values() if isinstance(v, int))
        _etl_jobs[job_id]["status"]   = "done"
        _etl_jobs[job_id]["results"]  = results
        _etl_jobs[job_id]["total"]    = total
    except Exception as e:
        logger.exception(f"ETL job {job_id} failed")
        _etl_jobs[job_id]["status"] = "error"
        _etl_jobs[job_id]["error"]  = str(e)
    _etl_jobs[job_id]["finished_at"] = datetime.now().isoformat()


@app.route("/admin/etl/run", methods=["POST"])
def admin_etl_run():
    data        = request.get_json(force=True, silent=True) or {}
    source      = data.get("source", "all")
    days_back   = int(data.get("days_back", 3))
    station_ids = data.get("station_ids", None)   # list or None

    valid = {"all","onlimo","onlimo_stasiun","onlimo_monitoring","onlimo_status",
             "bmkg","sparing","sparing_logger","sparing_monitoring","sitala"}
    if source not in valid:
        return jsonify({"error": f"Invalid source: {source}"}), 400

    job_id = f"etl-{uuid.uuid4().hex[:10]}"
    _etl_jobs[job_id] = {
        "status": "queued", "source": source, "days_back": days_back,
        "station_ids": station_ids,
        "created_at": datetime.now().isoformat(),
        "results": None, "error": None, "total": 0,
    }
    t = threading.Thread(target=_run_etl_job, args=(job_id, source, days_back, station_ids), daemon=True)
    t.start()
    logger.info(f"ETL job {job_id}: source={source} days_back={days_back}")
    return jsonify({"job_id": job_id, "status": "queued"}), 202


@app.route("/admin/etl/status/<job_id>")
def admin_etl_status(job_id: str):
    job = _etl_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify({"job_id": job_id, **job})


@app.route("/admin/etl/onlimo-stations")
def admin_etl_onlimo_stations():
    """List semua stasiun Onlimo dari DB beserta format ID-nya."""
    try:
        from data_layer import _query
        rows = _query("""
            SELECT station_id, station_name, nama_das, kabkot, status_aktif,
                   (station_id REGEXP '^KLHK[0-9]+') AS is_klhk_format
            FROM onlimo_stasiun
            ORDER BY is_klhk_format DESC, station_id
        """)
        return jsonify([{
            "station_id":    r["station_id"],
            "station_name":  r.get("station_name") or "",
            "das":           r.get("nama_das") or "",
            "kabkot":        r.get("kabkot") or "",
            "aktif":         bool(r.get("status_aktif")),
            "is_klhk":       bool(r.get("is_klhk_format")),
        } for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/admin/etl/config-check")
def admin_etl_config_check():
    """Check which API configs are set in wqsa.env."""
    from config import (
        ONLIMO_STASIUN_URL, ONLIMO_MONITORING_URL, ONLIMO_STATUS_URL,
        ONLIMO_API_KEY, ONLIMO_SECRET, ONLIMO_CLIENT_KEY,
        SPARING_LOGGER_URL, SPARING_MONITORING_URL, SPARING_API_KEY,
        SITALA_URL, SITALA_API_KEY,
        BMKG_API_URL, BMKG_ADM4_CODES,
    )
    def chk(val): return bool(val and str(val).strip())
    return jsonify({
        "onlimo": {
            "stasiun_url":    {"set": chk(ONLIMO_STASIUN_URL),    "value": (ONLIMO_STASIUN_URL or "")[:60]},
            "monitoring_url": {"set": chk(ONLIMO_MONITORING_URL), "value": (ONLIMO_MONITORING_URL or "")[:60]},
            "status_url":     {"set": chk(ONLIMO_STATUS_URL),     "value": (ONLIMO_STATUS_URL or "")[:60]},
            "api_key":        {"set": chk(ONLIMO_API_KEY),        "value": ("***" if ONLIMO_API_KEY else "")},
            "secret":         {"set": chk(ONLIMO_SECRET),         "value": ("***" if ONLIMO_SECRET else "")},
            "client_key":     {"set": chk(ONLIMO_CLIENT_KEY),     "value": ("***" if ONLIMO_CLIENT_KEY else "")},
        },
        "bmkg": {
            "api_url":   {"set": chk(BMKG_API_URL),    "value": (BMKG_API_URL or "")[:60]},
            "adm4_codes":{"set": chk(BMKG_ADM4_CODES), "value": f"{len(BMKG_ADM4_CODES)} kode" if BMKG_ADM4_CODES else ""},
        },
        "sparing": {
            "logger_url":     {"set": chk(SPARING_LOGGER_URL),     "value": (SPARING_LOGGER_URL or "")[:60]},
            "monitoring_url": {"set": chk(SPARING_MONITORING_URL), "value": (SPARING_MONITORING_URL or "")[:60]},
            "api_key":        {"set": chk(SPARING_API_KEY),        "value": ("***" if SPARING_API_KEY else "")},
        },
        "sitala": {
            "url":     {"set": chk(SITALA_URL),     "value": (SITALA_URL or "")[:60]},
            "api_key": {"set": chk(SITALA_API_KEY), "value": ("***" if SITALA_API_KEY else "")},
        },
    })


@app.route("/admin/etl/history")
def admin_etl_history():
    """Last 50 ETL sync logs from api_sync_log table."""
    try:
        from data_layer import _query
        rows = _query("""
            SELECT id, api_name, endpoint, sync_start, sync_end,
                   status, records_synced, error_message
            FROM api_sync_log
            ORDER BY sync_start DESC
            LIMIT 50
        """)
        return jsonify([{
            "id":             r["id"],
            "api_name":       r["api_name"],
            "endpoint":       (r.get("endpoint") or "")[:60],
            "sync_start":     str(r.get("sync_start") or ""),
            "sync_end":       str(r.get("sync_end") or ""),
            "status":         r.get("status"),
            "records_synced": r.get("records_synced") or 0,
            "error_message":  (r.get("error_message") or "")[:200],
        } for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import os
    os.environ["PYTHONUNBUFFERED"] = "1"

    # Log ke file agar bisa dipantau dari terminal manapun
    import logging as _logging
    file_handler = _logging.FileHandler("chain.log", encoding="utf-8")
    file_handler.setLevel(_logging.INFO)
    file_handler.setFormatter(_logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    _logging.getLogger().addHandler(file_handler)

    print("=" * 60, flush=True)
    print(f"  WQSA Dashboard — {TARGET_DAS} / {TARGET_REGION}", flush=True)
    print(f"  Open: http://127.0.0.1:5000", flush=True)
    print(f"  Log : chain.log (tail -f chain.log untuk monitor)", flush=True)
    print("=" * 60, flush=True)

    # use_reloader=False wajib agar background thread & print terlihat
    app.run(debug=False, host="0.0.0.0", port=5000, use_reloader=False)


