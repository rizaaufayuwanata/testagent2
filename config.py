# ─────────────────────────────────────────────────────────────────────────────
# config.py — Configuration for WQSA (Local / Dummy Data Mode)
# ─────────────────────────────────────────────────────────────────────────────

import os
from dotenv import load_dotenv

load_dotenv("wqsa.env", override=True)

# ── API Keys ──────────────────────────────────────────────────────────────────
OPENROUTER_API_KEY   = os.getenv("OPENROUTER_API_KEY")
TELEGRAM_BOT_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN")
ANTHROPIC_API_KEY    = os.getenv("ANTHROPIC_API_KEY", "")

# ── Data Source API Endpoints ─────────────────────────────────────────────────
# Onlimo KLHK — 3 endpoint terpisah
ONLIMO_STASIUN_URL   = os.getenv("ONLIMO_STASIUN_URL", "")
ONLIMO_MONITORING_URL= os.getenv("ONLIMO_MONITORING_URL", "")
ONLIMO_STATUS_URL    = os.getenv("ONLIMO_STATUS_URL", "")
ONLIMO_API_KEY       = os.getenv("ONLIMO_API_KEY", "")
ONLIMO_SECRET        = os.getenv("ONLIMO_SECRET", "")
ONLIMO_CLIENT_KEY    = os.getenv("ONLIMO_CLIENT-KEY", "")

# Sparing KLHK — 2 endpoint terpisah
SPARING_LOGGER_URL   = os.getenv("SPARING_LOGGER_URL", "")
SPARING_MONITORING_URL = os.getenv("SPARING_MONITORING_URL", "")
SPARING_API_KEY      = os.getenv("SPARING_API_KEY", "")

# SITALA KLHK
SITALA_URL           = os.getenv("SITALA_URL", "")
SITALA_API_KEY       = os.getenv("SITALA_API_KEY", "")

# BMKG (free)
BMKG_API_URL         = os.getenv("BMKG_API_URL", "https://api.bmkg.go.id/publik/prakiraan-cuaca")
BMKG_ADM4_CODES      = [c.strip() for c in os.getenv("BMKG_ADM4_CODES", "").split(",") if c.strip()]

# ── MySQL Database ────────────────────────────────────────────────────────────
MYSQL_HOST           = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT           = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER           = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD       = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE       = os.getenv("MYSQL_DATABASE", "wqsa_db")

# ── Local Data Directory (dummy fallback) ─────────────────────────────────────
DUMMY_DATA_DIR       = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dummy_data")

# ── Security ─────────────────────────────────────────────────────────────────
ALLOWED_DEVICES      = [os.getenv("ALLOWED_DEVICES", "")]
ALLOWED_USER_IDS     = [int(x) for x in os.getenv("ALLOWED_USER_IDS", "0").split(",")]
MASTER_PASSWORD      = os.getenv("MASTER_PASSWORD", "")
SESSION_TIMEOUT      = int(os.getenv("SESSION_TIMEOUT", "3600"))
RATE_LIMIT_SECONDS   = int(os.getenv("RATE_LIMIT_SECONDS", "5"))

# ── LLM Models ───────────────────────────────────────────────────────────────
AGENT_MODEL          = os.getenv("AGENT_MODEL", "openai/gpt-5-nano")
EVALUATOR_MODEL      = os.getenv("EVALUATOR_MODEL", AGENT_MODEL)
ANALYST_MODEL        = os.getenv("ANALYST_MODEL", AGENT_MODEL)
REPORTER_MODEL       = os.getenv("REPORTER_MODEL", AGENT_MODEL)
REC_MODEL            = os.getenv("REC_MODEL", "openai/gpt-5-nano")

# ── DAS & Domain Config ──────────────────────────────────────────────────────
TARGET_DAS           = os.getenv("TARGET_DAS", "Citarum")
TARGET_REGION        = os.getenv("TARGET_REGION", "Kab. Bandung")

# ── Reasoning Thresholds ─────────────────────────────────────────────────────
ANOMALY_INDEX_THRESHOLD     = 3.0
ANOMALY_CHANGE_PERCENT      = 20.0
RATIO_INDUSTRY_THRESHOLD    = 4.0
RATIO_MIXED_LOW             = 2.0
RATIO_DOMESTIC_THRESHOLD    = 2.0
RAINFALL_HIGH_MM            = 50.0
RAINFALL_WINDOW_HOURS       = 24
IKA_GAP_WARNING             = -3.0
IKA_GAP_CRITICAL            = -7.0

# ── Urgency Levels ───────────────────────────────────────────────────────────
URGENCY_LEVELS = {
    "PANTAU":   {"emoji": "🟢", "description": "Monitoring rutin, tidak ada tindakan mendesak"},
    "WASPADA":  {"emoji": "🟡", "description": "Perlu perhatian, sampling lanjutan disarankan"},
    "TINDAK":   {"emoji": "🔴", "description": "Tindakan segera diperlukan, inspeksi mendadak"},
}

# ── Agentic Loop ─────────────────────────────────────────────────────────────
MAX_AGENT_STEPS      = 25
CACHE_TTL_HOURS      = 72

# ── Anomaly Log File (replaces MySQL in local mode) ──────────────────────────
ANOMALY_LOG_FILE     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "anomaly_log.json")