# ─────────────────────────────────────────────────────────────────────────────
# config.py — Configuration for WQSA (Local / Dummy Data Mode)
# ─────────────────────────────────────────────────────────────────────────────

import os
from dotenv import load_dotenv

load_dotenv("wqsa.env")

# ── API Keys (only OpenRouter + Telegram needed for local mode) ──────────────
OPENROUTER_API_KEY   = os.getenv("OPENROUTER_API_KEY")
TELEGRAM_BOT_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN")

# ── Data Source API Keys / Endpoints (DISABLED for local mode) ───────────────
# These are commented out — agent reads from dummy_data/ folder instead.
# Uncomment and fill when connecting to real APIs.
# ONLIMO_API_URL     = os.getenv("ONLIMO_API_URL", "")
# ONLIMO_API_KEY     = os.getenv("ONLIMO_API_KEY", "")
# SPARING_API_URL    = os.getenv("SPARING_API_URL", "")
# SPARING_API_KEY    = os.getenv("SPARING_API_KEY", "")
# IBEX_API_URL       = os.getenv("IBEX_API_URL", "")
# IBEX_API_KEY       = os.getenv("IBEX_API_KEY", "")
# SITALA_API_URL     = os.getenv("SITALA_API_URL", "")
# SITALA_API_KEY     = os.getenv("SITALA_API_KEY", "")
# ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY", "")

# ── Local Data Directory ─────────────────────────────────────────────────────
DUMMY_DATA_DIR       = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dummy_data")

# ── Security ─────────────────────────────────────────────────────────────────
ALLOWED_DEVICES      = [os.getenv("ALLOWED_DEVICES", "")]
ALLOWED_USER_IDS     = [int(x) for x in os.getenv("ALLOWED_USER_IDS", "0").split(",")]
MASTER_PASSWORD      = os.getenv("MASTER_PASSWORD", "")
SESSION_TIMEOUT      = int(os.getenv("SESSION_TIMEOUT", "3600"))
RATE_LIMIT_SECONDS   = int(os.getenv("RATE_LIMIT_SECONDS", "5"))

# ── LLM Models ───────────────────────────────────────────────────────────────
AGENT_MODEL          = os.getenv("AGENT_MODEL", "openai/gpt-5-nano")
# REC_MODEL uses OpenRouter too in local mode (no Anthropic API needed)
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
MAX_AGENT_STEPS      = 100
CACHE_TTL_HOURS      = 72

# ── Anomaly Log File (replaces MySQL in local mode) ──────────────────────────
ANOMALY_LOG_FILE     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "anomaly_log.json")
