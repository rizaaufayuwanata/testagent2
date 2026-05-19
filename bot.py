# ─────────────────────────────────────────────────────────────────────────────
# bot.py — WQSA Telegram Bot (MySQL-backed, Chain-enforced)
# ─────────────────────────────────────────────────────────────────────────────
# Reads from MySQL (wqsa_db) via data_layer.py.
# Uses chain.py for enforced 5-step reasoning (Steps 1-4 = Python, Step 5 = LLM).
# ─────────────────────────────────────────────────────────────────────────────

import os
import time
import uuid
import logging
from datetime import datetime

from telegram import Update
from telegram.ext import (
    ApplicationBuilder, MessageHandler, CommandHandler,
    filters, ContextTypes
)

from config import (
    TELEGRAM_BOT_TOKEN,
    ALLOWED_DEVICES, ALLOWED_USER_IDS, MASTER_PASSWORD,
    SESSION_TIMEOUT, RATE_LIMIT_SECONDS,
    TARGET_DAS, TARGET_REGION,
    MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE,
    MAX_AGENT_STEPS,
)
from chain import run_chain

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    filename="wqsa_audit.log",
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logger.addHandler(console)

# ── Device Authorization ─────────────────────────────────────────────────────
def check_device():
    device_id = str(uuid.getnode())
    if ALLOWED_DEVICES[0] and device_id not in ALLOWED_DEVICES:
        print(f"❌ Unauthorized device: {device_id}")
        exit()
    print(f"✅ Device ID: {device_id}")

check_device()

# ── Rate Limiting ─────────────────────────────────────────────────────────────
user_last_message = {}


# ─────────────────────────────────────────────────────────────────────────────
# INTENT CLASSIFIER
# ─────────────────────────────────────────────────────────────────────────────

ANALYSIS_TRIGGERS = [
    "mulai analisis", "start analysis", "analisis semua", "analisis stasiun",
    "analisis sekarang", "cek stasiun", "cek anomali", "cek kualitas",
    "deteksi anomali", "scan stasiun", "periksa kualitas", "lihat data",
    "run analysis", "jalankan analisis", "apakah ada anomali",
    "status kualitas", "kualitas air", "kondisi sungai", "mulai",
]

GREETING_TRIGGERS = [
    "hi", "hello", "halo", "hey", "test", "tes", "hei",
    "siapa kamu", "kamu siapa", "apa itu wqsa", "what is wqsa",
    "perkenalan", "introduce", "help", "bantuan", "menu",
    "what can you do", "apa yang bisa kamu lakukan",
    "/start",
]

INTRO_MESSAGE = (
    f"🌊 *WQSA — Water Quality Status Decision Support Agent*\n"
    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    f"🏞️ DAS: *{TARGET_DAS}* | 📍 *{TARGET_REGION}*\n"
    f"🗄️ Mode: MySQL-backed\n\n"
    f"Saya adalah agen AI yang memantau kualitas air sungai secara otonom "
    f"melalui 5-step reasoning chain:\n\n"
    f"1️⃣ *Scan & Deteksi Anomali* — query semua stasiun Onlimo\n"
    f"2️⃣ *Profil Pencemar* — analisis rasio COD/BOD\n"
    f"3️⃣ *Curah Hujan* — branching limpasan vs industri\n"
    f"4️⃣ *Korelasi Sparing* — identifikasi industri pelanggar\n"
    f"5️⃣ *IKA & Rekomendasi* — benchmark vs target RPJMN\n\n"
    f"📋 *Contoh input yang valid:*\n"
    f"• `mulai analisis` — analisis semua stasiun\n"
    f"• `cek stasiun KLHK2` — analisis 1 stasiun\n"
    f"• `apakah ada anomali di Majalaya?` — fokus lokasi\n"
    f"• `analisis semua stasiun sekarang` — full scan\n"
    f"• `kualitas air hari ini` — status terkini\n"
)

OUT_OF_SCOPE_MESSAGE = "😊 Kindly type a purposeful input."


def classify_intent(text: str) -> str:
    """Classify user input into GREET, ANALYSIS, or OUT_OF_SCOPE."""
    normalized = text.strip().lower()

    for phrase in GREETING_TRIGGERS:
        if normalized == phrase or normalized.startswith(phrase + " ") or normalized.startswith(phrase + ","):
            return "GREET"

    for phrase in ANALYSIS_TRIGGERS:
        if phrase in normalized:
            return "ANALYSIS"

    # Station-specific queries (e.g. "KLHK2", "stasiun majalaya")
    if any(kw in normalized for kw in ["klhk", "stasiun", "station"]):
        return "ANALYSIS"

    return "OUT_OF_SCOPE"


# ─────────────────────────────────────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────────────────────────────────────

async def check_auth(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.message.from_user.id
    now = time.time()

    if ALLOWED_USER_IDS[0] != 0 and user_id not in ALLOWED_USER_IDS:
        await update.message.reply_text("⛔ Unauthorized.")
        return False

    last_active = context.user_data.get("last_active", 0)
    if now - last_active > SESSION_TIMEOUT:
        context.user_data["authenticated"] = False

    if not context.user_data.get("authenticated"):
        if MASTER_PASSWORD and update.message.text == MASTER_PASSWORD:
            context.user_data["authenticated"] = True
            context.user_data["last_active"] = now
            await update.message.reply_text("✅ Access granted!")
            return False
        elif MASTER_PASSWORD:
            await update.message.reply_text("🔒 Password required.")
            return False
        else:
            context.user_data["authenticated"] = True

    context.user_data["last_active"] = now
    return True


# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM HANDLERS
# ─────────────────────────────────────────────────────────────────────────────

async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    if ALLOWED_USER_IDS[0] != 0 and user_id not in ALLOWED_USER_IDS:
        await update.message.reply_text("⛔ Unauthorized.")
        return
    if MASTER_PASSWORD and not context.user_data.get("authenticated"):
        await update.message.reply_text("🔒 Password required.")
        return
    await update.message.reply_text(INTRO_MESSAGE, parse_mode="Markdown")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await check_auth(update, context):
        return

    user_id = update.message.from_user.id
    now = time.time()
    if now - user_last_message.get(user_id, 0) < RATE_LIMIT_SECONDS:
        await update.message.reply_text("⏳ Tunggu sebentar.")
        return
    user_last_message[user_id] = now

    user_text = update.message.text

    # ── Intent gate ──────────────────────────────────────────────────────────
    intent = classify_intent(user_text)

    if intent == "GREET":
        await update.message.reply_text(INTRO_MESSAGE, parse_mode="Markdown")
        return

    if intent == "OUT_OF_SCOPE":
        await update.message.reply_text(OUT_OF_SCOPE_MESSAGE)
        return

    # ── ANALYSIS — run the enforced chain ────────────────────────────────────
    await update.message.chat.send_action("typing")

    try:
        session_id = f"{user_id}-{int(now)}"
        reply = run_chain(user_text, user_id=user_id, session_id=session_id)

        # Split into chunks for Telegram's 4096 char limit
        if len(reply) > 4000:
            for i in range(0, len(reply), 4000):
                chunk = reply[i:i+4000]
                try:
                    await update.message.reply_text(chunk, parse_mode="Markdown")
                except Exception:
                    # Fallback if markdown parsing fails on chunk boundary
                    await update.message.reply_text(chunk)
        else:
            try:
                await update.message.reply_text(reply, parse_mode="Markdown")
            except Exception:
                await update.message.reply_text(reply)

        logger.info(f"[User {user_id}] {user_text[:50]}...")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        await update.message.reply_text("❌ Terjadi kesalahan. Coba lagi.")


# ─────────────────────────────────────────────────────────────────────────────
# STARTUP CHECK
# ─────────────────────────────────────────────────────────────────────────────

def check_mysql():
    try:
        import pymysql
        conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", ""),
            database=MYSQL_DATABASE,
            connect_timeout=5,
        )
        with conn.cursor() as cur:
            cur.execute("SHOW TABLES")
            tables = {row[0] for row in cur.fetchall()}
        conn.close()

        required = [
            "onlimo_stasiun", "onlimo_pembacaan", "onlimo_status",
            "bmkg_lokasi", "bmkg_prakiraan", "bmkg_summary_harian",
            "sparing_industri", "sparing_logger", "sparing_monitoring",
            "sitala_ika", "anomaly_log",
            "v_onlimo_terbaru", "v_bmkg_terbaru", "v_sitala_terbaru",
        ]
        all_ok = True
        for t in required:
            tag = "(view)" if t.startswith("v_") else ""
            if t in tables:
                print(f"  ✅ {t} {tag}")
            else:
                print(f"  ❌ {t} {tag} — MISSING!")
                all_ok = False
        return all_ok

    except Exception as e:
        print(f"  ❌ MySQL connection failed: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("🌊 WQSA — Water Quality Decision Support Agent")
    print(f"🏞️  DAS: {TARGET_DAS} | Region: {TARGET_REGION}")
    print(f"🗄️  Database: {MYSQL_DATABASE}@{MYSQL_HOST}:{MYSQL_PORT}")
    print(f"🔗 Mode: Enforced 5-Step Chain (chain.py)")
    print("━" * 50)

    db_ok = check_mysql()
    if not db_ok:
        print("\n⚠️  Some tables/views are missing — run database_schema.sql first.")
        print("   Then run etl.py to populate data.\n")

    print("━" * 50)
    print("🤖 Starting Telegram bot...")
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", handle_start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    print("✅ Bot is running!")
    app.run_polling()
