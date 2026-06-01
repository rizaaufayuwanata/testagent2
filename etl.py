# =============================================================================
# etl.py — WQSA ETL Runner: API → MySQL
# =============================================================================
# Tarik data dari semua API eksternal (Onlimo, BMKG, Sparing, SITALA)
# dan simpan ke database MySQL wqsa_db.
#
# Cara pakai:
#   python etl.py              — sync semua API
#   python etl.py --onlimo     — sync Onlimo saja
#   python etl.py --sparing    — sync Sparing saja
#   python etl.py --bmkg       — sync BMKG saja
#   python etl.py --sitala     — sync SITALA saja
#   python etl.py --days 7     — sync data 7 hari ke belakang (default: 3)
# =============================================================================

import argparse
import logging
import math
import sys
from datetime import datetime, date, timedelta
from typing import Any, Optional

import pymysql
import pymysql.cursors
import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from urllib.parse import urlsplit, urlunsplit

from config import (
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE,
    ONLIMO_STASIUN_URL, ONLIMO_MONITORING_URL, ONLIMO_STATUS_URL,
    ONLIMO_API_KEY, ONLIMO_SECRET, ONLIMO_CLIENT_KEY,
    SPARING_LOGGER_URL, SPARING_MONITORING_URL, SPARING_API_KEY,
    SITALA_URL, SITALA_API_KEY,
    BMKG_API_URL, BMKG_ADM4_CODES,
    TARGET_DAS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("wqsa.etl")

ETL_PAGE_SIZE = 50
SPARING_MON_PAGE_SIZE = 1000


# =============================================================================
# DATABASE HELPER
# =============================================================================

def get_db() -> pymysql.Connection:
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def _sync_start(db: pymysql.Connection, api_name: str, endpoint: str) -> int:
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO api_sync_log (api_name, endpoint, sync_start, status) VALUES (%s, %s, %s, 'running')",
            (api_name, endpoint, datetime.now()),
        )
    db.commit()
    with db.cursor() as cur:
        cur.execute("SELECT LAST_INSERT_ID() AS id")
        return cur.fetchone()["id"]


def _sync_end(db: pymysql.Connection, log_id: int, status: str, records: int, error: str = None):
    with db.cursor() as cur:
        cur.execute(
            "UPDATE api_sync_log SET sync_end=%s, status=%s, records_synced=%s, error_message=%s WHERE id=%s",
            (datetime.now(), status, records, error, log_id),
        )
    db.commit()


# =============================================================================
# HTTP CLIENT WITH RETRY
# =============================================================================

def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.timeout = 30
    session.verify = False  # beberapa server pemerintah pakai sertifikat self-signed
    return session


def _paginate(session: requests.Session, url: str, headers: dict, params: dict,
              item_key: str = "item", data_key: str = "data") -> list[dict]:
    """Fetch all pages from a paginated API. Returns flat list of items."""
    all_items = []
    params = {**params, "per_page": ETL_PAGE_SIZE, "page": 1}
    while True:
        resp = session.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        data = body.get(data_key, body)
        items = data.get(item_key, []) if isinstance(data, dict) else []
        if not items:
            break
        all_items.extend(items)
        last_page = data.get("last_page", 1)
        if params["page"] >= last_page:
            break
        params["page"] += 1
        logger.debug(f"  page {params['page']}/{last_page} — {len(all_items)} items so far")
    return all_items


def _safe(val: Any, default=None):
    """Return None for empty/null-like values."""
    if val is None or val == "" or val == "null":
        return default
    return val


def _to_float(val: Any) -> Optional[float]:
    try:
        v = float(val)
        return None if math.isnan(v) else v
    except (TypeError, ValueError):
        return None


def _to_int(val: Any) -> Optional[int]:
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None


def _parse_coord(val: Any, is_lat: bool) -> Optional[float]:
    """Parse a dirty coordinate value. Returns None if clearly invalid."""
    if val is None:
        return None
    s = str(val).strip()
    # Strip common non-numeric suffixes/prefixes
    for ch in ["°", "deg", " "]:
        s = s.replace(ch, "")
    # European decimal comma
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        f = float(s)
    except ValueError:
        return None
    # Validate range
    if is_lat and not (-90 <= f <= 90):
        return None
    if not is_lat and not (-180 <= f <= 180):
        return None
    return f


def _base_url(url: str) -> str:
    """Strip query string from a URL so ETL can supply its own params."""
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def _onlimo_headers() -> dict:
    """Build Onlimo auth headers from all available credentials."""
    h = {}
    if ONLIMO_API_KEY:
        h["Authorization"] = f"Bearer {ONLIMO_API_KEY}"
    if ONLIMO_SECRET:
        h["X-Secret"] = ONLIMO_SECRET
    if ONLIMO_CLIENT_KEY:
        h["Client-Key"] = ONLIMO_CLIENT_KEY
    return h


def _sparing_headers() -> dict:
    """Build Sparing IBEX auth headers (member/key/secret, not Bearer)."""
    return {
        "member": "ibex",
        "key": SPARING_API_KEY,
        "secret": ONLIMO_SECRET,
    }


# =============================================================================
# 1. ONLIMO — STASIUN
# =============================================================================

def sync_onlimo_stasiun(db: pymysql.Connection, session: requests.Session) -> int:
    if not ONLIMO_STASIUN_URL:
        logger.warning("ONLIMO_STASIUN_URL not set — skipping stasiun sync")
        return 0

    endpoint = _base_url(ONLIMO_STASIUN_URL)
    log_id = _sync_start(db, "onlimo_stasiun", endpoint)
    records = 0

    try:
        headers = _onlimo_headers()
        items = _paginate(session, endpoint, headers, {})

        sql = """
            INSERT INTO onlimo_stasiun
                (station_id, station_name, nama_sungai, nama_das, provinsi, kabkot, latitude, longitude, synced_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                station_name    = VALUES(station_name),
                nama_sungai     = VALUES(nama_sungai),
                nama_das        = VALUES(nama_das),
                provinsi        = VALUES(provinsi),
                kabkot          = VALUES(kabkot),
                latitude        = VALUES(latitude),
                longitude       = VALUES(longitude),
                synced_at       = NOW()
        """
        with db.cursor() as cur:
            for item in items:
                cur.execute(sql, (
                    item["IDStasiun"],
                    item.get("NamaStasiun"),
                    _safe(item.get("nama_sungai")),
                    _safe(item.get("nama_das")),
                    _safe(item.get("provinsi")),
                    _safe(item.get("kabkota")),
                    _to_float(item.get("latitude")),
                    _to_float(item.get("longitude")),
                ))
                records += 1
        db.commit()
        logger.info(f"onlimo_stasiun: {records} upserted")
        _sync_end(db, log_id, "success", records)
    except Exception as e:
        db.rollback()
        logger.error(f"onlimo_stasiun sync failed: {e}")
        _sync_end(db, log_id, "failed", records, str(e))

    return records


# =============================================================================
# 2. ONLIMO — MONITORING (pembacaan per jam per stasiun)
# =============================================================================

def sync_onlimo_monitoring(db: pymysql.Connection, session: requests.Session,
                           days_back: int = 3,
                           station_ids: list = None) -> int:
    """
    Sync Onlimo monitoring data.
    station_ids: optional list of station IDs to sync. If None, syncs all KLHK-format stations.
    """
    if not ONLIMO_MONITORING_URL:
        logger.warning("ONLIMO_MONITORING_URL not set — skipping monitoring sync")
        return 0

    if station_ids is not None:
        # Gunakan station_ids yang diberikan langsung
        ids_to_sync = [s for s in station_ids if s and str(s).strip()]
    else:
        # Default: hanya stasiun KLHK-format (menghindari 400 Bad Request)
        with db.cursor() as cur:
            cur.execute("""
                SELECT station_id FROM onlimo_stasiun
                WHERE status_aktif = 1
                  AND station_id REGEXP '^KLHK[0-9]+'
                ORDER BY station_id
            """)
            ids_to_sync = [r["station_id"] for r in cur.fetchall()]

        if not ids_to_sync:
            # Fallback: semua stasiun aktif
            with db.cursor() as cur:
                cur.execute("SELECT station_id FROM onlimo_stasiun WHERE status_aktif = 1 ORDER BY station_id")
                ids_to_sync = [r["station_id"] for r in cur.fetchall()]

    if not ids_to_sync:
        logger.warning("No stations to sync — run sync_onlimo_stasiun first")
        return 0

    station_ids = ids_to_sync
    logger.info(f"onlimo_monitoring: akan sync {len(station_ids)} stasiun")
    total_records = 0
    headers = _onlimo_headers()

    sql = """
        INSERT INTO onlimo_pembacaan (
            data_uid, station_id, tanggal_ukur, crdate, deleted,
            suhu, kedalaman, turbidity,
            ph, do_val, orp,
            dhl, tds, salinitas, swsg,
            nitrat, nitrit, amonia,
            cod, bod, tss,
            param2, param3, ews_per, ip_addr, synced_at
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, NOW()
        )
        ON DUPLICATE KEY UPDATE
            deleted     = VALUES(deleted),
            suhu        = VALUES(suhu),
            turbidity   = VALUES(turbidity),
            ph          = VALUES(ph),
            do_val      = VALUES(do_val),
            orp         = VALUES(orp),
            dhl         = VALUES(dhl),
            tds         = VALUES(tds),
            salinitas   = VALUES(salinitas),
            nitrat      = VALUES(nitrat),
            nitrit      = VALUES(nitrit),
            amonia      = VALUES(amonia),
            cod         = VALUES(cod),
            bod         = VALUES(bod),
            tss         = VALUES(tss),
            ews_per     = VALUES(ews_per),
            ip_addr     = VALUES(ip_addr),
            synced_at   = NOW()
    """

    endpoint = _base_url(ONLIMO_MONITORING_URL)
    log_id = _sync_start(db, "onlimo_monitoring", endpoint)
    date_end = date.today().isoformat()
    date_start = (date.today() - timedelta(days=days_back)).isoformat()

    try:
        for station_id in station_ids:
            params = {
                "station_id": station_id,
                "date_start": date_start,
                "date_end": date_end,
            }
            items = _paginate(session, endpoint, headers, params)

            with db.cursor() as cur:
                for item in items:
                    tanggal = item.get("Tanggal", "")
                    jam = item.get("Jam", "00:00:00")
                    tanggal_ukur = f"{tanggal} {jam}" if tanggal else None

                    crdate = _safe(item.get("crdate"))

                    cur.execute(sql, (
                        _to_int(item.get("data_uid")),
                        item.get("IDStasiun"),
                        tanggal_ukur,
                        crdate,
                        _to_int(item.get("deleted", 0)),
                        _to_float(item.get("Suhu")),
                        _to_float(item.get("Kedalaman")),
                        _to_float(item.get("Turbidity")),
                        _to_float(item.get("PH")),
                        _to_float(item.get("DO")),
                        _to_float(item.get("ORP")),
                        _to_float(item.get("DHL")),
                        _to_float(item.get("TDS")),
                        _to_float(item.get("Salinitas")),
                        _to_float(item.get("SwSG")),
                        _to_float(item.get("Nitrat")),
                        _to_float(item.get("Nitrit")),
                        _to_float(item.get("Amonia")),
                        _to_float(item.get("COD")),
                        _to_float(item.get("BOD")),
                        _to_float(item.get("TSS")),
                        _to_float(item.get("Param2")),
                        _to_float(item.get("Param3")),
                        _to_float(item.get("EWS_PER")),
                        _safe(item.get("ip_addr")),
                    ))
                    total_records += 1
            db.commit()
            logger.info(f"  {station_id}: {len(items)} rows")

        logger.info(f"onlimo_monitoring: {total_records} total upserted")
        _sync_end(db, log_id, "success", total_records)
    except Exception as e:
        db.rollback()
        logger.error(f"onlimo_monitoring sync failed: {e}")
        _sync_end(db, log_id, "failed", total_records, str(e))

    return total_records


# =============================================================================
# 3. ONLIMO — STATUS (indeks mutu harian tervalidasi)
# =============================================================================

def sync_onlimo_status(db: pymysql.Connection, session: requests.Session,
                       station_ids: list = None, days_back: int = 3) -> int:
    """
    Sync status indeks mutu harian per stasiun per tanggal.
    API hanya menerima satu tanggal per request, sehingga perlu loop
    per tanggal × per stasiun untuk mendapatkan data historis.
    """
    if not ONLIMO_STATUS_URL:
        logger.warning("ONLIMO_STATUS_URL not set — skipping status sync")
        return 0

    if station_ids is not None:
        station_ids = [s for s in station_ids if s and str(s).strip()]
    else:
        with db.cursor() as cur:
            cur.execute("""
                SELECT station_id FROM onlimo_stasiun
                WHERE status_aktif = 1
                  AND station_id REGEXP '^KLHK[0-9]+'
                ORDER BY station_id
            """)
            station_ids = [r["station_id"] for r in cur.fetchall()]
        if not station_ids:
            with db.cursor() as cur:
                cur.execute("SELECT station_id FROM onlimo_stasiun WHERE status_aktif = 1 ORDER BY station_id")
                station_ids = [r["station_id"] for r in cur.fetchall()]

    if not station_ids:
        return 0

    # Buat daftar tanggal: dari (hari ini - days_back) sampai hari ini
    today = date.today()
    date_list = [
        (today - timedelta(days=i)).isoformat()
        for i in range(days_back, -1, -1)   # urutan dari lama ke baru
    ]

    endpoint = _base_url(ONLIMO_STATUS_URL)
    log_id   = _sync_start(db, "onlimo_status", endpoint)
    headers  = _onlimo_headers()
    records  = 0

    logger.info(f"onlimo_status: {len(station_ids)} stasiun × {len(date_list)} hari "
                f"({date_list[0]} ~ {date_list[-1]})")

    sql = """
        INSERT INTO onlimo_status (
            station_id, tanggal_validasi, tanggal_data,
            indeks, status_nama, status_warna,
            parameter_kritis, max_parameter, max_nilai, keterangan, synced_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            tanggal_data        = VALUES(tanggal_data),
            indeks              = VALUES(indeks),
            status_nama         = VALUES(status_nama),
            status_warna        = VALUES(status_warna),
            parameter_kritis    = VALUES(parameter_kritis),
            max_parameter       = VALUES(max_parameter),
            max_nilai           = VALUES(max_nilai),
            keterangan          = VALUES(keterangan),
            synced_at           = NOW()
    """

    try:
        for station_id in station_ids:
            station_records = 0
            for date_str in date_list:
                try:
                    resp = session.get(
                        endpoint, headers=headers,
                        params={"station_id": station_id, "date": date_str},
                        timeout=30,
                    )
                    resp.raise_for_status()
                    body = resp.json()

                    # API mengembalikan objek tunggal di bawah "data"
                    data = body.get("data", {})
                    if not data or not data.get("tanggal_validasi"):
                        continue

                    with db.cursor() as cur:
                        cur.execute(sql, (
                            station_id,
                            data.get("tanggal_validasi"),
                            data.get("tanggal_data"),
                            _to_float(data.get("indeks")),
                            _safe(data.get("status_nama")),
                            _safe(data.get("status_warna")),
                            _safe(data.get("kritis")),
                            _safe(data.get("max_parameter")),
                            _to_float(data.get("max_nilai")),
                            _safe(data.get("keterangan")),
                        ))
                    db.commit()
                    records += 1
                    station_records += 1

                except Exception as date_err:
                    # Skip tanggal yang gagal, lanjut ke tanggal berikutnya
                    logger.debug(f"  {station_id} {date_str}: {date_err}")
                    continue

            if station_records > 0:
                logger.info(f"  {station_id}: {station_records} records")

        logger.info(f"onlimo_status: {records} total upserted")
        _sync_end(db, log_id, "success", records)
    except Exception as e:
        db.rollback()
        logger.error(f"onlimo_status sync failed: {e}")
        _sync_end(db, log_id, "failed", records, str(e))

    return records


# =============================================================================
# 4. BMKG — PRAKIRAAN CUACA (per adm4 code)
# =============================================================================

def _compute_bmkg_summary(db: pymysql.Connection, lokasi_id: int, tanggal: date):
    """Hitung ringkasan curah hujan harian dari bmkg_prakiraan dan upsert ke bmkg_summary_harian."""
    with db.cursor() as cur:
        cur.execute(
            """
            SELECT
                SUM(tp)   AS total_tp,
                MAX(tp)   AS max_tp,
                COUNT(*)  AS jumlah
            FROM bmkg_prakiraan
            WHERE lokasi_id = %s AND tanggal_lokal = %s
            """,
            (lokasi_id, tanggal),
        )
        row = cur.fetchone()

    if not row or row["jumlah"] == 0:
        return

    with db.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bmkg_summary_harian (lokasi_id, tanggal, total_tp_mm, max_tp_mm, jumlah_periode, synced_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                total_tp_mm     = VALUES(total_tp_mm),
                max_tp_mm       = VALUES(max_tp_mm),
                jumlah_periode  = VALUES(jumlah_periode),
                updated_at      = NOW()
            """,
            (lokasi_id, tanggal, row["total_tp"] or 0, row["max_tp"] or 0, row["jumlah"]),
        )


def sync_bmkg(db: pymysql.Connection, session: requests.Session) -> int:
    if not BMKG_ADM4_CODES:
        logger.warning("BMKG_ADM4_CODES not set in wqsa.env — skipping BMKG sync")
        return 0

    log_id = _sync_start(db, "bmkg", BMKG_API_URL)
    total_records = 0

    sql_prakiraan = """
        INSERT INTO bmkg_prakiraan (
            lokasi_id, datetime_utc, datetime_lokal, tanggal_lokal,
            tp, weather, weather_desc, weather_desc_en,
            t, tcc, hu, wd_deg, wd, wd_to, ws,
            vs, vs_text, analysis_date, time_index, image_url, synced_at
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, NOW()
        )
        ON DUPLICATE KEY UPDATE
            tp              = VALUES(tp),
            weather         = VALUES(weather),
            weather_desc    = VALUES(weather_desc),
            t               = VALUES(t),
            tcc             = VALUES(tcc),
            hu              = VALUES(hu),
            wd_deg          = VALUES(wd_deg),
            ws              = VALUES(ws),
            vs              = VALUES(vs),
            synced_at       = NOW()
    """

    try:
        for adm4 in BMKG_ADM4_CODES:
            resp = session.get(BMKG_API_URL, params={"adm4": adm4}, timeout=30)
            resp.raise_for_status()
            body = resp.json()

            # Upsert lokasi
            lokasi_raw = body.get("lokasi", {})
            if not lokasi_raw:
                continue

            with db.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO bmkg_lokasi
                        (adm1, adm2, adm3, adm4, provinsi, kotkab, kecamatan, desa, latitude, longitude, timezone, synced_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE
                        provinsi    = VALUES(provinsi),
                        kotkab      = VALUES(kotkab),
                        kecamatan   = VALUES(kecamatan),
                        desa        = VALUES(desa),
                        latitude    = VALUES(latitude),
                        longitude   = VALUES(longitude),
                        timezone    = VALUES(timezone),
                        updated_at  = NOW()
                    """,
                    (
                        lokasi_raw.get("adm1"), lokasi_raw.get("adm2"),
                        lokasi_raw.get("adm3"), lokasi_raw.get("adm4"),
                        lokasi_raw.get("provinsi"), lokasi_raw.get("kotkab"),
                        lokasi_raw.get("kecamatan"), lokasi_raw.get("desa"),
                        _to_float(lokasi_raw.get("lat")), _to_float(lokasi_raw.get("lon")),
                        lokasi_raw.get("timezone", "Asia/Jakarta"),
                    ),
                )
                cur.execute("SELECT id FROM bmkg_lokasi WHERE adm4 = %s", (adm4,))
                row = cur.fetchone()
                if not row:
                    continue
                lokasi_id = row["id"]

            db.commit()

            # Insert prakiraan — data adalah array of arrays (per hari, per periode)
            tanggal_set = set()
            for day_forecasts in body.get("data", []):
                cuaca_list = day_forecasts.get("cuaca", [])
                if not cuaca_list:
                    continue
                # Flatten nested arrays
                for period_list in cuaca_list:
                    if not isinstance(period_list, list):
                        period_list = [period_list]
                    for item in period_list:
                        utc_str = item.get("utc_datetime")
                        local_str = item.get("local_datetime")
                        if not utc_str:
                            continue

                        local_dt = datetime.strptime(local_str, "%Y-%m-%d %H:%M:%S") if local_str else None
                        tanggal_lokal = local_dt.date() if local_dt else None
                        if tanggal_lokal:
                            tanggal_set.add(tanggal_lokal)

                        analysis_raw = item.get("analysis_date")
                        try:
                            analysis_dt = datetime.fromisoformat(analysis_raw) if analysis_raw else None
                        except ValueError:
                            analysis_dt = None

                        with db.cursor() as cur:
                            cur.execute(sql_prakiraan, (
                                lokasi_id,
                                utc_str, local_str, tanggal_lokal,
                                _to_float(item.get("tp")) or 0,
                                _to_int(item.get("weather")),
                                _safe(item.get("weather_desc")),
                                _safe(item.get("weather_desc_en")),
                                _to_int(item.get("t")),
                                _to_int(item.get("tcc")),
                                _to_int(item.get("hu")),
                                _to_int(item.get("wd_deg")),
                                _safe(item.get("wd")),
                                _safe(item.get("wd_to")),
                                _to_float(item.get("ws")),
                                _to_int(item.get("vs")),
                                _safe(item.get("vs_text")),
                                analysis_dt,
                                _safe(item.get("time_index")),
                                _safe(item.get("image")),
                            ))
                            total_records += 1

            db.commit()

            # Hitung summary harian untuk setiap tanggal yang ada
            with db.cursor() as cur:
                for tgl in tanggal_set:
                    _compute_bmkg_summary(db, lokasi_id, tgl)
            db.commit()
            logger.info(f"  adm4={adm4}: {total_records} prakiraan, {len(tanggal_set)} hari")

        logger.info(f"bmkg: {total_records} total upserted")
        _sync_end(db, log_id, "success", total_records)
    except Exception as e:
        db.rollback()
        logger.error(f"bmkg sync failed: {e}")
        _sync_end(db, log_id, "failed", total_records, str(e))

    return total_records


# =============================================================================
# 5. SPARING — LOGGER (industri + logger + parameter_logger)
# =============================================================================

def sync_sparing_logger(db: pymysql.Connection, session: requests.Session) -> int:
    if not SPARING_LOGGER_URL:
        logger.warning("SPARING_LOGGER_URL not set — skipping Sparing Logger sync")
        return 0

    endpoint = _base_url(SPARING_LOGGER_URL)
    log_id = _sync_start(db, "sparing_logger", endpoint)
    headers = _sparing_headers()
    records = 0

    sql_industri = """
        INSERT INTO sparing_industri (id, name, type, address, phone, email, id_simpel, id_ppa, synced_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            name        = VALUES(name),
            type        = VALUES(type),
            address     = VALUES(address),
            phone       = VALUES(phone),
            email       = VALUES(email),
            id_simpel   = VALUES(id_simpel),
            id_ppa      = VALUES(id_ppa),
            updated_at  = NOW()
    """
    sql_logger = """
        INSERT INTO sparing_logger (
            id, id_logger, id_industries, name, brand, type, model,
            serial_number, mac_address, waste_water_source,
            latitude, longitude, status, logger_existing, synced_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            id_logger           = VALUES(id_logger),
            name                = VALUES(name),
            status              = VALUES(status),
            logger_existing     = VALUES(logger_existing),
            latitude            = VALUES(latitude),
            longitude           = VALUES(longitude),
            updated_at          = NOW()
    """
    sql_param = """
        INSERT INTO sparing_parameter_logger (
            id, id_logger, id_logger_int, id_parameter,
            brand, type, category, schedule, range_sensor, brosur_url,
            bm, bm_max, parameter_name, parameter_unit, synced_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            bm              = VALUES(bm),
            bm_max          = VALUES(bm_max),
            schedule        = VALUES(schedule),
            updated_at      = NOW()
    """

    try:
        items = _paginate(session, endpoint, headers, {})
        with db.cursor() as cur:
            for item in items:
                ind = item.get("industri", {})
                if ind:
                    cur.execute(sql_industri, (
                        ind["id"], ind.get("name"), _safe(ind.get("type")),
                        _safe(ind.get("address")), _safe(ind.get("phone")),
                        _safe(ind.get("email")), _to_int(ind.get("id_simpel")),
                        _to_int(ind.get("id_ppa")),
                    ))

                coord = item.get("coordinate") or []
                lat = _parse_coord(coord[0], is_lat=True)  if len(coord) > 0 else None
                lon = _parse_coord(coord[1], is_lat=False) if len(coord) > 1 else None

                cur.execute(sql_logger, (
                    item["id"], item.get("id_logger"),
                    item.get("id_industries"),
                    item.get("name"), _safe(item.get("brand")),
                    _safe(item.get("type")), _safe(item.get("model")),
                    _safe(item.get("serial_number")), _safe(item.get("mac_address")),
                    _safe(item.get("waste_water_source")),
                    lat, lon,
                    _safe(item.get("status")),
                    1 if item.get("logger_existing") else 0,
                ))

                for param in item.get("parameters", []):
                    p = param.get("parameter", {})
                    cur.execute(sql_param, (
                        param["id"], param.get("id_logger"), item["id"],
                        param.get("id_parameter"),
                        _safe(param.get("brand")), _safe(param.get("type")),
                        _safe(param.get("category")), _to_int(param.get("schedule")),
                        _safe(param.get("range_sensor")), _safe(param.get("brosur_url")),
                        _to_float(param.get("bm")), _to_float(param.get("bm_max")),
                        p.get("name"), p.get("unit"),
                    ))
                records += 1

        db.commit()
        logger.info(f"sparing_logger: {records} loggers upserted")
        _sync_end(db, log_id, "success", records)
    except Exception as e:
        db.rollback()
        logger.error(f"sparing_logger sync failed: {e}")
        _sync_end(db, log_id, "failed", records, str(e))

    return records


# =============================================================================
# 6. SPARING — MONITORING (data harian agregasi)
# =============================================================================

def _calc_status_taat(value: Optional[float], param_name: str,
                      bm_min_use: Optional[float], bm_max_use: Optional[float],
                      counter: Optional[int], min_valid: Optional[int]) -> str:
    if counter is not None and min_valid is not None and counter < min_valid:
        return "TIDAK_VALID"
    if value is None or bm_max_use is None:
        return "TIDAK_VALID"
    if param_name and param_name.lower() == "ph":
        lo = bm_min_use if bm_min_use is not None else 0
        return "TAAT" if lo <= value <= bm_max_use else "LANGGAR"
    return "TAAT" if value <= bm_max_use else "LANGGAR"


def sync_sparing_monitoring(db: pymysql.Connection, session: requests.Session,
                            days_back: int = 3) -> int:
    if not SPARING_MONITORING_URL:
        logger.warning("SPARING_MONITORING_URL not set — skipping Sparing Monitoring sync")
        return 0

    # Ambil semua id_logger dari DB (isian sync_sparing_logger)
    with db.cursor() as cur:
        cur.execute("SELECT id_logger FROM sparing_logger WHERE id_logger IS NOT NULL")
        id_loggers = [r["id_logger"] for r in cur.fetchall()]

    if not id_loggers:
        logger.warning("No loggers in DB — run sync_sparing_logger first")
        return 0

    endpoint = _base_url(SPARING_MONITORING_URL)
    log_id = _sync_start(db, "sparing_monitoring", endpoint)
    headers = _sparing_headers()
    records = 0

    date_start = (date.today() - timedelta(days=days_back)).isoformat()
    date_end = date.today().isoformat()

    sql = """
        INSERT INTO sparing_monitoring (
            id, id_logger, id_logger_int, id_parameter,
            reported_at, counter, total, average, value, unit, min_valid,
            bm, bm_max, bm_min_use, bm_max_use,
            parameter_name, status_taat, updated_at, synced_at
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, NOW()
        )
        ON DUPLICATE KEY UPDATE
            counter         = VALUES(counter),
            total           = VALUES(total),
            average         = VALUES(average),
            value           = VALUES(value),
            bm_min_use      = VALUES(bm_min_use),
            bm_max_use      = VALUES(bm_max_use),
            status_taat     = VALUES(status_taat),
            updated_at      = VALUES(updated_at),
            synced_at       = NOW()
    """

    try:
        for id_logger in id_loggers:
            page = 1
            logger_records = 0
            while True:
                params = {
                    "id_logger": id_logger,
                    "date_start": date_start,
                    "date_end": date_end,
                    "per_page": SPARING_MON_PAGE_SIZE,
                    "page": page,
                }
                resp = session.get(endpoint, headers=headers, params=params, timeout=30)
                resp.raise_for_status()
                body = resp.json()
                data = body.get("data", {})
                items = data.get("item", [])
                if not items:
                    break

                with db.cursor() as cur:
                    for item in items:
                        bm_info = item.get("parameter_bm", {}) or {}
                        param = item.get("parameter", {}) or {}
                        value = _to_float(item.get("value"))
                        bm_min_use = _to_float(bm_info.get("bm_min_use"))
                        bm_max_use = _to_float(bm_info.get("bm_max_use"))
                        counter = _to_int(item.get("counter"))
                        min_valid = _to_int(item.get("min_valid"))
                        param_name = param.get("name", "")

                        status = _calc_status_taat(value, param_name, bm_min_use, bm_max_use, counter, min_valid)

                        updated_raw = item.get("updated_at", "")
                        try:
                            updated_dt = datetime.fromisoformat(updated_raw.replace("Z", "+00:00")) if updated_raw else None
                        except ValueError:
                            updated_dt = None

                        cur.execute(sql, (
                            item["id"],
                            item.get("id_logger"),
                            _to_int(item.get("id_logger_")),
                            item.get("id_parameter"),
                            item.get("reported_at"),
                            counter,
                            _to_float(item.get("total")),
                            _to_float(item.get("average")),
                            value,
                            _safe(item.get("unit")),
                            min_valid,
                            _to_float(bm_info.get("bm")),
                            _to_float(bm_info.get("bm_max")),
                            bm_min_use,
                            bm_max_use,
                            param_name,
                            status,
                            updated_dt,
                        ))
                        logger_records += 1

                db.commit()

                last_page = data.get("last_page", 1)
                if page >= last_page:
                    break
                page += 1

            if logger_records:
                logger.info(f"  {id_logger}: {logger_records} rows")
            records += logger_records

        logger.info(f"sparing_monitoring: {records} upserted")
        _sync_end(db, log_id, "success", records)
    except Exception as e:
        db.rollback()
        logger.error(f"sparing_monitoring sync failed: {e}")
        _sync_end(db, log_id, "failed", records, str(e))

    return records


# =============================================================================
# 7. SITALA — IKA per kabupaten/kota
# =============================================================================

def sync_sitala(db: pymysql.Connection, session: requests.Session) -> int:
    if not SITALA_URL:
        logger.warning("SITALA_URL not set — skipping SITALA sync")
        return 0

    log_id = _sync_start(db, "sitala", SITALA_URL)
    headers = {"X-Api-Key": SITALA_API_KEY} if SITALA_API_KEY else {}
    records = 0

    sql = """
        INSERT INTO sitala_ika (
            uid_indeks_history, uid_provinsi, uid_kabkota, tahun,
            nama_provinsi, nama_kabkota, kd_regional,
            ika, iku, ikl, ikal, ikeg, iklh, jenis_indeks,
            target_iklh, target_ika, target_iku, target_ikl, target_ikal,
            ir_lb, ir_kb, ir_ih, ir_pl, ir_gl, ir_lh,
            rekomendasi_ika, rekomendasi_iku, rekomendasi_ikl, rekomendasi_ikal, rekomendasi_iklh,
            peta_iku, peta_ika, peta_ikl,
            gambut_provinsi, gambut_kabkota,
            deleted, hidden, crdate, chdate, synced_at
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s, NOW()
        )
        ON DUPLICATE KEY UPDATE
            ika             = VALUES(ika),
            iku             = VALUES(iku),
            ikl             = VALUES(ikl),
            ikal            = VALUES(ikal),
            iklh            = VALUES(iklh),
            target_ika      = VALUES(target_ika),
            target_iku      = VALUES(target_iku),
            target_ikl      = VALUES(target_ikl),
            target_iklh     = VALUES(target_iklh),
            ir_lb           = VALUES(ir_lb),
            ir_lh           = VALUES(ir_lh),
            rekomendasi_ika = VALUES(rekomendasi_ika),
            deleted         = VALUES(deleted),
            hidden          = VALUES(hidden),
            chdate          = VALUES(chdate),
            updated_at      = NOW()
    """

    try:
        # SITALA returns all records in one shot — pagination params break the response
        resp = session.get(SITALA_URL, headers=headers, timeout=60)
        resp.raise_for_status()
        body = resp.json()

        items = body.get("rows", {}).get("kabkota", [])
        logger.info(f"sitala: {len(items)} kabkota items received")

        with db.cursor() as cur:
            for item in items:
                crdate_ts = _to_int(item.get("crdate"))
                chdate_ts = _to_int(item.get("chdate"))
                crdate_dt = datetime.fromtimestamp(crdate_ts) if crdate_ts else None
                chdate_dt = datetime.fromtimestamp(chdate_ts) if chdate_ts else None

                cur.execute(sql, (
                    _to_int(item["uid_indeks_history"]),
                    _to_int(item.get("uid_provinsi")),
                    _to_int(item.get("uid_kabkota")),
                    _to_int(item.get("tahun")),
                    item.get("nama_provinsi"), item.get("nama_kabkota"),
                    _to_int(item.get("kd_regional")),
                    _to_float(item.get("ika")),
                    _to_float(item.get("iku")),
                    _to_float(item.get("ikl")),
                    _to_float(item.get("ikal")),
                    _to_float(item.get("ikeg")),
                    _to_float(item.get("iklh")),
                    _to_int(item.get("jenis_indeks", 0)),
                    _to_float(item.get("target")),
                    _to_float(item.get("target_ika")),
                    _to_float(item.get("target_iku")),
                    _to_float(item.get("target_ikl")),
                    _to_float(item.get("target_ikal")),
                    _to_float(item.get("ir_lb")),
                    _to_float(item.get("ir_kb")),
                    _to_float(item.get("ir_ih")),
                    _to_float(item.get("ir_pl")),
                    _to_float(item.get("ir_gl")),
                    _to_float(item.get("ir_lh")),
                    _safe(item.get("rekomendasi_ika")),
                    _safe(item.get("rekomendasi_iku")),
                    _safe(item.get("rekomendasi_ikl")),
                    _safe(item.get("rekomendasi_ikal")),
                    _safe(item.get("rekomendasi_iklh")),
                    _safe(item.get("peta_sebaran_iku")),
                    _safe(item.get("peta_sebaran_ika")),
                    _safe(item.get("peta_sebaran_ikl")),
                    _to_int(item.get("gambut_provinsi")),
                    _to_int(item.get("gambut_kabkota")),
                    _to_int(item.get("deleted", 0)),
                    _to_int(item.get("hidden", 0)),
                    crdate_dt, chdate_dt,
                ))
                records += 1

        db.commit()

        # Hitung trend_yoy (IKA tahun ini - IKA tahun lalu) per kabkota
        _compute_sitala_trend(db)

        logger.info(f"sitala: {records} upserted")
        _sync_end(db, log_id, "success", records)
    except Exception as e:
        db.rollback()
        logger.error(f"sitala sync failed: {e}")
        _sync_end(db, log_id, "failed", records, str(e))

    return records


def _compute_sitala_trend(db: pymysql.Connection):
    """Hitung trend_yoy = IKA tahun ini - IKA tahun lalu, per kabkota."""
    with db.cursor() as cur:
        cur.execute("""
            UPDATE sitala_ika s
            JOIN sitala_ika prev
              ON s.uid_kabkota = prev.uid_kabkota
             AND s.tahun = prev.tahun + 1
            SET s.trend_yoy = ROUND(s.ika - prev.ika, 2)
        """)
    db.commit()
    logger.info("sitala trend_yoy computed")


# =============================================================================
# ENTRY POINT
# =============================================================================

def run_all(days_back: int = 3):
    logger.info("=" * 60)
    logger.info(f"WQSA ETL — full sync (days_back={days_back})")
    logger.info("=" * 60)

    db = get_db()
    session = make_session()
    try:
        sync_onlimo_stasiun(db, session)
        sync_onlimo_monitoring(db, session, days_back)
        sync_onlimo_status(db, session, days_back=days_back)
        sync_bmkg(db, session)
        sync_sparing_logger(db, session)
        sync_sparing_monitoring(db, session, days_back)
        sync_sitala(db, session)
    finally:
        db.close()
        session.close()

    logger.info("ETL complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WQSA ETL Runner")
    parser.add_argument("--onlimo",  action="store_true", help="Sync Onlimo only")
    parser.add_argument("--bmkg",    action="store_true", help="Sync BMKG only")
    parser.add_argument("--sparing", action="store_true", help="Sync Sparing only")
    parser.add_argument("--sitala",  action="store_true", help="Sync SITALA only")
    parser.add_argument("--days",    type=int, default=3, help="Days back to sync (default: 3)")
    args = parser.parse_args()

    db = get_db()
    session = make_session()
    try:
        any_flag = args.onlimo or args.bmkg or args.sparing or args.sitala
        if not any_flag or args.onlimo:
            sync_onlimo_stasiun(db, session)
            sync_onlimo_monitoring(db, session, args.days)
            sync_onlimo_status(db, session, days_back=days_back)
        if not any_flag or args.bmkg:
            sync_bmkg(db, session)
        if not any_flag or args.sparing:
            sync_sparing_logger(db, session)
            sync_sparing_monitoring(db, session, args.days)
        if not any_flag or args.sitala:
            sync_sitala(db, session)
    finally:
        db.close()
        session.close()
