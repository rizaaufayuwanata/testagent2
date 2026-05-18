# =============================================================================
# data_layer.py — MySQL-backed Data Layer for WQSA Agent
# =============================================================================
# Menggantikan pembacaan file dummy dengan query ke MySQL (wqsa_db).
# Semua fungsi publik mempertahankan signature yang sama agar tools.py tidak
# perlu diubah.
#
# Fungsi publik (sama dengan versi lama):
#   get_onlimo_data(station_id, das)
#   get_rainfall_data(location, lat, lon)
#   get_sparing_logger_data(das, district)
#   get_sparing_monitoring_data(company_id, days)
#   get_sitala_data(district)
#   log_anomaly(data)
#   get_station_history(station_id, days)
#   safe_float(value, default)
#   cache  (MemoryCache instance)
# =============================================================================

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Optional

import pymysql
import pymysql.cursors

from config import (
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE,
    CACHE_TTL_HOURS, RAINFALL_HIGH_MM,
)

logger = logging.getLogger(__name__)

# =============================================================================
# DATABASE CONNECTION (reconnect-on-demand singleton)
# =============================================================================

_db_conn: Optional[pymysql.Connection] = None


def _get_db() -> pymysql.Connection:
    global _db_conn
    try:
        if _db_conn is None or not _db_conn.open:
            raise pymysql.err.InterfaceError
        _db_conn.ping(reconnect=True)
    except Exception:
        _db_conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )
    return _db_conn


def _query(sql: str, params: tuple = ()) -> list[dict]:
    db = _get_db()
    with db.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def _execute(sql: str, params: tuple = ()):
    db = _get_db()
    with db.cursor() as cur:
        cur.execute(sql, params)


# =============================================================================
# UTILITY FUNCTIONS (API publik — dipakai tools.py)
# =============================================================================

def safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "" or value == "-" or value == "N/A":
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    if value is None or value == "" or value == "-":
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


# =============================================================================
# IN-MEMORY CACHE (72-jam TTL, sama dengan versi lama)
# =============================================================================

class MemoryCache:
    def __init__(self, ttl_hours: int = CACHE_TTL_HOURS):
        self._store: dict[str, dict] = {}
        self._ttl = timedelta(hours=ttl_hours)

    def set(self, key: str, value: Any) -> None:
        self._store[key] = {"value": value, "timestamp": datetime.now()}

    def get(self, key: str) -> Optional[Any]:
        entry = self._store.get(key)
        if not entry:
            return None
        if datetime.now() - entry["timestamp"] > self._ttl:
            del self._store[key]
            return None
        return entry["value"]

    def clear(self) -> None:
        self._store.clear()


cache = MemoryCache()


# =============================================================================
# 1. ONLIMO — Data Stasiun + Pembacaan Terbaru
# =============================================================================

def get_onlimo_data(station_id: str = "", das: str = "") -> list[dict]:
    """
    Ambil data stasiun Onlimo dari view v_onlimo_terbaru.
    Return format kompatibel dengan tools.py (detect_anomaly, dll).
    """
    cache_key = f"onlimo:{station_id}:{das}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    conditions = ["1=1"]
    params = []

    if station_id:
        conditions.append("station_id = %s")
        params.append(station_id.upper())
    if das:
        conditions.append("nama_das = %s")
        params.append(das)

    sql = f"""
        SELECT *
        FROM v_onlimo_terbaru
        WHERE {' AND '.join(conditions)}
        ORDER BY station_id
    """

    try:
        rows = _query(sql, tuple(params))
        if not rows:
            return [{"info": "No stations found matching criteria"}]

        result = [_normalize_onlimo(r) for r in rows]
        cache.set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"get_onlimo_data failed: {e}")
        return [{"error": str(e)}]


def _normalize_onlimo(row: dict) -> dict:
    """Konversi baris v_onlimo_terbaru ke format yang diharapkan tools.py."""
    ts = row.get("tanggal_ukur")
    return {
        "station_id":   row.get("station_id"),
        "station_name": row.get("station_name"),
        "das":          row.get("nama_das"),
        "provinsi":     row.get("provinsi"),
        "kabkot":       row.get("kabkot"),
        "kecamatan":    row.get("kecamatan"),
        "latitude":     safe_float(row.get("latitude")),
        "longitude":    safe_float(row.get("longitude")),
        # Indeks & status dari onlimo_status (tervalidasi harian)
        "indeks_mutu":  safe_float(row.get("indeks_mutu")),
        "status":       row.get("status_mutu") or "TIDAK DIKETAHUI",
        "status_warna": row.get("status_warna"),
        "parameter_kritis": row.get("parameter_kritis"),
        # Pembacaan sensor terbaru
        "parameter": {
            "cod":      safe_float(row.get("cod")),
            "bod":      safe_float(row.get("bod")),
            "tss":      safe_float(row.get("tss")),
            "do":       safe_float(row.get("do_val")),
            "ph":       safe_float(row.get("ph")),
            "nitrat":   safe_float(row.get("nitrat")),
            "nitrit":   safe_float(row.get("nitrit")),
            "amonia":   safe_float(row.get("amonia")),
            "suhu":     safe_float(row.get("suhu")),
            "turbidity": safe_float(row.get("turbidity")),
            "dhl":      safe_float(row.get("dhl")),
            "ews_per":  safe_float(row.get("ews_per")),
        },
        "timestamp": ts.isoformat() if isinstance(ts, datetime) else str(ts or ""),
        "tanggal_validasi": str(row.get("tanggal_validasi") or ""),
    }


# =============================================================================
# 2. BMKG — Data Curah Hujan Terbaru
# =============================================================================

def get_rainfall_data(location: str = "", lat: float = 0.0, lon: float = 0.0) -> dict:
    """
    Cari data curah hujan BMKG terdekat dari v_bmkg_terbaru.
    Prioritas: 1) nama lokasi, 2) koordinat terdekat, 3) entri pertama.
    """
    cache_key = f"bmkg:{location}:{lat:.3f}:{lon:.3f}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        rows = _query("SELECT * FROM v_bmkg_terbaru", ())
        if not rows:
            return {"error": "No BMKG data in database"}

        chosen = None

        # 1) Cari berdasarkan nama lokasi
        if location:
            loc_lower = location.lower()
            for r in rows:
                if (loc_lower in (r.get("kecamatan") or "").lower() or
                        loc_lower in (r.get("desa") or "").lower() or
                        loc_lower in (r.get("kotkab") or "").lower()):
                    chosen = r
                    break

        # 2) Cari berdasarkan koordinat terdekat (Manhattan distance)
        if not chosen and lat and lon:
            chosen = min(rows, key=lambda r: (
                abs(safe_float(r.get("latitude")) - lat) +
                abs(safe_float(r.get("longitude")) - lon)
            ))

        # 3) Default ke baris pertama
        if not chosen:
            chosen = rows[0]

        total_mm = safe_float(chosen.get("total_rainfall_mm"))
        result = {
            "source":           "mysql_bmkg",
            "location":         chosen.get("desa") or chosen.get("kecamatan"),
            "kotkab":           chosen.get("kotkab"),
            "lat":              safe_float(chosen.get("latitude")),
            "lon":              safe_float(chosen.get("longitude")),
            "tanggal":          str(chosen.get("tanggal") or ""),
            "total_rainfall_mm": total_mm,
            "max_rainfall_mm":  safe_float(chosen.get("max_rainfall_mm")),
            "window_hours":     24,
            "is_high_rainfall": total_mm > RAINFALL_HIGH_MM,
        }
        cache.set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"get_rainfall_data failed: {e}")
        return {"error": str(e)}


# =============================================================================
# 3. SPARING — Logger / Outlet IPAL
# =============================================================================

def get_sparing_logger_data(das: str = "", district: str = "") -> list[dict]:
    """
    Ambil data logger Sparing dari sparing_logger JOIN sparing_industri.
    Filter berdasarkan wilayah (district = kota/kecamatan dalam address industri).
    """
    cache_key = f"sparing_logger:{das}:{district}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    conditions = ["l.status = 'VALID'", "l.logger_existing = 1"]
    params = []

    if district:
        conditions.append("(i.address LIKE %s OR i.name LIKE %s)")
        like = f"%{district}%"
        params.extend([like, like])

    sql = f"""
        SELECT
            l.id            AS logger_id,
            l.id_logger,
            l.name          AS outlet_name,
            l.latitude,
            l.longitude,
            l.waste_water_source,
            l.status        AS logger_status,
            i.id            AS company_id,
            i.name          AS company_name,
            i.type          AS industry_type,
            i.address,
            i.id_simpel,
            i.id_ppa
        FROM sparing_logger l
        JOIN sparing_industri i ON l.id_industries = i.id
        WHERE {' AND '.join(conditions)}
        ORDER BY i.name, l.id
    """

    try:
        rows = _query(sql, tuple(params))
        if not rows:
            return [{"info": "No Sparing Logger entries found"}]

        result = [_normalize_sparing_logger(r) for r in rows]
        cache.set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"get_sparing_logger_data failed: {e}")
        return [{"error": str(e)}]


def _normalize_sparing_logger(row: dict) -> dict:
    return {
        "company_id":       str(row.get("logger_id")),
        "id_logger":        row.get("id_logger"),
        "company_name":     row.get("company_name"),
        "outlet_name":      row.get("outlet_name"),
        "industry_type":    row.get("industry_type"),
        "address":          row.get("address"),
        "latitude":         safe_float(row.get("latitude")),
        "longitude":        safe_float(row.get("longitude")),
        "waste_water_source": row.get("waste_water_source"),
        "status_aktif":     row.get("logger_status") == "VALID",
        "id_simpel":        row.get("id_simpel"),
        "id_ppa":           row.get("id_ppa"),
    }


# =============================================================================
# 4. SPARING — Monitoring Harian
# =============================================================================

def get_sparing_monitoring_data(company_id: str = "", days: int = 3) -> list[dict]:
    """
    Ambil data monitoring Sparing.
    company_id bisa berupa: id_logger (ObjectID string) atau id integer logger.
    """
    cache_key = f"sparing_mon:{company_id}:{days}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    date_from = (datetime.now() - timedelta(days=days)).date().isoformat()
    conditions = ["m.reported_at >= %s"]
    params: list = [date_from]

    if company_id:
        conditions.append("(m.id_logger = %s OR m.id_logger_int = %s)")
        params.extend([company_id, safe_int(company_id)])

    sql = f"""
        SELECT
            m.id,
            m.id_logger,
            m.reported_at,
            m.parameter_name    AS parameter,
            m.value,
            m.average,
            m.total,
            m.counter,
            m.unit,
            m.bm_max_use        AS baku_mutu,
            m.bm_min_use        AS baku_mutu_min,
            m.status_taat,
            i.name              AS company_name,
            i.type              AS industry_type
        FROM sparing_monitoring m
        JOIN sparing_logger l  ON m.id_logger = l.id_logger
        JOIN sparing_industri i ON l.id_industries = i.id
        WHERE {' AND '.join(conditions)}
        ORDER BY m.reported_at DESC, i.name, m.parameter_name
    """

    try:
        rows = _query(sql, tuple(params))
        if not rows:
            return [{"info": f"No monitoring data found for {company_id or 'all'}"}]

        result = [_normalize_sparing_monitoring(r) for r in rows]
        cache.set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"get_sparing_monitoring_data failed: {e}")
        return [{"error": str(e)}]


def _normalize_sparing_monitoring(row: dict) -> dict:
    reported = row.get("reported_at")
    return {
        "company_id":   row.get("id_logger"),
        "company_name": row.get("company_name"),
        "industry_type": row.get("industry_type"),
        "date":         str(reported) if reported else "",
        "parameter":    row.get("parameter"),
        "value":        safe_float(row.get("value")),
        "average":      safe_float(row.get("average")),
        "total":        safe_float(row.get("total")),
        "counter":      safe_int(row.get("counter")),
        "baku_mutu":    safe_float(row.get("baku_mutu")),
        "baku_mutu_min": safe_float(row.get("baku_mutu_min")),
        "unit":         row.get("unit") or "mg/L",
        "status":       row.get("status_taat") or "TIDAK_VALID",
    }


# =============================================================================
# 5. SITALA — Indeks Kualitas Air per Kabupaten/Kota
# =============================================================================

def get_sitala_data(district: str = "") -> list[dict]:
    """
    Ambil data IKA dari v_sitala_terbaru (tahun terbaru per kabkota).
    Filter berdasarkan nama kabupaten/kota atau provinsi.
    """
    cache_key = f"sitala:{district}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    conditions = ["1=1"]
    params = []

    if district:
        conditions.append("(nama_kabkota LIKE %s OR nama_provinsi LIKE %s)")
        like = f"%{district}%"
        params.extend([like, like])

    sql = f"""
        SELECT *
        FROM v_sitala_terbaru
        WHERE {' AND '.join(conditions)}
        ORDER BY nama_kabkota
    """

    try:
        rows = _query(sql, tuple(params))
        if not rows:
            return [{"info": "No IKA data found for district"}]

        result = [_normalize_sitala(r) for r in rows]
        cache.set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"get_sitala_data failed: {e}")
        return [{"error": str(e)}]


def _normalize_sitala(row: dict) -> dict:
    return {
        "provinsi":     row.get("nama_provinsi"),
        "kabkot":       row.get("nama_kabkota"),
        "year":         safe_int(row.get("tahun")),
        # Nilai indeks (sesuai format lama agar tools.py tidak berubah)
        "ika_actual":   safe_float(row.get("ika")),
        "ika_target":   safe_float(row.get("target_ika")),
        "iku":          safe_float(row.get("iku")),
        "ikl":          safe_float(row.get("ikl")),
        "iklh":         safe_float(row.get("iklh")),
        "gap_ika":      safe_float(row.get("gap_ika")),
        "gap_iklh":     safe_float(row.get("gap_iklh")),
        "trend_yoy":    safe_float(row.get("trend_yoy")),
        # Indeks risiko
        "ir_lh":        safe_float(row.get("ir_lh")),
        "ir_lb":        safe_float(row.get("ir_lb")),
        "ir_kb":        safe_float(row.get("ir_kb")),
        # Rekomendasi dari KLHK (sering null)
        "note":         row.get("rekomendasi_ika"),
    }


# =============================================================================
# 6. ANOMALY LOG — Tulis & Baca dari MySQL
# =============================================================================

def log_anomaly(anomaly_data: dict) -> str:
    """Simpan hasil deteksi anomali ke tabel anomaly_log di MySQL."""
    try:
        sql = """
            INSERT INTO anomaly_log (
                station_id, tanggal_deteksi,
                indeks_mutu, status_mutu, is_anomaly,
                reasons, critical_parameter, critical_value,
                pollution_profile, cod_bod_ratio,
                rainfall_mm_24h, is_runoff,
                urgency_level, ika_gap, recommendation,
                telegram_user_id, session_id
            ) VALUES (
                %s, NOW(),
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s
            )
        """
        _execute(sql, (
            anomaly_data.get("station_id"),
            safe_float(anomaly_data.get("indeks_mutu")),
            anomaly_data.get("status", anomaly_data.get("status_mutu")),
            1 if anomaly_data.get("is_anomaly") else 0,
            json.dumps(anomaly_data.get("reasons", []), ensure_ascii=False),
            anomaly_data.get("critical_parameter"),
            safe_float(anomaly_data.get("critical_value")),
            anomaly_data.get("pollution_profile"),
            safe_float(anomaly_data.get("cod_bod_ratio")),
            safe_float(anomaly_data.get("rainfall_mm_24h")),
            1 if anomaly_data.get("is_runoff") else 0,
            anomaly_data.get("urgency_level"),
            safe_float(anomaly_data.get("ika_gap")),
            anomaly_data.get("recommendation"),
            anomaly_data.get("telegram_user_id"),
            anomaly_data.get("session_id"),
        ))
        station = anomaly_data.get("station_name", anomaly_data.get("station_id", "unknown"))
        logger.info(f"Anomaly logged: {station}")
        return f"Anomaly logged: {station}"
    except Exception as e:
        logger.error(f"log_anomaly failed: {e}")
        return f"ERROR: Failed to log anomaly: {e}"


def get_station_history(station_id: str, days: int = 30) -> list[dict]:
    """Ambil riwayat anomali per stasiun dari tabel anomaly_log."""
    try:
        sql = """
            SELECT
                station_id, tanggal_deteksi,
                indeks_mutu, status_mutu, is_anomaly,
                critical_parameter, critical_value,
                urgency_level, ika_gap
            FROM anomaly_log
            WHERE station_id = %s
              AND tanggal_deteksi >= DATE_SUB(NOW(), INTERVAL %s DAY)
            ORDER BY tanggal_deteksi DESC
        """
        rows = _query(sql, (station_id, days))
        return [
            {
                "station_id":       r["station_id"],
                "water_quality_index": safe_float(r.get("indeks_mutu")),
                "indeks_mutu":      safe_float(r.get("indeks_mutu")),
                "status":           r.get("status_mutu"),
                "is_anomaly":       bool(r.get("is_anomaly")),
                "tanggal_deteksi":  str(r.get("tanggal_deteksi") or ""),
            }
            for r in rows
        ]
    except Exception as e:
        logger.error(f"get_station_history failed: {e}")
        return []
