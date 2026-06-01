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

import threading

# Thread-local storage — each Flask thread gets its own connection
_local = threading.local()


def _get_db() -> pymysql.Connection:
    """Return a per-thread MySQL connection, reconnecting if needed."""
    conn = getattr(_local, "conn", None)
    try:
        if conn is None or not conn.open:
            raise pymysql.err.InterfaceError("no connection")
        conn.ping(reconnect=True)
    except Exception:
        conn = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )
        _local.conn = conn
    return conn


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


def safe_float_nullable(value: Any) -> Optional[float]:
    """Like safe_float but returns None for null/empty — preserves sensor 'no data' info."""
    if value is None or value == "" or value == "-" or value == "N/A":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


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
    def __init__(self, ttl_minutes: int = 5):
        self._store: dict[str, dict] = {}
        self._ttl = timedelta(minutes=ttl_minutes)

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

    def size(self) -> int:
        return len(self._store)


cache = MemoryCache(ttl_minutes=5)


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
            # Fallback: stasiun ada di master tapi belum ada data monitoring
            # Ambil data minimal dari onlimo_stasiun + onlimo_status
            fallback = _get_onlimo_fallback(station_id, das)
            if fallback:
                return fallback
            return [{"info": "No stations found matching criteria", "missing_etl": "onlimo_monitoring"}]

        result = [_normalize_onlimo(r) for r in rows]
        cache.set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"get_onlimo_data failed: {e}")
        return [{"error": str(e)}]


def _normalize_onlimo(row: dict) -> dict:
    """Konversi baris v_onlimo_terbaru ke format yang diharapkan tools.py.
    Sensor parameters pakai safe_float_nullable agar NULL dari DB tetap None
    (bukan 0.0) — penting untuk membedakan sensor mati vs nilai memang 0."""
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
        # Indeks & status — pakai safe_float (0.0 default OK di sini)
        "indeks_mutu":  safe_float(row.get("indeks_mutu")),
        "status":       row.get("status_mutu") or "TIDAK DIKETAHUI",
        "status_warna": row.get("status_warna"),
        "parameter_kritis": row.get("parameter_kritis"),
        # Pembacaan sensor — pakai safe_float_nullable (None = sensor tidak kirim)
        "parameter": {
            "cod":      safe_float_nullable(row.get("cod")),
            "bod":      safe_float_nullable(row.get("bod")),
            "tss":      safe_float_nullable(row.get("tss")),
            "do":       safe_float_nullable(row.get("do_val")),
            "ph":       safe_float_nullable(row.get("ph")),
            "nitrat":   safe_float_nullable(row.get("nitrat")),
            "nitrit":   safe_float_nullable(row.get("nitrit")),
            "amonia":   safe_float_nullable(row.get("amonia")),
            "suhu":     safe_float_nullable(row.get("suhu")),
            "turbidity": safe_float_nullable(row.get("turbidity")),
            "dhl":      safe_float_nullable(row.get("dhl")),
            "ews_per":  safe_float_nullable(row.get("ews_per")),
        },
        "timestamp": ts.isoformat() if isinstance(ts, datetime) else str(ts or ""),
        "tanggal_validasi": str(row.get("tanggal_validasi") or ""),
    }


def _get_onlimo_fallback(station_id: str = "", das: str = "") -> list[dict]:
    """
    Fallback: stasiun ada di master tapi belum ada data pembacaan sensor.
    Ambil data dari onlimo_stasiun + onlimo_status saja (tanpa parameter sensor).
    """
    try:
        conditions = ["s.station_id IS NOT NULL"]
        params = []
        if station_id:
            conditions.append("s.station_id = %s")
            params.append(station_id.upper())
        if das:
            conditions.append("s.nama_das = %s")
            params.append(das)

        rows = _query(f"""
            SELECT
                s.station_id, s.station_name, s.nama_das, s.provinsi,
                s.kabkot, s.kecamatan, s.latitude, s.longitude,
                st.indeks AS indeks_mutu,
                st.status_nama AS status_mutu,
                st.status_warna,
                st.parameter_kritis,
                st.tanggal_validasi,
                st.keterangan
            FROM onlimo_stasiun s
            LEFT JOIN onlimo_status st ON s.station_id = st.station_id
                AND st.tanggal_validasi = (
                    SELECT MAX(tanggal_validasi)
                    FROM onlimo_status
                    WHERE station_id = s.station_id
                )
            WHERE {' AND '.join(conditions)}
            ORDER BY s.station_id
            LIMIT 20
        """, tuple(params))

        if not rows:
            return []

        result = []
        for row in rows:
            result.append({
                "station_id":       row.get("station_id"),
                "station_name":     row.get("station_name"),
                "das":              row.get("nama_das"),
                "provinsi":         row.get("provinsi"),
                "kabkot":           row.get("kabkot"),
                "kecamatan":        row.get("kecamatan"),
                "latitude":         safe_float(row.get("latitude")),
                "longitude":        safe_float(row.get("longitude")),
                "indeks_mutu":      safe_float(row.get("indeks_mutu")),
                "status":           row.get("status_mutu") or "TIDAK DIKETAHUI",
                "status_warna":     row.get("status_warna"),
                "parameter_kritis": row.get("parameter_kritis"),
                # Semua parameter sensor kosong — belum ada data monitoring
                "parameter": {
                    "cod": 0.0, "bod": 0.0, "tss": 0.0,
                    "do": 0.0,  "ph": 0.0,  "nitrat": 0.0,
                    "nitrit": 0.0, "amonia": 0.0, "suhu": 0.0,
                    "turbidity": 0.0, "dhl": 0.0, "ews_per": 0.0,
                },
                "timestamp": "",
                "tanggal_validasi": str(row.get("tanggal_validasi") or ""),
                "_no_sensor_data": True,   # flag: sensor belum ada
                "_missing_etl": "Onlimo Monitoring (ETL sensor belum dijalankan untuk stasiun ini)",
            })
        logger.info(f"Fallback onlimo data: {len(result)} stations (no sensor data)")
        return result
    except Exception as e:
        logger.error(f"_get_onlimo_fallback failed: {e}")
        return []


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


# =============================================================================
# 7. DASHBOARD-SPECIFIC QUERIES (used by dashboard.py)
# =============================================================================

def get_all_onlimo_flat(das: str = "") -> list[dict]:
    conditions = ["1=1"]
    params = []
    if das:
        conditions.append("nama_das = %s")
        params.append(das)
    sql = f"SELECT * FROM v_onlimo_terbaru WHERE {' AND '.join(conditions)} ORDER BY station_id"
    try:
        rows = _query(sql, tuple(params))
        return [{
            "station_id":   r.get("station_id"),
            "station_name": r.get("station_name"),
            "das":          r.get("nama_das"),
            "kabkot":       r.get("kabkot"),
            "kecamatan":    r.get("kecamatan"),
            "latitude":     safe_float(r.get("latitude")),
            "longitude":    safe_float(r.get("longitude")),
            "indeks_mutu":  safe_float(r.get("indeks_mutu")),
            "status":       r.get("status_mutu") or "TIDAK DIKETAHUI",
            "status_warna": r.get("status_warna"),
            "cod":          safe_float(r.get("cod")),
            "bod":          safe_float(r.get("bod")),
            "tss":          safe_float(r.get("tss")),
            "do":           safe_float(r.get("do_val")),
            "ph":           safe_float(r.get("ph")),
            "amonia":       safe_float(r.get("amonia")),
            "timestamp":    str(r.get("tanggal_ukur") or ""),
        } for r in rows]
    except Exception as e:
        logger.error(f"get_all_onlimo_flat: {e}")
        return []


def get_all_rainfall_data() -> list[dict]:
    try:
        rows = _query("SELECT * FROM v_bmkg_terbaru ORDER BY total_rainfall_mm DESC")
        return [{
            "adm4_code":         r.get("adm4"),
            "kotkab":            r.get("kotkab"),
            "kecamatan":         r.get("kecamatan"),
            "desa":              r.get("desa"),
            "location":          r.get("desa") or r.get("kecamatan") or "?",
            "latitude":          safe_float(r.get("latitude")),
            "longitude":         safe_float(r.get("longitude")),
            "tanggal":           str(r.get("tanggal") or ""),
            "total_rainfall_mm": safe_float(r.get("total_rainfall_mm")),
            "max_rainfall_mm":   safe_float(r.get("max_rainfall_mm")),
            "is_high_rainfall":  safe_float(r.get("total_rainfall_mm")) > RAINFALL_HIGH_MM,
        } for r in rows]
    except Exception as e:
        logger.error(f"get_all_rainfall_data: {e}")
        return []


def get_sparing_summary() -> dict:
    try:
        rows = _query("""
            SELECT
                SUM(CASE WHEN status_taat = 'TAAT' THEN 1 ELSE 0 END)       AS taat,
                SUM(CASE WHEN status_taat = 'TIDAK TAAT' THEN 1 ELSE 0 END) AS langgar,
                COUNT(*) AS total
            FROM v_sparing_kepatuhan_terkini
        """)
        r = rows[0] if rows else {}
        return {"taat": safe_int(r.get("taat")), "langgar": safe_int(r.get("langgar")), "total": safe_int(r.get("total"))}
    except Exception as e:
        logger.error(f"get_sparing_summary: {e}")
        return {"taat": 0, "langgar": 0, "total": 0}


def get_sparing_violations() -> list[dict]:
    try:
        rows = _query("""
            SELECT * FROM v_sparing_kepatuhan_terkini
            WHERE status_taat = 'TIDAK TAAT'
            ORDER BY reported_at DESC
        """)
        return [{
            "company_name":  r.get("industri_name"),
            "industry_type": r.get("industri_type"),
            "outlet_name":   r.get("outlet_name"),
            "parameter":     r.get("parameter_name"),
            "value":         safe_float(r.get("value")),
            "baku_mutu":     safe_float(r.get("baku_mutu")),
            "unit":          r.get("unit") or "mg/L",
            "pct_of_bm":     round(safe_float(r.get("value")) / safe_float(r.get("baku_mutu")) * 100, 1) if safe_float(r.get("baku_mutu")) > 0 else 0,
            "reported_at":   str(r.get("reported_at") or ""),
            "status":        "LANGGAR",
        } for r in rows]
    except Exception as e:
        logger.error(f"get_sparing_violations: {e}")
        return []


def get_sparing_company_summary() -> list[dict]:
    try:
        rows = _query("""
            SELECT
                industri_name, industri_type,
                SUM(CASE WHEN status_taat = 'TAAT' THEN 1 ELSE 0 END)       AS taat,
                SUM(CASE WHEN status_taat = 'TIDAK TAAT' THEN 1 ELSE 0 END) AS langgar,
                COUNT(*) AS total
            FROM v_sparing_kepatuhan_terkini
            GROUP BY industri_name, industri_type
            ORDER BY langgar DESC, industri_name
        """)
        return [{"company_name": r.get("industri_name"), "industry_type": r.get("industri_type"),
                 "taat": safe_int(r.get("taat")), "langgar": safe_int(r.get("langgar")), "total": safe_int(r.get("total"))}
                for r in rows]
    except Exception as e:
        logger.error(f"get_sparing_company_summary: {e}")
        return []


def get_all_sitala_with_urgency() -> list[dict]:
    from config import IKA_GAP_WARNING, IKA_GAP_CRITICAL
    try:
        rows = _query("SELECT * FROM v_sitala_terbaru ORDER BY nama_kabkota")
        result = []
        for r in rows:
            gap = safe_float(r.get("gap_ika"))
            urgency = "TINDAK" if gap <= IKA_GAP_CRITICAL else "WASPADA" if gap <= IKA_GAP_WARNING else "PANTAU"
            result.append({"kabkot": r.get("nama_kabkota"), "provinsi": r.get("nama_provinsi"),
                           "tahun": safe_int(r.get("tahun")), "ika": safe_float(r.get("ika")),
                           "target_ika": safe_float(r.get("target_ika")), "gap_ika": gap,
                           "iklh": safe_float(r.get("iklh")), "trend_yoy": safe_float(r.get("trend_yoy")),
                           "urgency": urgency})
        return result
    except Exception as e:
        logger.error(f"get_all_sitala_with_urgency: {e}")
        return []


def get_all_anomaly_log(limit: int = 30) -> list[dict]:
    try:
        rows = _query("SELECT * FROM anomaly_log ORDER BY tanggal_deteksi DESC LIMIT %s", (limit,))
        return [{"id": r.get("id"), "station_id": r.get("station_id"),
                 "logged_at": str(r.get("tanggal_deteksi") or ""),
                 "indeks_mutu": safe_float(r.get("indeks_mutu")), "status_mutu": r.get("status_mutu"),
                 "is_anomaly": bool(r.get("is_anomaly")), "critical_parameter": r.get("critical_parameter"),
                 "pollution_profile": r.get("pollution_profile"), "cod_bod_ratio": safe_float(r.get("cod_bod_ratio")),
                 "rainfall_mm_24h": safe_float(r.get("rainfall_mm_24h")), "is_runoff": bool(r.get("is_runoff")),
                 "urgency_level": r.get("urgency_level"), "ika_gap": safe_float(r.get("ika_gap")),
                 "recommendation": r.get("recommendation"), "session_id": r.get("session_id")}
                for r in rows]
    except Exception as e:
        logger.error(f"get_all_anomaly_log: {e}")
        return []


def get_station_trend(station_id: str, days: int = 30) -> dict:
    """
    Ambil data historis sebuah stasiun untuk visualisasi trend.
    Returns: info stasiun, trend indeks harian, trend sensor harian, anomaly events.
    """
    try:
        # ── Info stasiun ───────────────────────────────────────────────
        info_rows = _query("""
            SELECT s.station_id, s.station_name, s.nama_das, s.provinsi,
                   s.kabkot, s.kecamatan, s.latitude, s.longitude,
                   st.indeks AS indeks_terkini, st.status_nama AS status_terkini,
                   st.status_warna, st.tanggal_validasi
            FROM onlimo_stasiun s
            LEFT JOIN onlimo_status st ON s.station_id = st.station_id
                AND st.tanggal_validasi = (
                    SELECT MAX(tanggal_validasi) FROM onlimo_status
                    WHERE station_id = s.station_id
                )
            WHERE s.station_id = %s
        """, (station_id.upper(),))
        info = info_rows[0] if info_rows else {}

        # ── Trend indeks mutu harian (dari onlimo_status) ──────────────
        status_rows = _query("""
            SELECT tanggal_validasi,
                   indeks, status_nama, status_warna, parameter_kritis
            FROM onlimo_status
            WHERE station_id = %s
              AND tanggal_validasi >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            ORDER BY tanggal_validasi
        """, (station_id.upper(), days))

        trend_ika = [{
            "tanggal":        str(r["tanggal_validasi"]),
            "indeks":         safe_float(r.get("indeks")),
            "status":         r.get("status_nama") or "",
            "status_warna":   r.get("status_warna") or "",
            "param_kritis":   r.get("parameter_kritis") or "",
        } for r in status_rows]

        # ── Trend sensor harian (agregasi dari onlimo_pembacaan) ────────
        sensor_rows = _query("""
            SELECT
                DATE(tanggal_ukur)          AS tanggal,
                COUNT(*)                    AS n_readings,
                ROUND(AVG(cod), 2)          AS cod_avg,
                ROUND(MAX(cod), 2)          AS cod_max,
                ROUND(AVG(bod), 2)          AS bod_avg,
                ROUND(MAX(bod), 2)          AS bod_max,
                ROUND(AVG(tss), 2)          AS tss_avg,
                ROUND(AVG(do_val), 2)       AS do_avg,
                ROUND(AVG(ph), 2)           AS ph_avg,
                ROUND(AVG(amonia), 2)       AS amonia_avg,
                ROUND(MAX(amonia), 2)       AS amonia_max,
                ROUND(AVG(turbidity), 2)    AS turbidity_avg,
                ROUND(AVG(nitrat), 2)       AS nitrat_avg,
                ROUND(AVG(suhu), 2)         AS suhu_avg
            FROM onlimo_pembacaan
            WHERE station_id = %s
              AND tanggal_ukur >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
              AND deleted = 0
            GROUP BY DATE(tanggal_ukur)
            ORDER BY tanggal
        """, (station_id.upper(), days))

        trend_sensor = [{
            "tanggal":       str(r["tanggal"]),
            "n":             safe_int(r.get("n_readings")),
            "cod":           safe_float(r.get("cod_avg")),
            "cod_max":       safe_float(r.get("cod_max")),
            "bod":           safe_float(r.get("bod_avg")),
            "bod_max":       safe_float(r.get("bod_max")),
            "tss":           safe_float(r.get("tss_avg")),
            "do":            safe_float(r.get("do_avg")),
            "ph":            safe_float(r.get("ph_avg")),
            "amonia":        safe_float(r.get("amonia_avg")),
            "amonia_max":    safe_float(r.get("amonia_max")),
            "turbidity":     safe_float(r.get("turbidity_avg")),
            "nitrat":        safe_float(r.get("nitrat_avg")),
            "suhu":          safe_float(r.get("suhu_avg")),
        } for r in sensor_rows]

        # ── Anomaly events (dari anomaly_log) ──────────────────────────
        anomaly_rows = _query("""
            SELECT tanggal_deteksi, urgency_level, is_anomaly,
                   pollution_profile, critical_parameter,
                   indeks_mutu, status_mutu, recommendation
            FROM anomaly_log
            WHERE station_id = %s
              AND tanggal_deteksi >= DATE_SUB(NOW(), INTERVAL %s DAY)
            ORDER BY tanggal_deteksi DESC
        """, (station_id.upper(), days))

        anomaly_events = [{
            "tanggal":       str(r.get("tanggal_deteksi") or ""),
            "urgency":       r.get("urgency_level") or "",
            "is_anomaly":    bool(r.get("is_anomaly")),
            "profil":        r.get("pollution_profile") or "",
            "param_kritis":  r.get("critical_parameter") or "",
            "indeks":        safe_float(r.get("indeks_mutu")),
            "status":        r.get("status_mutu") or "",
            "rekomendasi":   (r.get("recommendation") or "")[:200],
        } for r in anomaly_rows]

        # ── Statistik ringkas ──────────────────────────────────────────
        indeks_vals = [t["indeks"] for t in trend_ika if t["indeks"] > 0]
        stats = {
            "indeks_min":  round(min(indeks_vals), 2) if indeks_vals else None,
            "indeks_max":  round(max(indeks_vals), 2) if indeks_vals else None,
            "indeks_avg":  round(sum(indeks_vals)/len(indeks_vals), 2) if indeks_vals else None,
            "days_data":   len(trend_ika),
            "sensor_days": len(trend_sensor),
            "anomaly_count": len([a for a in anomaly_events if a["is_anomaly"]]),
        }

        return {
            "station_id":    info.get("station_id", station_id),
            "station_name":  info.get("station_name") or "",
            "das":           info.get("nama_das") or "",
            "provinsi":      info.get("provinsi") or "",
            "kabkot":        info.get("kabkot") or "",
            "kecamatan":     info.get("kecamatan") or "",
            "latitude":      safe_float(info.get("latitude")),
            "longitude":     safe_float(info.get("longitude")),
            "status_terkini":  info.get("status_terkini") or "–",
            "indeks_terkini":  safe_float(info.get("indeks_terkini")),
            "status_warna":    info.get("status_warna") or "",
            "tanggal_validasi": str(info.get("tanggal_validasi") or ""),
            "trend_ika":     trend_ika,
            "trend_sensor":  trend_sensor,
            "anomaly_events": anomaly_events,
            "stats":         stats,
            "days":          days,
        }
    except Exception as e:
        logger.error(f"get_station_trend failed: {e}")
        return {"error": str(e)}


def get_dashboard_summary() -> dict:
    try:
        # status_warna berisi hex color (FC0004/FDF92F/02AE4E/4F81BC), bukan nama warna.
        # Gunakan status_mutu sebagai klasifikasi utama.
        s = (_query("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE
                    WHEN status_mutu = 'CEMAR BERAT' THEN 1
                    ELSE 0 END) AS critical,
                SUM(CASE
                    WHEN status_mutu IN ('CEMAR SEDANG', 'CEMAR RINGAN') THEN 1
                    ELSE 0 END) AS warning,
                SUM(CASE
                    WHEN status_mutu IN ('MEMENUHI BAKUMUTU', 'BAIK') THEN 1
                    ELSE 0 END) AS good,
                SUM(CASE
                    WHEN status_mutu IS NULL THEN 1
                    ELSE 0 END) AS no_status
            FROM v_onlimo_terbaru
        """) or [{}])[0]

        max_rain = safe_float(((_query(
            "SELECT MAX(total_rainfall_mm) AS m FROM v_bmkg_terbaru"
        ) or [{}])[0]).get("m"))

        langgar = safe_int(((_query("""
            SELECT COUNT(*) AS cnt FROM sparing_monitoring
            WHERE status_taat = 'TIDAK TAAT'
              AND reported_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
        """) or [{}])[0]).get("cnt"))

        return {
            "total_stations":  safe_int(s.get("total")),
            "critical":        safe_int(s.get("critical")),
            "warning":         safe_int(s.get("warning")),
            "good":            safe_int(s.get("good")),
            "no_status":       safe_int(s.get("no_status")),
            "max_rainfall_mm": max_rain,
            "sparing_langgar": langgar,
        }
    except Exception as e:
        logger.error(f"get_dashboard_summary: {e}")
        return {"total_stations": 0, "critical": 0, "warning": 0, "good": 0,
                "no_status": 0, "max_rainfall_mm": 0, "sparing_langgar": 0}


# =============================================================================
# ANALYSIS CACHE — simpan hasil analisis agent ke MySQL
# =============================================================================

CACHE_TTL_HOURS = 24  # hasil analisis valid 24 jam


def _make_cache_key(analysis_type: str, target_value: str) -> str:
    from datetime import date
    day = date.today().isoformat()
    target = (target_value or "").strip().upper()
    return f"{analysis_type}:{target}:{day}"


def get_cached_analysis(analysis_type: str, target_value: str) -> Optional[dict]:
    """Ambil hasil analisis dari cache jika masih valid."""
    key = _make_cache_key(analysis_type, target_value)
    try:
        rows = _query(
            "SELECT * FROM analysis_cache WHERE cache_key = %s AND expires_at > NOW()",
            (key,)
        )
        if not rows:
            return None
        row = rows[0]
        result = json.loads(row["result_json"])
        result["_from_cache"] = True
        result["_cached_at"]  = str(row["created_at"])
        result["_elapsed_sec"] = row["elapsed_sec"]
        result["_cache_key"]  = key
        logger.info(f"Cache HIT: {key} ({row['elapsed_sec']}s saved)")
        return result
    except Exception as e:
        logger.error(f"get_cached_analysis failed: {e}")
        return None


def save_cached_analysis(
    analysis_type: str,
    target_value: str,
    result: dict,
    elapsed_sec: int,
) -> bool:
    """Simpan hasil analisis ke cache."""
    key = _make_cache_key(analysis_type, target_value)
    try:
        # Hapus key yang akan distrip — tidak perlu kirim field internal
        clean = {k: v for k, v in result.items()
                 if not k.startswith("_")}
        station_count = len(
            (result.get("analysis_raw") or {}).get("anomalous_stations", [])
        )
        _execute("""
            INSERT INTO analysis_cache
                (cache_key, analysis_type, target_value, expires_at, elapsed_sec, station_count, result_json)
            VALUES (%s, %s, %s, DATE_ADD(NOW(), INTERVAL %s HOUR), %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                created_at   = NOW(),
                expires_at   = DATE_ADD(NOW(), INTERVAL %s HOUR),
                elapsed_sec  = VALUES(elapsed_sec),
                station_count= VALUES(station_count),
                result_json  = VALUES(result_json)
        """, (
            key, analysis_type, target_value or "",
            CACHE_TTL_HOURS, elapsed_sec, station_count,
            json.dumps(clean, ensure_ascii=False, default=str),
            CACHE_TTL_HOURS,
        ))
        logger.info(f"Cache SAVED: {key}")
        return True
    except Exception as e:
        logger.error(f"save_cached_analysis failed: {e}")
        return False


def list_analysis_cache() -> list[dict]:
    """List semua cache yang masih valid."""
    try:
        rows = _query("""
            SELECT cache_key, analysis_type, target_value,
                   created_at, expires_at, elapsed_sec, station_count
            FROM analysis_cache
            WHERE expires_at > NOW()
            ORDER BY created_at DESC
            LIMIT 50
        """)
        return [{
            "cache_key":     r["cache_key"],
            "analysis_type": r["analysis_type"],
            "target_value":  r["target_value"] or "semua stasiun",
            "created_at":    str(r["created_at"]),
            "expires_at":    str(r["expires_at"]),
            "elapsed_sec":   r["elapsed_sec"],
            "station_count": r["station_count"],
        } for r in rows]
    except Exception as e:
        logger.error(f"list_analysis_cache: {e}")
        return []


def invalidate_analysis_cache() -> int:
    """Hapus semua cache (dipanggil setelah ETL selesai)."""
    try:
        _execute("DELETE FROM analysis_cache WHERE 1=1")
        logger.info("Analysis cache invalidated (ETL completed)")
        return 1
    except Exception as e:
        logger.error(f"invalidate_analysis_cache: {e}")
        return 0