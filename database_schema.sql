-- =============================================================================
-- WQSA Database Schema — Water Quality Status Agent
-- =============================================================================
-- Sumber data: Onlimo KLHK, BMKG, Sparing Logger, Sparing Monitoring, SITALA
-- Tujuan     : Cache data API ke MySQL untuk mengurangi latency
-- Charset    : utf8mb4 (mendukung karakter Indonesia)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS wqsa_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE wqsa_db;

-- =============================================================================
-- 1. ONLIMO — Master Data Stasiun
--    Sumber: API /stasiun (field: IDStasiun, NamaStasiun, nama_sungai, ...)
-- =============================================================================
CREATE TABLE IF NOT EXISTS onlimo_stasiun (
    station_id      VARCHAR(20)     NOT NULL COMMENT 'ID stasiun dari API (contoh: KLHK1, KLHK02)',
    station_name    VARCHAR(150)    NOT NULL COMMENT 'Nama stasiun pemantauan',
    nama_sungai     VARCHAR(150)    DEFAULT NULL COMMENT 'Nama sungai / danau',
    nama_das        VARCHAR(100)    DEFAULT NULL COMMENT 'Nama Daerah Aliran Sungai',
    provinsi        VARCHAR(100)    DEFAULT NULL,
    kabkot          VARCHAR(100)    DEFAULT NULL COMMENT 'Kabupaten / Kota',
    kecamatan       VARCHAR(100)    DEFAULT NULL,
    latitude        DECIMAL(10, 7)  NOT NULL,
    longitude       DECIMAL(10, 7)  NOT NULL,
    status_aktif    TINYINT(1)      NOT NULL DEFAULT 1,
    synced_at       DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Waktu terakhir data di-sync dari API',
    updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (station_id),
    INDEX idx_das           (nama_das),
    INDEX idx_kabkot        (kabkot),
    INDEX idx_koordinat     (latitude, longitude)
) ENGINE=InnoDB
  COMMENT='Master data stasiun Onlimo KLHK';


-- =============================================================================
-- 2. ONLIMO — Pembacaan / Pengukuran Kualitas Air (Per Jam)
--    Sumber: API Onlimo Monitoring
--    Contoh field API: data_uid, IDStasiun, Tanggal, Jam, Suhu, DHL, DO, PH, ...
--    Catatan: API menyimpan Tanggal dan Jam secara terpisah → digabung jadi
--             tanggal_ukur (DATETIME) untuk kemudahan query time-series.
-- =============================================================================
CREATE TABLE IF NOT EXISTS onlimo_pembacaan (
    -- Primary key dari API (data_uid), bukan auto-increment
    data_uid        BIGINT UNSIGNED NOT NULL COMMENT 'ID unik pembacaan dari API (field: data_uid)',
    station_id      VARCHAR(20)     NOT NULL COMMENT 'FK ke onlimo_stasiun (field: IDStasiun)',
    tanggal_ukur    DATETIME        NOT NULL COMMENT 'Gabungan Tanggal + Jam dari API',
    crdate          DATETIME        DEFAULT NULL COMMENT 'Waktu data masuk ke server Onlimo (field: crdate)',
    deleted         TINYINT(1)      NOT NULL DEFAULT 0 COMMENT 'Soft-delete flag dari API (0=aktif, 1=dihapus)',

    -- Parameter fisik
    suhu            DECIMAL(6, 2)   DEFAULT NULL COMMENT 'Suhu air (°C)',
    kedalaman       DECIMAL(8, 3)   DEFAULT NULL COMMENT 'Kedalaman air (m)',
    turbidity       DECIMAL(10, 3)  DEFAULT NULL COMMENT 'Kekeruhan / Turbidity (NTU)',

    -- Parameter kimia dasar
    ph              DECIMAL(5, 2)   DEFAULT NULL COMMENT 'Derajat keasaman (tanpa satuan, field: PH)',
    do_val          DECIMAL(6, 3)   DEFAULT NULL COMMENT 'Dissolved Oxygen / Oksigen terlarut (mg/L, field: DO)',
    orp             DECIMAL(8, 2)   DEFAULT NULL COMMENT 'Oxidation-Reduction Potential (mV, field: ORP)',

    -- Konduktivitas & salinitas
    dhl             DECIMAL(10, 3)  DEFAULT NULL COMMENT 'Daya Hantar Listrik / Konduktivitas (µS/cm, field: DHL)',
    tds             DECIMAL(10, 3)  DEFAULT NULL COMMENT 'Total Dissolved Solids (mg/L, field: TDS)',
    salinitas       DECIMAL(6, 3)   DEFAULT NULL COMMENT 'Salinitas (ppt, field: Salinitas)',
    swsg            DECIMAL(8, 4)   DEFAULT NULL COMMENT 'Specific Water Gravity (field: SwSG)',

    -- Parameter nitrogen
    nitrat          DECIMAL(10, 3)  DEFAULT NULL COMMENT 'Nitrat / NO3 (mg/L, field: Nitrat)',
    nitrit          DECIMAL(10, 3)  DEFAULT NULL COMMENT 'Nitrit / NO2 (mg/L, field: Nitrit) — bisa null',
    amonia          DECIMAL(10, 3)  DEFAULT NULL COMMENT 'Amonia / NH3 (mg/L, field: Amonia)',

    -- Parameter organik utama (digunakan agent untuk analisis pencemar)
    cod             DECIMAL(10, 3)  DEFAULT NULL COMMENT 'Chemical Oxygen Demand (mg/L, field: COD)',
    bod             DECIMAL(10, 3)  DEFAULT NULL COMMENT 'Biological Oxygen Demand (mg/L, field: BOD)',
    tss             DECIMAL(10, 3)  DEFAULT NULL COMMENT 'Total Suspended Solids (mg/L, field: TSS)',

    -- Parameter tambahan (ekspansi sensor di masa depan)
    param2          DECIMAL(12, 3)  DEFAULT NULL COMMENT 'Parameter tambahan 2 (field: Param2)',
    param3          DECIMAL(12, 3)  DEFAULT NULL COMMENT 'Parameter tambahan 3 (field: Param3)',

    -- Early Warning System
    ews_per         DECIMAL(6, 2)   DEFAULT NULL COMMENT 'Early Warning System persentase (field: EWS_PER)',

    -- Metadata teknis
    ip_addr         VARCHAR(45)     DEFAULT NULL COMMENT 'IP address sensor pengirim data',

    synced_at       DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (data_uid),
    UNIQUE KEY uq_stasiun_waktu     (station_id, tanggal_ukur),
    INDEX idx_station_id            (station_id),
    INDEX idx_tanggal_ukur          (tanggal_ukur),
    INDEX idx_deleted               (deleted),
    INDEX idx_do_ph                 (do_val, ph),
    INDEX idx_cod_bod               (cod, bod),

    CONSTRAINT fk_pembacaan_stasiun
        FOREIGN KEY (station_id) REFERENCES onlimo_stasiun (station_id)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB
  COMMENT='Time-series pengukuran kualitas air per jam dari API Onlimo KLHK';


-- =============================================================================
-- 3. ONLIMO — Status / Indeks Mutu Air Harian (Tervalidasi)
--    Sumber: API Onlimo Status (endpoint terpisah dari monitoring)
--    Satu record per stasiun per hari — indeks sudah divalidasi oleh sistem Onlimo
--    Field API: tanggal_validasi, tanggal_data, indeks, kritis, max_parameter,
--               max_nilai, status_warna, status_nama, keterangan
-- =============================================================================
CREATE TABLE IF NOT EXISTS onlimo_status (
    id                  INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    station_id          VARCHAR(20)     NOT NULL COMMENT 'FK ke onlimo_stasiun (IDStasiun)',
    tanggal_validasi    DATE            NOT NULL COMMENT 'Tanggal status divalidasi oleh Onlimo',
    tanggal_data        DATE            NOT NULL COMMENT 'Tanggal data sensor yang jadi dasar status',

    indeks              DECIMAL(5, 2)   DEFAULT NULL COMMENT 'Indeks Mutu Air tervalidasi (field: indeks)',
    status_nama         VARCHAR(50)     DEFAULT NULL COMMENT 'Label status: BAIK / CEMAR RINGAN / CEMAR SEDANG / CEMAR BERAT (field: status_nama)',
    status_warna        VARCHAR(10)     DEFAULT NULL COMMENT 'Hex color code untuk UI (field: status_warna, contoh: 4F81BC)',

    parameter_kritis    VARCHAR(30)     DEFAULT NULL COMMENT 'Parameter penentu status terburuk (field: kritis, contoh: BOD)',
    max_parameter       VARCHAR(30)     DEFAULT NULL COMMENT 'Parameter dengan nilai tertinggi (field: max_parameter)',
    max_nilai           DECIMAL(12, 3)  DEFAULT NULL COMMENT 'Nilai tertinggi dari max_parameter (field: max_nilai)',

    keterangan          TEXT            DEFAULT NULL COMMENT 'Catatan tambahan dari Onlimo (field: keterangan)',

    synced_at           DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    UNIQUE KEY uq_station_tgl_validasi  (station_id, tanggal_validasi),
    INDEX idx_station_id                (station_id),
    INDEX idx_tanggal_validasi          (tanggal_validasi),
    INDEX idx_indeks                    (indeks),
    INDEX idx_status_nama               (status_nama),
    INDEX idx_parameter_kritis          (parameter_kritis),

    CONSTRAINT fk_status_stasiun
        FOREIGN KEY (station_id) REFERENCES onlimo_stasiun (station_id)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB
  COMMENT='Status indeks mutu air harian tervalidasi per stasiun Onlimo KLHK';


-- =============================================================================
-- 6. BMKG — Master Lokasi Prakiraan Cuaca
--    Sumber: API BMKG (field lokasi per request)
--    Kunci unik: kode administrasi adm4 (level desa/kelurahan)
--    Format kode: adm1="31", adm2="31.71", adm3="31.71.01", adm4="31.71.01.1001"
-- =============================================================================
CREATE TABLE IF NOT EXISTS bmkg_lokasi (
    id          INT UNSIGNED    NOT NULL AUTO_INCREMENT,

    -- Kode administrasi BPS (diisi dari field lokasi.adm1 – adm4)
    adm1        VARCHAR(10)     NOT NULL COMMENT 'Kode provinsi BPS (field: adm1)',
    adm2        VARCHAR(10)     NOT NULL COMMENT 'Kode kab/kota BPS (field: adm2)',
    adm3        VARCHAR(15)     NOT NULL COMMENT 'Kode kecamatan BPS (field: adm3)',
    adm4        VARCHAR(20)     NOT NULL COMMENT 'Kode desa/kelurahan BPS (field: adm4) — unik',

    -- Nama wilayah
    provinsi    VARCHAR(100)    NOT NULL COMMENT 'Nama provinsi (field: provinsi)',
    kotkab      VARCHAR(150)    NOT NULL COMMENT 'Nama kota/kabupaten (field: kotkab)',
    kecamatan   VARCHAR(100)    NOT NULL COMMENT 'Nama kecamatan (field: kecamatan)',
    desa        VARCHAR(100)    NOT NULL COMMENT 'Nama desa/kelurahan (field: desa)',

    -- Koordinat
    latitude    DECIMAL(13, 10) NOT NULL COMMENT 'Latitude (field: lat)',
    longitude   DECIMAL(13, 10) NOT NULL COMMENT 'Longitude (field: lon)',
    timezone    VARCHAR(50)     DEFAULT 'Asia/Jakarta' COMMENT 'Timezone IANA (field: timezone)',

    synced_at   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    UNIQUE KEY uq_adm4          (adm4),
    INDEX idx_adm2              (adm2),
    INDEX idx_adm3              (adm3),
    INDEX idx_kotkab            (kotkab),
    INDEX idx_koordinat         (latitude, longitude)
) ENGINE=InnoDB
  COMMENT='Master lokasi prakiraan cuaca BMKG (level desa/kelurahan, kode BPS adm4)';


-- =============================================================================
-- 7. BMKG — Prakiraan Cuaca Per Periode (3-Jam)
--    Sumber: API BMKG field data[].cuaca[][] (nested array, interval 3 jam)
--    Berisi semua parameter cuaca: curah hujan, suhu, angin, kelembaban, dll.
--    Field utama untuk agent: tp (curah hujan mm) → Step 3 rainfall branching
-- =============================================================================
CREATE TABLE IF NOT EXISTS bmkg_prakiraan (
    id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    lokasi_id           INT UNSIGNED    NOT NULL COMMENT 'FK ke bmkg_lokasi',

    -- Waktu prakiraan (simpan keduanya untuk fleksibilitas query)
    datetime_utc        DATETIME        NOT NULL COMMENT 'Waktu prakiraan UTC (field: utc_datetime)',
    datetime_lokal      DATETIME        NOT NULL COMMENT 'Waktu prakiraan lokal WIB/WITA/WIT (field: local_datetime)',
    tanggal_lokal       DATE            NOT NULL COMMENT 'Tanggal lokal — generated column untuk GROUP BY harian',

    -- Parameter utama untuk agent WQSA (Step 3 branching)
    tp                  DECIMAL(7, 2)   NOT NULL DEFAULT 0.00 COMMENT 'Curah hujan prakiraan (mm, field: tp)',

    -- Kondisi cuaca
    weather             TINYINT UNSIGNED DEFAULT NULL COMMENT 'Kode kondisi cuaca BMKG (field: weather)',
    weather_desc        VARCHAR(80)     DEFAULT NULL COMMENT 'Deskripsi cuaca (Indonesia, field: weather_desc)',
    weather_desc_en     VARCHAR(80)     DEFAULT NULL COMMENT 'Deskripsi cuaca (Inggris, field: weather_desc_en)',

    -- Parameter atmosfer
    t                   TINYINT         DEFAULT NULL COMMENT 'Suhu udara (°C, field: t)',
    tcc                 TINYINT UNSIGNED DEFAULT NULL COMMENT 'Total cloud cover / tutupan awan (%, field: tcc)',
    hu                  TINYINT UNSIGNED DEFAULT NULL COMMENT 'Kelembaban udara / humidity (%, field: hu)',

    -- Angin
    wd_deg              SMALLINT UNSIGNED DEFAULT NULL COMMENT 'Arah angin datang (derajat, field: wd_deg)',
    wd                  VARCHAR(5)      DEFAULT NULL COMMENT 'Arah angin datang (singkatan, field: wd)',
    wd_to               VARCHAR(5)      DEFAULT NULL COMMENT 'Arah angin menuju (singkatan, field: wd_to)',
    ws                  DECIMAL(5, 2)   DEFAULT NULL COMMENT 'Kecepatan angin (m/s, field: ws)',

    -- Jarak pandang
    vs                  INT UNSIGNED    DEFAULT NULL COMMENT 'Jarak pandang / visibility (meter, field: vs)',
    vs_text             VARCHAR(30)     DEFAULT NULL COMMENT 'Jarak pandang teks (field: vs_text)',

    -- Metadata prakiraan
    analysis_date       DATETIME        DEFAULT NULL COMMENT 'Tanggal rilis analisis BMKG (field: analysis_date)',
    time_index          VARCHAR(10)     DEFAULT NULL COMMENT 'Indeks slot waktu BMKG (field: time_index)',
    image_url           VARCHAR(300)    DEFAULT NULL COMMENT 'URL ikon cuaca (field: image)',

    synced_at           DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    UNIQUE KEY uq_lokasi_datetime_utc   (lokasi_id, datetime_utc),
    INDEX idx_lokasi_id                 (lokasi_id),
    INDEX idx_datetime_utc              (datetime_utc),
    INDEX idx_tanggal_lokal             (tanggal_lokal),
    INDEX idx_tp                        (tp),

    CONSTRAINT fk_prakiraan_lokasi
        FOREIGN KEY (lokasi_id) REFERENCES bmkg_lokasi (id)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB
  COMMENT='Prakiraan cuaca BMKG per 3 jam per lokasi — field tp dipakai agent untuk rainfall branching';


-- =============================================================================
-- 8. BMKG — Ringkasan Curah Hujan Harian (Aggregasi dari Prakiraan)
--    Dihitung oleh ETL dari SUM(tp) dan MAX(tp) per lokasi per tanggal_lokal.
--    Mempercepat query agent Step 3: tidak perlu SUM setiap kali query.
--    Tidak ada di API secara langsung — sepenuhnya computed/derived.
-- =============================================================================
CREATE TABLE IF NOT EXISTS bmkg_summary_harian (
    id                  INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    lokasi_id           INT UNSIGNED    NOT NULL COMMENT 'FK ke bmkg_lokasi',
    tanggal             DATE            NOT NULL COMMENT 'Tanggal lokal ringkasan',

    total_tp_mm         DECIMAL(8, 2)   NOT NULL DEFAULT 0.00 COMMENT 'Total curah hujan 24 jam (SUM tp dari bmkg_prakiraan)',
    max_tp_mm           DECIMAL(7, 2)   NOT NULL DEFAULT 0.00 COMMENT 'Curah hujan tertinggi dalam satu periode 3 jam',
    jumlah_periode      TINYINT UNSIGNED DEFAULT 0 COMMENT 'Jumlah periode prakiraan yang teragregasi (maks 8 per hari)',

    synced_at           DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    UNIQUE KEY uq_lokasi_tanggal    (lokasi_id, tanggal),
    INDEX idx_lokasi_id             (lokasi_id),
    INDEX idx_tanggal               (tanggal),
    INDEX idx_total_tp              (total_tp_mm),

    CONSTRAINT fk_summary_lokasi
        FOREIGN KEY (lokasi_id) REFERENCES bmkg_lokasi (id)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB
  COMMENT='Ringkasan curah hujan harian (derived dari bmkg_prakiraan) — dipakai agent Step 3 rainfall branching';


-- =============================================================================
-- 9. SPARING — Master Data Industri / Perusahaan
--    Sumber: API Logger Sparing, field "industri" (nested di tiap logger)
--    Satu industri bisa memiliki banyak logger (outlet IPAL)
-- =============================================================================
CREATE TABLE IF NOT EXISTS sparing_industri (
    id              INT UNSIGNED    NOT NULL COMMENT 'ID industri dari API (field: industri.id)',
    name            VARCHAR(300)    NOT NULL COMMENT 'Nama perusahaan (field: industri.name)',
    type            VARCHAR(100)    DEFAULT NULL COMMENT 'Jenis industri (field: industri.type, contoh: KERTAS DAN PULP)',
    address         TEXT            DEFAULT NULL COMMENT 'Alamat lengkap (field: industri.address)',
    phone           VARCHAR(30)     DEFAULT NULL COMMENT 'No. telepon (field: industri.phone)',
    email           VARCHAR(150)    DEFAULT NULL COMMENT 'Email (field: industri.email)',
    id_simpel       INT UNSIGNED    DEFAULT NULL COMMENT 'ID di sistem SIMPEL KLHK (field: industri.id_simpel)',
    id_ppa          BIGINT UNSIGNED DEFAULT NULL COMMENT 'ID di sistem PPA KLHK (field: industri.id_ppa)',

    synced_at       DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    INDEX idx_type          (type),
    INDEX idx_id_simpel     (id_simpel)
) ENGINE=InnoDB
  COMMENT='Master data perusahaan/industri yang terdaftar di Sparing KLHK';


-- =============================================================================
-- 10. SPARING — Logger / Outlet IPAL
--     Sumber: API Logger Sparing (item level)
--     Satu industri bisa punya beberapa logger (beberapa outlet IPAL).
--     id_logger (ObjectID MongoDB) digunakan sebagai FK di tabel monitoring.
-- =============================================================================
CREATE TABLE IF NOT EXISTS sparing_logger (
    id                  INT UNSIGNED    NOT NULL COMMENT 'ID integer logger dari API (field: id)',
    id_logger           VARCHAR(30)     NOT NULL COMMENT 'MongoDB ObjectID logger (field: id_logger) — dipakai di monitoring API',
    id_industries       INT UNSIGNED    NOT NULL COMMENT 'FK ke sparing_industri (field: id_industries)',

    name                VARCHAR(200)    NOT NULL COMMENT 'Nama outlet/logger (field: name)',
    brand               VARCHAR(100)    DEFAULT NULL COMMENT 'Merek perangkat logger (field: brand)',
    type                VARCHAR(150)    DEFAULT NULL COMMENT 'Tipe perangkat (field: type)',
    model               VARCHAR(100)    DEFAULT NULL COMMENT 'Model perangkat (field: model)',
    serial_number       VARCHAR(100)    DEFAULT NULL COMMENT 'Nomor seri (field: serial_number)',
    mac_address         VARCHAR(150)    DEFAULT NULL COMMENT 'MAC address perangkat (field: mac_address)',
    waste_water_source  TEXT            DEFAULT NULL COMMENT 'Sumber air limbah (field: waste_water_source)',

    latitude            FLOAT           DEFAULT NULL COMMENT 'Koordinat lat (field: coordinate[0])',
    longitude           FLOAT           DEFAULT NULL COMMENT 'Koordinat lon (field: coordinate[1])',

    status              VARCHAR(20)     DEFAULT NULL COMMENT 'Status logger: VALID/INVALID dll (field: status)',
    logger_existing     TINYINT(1)      DEFAULT 1 COMMENT 'Flag logger masih terpasang (field: logger_existing)',

    synced_at           DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    UNIQUE KEY uq_id_logger         (id_logger),
    INDEX idx_id_industries         (id_industries),
    INDEX idx_status                (status),
    INDEX idx_koordinat             (latitude, longitude),

    CONSTRAINT fk_logger_industri
        FOREIGN KEY (id_industries) REFERENCES sparing_industri (id)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB
  COMMENT='Logger/outlet IPAL per perusahaan Sparing — satu industri bisa punya banyak logger';


-- =============================================================================
-- 11. SPARING — Registrasi Sensor / Parameter per Logger
--     Sumber: API Logger Sparing, field "parameters" (array per logger)
--     Menyimpan info sensor + baku mutu yang berlaku per parameter per logger.
--     Digunakan sebagai referensi saat evaluasi kepatuhan monitoring.
-- =============================================================================
CREATE TABLE IF NOT EXISTS sparing_parameter_logger (
    id              INT UNSIGNED    NOT NULL COMMENT 'ID registrasi sensor dari API (field: parameters[].id)',
    id_logger       VARCHAR(30)     NOT NULL COMMENT 'FK ke sparing_logger.id_logger (field: id_logger)',
    id_logger_int   INT UNSIGNED    DEFAULT NULL COMMENT 'FK integer ke sparing_logger.id',
    id_parameter    TINYINT UNSIGNED NOT NULL COMMENT 'ID tipe parameter (1=pH, 2=COD, 3=TSS, 5=debit, field: id_parameter)',

    -- Informasi sensor
    brand           VARCHAR(100)    DEFAULT NULL COMMENT 'Merek sensor (field: parameters[].brand)',
    type            VARCHAR(100)    DEFAULT NULL COMMENT 'Tipe sensor (field: parameters[].type)',
    category        VARCHAR(50)     DEFAULT NULL COMMENT 'Kategori sensor: Single/Multi Sensor (field: category)',
    schedule        TINYINT UNSIGNED DEFAULT NULL COMMENT 'Jadwal pelaporan (jam, field: schedule)',
    range_sensor    VARCHAR(50)     DEFAULT NULL COMMENT 'Rentang ukur sensor (field: range_sensor)',
    brosur_url      VARCHAR(500)    DEFAULT NULL COMMENT 'URL brosur sensor (field: brosur_url)',

    -- Baku mutu yang berlaku untuk sensor ini
    bm              DECIMAL(15, 4)  DEFAULT NULL COMMENT 'Baku mutu maksimum (field: bm)',
    bm_max          DECIMAL(15, 4)  DEFAULT NULL COMMENT 'Batas atas baku mutu (untuk pH, field: bm_max)',

    -- Nama & satuan parameter (denormalized dari nested parameter object)
    parameter_name  VARCHAR(30)     NOT NULL COMMENT 'Nama parameter: pH, cod, tss, debit dll (field: parameter.name)',
    parameter_unit  VARCHAR(20)     DEFAULT NULL COMMENT 'Satuan: mg/L, -, m3/m dll (field: parameter.unit)',

    synced_at       DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    UNIQUE KEY uq_logger_parameter  (id_logger, id_parameter),
    INDEX idx_id_logger             (id_logger),
    INDEX idx_id_parameter          (id_parameter),
    INDEX idx_parameter_name        (parameter_name),

    CONSTRAINT fk_param_logger
        FOREIGN KEY (id_logger) REFERENCES sparing_logger (id_logger)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB
  COMMENT='Registrasi sensor dan baku mutu per parameter per logger Sparing';


-- =============================================================================
-- 12. SPARING — Data Monitoring Harian (Agregasi Per Logger Per Parameter)
--     Sumber: API Monitoring Sparing
--     Satu record = satu logger + satu parameter + satu hari.
--     Nilai yang disimpan adalah agregat harian: average (utama), total, counter.
--     status_taat dihitung ETL berdasarkan value vs bm_min_use / bm_max_use.
-- =============================================================================
CREATE TABLE IF NOT EXISTS sparing_monitoring (
    id              INT UNSIGNED    NOT NULL COMMENT 'ID record monitoring dari API (field: id)',
    id_logger       VARCHAR(30)     NOT NULL COMMENT 'MongoDB ObjectID logger (field: id_logger)',
    id_logger_int   INT UNSIGNED    DEFAULT NULL COMMENT 'ID integer logger (field: id_logger_)',
    id_parameter    TINYINT UNSIGNED NOT NULL COMMENT 'ID tipe parameter (field: id_parameter)',

    -- Nilai agregasi harian
    reported_at     DATE            NOT NULL COMMENT 'Tanggal laporan (field: reported_at)',
    counter         SMALLINT UNSIGNED DEFAULT NULL COMMENT 'Jumlah pembacaan valid dalam sehari (field: counter)',
    total           DECIMAL(18, 6)  DEFAULT NULL COMMENT 'Jumlah total semua pembacaan (field: total)',
    average         DECIMAL(15, 6)  DEFAULT NULL COMMENT 'Rata-rata pembacaan (field: average)',
    value           DECIMAL(15, 6)  DEFAULT NULL COMMENT 'Nilai dilaporkan (= average untuk kualitas, = total untuk debit, field: value)',
    unit            VARCHAR(20)     DEFAULT NULL COMMENT 'Satuan (field: unit)',
    min_valid       TINYINT UNSIGNED DEFAULT NULL COMMENT 'Minimum pembacaan valid yang dibutuhkan (field: min_valid)',

    -- Baku mutu yang berlaku (dari parameter_bm nested object)
    bm              DECIMAL(15, 4)  DEFAULT NULL COMMENT 'Baku mutu dari registrasi sensor (field: parameter_bm.bm)',
    bm_max          DECIMAL(15, 4)  DEFAULT NULL COMMENT 'Batas atas BM (field: parameter_bm.bm_max)',
    bm_min_use      DECIMAL(15, 4)  DEFAULT NULL COMMENT 'BM minimum efektif yang digunakan (field: parameter_bm.bm_min_use)',
    bm_max_use      DECIMAL(15, 4)  DEFAULT NULL COMMENT 'BM maksimum efektif yang digunakan (field: parameter_bm.bm_max_use)',

    -- Nama & satuan parameter (denormalized dari nested parameter object)
    parameter_name  VARCHAR(30)     DEFAULT NULL COMMENT 'Nama parameter: pH, cod, tss, debit (field: parameter.name)',

    -- Status kepatuhan — dihitung ETL:
    --   pH     : bm_min_use <= value <= bm_max_use → TAAT
    --   lainnya: value <= bm_max_use → TAAT
    --   debit  : value <= bm_max_use → TAAT (atau selalu TAAT jika tanpa limit)
    status_taat     ENUM('TAAT', 'LANGGAR', 'TIDAK_VALID') DEFAULT NULL
                    COMMENT 'Kepatuhan baku mutu — dihitung ETL; TIDAK_VALID jika counter < min_valid',

    updated_at      DATETIME        DEFAULT NULL COMMENT 'Waktu update di Sparing (field: updated_at)',
    synced_at       DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    UNIQUE KEY uq_logger_param_tgl  (id_logger, id_parameter, reported_at),
    INDEX idx_id_logger             (id_logger),
    INDEX idx_id_logger_int         (id_logger_int),
    INDEX idx_reported_at           (reported_at),
    INDEX idx_parameter_name        (parameter_name),
    INDEX idx_status_taat           (status_taat),

    CONSTRAINT fk_monitoring_logger
        FOREIGN KEY (id_logger) REFERENCES sparing_logger (id_logger)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB
  COMMENT='Monitoring harian agregasi per logger per parameter dari API Sparing KLHK';


-- =============================================================================
-- 11. SITALA — Indeks Kualitas Lingkungan Hidup per Kabupaten/Kota
--    Sumber: API SITALA KLHK (rows.kabkota)
--    Field kunci: uid_indeks_history, uid_kabkota, tahun, ika, iku, ikl, iklh
--    Catatan:
--      - crdate / chdate dari API berupa Unix timestamp → dikonversi ke DATETIME saat ETL
--      - trend_yoy tidak ada di API → dihitung ETL (ika tahun ini vs tahun lalu)
--      - Kolom rekomendasi & peta_sebaran disimpan apa adanya (umumnya null)
-- =============================================================================
CREATE TABLE IF NOT EXISTS sitala_ika (
    -- Primary key dari API (field: uid_indeks_history)
    uid_indeks_history  INT UNSIGNED    NOT NULL COMMENT 'ID unik dari API SITALA (field: uid_indeks_history)',

    -- Referensi wilayah (ID internal SITALA)
    uid_provinsi        SMALLINT UNSIGNED NOT NULL COMMENT 'ID provinsi di SITALA (field: uid_provinsi)',
    uid_kabkota         SMALLINT UNSIGNED NOT NULL COMMENT 'ID kabupaten/kota di SITALA (field: uid_kabkota)',
    tahun               YEAR            NOT NULL COMMENT 'Tahun data indeks (field: tahun)',

    -- Nama wilayah (denormalized dari API untuk kemudahan query agent)
    nama_provinsi       VARCHAR(100)    NOT NULL COMMENT 'Nama provinsi (field: nama_provinsi)',
    nama_kabkota        VARCHAR(150)    NOT NULL COMMENT 'Nama kabupaten/kota (field: nama_kabkota)',
    kd_regional         TINYINT UNSIGNED DEFAULT NULL COMMENT 'Kode regional (field: kd_regional)',

    -- ── Indeks Aktual ──────────────────────────────────────────────────────────
    ika                 DECIMAL(6, 2)   DEFAULT NULL COMMENT 'Indeks Kualitas Air aktual (field: ika)',
    iku                 DECIMAL(6, 2)   DEFAULT NULL COMMENT 'Indeks Kualitas Udara aktual (field: iku)',
    ikl                 DECIMAL(6, 2)   DEFAULT NULL COMMENT 'Indeks Kualitas Lahan aktual (field: ikl)',
    ikal                DECIMAL(6, 2)   DEFAULT NULL COMMENT 'Indeks Kualitas Alam (field: ikal)',
    ikeg                DECIMAL(6, 2)   DEFAULT NULL COMMENT 'Indeks Kualitas Ekosistem Gambut (field: ikeg) — sering null',
    iklh                DECIMAL(6, 2)   DEFAULT NULL COMMENT 'Indeks Kualitas Lingkungan Hidup (field: iklh)',
    jenis_indeks        TINYINT UNSIGNED DEFAULT 0 COMMENT 'Tipe perhitungan indeks (field: jenis_indeks)',

    -- ── Target RPJMN ──────────────────────────────────────────────────────────
    target_iklh         DECIMAL(6, 2)   DEFAULT NULL COMMENT 'Target IKLH RPJMN (field: target)',
    target_ika          DECIMAL(6, 2)   DEFAULT NULL COMMENT 'Target IKA RPJMN (field: target_ika)',
    target_iku          DECIMAL(6, 2)   DEFAULT NULL COMMENT 'Target IKU RPJMN (field: target_iku)',
    target_ikl          DECIMAL(6, 2)   DEFAULT NULL COMMENT 'Target IKL RPJMN (field: target_ikl)',
    target_ikal         DECIMAL(6, 2)   DEFAULT NULL COMMENT 'Target IKAL RPJMN (field: target_ikal)',

    -- ── Indeks Risiko (IR) ─────────────────────────────────────────────────────
    ir_lb               DECIMAL(6, 2)   DEFAULT NULL COMMENT 'IR Lahan Baik (field: ir_lb)',
    ir_kb               DECIMAL(6, 2)   DEFAULT NULL COMMENT 'IR Kerusakan Badan Air (field: ir_kb)',
    ir_ih               DECIMAL(6, 2)   DEFAULT NULL COMMENT 'IR Irigasi/Hidrologi (field: ir_ih)',
    ir_pl               DECIMAL(6, 2)   DEFAULT NULL COMMENT 'IR Pencemaran Laut (field: ir_pl)',
    ir_gl               DECIMAL(6, 2)   DEFAULT NULL COMMENT 'IR Gambut/Lahan (field: ir_gl)',
    ir_lh               DECIMAL(6, 2)   DEFAULT NULL COMMENT 'IR Total Lingkungan Hidup (field: ir_lh)',

    -- ── Rekomendasi (umumnya null, diisi oleh verifikator KLHK) ───────────────
    rekomendasi_ika     TEXT            DEFAULT NULL COMMENT 'Rekomendasi untuk IKA (field: rekomendasi_ika)',
    rekomendasi_iku     TEXT            DEFAULT NULL COMMENT 'Rekomendasi untuk IKU (field: rekomendasi_iku)',
    rekomendasi_ikl     TEXT            DEFAULT NULL COMMENT 'Rekomendasi untuk IKL (field: rekomendasi_ikl)',
    rekomendasi_ikal    TEXT            DEFAULT NULL COMMENT 'Rekomendasi untuk IKAL (field: rekomendasi_ikal)',
    rekomendasi_iklh    TEXT            DEFAULT NULL COMMENT 'Rekomendasi untuk IKLH (field: rekomendasi_iklh)',

    -- ── Peta Sebaran (nama file gambar di server SITALA) ──────────────────────
    peta_iku            VARCHAR(200)    DEFAULT NULL COMMENT 'Nama file peta IKU (field: peta_sebaran_iku)',
    peta_ika            VARCHAR(200)    DEFAULT NULL COMMENT 'Nama file peta IKA (field: peta_sebaran_ika)',
    peta_ikl            VARCHAR(200)    DEFAULT NULL COMMENT 'Nama file peta IKL (field: peta_sebaran_ikl)',

    -- ── Metadata Wilayah ──────────────────────────────────────────────────────
    gambut_provinsi     TINYINT(1)      DEFAULT NULL COMMENT 'Flag wilayah gambut tingkat provinsi',
    gambut_kabkota      TINYINT(1)      DEFAULT NULL COMMENT 'Flag wilayah gambut tingkat kabkota',

    -- ── Kontrol Record ────────────────────────────────────────────────────────
    deleted             TINYINT(1)      NOT NULL DEFAULT 0 COMMENT 'Soft-delete flag dari API',
    hidden              TINYINT(1)      NOT NULL DEFAULT 0 COMMENT 'Hidden flag dari API',
    crdate              DATETIME        DEFAULT NULL COMMENT 'Tanggal dibuat di SITALA (konversi Unix ts dari field: crdate)',
    chdate              DATETIME        DEFAULT NULL COMMENT 'Tanggal diubah di SITALA (konversi Unix ts dari field: chdate)',

    -- ── Derived / Computed (dihitung oleh ETL, bukan dari API) ───────────────
    trend_yoy           DECIMAL(6, 2)   DEFAULT NULL COMMENT 'Selisih IKA vs tahun lalu (dihitung ETL)',

    synced_at           DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (uid_indeks_history),
    UNIQUE KEY uq_kabkota_tahun     (uid_kabkota, tahun),
    INDEX idx_uid_kabkota           (uid_kabkota),
    INDEX idx_uid_provinsi          (uid_provinsi),
    INDEX idx_nama_kabkota          (nama_kabkota),
    INDEX idx_tahun                 (tahun),
    INDEX idx_ika                   (ika),
    INDEX idx_deleted_hidden        (deleted, hidden)
) ENGINE=InnoDB
  COMMENT='Indeks Kualitas Lingkungan Hidup (IKLH/IKA/IKU/IKL) per kabupaten-kota dari API SITALA KLHK';


-- =============================================================================
-- 12. ANOMALY LOG — Hasil Deteksi Anomali oleh Agent
--    Menggantikan anomaly_log.json (file lokal)
-- =============================================================================
CREATE TABLE IF NOT EXISTS anomaly_log (
    id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    station_id          VARCHAR(20)     DEFAULT NULL COMMENT 'FK ke onlimo_stasiun',
    tanggal_deteksi     DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Waktu agent mendeteksi anomali',

    indeks_mutu         DECIMAL(5, 2)   DEFAULT NULL,
    status_mutu         VARCHAR(30)     DEFAULT NULL,
    is_anomaly          TINYINT(1)      NOT NULL DEFAULT 0,
    reasons             JSON            DEFAULT NULL COMMENT 'Array alasan anomali dari agent',
    critical_parameter  VARCHAR(20)     DEFAULT NULL COMMENT 'Parameter yang paling bermasalah',
    critical_value      DECIMAL(12, 3)  DEFAULT NULL,

    -- Hasil analisis lanjutan
    pollution_profile   ENUM('INDUSTRI', 'CAMPURAN', 'DOMESTIK') DEFAULT NULL,
    cod_bod_ratio       DECIMAL(8, 3)   DEFAULT NULL,
    rainfall_mm_24h     DECIMAL(7, 2)   DEFAULT NULL,
    is_runoff           TINYINT(1)      DEFAULT NULL,

    -- Kesimpulan akhir
    urgency_level       ENUM('PANTAU', 'WASPADA', 'TINDAK') DEFAULT NULL,
    ika_gap             DECIMAL(6, 2)   DEFAULT NULL,
    recommendation      TEXT            DEFAULT NULL COMMENT 'Teks rekomendasi dari generate_rec()',

    -- Konteks conversation Telegram
    telegram_user_id    BIGINT          DEFAULT NULL,
    session_id          VARCHAR(64)     DEFAULT NULL,

    PRIMARY KEY (id),
    INDEX idx_station_id        (station_id),
    INDEX idx_tanggal_deteksi   (tanggal_deteksi),
    INDEX idx_urgency_level     (urgency_level),
    INDEX idx_is_anomaly        (is_anomaly),

    CONSTRAINT fk_anomaly_stasiun
        FOREIGN KEY (station_id) REFERENCES onlimo_stasiun (station_id)
        ON UPDATE CASCADE ON DELETE SET NULL
) ENGINE=InnoDB
  COMMENT='Log hasil deteksi anomali kualitas air oleh WQSA agent';


-- =============================================================================
-- 13. API SYNC LOG — Tracking waktu & status sinkronisasi dari setiap API
--     Digunakan oleh script ETL / scheduler untuk memantau kesehatan sync
-- =============================================================================
CREATE TABLE IF NOT EXISTS api_sync_log (
    id              INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    api_name        VARCHAR(50)     NOT NULL COMMENT 'Nama API (onlimo, bmkg, sparing_logger, sparing_monitoring, sitala)',
    endpoint        VARCHAR(300)    DEFAULT NULL COMMENT 'URL endpoint yang di-hit',
    sync_start      DATETIME        NOT NULL,
    sync_end        DATETIME        DEFAULT NULL,
    status          ENUM('running', 'success', 'failed') NOT NULL DEFAULT 'running',
    records_synced  INT UNSIGNED    DEFAULT 0 COMMENT 'Jumlah record yang berhasil disimpan',
    error_message   TEXT            DEFAULT NULL,

    PRIMARY KEY (id),
    INDEX idx_api_name      (api_name),
    INDEX idx_sync_start    (sync_start),
    INDEX idx_status        (status)
) ENGINE=InnoDB
  COMMENT='Log sinkronisasi data dari API eksternal ke database lokal';


-- =============================================================================
-- VIEW PRAKTIS — Untuk mempermudah query oleh data_layer.py
-- =============================================================================

-- View: Kondisi terkini per stasiun — gabungan pembacaan sensor + status tervalidasi
--       Ini adalah view utama yang digunakan agent untuk query_onlimo()
CREATE OR REPLACE VIEW v_onlimo_terbaru AS
    SELECT
        -- Identitas stasiun
        s.station_id,
        s.station_name,
        s.nama_das,
        s.provinsi,
        s.kabkot,
        s.kecamatan,
        s.latitude,
        s.longitude,

        -- Pembacaan sensor terbaru (dari onlimo_pembacaan)
        p.data_uid,
        p.tanggal_ukur,
        p.ph,
        p.do_val,
        p.turbidity,
        p.suhu,
        p.dhl,
        p.tds,
        p.salinitas,
        p.nitrat,
        p.nitrit,
        p.amonia,
        p.cod,
        p.bod,
        p.tss,
        p.orp,
        p.ews_per,

        -- Status tervalidasi harian (dari onlimo_status — API terpisah)
        st.tanggal_validasi,
        st.indeks              AS indeks_mutu,
        st.status_nama         AS status_mutu,
        st.status_warna,
        st.parameter_kritis,
        st.max_parameter,
        st.max_nilai           AS max_nilai_kritis

    FROM onlimo_stasiun s

    -- JOIN ke pembacaan sensor terbaru (non-deleted)
    INNER JOIN onlimo_pembacaan p ON s.station_id = p.station_id
    INNER JOIN (
        SELECT station_id, MAX(tanggal_ukur) AS max_tgl
        FROM onlimo_pembacaan
        WHERE deleted = 0
        GROUP BY station_id
    ) latest_p ON p.station_id = latest_p.station_id
             AND p.tanggal_ukur = latest_p.max_tgl

    -- LEFT JOIN ke status tervalidasi TERBARU per stasiun (correlated subquery)
    -- Menggunakan correlated subquery agar hanya 1 baris per stasiun (tidak duplikat)
    LEFT JOIN onlimo_status st ON s.station_id = st.station_id
        AND st.tanggal_validasi = (
            SELECT MAX(tanggal_validasi)
            FROM onlimo_status
            WHERE station_id = s.station_id
        )

    WHERE s.status_aktif = 1
      AND p.deleted = 0;


-- View: Ringkasan curah hujan terbaru per lokasi (untuk Step 3 rainfall branching)
--       Mengambil tanggal terbaru per lokasi dari bmkg_summary_harian
CREATE OR REPLACE VIEW v_bmkg_terbaru AS
    SELECT
        l.id           AS lokasi_id,
        l.adm4,
        l.provinsi,
        l.kotkab,
        l.kecamatan,
        l.desa,
        l.latitude,
        l.longitude,
        l.timezone,
        h.tanggal,
        h.total_tp_mm  AS total_rainfall_mm,
        h.max_tp_mm    AS max_rainfall_mm,
        h.jumlah_periode
    FROM bmkg_lokasi l
    INNER JOIN bmkg_summary_harian h ON l.id = h.lokasi_id
    INNER JOIN (
        SELECT lokasi_id, MAX(tanggal) AS max_tgl
        FROM bmkg_summary_harian
        GROUP BY lokasi_id
    ) latest ON h.lokasi_id = latest.lokasi_id
           AND h.tanggal = latest.max_tgl;


-- View: IKA terbaru per kabupaten/kota + gap vs target (untuk Step 5 agent)
--       Hanya record deleted=0 dan hidden=0, tahun terbaru per kabkota
CREATE OR REPLACE VIEW v_sitala_terbaru AS
    SELECT
        s.uid_indeks_history,
        s.uid_kabkota,
        s.uid_provinsi,
        s.nama_provinsi,
        s.nama_kabkota,
        s.tahun,
        -- Nilai indeks aktual
        s.ika,
        s.iku,
        s.ikl,
        s.ikal,
        s.iklh,
        -- Target RPJMN
        s.target_ika,
        s.target_iku,
        s.target_ikl,
        s.target_iklh,
        -- Gap IKA (negatif = di bawah target)
        ROUND(s.ika - s.target_ika, 2)   AS gap_ika,
        ROUND(s.iklh - s.target_iklh, 2) AS gap_iklh,
        -- Indeks risiko total (ringkasan)
        s.ir_lh,
        s.ir_lb,
        s.ir_kb,
        -- Trend tahunan (dihitung ETL)
        s.trend_yoy,
        -- Rekomendasi (dari KLHK, sering null)
        s.rekomendasi_ika
    FROM sitala_ika s
    INNER JOIN (
        SELECT uid_kabkota, MAX(tahun) AS max_tahun
        FROM sitala_ika
        WHERE deleted = 0 AND hidden = 0
        GROUP BY uid_kabkota
    ) latest ON s.uid_kabkota = latest.uid_kabkota
           AND s.tahun = latest.max_tahun
    WHERE s.deleted = 0
      AND s.hidden = 0;


-- View: Status kepatuhan monitoring terkini per logger per parameter (untuk Step 4)
--       Menggabungkan data industri + logger + monitoring hari terakhir
CREATE OR REPLACE VIEW v_sparing_kepatuhan_terkini AS
    SELECT
        -- Identitas industri
        i.id            AS industri_id,
        i.name          AS industri_name,
        i.type          AS industri_type,
        -- Identitas logger/outlet
        l.id            AS logger_id,
        l.id_logger,
        l.name          AS outlet_name,
        l.latitude,
        l.longitude,
        l.status        AS logger_status,
        -- Data monitoring terbaru
        m.reported_at,
        m.parameter_name,
        m.value,
        m.unit,
        m.bm_max_use    AS baku_mutu,
        m.bm_min_use    AS baku_mutu_min,
        m.counter,
        m.min_valid,
        m.status_taat
    FROM sparing_industri i
    INNER JOIN sparing_logger l        ON l.id_industries = i.id
    INNER JOIN sparing_monitoring m    ON m.id_logger = l.id_logger
    INNER JOIN (
        SELECT id_logger, MAX(reported_at) AS max_tgl
        FROM sparing_monitoring
        GROUP BY id_logger
    ) latest ON m.id_logger = latest.id_logger
           AND m.reported_at = latest.max_tgl
    WHERE l.logger_existing = 1
      AND l.status = 'VALID';
