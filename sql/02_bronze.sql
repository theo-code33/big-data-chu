CREATE TABLE IF NOT EXISTS eds_bronze.patients
(
    patient_pseudo String,
    birth_year     UInt16,
    sex            String,
    region_code    String,
    _source_date   Date,
    _ingested_at   DateTime
)
ENGINE = MergeTree
PARTITION BY _source_date
ORDER BY (patient_pseudo, _source_date);

CREATE TABLE IF NOT EXISTS eds_bronze.sejours
(
    stay_id          String,
    patient_pseudo   String,
    service_code     LowCardinality(String),
    admission_ts     DateTime,
    discharge_ts     Nullable(DateTime),
    admission_mode   LowCardinality(String),
    discharge_mode   LowCardinality(String),
    _source_date     Date,
    _ingested_at     DateTime
)
ENGINE = MergeTree
PARTITION BY _source_date
ORDER BY stay_id;

CREATE TABLE IF NOT EXISTS eds_bronze.diagnostics
(
    stay_id        String,
    code_cim10     LowCardinality(String),
    type           LowCardinality(String),
    _source_date   Date,
    _ingested_at   DateTime
)
ENGINE = MergeTree
PARTITION BY _source_date
ORDER BY stay_id;

CREATE TABLE IF NOT EXISTS eds_bronze.monitoring
(
    stay_id        String,
    ts             DateTime,
    heart_rate     Int32,
    spo2           Int32,
    temp_c         Float64,
    _source_date   Date,
    _ingested_at   DateTime
)
ENGINE = MergeTree
PARTITION BY _source_date
ORDER BY (stay_id, ts);

CREATE TABLE IF NOT EXISTS eds_bronze.ref_services
(
    service_code   String,
    service_label  String,
    _source_date   Date,
    _ingested_at   DateTime
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY service_code;

CREATE TABLE IF NOT EXISTS eds_bronze.ref_cim10
(
    code_cim10     String,
    libelle        String,
    _source_date   Date,
    _ingested_at   DateTime
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY code_cim10;
