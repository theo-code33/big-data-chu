-- Silver : modèle en étoile.
-- 3 faits (séjour, diagnostic, monitoring) + dimensions + quarantaine rejets.
-- Rebuild à chaque run depuis bronze.

DROP TABLE IF EXISTS eds_silver.rejets;
DROP TABLE IF EXISTS eds_silver.dim_patient;
DROP TABLE IF EXISTS eds_silver.dim_service;
DROP TABLE IF EXISTS eds_silver.dim_cim10;
DROP TABLE IF EXISTS eds_silver.fact_sejour;
DROP TABLE IF EXISTS eds_silver.fact_diagnostic;
DROP TABLE IF EXISTS eds_silver.fact_monitoring;

CREATE TABLE eds_silver.rejets
(
    domaine          LowCardinality(String),
    regle            String,
    stay_id          String DEFAULT '',
    patient_pseudo   String DEFAULT '',
    source_date      Date,
    detail           String,
    rejected_at      DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (domaine, source_date, regle);

-- Dimensions
CREATE TABLE eds_silver.dim_patient
(
    patient_pseudo String,
    birth_year     UInt16,
    sex            LowCardinality(String),
    region_code    String,
    age_approx     Int16,
    source_date    Date
)
ENGINE = MergeTree
ORDER BY patient_pseudo;

CREATE TABLE eds_silver.dim_service
(
    service_code   String,
    service_label  String
)
ENGINE = MergeTree
ORDER BY service_code;

CREATE TABLE eds_silver.dim_cim10
(
    code_cim10 LowCardinality(String),
    libelle    String
)
ENGINE = MergeTree
ORDER BY code_cim10;

-- Faits
-- Fait 1 — grain : 1 ligne = 1 passage à l'hôpital.
CREATE TABLE eds_silver.fact_sejour
(
    stay_id          String,
    patient_pseudo   String,
    service_code     LowCardinality(String),
    admission_ts     DateTime,
    discharge_ts     Nullable(DateTime),
    admission_mode   LowCardinality(String),
    discharge_mode   LowCardinality(String),
    duree_heures     Nullable(Float64),
    est_en_cours     UInt8,
    source_date      Date
)
ENGINE = MergeTree
ORDER BY stay_id;

-- Fait 2 — grain : 1 ligne = 1 code CIM-10.
-- Autosuffisant : patient_pseudo est recopié à l'ETL depuis bronze.sejours
-- (pas une FK vers fact_sejour). stay_id = dimension dégénérée.
CREATE TABLE eds_silver.fact_diagnostic
(
    stay_id          String,
    patient_pseudo   String,
    code_cim10       LowCardinality(String),
    type             LowCardinality(String),
    source_date      Date
)
ENGINE = MergeTree
ORDER BY (stay_id, type, code_cim10);

-- Fait 3 — grain : 1 ligne = 1 relevé. Construit uniquement depuis bronze.monitoring.
CREATE TABLE eds_silver.fact_monitoring
(
    stay_id        String,
    ts             DateTime,
    heart_rate     Int32,
    spo2           Int32,
    temp_c         Float64,
    source_date    Date
)
ENGINE = MergeTree
ORDER BY (stay_id, ts);
