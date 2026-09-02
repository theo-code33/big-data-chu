-- Silver : modèle en étoile.
-- 3 faits (séjour, diagnostic, monitoring) + dimensions + quarantaine rejets.
-- Rebuild à chaque run depuis bronze.

DROP TABLE IF EXISTS eds_silver.rejets;
DROP TABLE IF EXISTS eds_silver.patients;
DROP TABLE IF EXISTS eds_silver.sejours;
DROP TABLE IF EXISTS eds_silver.diagnostics;
DROP TABLE IF EXISTS eds_silver.monitoring;
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
    code_cim10 String,
    libelle    String
)
ENGINE = MergeTree
ORDER BY code_cim10;

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

-- Dimensions
INSERT INTO eds_silver.rejets (domaine, regle, patient_pseudo, source_date, detail)
SELECT
    'patients',
    'sexe_ou_naissance_invalide',
    patient_pseudo,
    _source_date,
    concat('sex=', sex, ' birth_year=', toString(birth_year))
FROM eds_bronze.patients
WHERE upper(trim(sex)) NOT IN ('M', 'F')
   OR birth_year < 1900
   OR birth_year > toYear(today());

INSERT INTO eds_silver.dim_patient
SELECT
    patient_pseudo,
    birth_year,
    upper(trim(sex)) AS sex,
    region_code,
    toInt16(2026 - birth_year) AS age_approx,
    _source_date AS source_date
FROM
(
    SELECT
        *,
        row_number() OVER (PARTITION BY patient_pseudo ORDER BY _source_date DESC) AS rn
    FROM eds_bronze.patients
)
WHERE rn = 1
  AND upper(trim(sex)) IN ('M', 'F')
  AND birth_year >= 1900
  AND birth_year <= toYear(today());

INSERT INTO eds_silver.dim_service
SELECT
    service_code,
    argMax(service_label, _ingested_at) AS service_label
FROM eds_bronze.ref_services
GROUP BY service_code;

INSERT INTO eds_silver.dim_cim10
SELECT
    code_cim10,
    argMax(libelle, _ingested_at) AS libelle
FROM eds_bronze.ref_cim10
GROUP BY code_cim10;

-- Fait séjour
INSERT INTO eds_silver.rejets (domaine, regle, stay_id, patient_pseudo, source_date, detail)
SELECT
    'sejours',
    'discharge_avant_admission',
    stay_id,
    patient_pseudo,
    _source_date,
    concat('admission=', toString(admission_ts), ' discharge=', toString(discharge_ts))
FROM eds_bronze.sejours
WHERE discharge_ts IS NOT NULL
  AND discharge_ts < admission_ts;

INSERT INTO eds_silver.fact_sejour
SELECT
    stay_id,
    patient_pseudo,
    service_code,
    admission_ts,
    discharge_ts,
    admission_mode,
    discharge_mode,
    if(discharge_ts IS NULL, NULL, dateDiff('second', admission_ts, discharge_ts) / 3600.0) AS duree_heures,
    if(discharge_ts IS NULL, 1, 0) AS est_en_cours,
    _source_date AS source_date
FROM eds_bronze.sejours
WHERE discharge_ts IS NULL
   OR discharge_ts >= admission_ts;

-- Fait diagnostic : bronze.diagnostics ⋈ bronze.sejours (ETL), jamais fact_sejour.
-- On garde le code même si le séjour a des dates inversées : la prévalence compte
-- tout diagnostic posé (le séjour est écarté de fact_sejour, pas le codage).
INSERT INTO eds_silver.fact_diagnostic
SELECT
    d.stay_id,
    s.patient_pseudo,
    d.code_cim10,
    d.type,
    d._source_date AS source_date
FROM eds_bronze.diagnostics AS d
INNER JOIN eds_bronze.sejours AS s ON d.stay_id = s.stay_id;

-- Fait monitoring
INSERT INTO eds_silver.rejets (domaine, regle, stay_id, source_date, detail)
SELECT
    'monitoring',
    'valeur_hors_plage',
    stay_id,
    _source_date,
    concat('hr=', toString(heart_rate), ' spo2=', toString(spo2), ' temp=', toString(temp_c))
FROM eds_bronze.monitoring
WHERE heart_rate < 20 OR heart_rate > 250
   OR spo2 < 50 OR spo2 > 100
   OR temp_c < 30 OR temp_c > 45;

INSERT INTO eds_silver.fact_monitoring
SELECT
    stay_id,
    ts,
    heart_rate,
    spo2,
    temp_c,
    _source_date AS source_date
FROM eds_bronze.monitoring
WHERE heart_rate BETWEEN 20 AND 250
  AND spo2 BETWEEN 50 AND 100
  AND temp_c BETWEEN 30 AND 45;
