-- Gold reconstruit à chaque run. Deux bases = cloisonnement ClickHouse (GRANT).

DROP TABLE IF EXISTS eds_gold_pilotage.dms_par_service;
DROP TABLE IF EXISTS eds_gold_pilotage.passages_urgences_jour;
DROP TABLE IF EXISTS eds_gold_pilotage.readmission_30j;
DROP TABLE IF EXISTS eds_gold_pilotage.alertes_monitoring_jour;
DROP TABLE IF EXISTS eds_gold_pilotage.activite_par_service;
DROP TABLE IF EXISTS eds_gold_pilotage.qualite_rejets;
DROP TABLE IF EXISTS eds_gold_recherche.prevalence_pathologie;
DROP TABLE IF EXISTS eds_gold_recherche.cohorte_age_sexe;
DROP TABLE IF EXISTS eds_gold_recherche.cohorte_pathologie_age_sexe;

-- DMS : séjours SORTIS uniquement (un séjour en cours n'a pas de durée).
CREATE TABLE eds_gold_pilotage.dms_par_service
ENGINE = MergeTree
ORDER BY service_code
AS
SELECT
    service_code,
    any(service_label) AS service_label,
    count() AS nb_sejours_sortis,
    round(avg(duree_heures) / 24.0, 2) AS dms_jours,
    round(quantile(0.5)(duree_heures) / 24.0, 2) AS dms_mediane_jours
FROM eds_silver.sejours
WHERE est_en_cours = 0
GROUP BY service_code;

-- Activité du service URGENCES (pas le mode d'admission « urgence » en cardio).
CREATE TABLE eds_gold_pilotage.passages_urgences_jour
ENGINE = MergeTree
ORDER BY jour
AS
SELECT
    toDate(admission_ts) AS jour,
    count() AS nb_passages
FROM eds_silver.sejours
WHERE service_code = 'URGENCES'
GROUP BY jour;

-- Réadmission 30 jours : séjour sorti suivi d'une nouvelle admission du même patient ≤ 30 j.
-- Sous-requête INNER JOIN = uniquement les séjours qui ont réellement un successeur
-- (évite le piège join_use_nulls=0 qui remplit les DateTime par 1970-01-01).
CREATE TABLE eds_gold_pilotage.readmission_30j
ENGINE = MergeTree
ORDER BY service_code
AS
SELECT
    s.service_code,
    any(s.service_label) AS service_label,
    count() AS nb_sorties,
    countIf(r.stay_id != '') AS nb_readmissions,
    round(100.0 * countIf(r.stay_id != '') / count(), 2) AS taux_pct
FROM
(
    SELECT stay_id, service_code, service_label
    FROM eds_silver.sejours
    WHERE est_en_cours = 0
) AS s
LEFT JOIN
(
    SELECT
        s1.stay_id
    FROM eds_silver.sejours AS s1
    INNER JOIN eds_silver.sejours AS s2
        ON s1.patient_pseudo = s2.patient_pseudo
    WHERE s1.est_en_cours = 0
      AND s2.stay_id != s1.stay_id
      AND s2.admission_ts > s1.discharge_ts
      AND s2.admission_ts <= addDays(s1.discharge_ts, 30)
    GROUP BY s1.stay_id
) AS r ON s.stay_id = r.stay_id
GROUP BY s.service_code;

-- Bornes d'ALERTE (plus strictes que le rejet qualité 20-250 / 50-100 / 30-45).
-- FC < 50 ou > 120 bpm · SpO2 < 90 % · temp < 36 ou > 38.5 °C
CREATE TABLE eds_gold_pilotage.alertes_monitoring_jour
ENGINE = MergeTree
ORDER BY jour
AS
SELECT
    toDate(ts) AS jour,
    count() AS nb_releves,
    countIf(heart_rate < 50 OR heart_rate > 120) AS nb_alerte_fc,
    countIf(spo2 < 90) AS nb_alerte_spo2,
    countIf(temp_c < 36 OR temp_c > 38.5) AS nb_alerte_temp,
    countIf(
        heart_rate < 50 OR heart_rate > 120
        OR spo2 < 90
        OR temp_c < 36 OR temp_c > 38.5
    ) AS nb_alertes,
    round(100.0 * countIf(
        heart_rate < 50 OR heart_rate > 120
        OR spo2 < 90
        OR temp_c < 36 OR temp_c > 38.5
    ) / count(), 2) AS pct_alertes
FROM eds_silver.monitoring
GROUP BY jour;

CREATE TABLE eds_gold_pilotage.activite_par_service
ENGINE = MergeTree
ORDER BY service_code
AS
SELECT
    service_code,
    any(service_label) AS service_label,
    count() AS nb_admissions,
    countIf(est_en_cours = 1) AS nb_en_cours,
    countIf(admission_mode = 'urgence') AS nb_mode_urgence,
    countIf(discharge_mode = 'deces') AS nb_deces
FROM eds_silver.sejours
GROUP BY service_code;

CREATE TABLE eds_gold_pilotage.qualite_rejets
ENGINE = MergeTree
ORDER BY (domaine, regle)
AS
SELECT
    domaine,
    regle,
    count() AS nb_rejets
FROM eds_silver.rejets
GROUP BY domaine, regle;

-- Recherche : diagnostic PRINCIPAL, effectif masqué si < 5 patients.
CREATE TABLE eds_gold_recherche.prevalence_pathologie
ENGINE = MergeTree
ORDER BY code_cim10
AS
SELECT
    d.code_cim10,
    any(d.libelle) AS libelle,
    uniqExact(s.patient_pseudo) AS nb_patients,
    count() AS nb_sejours
FROM eds_silver.diagnostics AS d
INNER JOIN eds_silver.sejours AS s ON d.stay_id = s.stay_id
WHERE d.type = 'principal'
GROUP BY d.code_cim10
HAVING uniqExact(s.patient_pseudo) >= 5;

CREATE TABLE eds_gold_recherche.cohorte_age_sexe
ENGINE = MergeTree
ORDER BY (tranche_age, sex)
AS
SELECT
    multiIf(
        age_approx < 18, '0-17',
        age_approx < 40, '18-39',
        age_approx < 65, '40-64',
        '65+'
    ) AS tranche_age,
    sex,
    uniqExact(patient_pseudo) AS nb_patients
FROM eds_silver.sejours
WHERE sex IN ('M', 'F')
GROUP BY tranche_age, sex
HAVING uniqExact(patient_pseudo) >= 5;

CREATE TABLE eds_gold_recherche.cohorte_pathologie_age_sexe
ENGINE = MergeTree
ORDER BY (code_cim10, tranche_age, sex)
AS
SELECT
    d.code_cim10,
    any(d.libelle) AS libelle,
    multiIf(
        s.age_approx < 18, '0-17',
        s.age_approx < 40, '18-39',
        s.age_approx < 65, '40-64',
        '65+'
    ) AS tranche_age,
    s.sex,
    uniqExact(s.patient_pseudo) AS nb_patients
FROM eds_silver.diagnostics AS d
INNER JOIN eds_silver.sejours AS s ON d.stay_id = s.stay_id
WHERE d.type = 'principal'
  AND s.sex IN ('M', 'F')
GROUP BY d.code_cim10, tranche_age, s.sex
HAVING uniqExact(s.patient_pseudo) >= 5;

-- Ré-appliquer les droits (DROP TABLE les retire).
GRANT SELECT ON eds_gold_pilotage.* TO pilotage;
GRANT SELECT ON eds_gold_recherche.* TO recherche;
