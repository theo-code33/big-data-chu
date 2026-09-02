-- Gold : uniquement les indicateurs du §4 du sujet.
-- Pilotage : DMS, urgences / jour, réadmission 30 j, relevés en alerte / jour.
-- Recherche : prévalence par pathologie, cohorte âge × sexe (diag principal).
-- RGPD : on ne diffuse pas si l'effectif est strictement inférieur à 5
-- (colonne *_diffusable à NULL, la ligne reste pour contrôle).

DROP TABLE IF EXISTS eds_gold_pilotage.dms_par_service;
DROP TABLE IF EXISTS eds_gold_pilotage.passages_urgences_jour;
DROP TABLE IF EXISTS eds_gold_pilotage.readmission_30j;
DROP TABLE IF EXISTS eds_gold_pilotage.alertes_monitoring_jour;
DROP TABLE IF EXISTS eds_gold_pilotage.activite_par_service;
DROP TABLE IF EXISTS eds_gold_pilotage.qualite_rejets;
DROP TABLE IF EXISTS eds_gold_recherche.prevalence_pathologie;
DROP TABLE IF EXISTS eds_gold_recherche.cohorte_age_sexe;
DROP TABLE IF EXISTS eds_gold_recherche.cohorte_pathologie_age_sexe;

-- DMS par service — séjours SORTIS uniquement (un séjour en cours n'a pas de durée).
CREATE TABLE eds_gold_pilotage.dms_par_service
ENGINE = MergeTree
ORDER BY service_code
AS
SELECT
    s.service_code AS service_code,
    any(svc.service_label) AS service_label,
    count() AS nb_sejours,
    round(avg(s.duree_heures) / 24.0, 2) AS dms_jours,
    round(avg(s.duree_heures), 1) AS dms_heures
FROM eds_silver.fact_sejour AS s
LEFT JOIN eds_silver.dim_service AS svc ON s.service_code = svc.service_code
WHERE s.est_en_cours = 0
GROUP BY s.service_code;

-- Activité des urgences : passages du service URGENCES par jour d'admission.
CREATE TABLE eds_gold_pilotage.passages_urgences_jour
ENGINE = MergeTree
ORDER BY jour
AS
SELECT
    toDate(admission_ts) AS jour,
    count() AS nb_passages,
    countIf(est_en_cours = 1) AS nb_encore_presents,
    round(avg(duree_heures), 1) AS duree_moy_heures
FROM eds_silver.fact_sejour
WHERE service_code = 'URGENCES'
GROUP BY jour;

-- Taux de réadmission à 30 jours (qualité des soins), global.
-- Un séjour est une réadmission s'il existe un séjour antérieur DU MÊME patient
-- sorti, avec nouvelle admission dans les 30 jours après cette sortie.
-- Dénominateur = tous les séjours silver (y compris en cours).
CREATE TABLE eds_gold_pilotage.readmission_30j
ENGINE = MergeTree
ORDER BY tuple()
AS
SELECT
    countIf(est_readmission = 1) AS nb_readmissions_30j,
    count() AS nb_sejours,
    round(100.0 * countIf(est_readmission = 1) / count(), 2) AS taux_readmission_30j_pct
FROM
(
    SELECT
        s.stay_id AS stay_id,
        if(r.stay_id != '', 1, 0) AS est_readmission
    FROM eds_silver.fact_sejour AS s
    LEFT JOIN
    (
        SELECT s2.stay_id AS stay_id
        FROM eds_silver.fact_sejour AS s1
        INNER JOIN eds_silver.fact_sejour AS s2
            ON s1.patient_pseudo = s2.patient_pseudo
        WHERE s1.est_en_cours = 0
          AND s2.stay_id != s1.stay_id
          AND s2.admission_ts > s1.discharge_ts
          AND s2.admission_ts <= addDays(s1.discharge_ts, 30)
        GROUP BY s2.stay_id
    ) AS r ON s.stay_id = r.stay_id
);

-- Relevés en alerte / jour.
-- Seuils d'alerte (plus stricts que le rejet qualité 20-250 / 50-100 / 30-45) :
--   SpO2 < 92 · FC < 50 ou > 100 · T° > 38,5
CREATE TABLE eds_gold_pilotage.alertes_monitoring_jour
ENGINE = MergeTree
ORDER BY jour
AS
SELECT
    toDate(ts) AS jour,
    count() AS nb_releves,
    countIf(
        heart_rate < 50 OR heart_rate > 100
        OR spo2 < 92
        OR temp_c > 38.5
    ) AS nb_alertes,
    round(100.0 * countIf(
        heart_rate < 50 OR heart_rate > 100
        OR spo2 < 92
        OR temp_c > 38.5
    ) / count(), 1) AS taux_alertes_pct
FROM eds_silver.fact_monitoring
GROUP BY jour;

-- Prévalence par pathologie : patients distincts, tout code posé (principal ou associé).
-- RGPD : on ne diffuse pas si nb_patients < 5 (colonne diffusable).
CREATE TABLE eds_gold_recherche.prevalence_pathologie
ENGINE = MergeTree
ORDER BY code_cim10
AS
SELECT
    code_cim10,
    libelle,
    nb_patients,
    if(nb_patients < 5, NULL, nb_patients) AS nb_patients_diffusable
FROM
(
    SELECT
        d.code_cim10 AS code_cim10,
        any(c.libelle) AS libelle,
        uniqExact(d.patient_pseudo) AS nb_patients
    FROM eds_silver.fact_diagnostic AS d
    LEFT JOIN eds_silver.dim_cim10 AS c ON d.code_cim10 = c.code_cim10
    GROUP BY d.code_cim10
) AS agg;

-- Description de cohorte : diagnostic principal × tranche d'âge (10 ans) × sexe.
CREATE TABLE eds_gold_recherche.cohorte_age_sexe
ENGINE = MergeTree
ORDER BY (code_cim10, tranche_age, sex)
AS
SELECT
    code_cim10,
    tranche_age,
    sex,
    nb_patients,
    if(nb_patients < 5, NULL, nb_patients) AS nb_patients_diffusable
FROM
(
    SELECT
        d.code_cim10 AS code_cim10,
        concat(
            toString(intDiv(p.age_approx, 10) * 10),
            '-',
            toString(intDiv(p.age_approx, 10) * 10 + 9)
        ) AS tranche_age,
        p.sex AS sex,
        uniqExact(p.patient_pseudo) AS nb_patients
    FROM eds_silver.fact_diagnostic AS d
    INNER JOIN eds_silver.dim_patient AS p ON d.patient_pseudo = p.patient_pseudo
    WHERE d.type = 'principal'
      AND p.sex IN ('M', 'F')
    GROUP BY code_cim10, tranche_age, sex
) AS agg;

GRANT SELECT ON eds_gold_pilotage.* TO pilotage;
GRANT SELECT ON eds_gold_recherche.* TO recherche;
