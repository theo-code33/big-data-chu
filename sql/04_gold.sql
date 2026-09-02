-- Gold : uniquement les indicateurs du §4 du sujet.
-- Pilotage : DMS, urgences / jour, réadmission 30 j, relevés en alerte / jour.
-- Recherche : prévalence par pathologie, distribution âge × sexe (n < 5 masqué).

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
    count() AS nb_sejours_sortis,
    round(avg(s.duree_heures) / 24.0, 2) AS dms_jours
FROM eds_silver.fact_sejour AS s
LEFT JOIN eds_silver.dim_service AS svc ON s.service_code = svc.service_code
WHERE s.est_en_cours = 0
GROUP BY s.service_code;

-- Activité des urgences : passages par jour = séjours du service URGENCES.
CREATE TABLE eds_gold_pilotage.passages_urgences_jour
ENGINE = MergeTree
ORDER BY jour
AS
SELECT
    toDate(admission_ts) AS jour,
    count() AS nb_passages
FROM eds_silver.fact_sejour
WHERE service_code = 'URGENCES'
GROUP BY jour;

-- Taux de réadmission à 30 jours (qualité des soins), par service de la sortie index.
CREATE TABLE eds_gold_pilotage.readmission_30j
ENGINE = MergeTree
ORDER BY service_code
AS
SELECT
    agg.service_code AS service_code,
    svc.service_label AS service_label,
    agg.nb_sorties AS nb_sorties,
    agg.nb_readmissions AS nb_readmissions,
    round(100.0 * agg.nb_readmissions / agg.nb_sorties, 2) AS taux_pct
FROM
(
    SELECT
        s.service_code AS service_code,
        count() AS nb_sorties,
        countIf(r.stay_id != '') AS nb_readmissions
    FROM
    (
        SELECT stay_id, service_code
        FROM eds_silver.fact_sejour
        WHERE est_en_cours = 0
    ) AS s
    LEFT JOIN
    (
        SELECT s1.stay_id
        FROM eds_silver.fact_sejour AS s1
        INNER JOIN eds_silver.fact_sejour AS s2
            ON s1.patient_pseudo = s2.patient_pseudo
        WHERE s1.est_en_cours = 0
          AND s2.stay_id != s1.stay_id
          AND s2.admission_ts > s1.discharge_ts
          AND s2.admission_ts <= addDays(s1.discharge_ts, 30)
        GROUP BY s1.stay_id
    ) AS r ON s.stay_id = r.stay_id
    GROUP BY s.service_code
) AS agg
LEFT JOIN eds_silver.dim_service AS svc ON agg.service_code = svc.service_code;

-- Relevés en alerte / jour.
-- Alerte (plus strict que le rejet qualité 20-250 / 50-100 / 30-45) :
--   FC < 50 ou > 120 · SpO2 < 90 · temp < 36 ou > 38.5
CREATE TABLE eds_gold_pilotage.alertes_monitoring_jour
ENGINE = MergeTree
ORDER BY jour
AS
SELECT
    toDate(ts) AS jour,
    count() AS nb_releves,
    countIf(
        heart_rate < 50 OR heart_rate > 120
        OR spo2 < 90
        OR temp_c < 36 OR temp_c > 38.5
    ) AS nb_alertes
FROM eds_silver.fact_monitoring
GROUP BY jour;

-- Prévalence par pathologie : taille de cohorte (diagnostic PRINCIPAL), n < 5 masqué.
CREATE TABLE eds_gold_recherche.prevalence_pathologie
ENGINE = MergeTree
ORDER BY code_cim10
AS
SELECT
    code_cim10,
    libelle,
    nb_patients,
    nb_sejours
FROM
(
    SELECT
        d.code_cim10 AS code_cim10,
        any(c.libelle) AS libelle,
        uniqExact(d.patient_pseudo) AS nb_patients,
        count() AS nb_sejours
    FROM eds_silver.fact_diagnostic AS d
    LEFT JOIN eds_silver.dim_cim10 AS c ON d.code_cim10 = c.code_cim10
    WHERE d.type = 'principal'
    GROUP BY d.code_cim10
) AS agg
-- RGPD : on ne diffuse pas si l'effectif est strictement inférieur à 5.
WHERE NOT (nb_patients < 5);

-- Description de cohorte : distribution par âge et sexe (patients ayant au moins un séjour).
CREATE TABLE eds_gold_recherche.cohorte_age_sexe
ENGINE = MergeTree
ORDER BY (tranche_age, sex)
AS
SELECT
    tranche_age,
    sex,
    nb_patients
FROM
(
    SELECT
        multiIf(
            p.age_approx < 18, '0-17',
            p.age_approx < 40, '18-39',
            p.age_approx < 65, '40-64',
            '65+'
        ) AS tranche_age,
        p.sex AS sex,
        uniqExact(p.patient_pseudo) AS nb_patients
    FROM eds_silver.fact_sejour AS s
    INNER JOIN eds_silver.dim_patient AS p ON s.patient_pseudo = p.patient_pseudo
    WHERE p.sex IN ('M', 'F')
    GROUP BY tranche_age, sex
) AS agg
-- RGPD : on ne diffuse pas si l'effectif est strictement inférieur à 5.
WHERE NOT (nb_patients < 5);

GRANT SELECT ON eds_gold_pilotage.* TO pilotage;
GRANT SELECT ON eds_gold_recherche.* TO recherche;
