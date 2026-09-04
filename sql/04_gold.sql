-- Gold : KPI §4 + évolution 2026-08-29 (catégorie, actes, T2A).
-- Pilotage : DMS, urgences, réadmission, alertes, puis actes / lits / tarifs.
-- Recherche : prévalence, cohorte âge × sexe (diag principal).
-- RGPD : on ne diffuse pas si l'effectif est strictement inférieur à 5
-- (colonne *_diffusable à NULL, la ligne reste pour contrôle).

DROP TABLE IF EXISTS eds_gold_pilotage.dms_par_service;
DROP TABLE IF EXISTS eds_gold_pilotage.passages_urgences_jour;
DROP TABLE IF EXISTS eds_gold_pilotage.readmission_30j;
DROP TABLE IF EXISTS eds_gold_pilotage.alertes_monitoring_jour;
DROP TABLE IF EXISTS eds_gold_pilotage.activite_par_service;
DROP TABLE IF EXISTS eds_gold_pilotage.qualite_rejets;
DROP TABLE IF EXISTS eds_gold_pilotage.dms_par_categorie;
DROP TABLE IF EXISTS eds_gold_pilotage.actes_par_service;
DROP TABLE IF EXISTS eds_gold_pilotage.actes_par_type;
DROP TABLE IF EXISTS eds_gold_pilotage.densite_actes_par_lit;
DROP TABLE IF EXISTS eds_gold_pilotage.montant_t2a_par_service;
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

-- Évolution 2026-08-29 — KPI 1 : activité et DMS par catégorie de service.
-- NEURO sans description → categorie = 'non renseigne' (pas d'imputation métier).
CREATE TABLE eds_gold_pilotage.dms_par_categorie
ENGINE = MergeTree
ORDER BY categorie
AS
SELECT
    assumeNotNull(if(svc.categorie IS NULL OR svc.categorie = '', 'non renseigne', svc.categorie)) AS categorie,
    count() AS nb_sejours,
    countIf(s.est_en_cours = 0) AS nb_sejours_sortis,
    round(avgIf(s.duree_heures, s.est_en_cours = 0) / 24.0, 2) AS dms_jours,
    round(avgIf(s.duree_heures, s.est_en_cours = 0), 1) AS dms_heures
FROM eds_silver.fact_sejour AS s
LEFT JOIN eds_silver.dim_service AS svc ON s.service_code = svc.service_code
GROUP BY categorie;

-- KPI 2 : actes par service + moyenne par séjour (séjours qui ont au moins un acte).
-- Service recopié sur fact_acte : pas de join fact_acte ⋈ fact_sejour.
CREATE TABLE eds_gold_pilotage.actes_par_service
ENGINE = MergeTree
ORDER BY service_code
AS
SELECT
    a.service_code AS service_code,
    any(svc.service_label) AS service_label,
    count() AS nb_actes,
    uniqExact(a.stay_id) AS nb_sejours_avec_acte,
    round(count() / uniqExact(a.stay_id), 2) AS nb_actes_moyen_par_sejour
FROM eds_silver.fact_acte AS a
LEFT JOIN eds_silver.dim_service AS svc ON a.service_code = svc.service_code
GROUP BY a.service_code;

-- KPI 3 : répartition par type d'acte (les plus fréquents).
CREATE TABLE eds_gold_pilotage.actes_par_type
ENGINE = MergeTree
ORDER BY code_ccam
AS
SELECT
    a.code_ccam AS code_ccam,
    any(c.libelle) AS libelle,
    count() AS nb_actes
FROM eds_silver.fact_acte AS a
LEFT JOIN eds_silver.dim_ccam AS c ON a.code_ccam = c.code_ccam
GROUP BY a.code_ccam;

-- KPI 4 : densité d'actes par lit. Capacité NULL (NEURO) → densité NULL, pas 0.
CREATE TABLE eds_gold_pilotage.densite_actes_par_lit
ENGINE = MergeTree
ORDER BY service_code
AS
SELECT
    a.service_code AS service_code,
    any(svc.service_label) AS service_label,
    any(svc.capacite_lits) AS capacite_lits,
    count() AS nb_actes,
    if(
        any(svc.capacite_lits) IS NULL OR any(svc.capacite_lits) = 0,
        NULL,
        round(count() / any(svc.capacite_lits), 2)
    ) AS actes_par_lit
FROM eds_silver.fact_acte AS a
LEFT JOIN eds_silver.dim_service AS svc ON a.service_code = svc.service_code
GROUP BY a.service_code;

-- KPI 5 : montant facturé T2A par service (somme des tarifs des actes).
CREATE TABLE eds_gold_pilotage.montant_t2a_par_service
ENGINE = MergeTree
ORDER BY service_code
AS
SELECT
    a.service_code AS service_code,
    any(svc.service_label) AS service_label,
    count() AS nb_actes,
    sum(c.tarif_euros) AS montant_euros
FROM eds_silver.fact_acte AS a
LEFT JOIN eds_silver.dim_service AS svc ON a.service_code = svc.service_code
LEFT JOIN eds_silver.dim_ccam AS c ON a.code_ccam = c.code_ccam
GROUP BY a.service_code;

GRANT SELECT ON eds_gold_pilotage.* TO pilotage;
GRANT SELECT ON eds_gold_recherche.* TO recherche;
