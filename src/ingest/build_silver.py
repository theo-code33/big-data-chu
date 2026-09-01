"""Transformation bronze → silver."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from clickhouse_driver import Client

logger = logging.getLogger(__name__)


def build_silver(client: Client) -> None:
    """Reconstruit les tables silver depuis bronze.

    Étapes :
    1. Rejeter les anomalies (patients/séjours/diagnostics invalides, monitoring hors plage)
    2. Construire dim_patient (dédup, année de naissance, calcul âge)
    3. Construire dim_service et dim_cim10 depuis les référentiels
    4. Construire fact_sejour (jointure bronze, calcul durée, flaguer en cours)
    5. Construire fact_diagnostic (jointure bronze.diagnostics + sejours valides)
    6. Construire fact_monitoring (filtrer par plages physiologiques)
    """

    # ============================================================================
    # REJETS - Patients invalides (sexe ou naissance)
    # ============================================================================
    logger.info("Rejet patients invalides (sexe/naissance)...")
    client.execute("""
        INSERT INTO eds_silver.rejets (domaine, regle, stay_id, patient_pseudo, source_date, detail)
        SELECT
            'patients' AS domaine,
            'sexe_ou_naissance_invalide' AS regle,
            '' AS stay_id,
            patient_pseudo,
            _source_date,
            concat('sex=', sex, ' birth_year=', toString(birth_year)) AS detail
        FROM eds_bronze.patients
        WHERE upper(trim(sex)) NOT IN ('M', 'F')
           OR birth_year < 1900
           OR birth_year > toYear(today())
    """)

    # ============================================================================
    # DIM_PATIENT - Déduplication (garder version la plus récente)
    # ============================================================================
    logger.info("Construction dim_patient (déduplication)...")
    client.execute("""
        INSERT INTO eds_silver.dim_patient
        SELECT
            patient_pseudo,
            birth_year,
            upper(trim(sex)) AS sex,
            region_code,
            toInt16(toYear(today()) - birth_year) AS age_approx,
            _source_date AS source_date
        FROM
        (
            SELECT
                *,
                row_number() OVER (PARTITION BY patient_pseudo ORDER BY _source_date DESC) AS rn
            FROM eds_bronze.patients
            WHERE upper(trim(sex)) IN ('M', 'F')
              AND birth_year >= 1900
              AND birth_year <= toYear(today())
        )
        WHERE rn = 1
    """)

    # ============================================================================
    # DIM_SERVICE et DIM_CIM10 - Référentiels
    # ============================================================================
    logger.info("Construction dim_service...")
    client.execute("""
        INSERT INTO eds_silver.dim_service
        SELECT
            service_code,
            argMax(service_label, _ingested_at) AS service_label
        FROM eds_bronze.ref_services
        GROUP BY service_code
    """)

    logger.info("Construction dim_cim10...")
    client.execute("""
        INSERT INTO eds_silver.dim_cim10
        SELECT
            code_cim10,
            argMax(libelle, _ingested_at) AS libelle
        FROM eds_bronze.ref_cim10
        GROUP BY code_cim10
    """)

    # ============================================================================
    # REJETS - Séjours invalides (discharge < admission)
    # ============================================================================
    logger.info("Rejet séjours invalides (temporalité)...")
    client.execute("""
        INSERT INTO eds_silver.rejets (domaine, regle, stay_id, patient_pseudo, source_date, detail)
        SELECT
            'sejours' AS domaine,
            'discharge_avant_admission' AS regle,
            stay_id,
            patient_pseudo,
            _source_date,
            concat('admission=', toString(admission_ts), ' discharge=', toString(discharge_ts)) AS detail
        FROM eds_bronze.sejours
        WHERE discharge_ts IS NOT NULL
          AND discharge_ts < admission_ts
    """)

    # ============================================================================
    # FACT_SEJOUR
    # ============================================================================
    logger.info("Construction fact_sejour...")
    client.execute("""
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
           OR discharge_ts >= admission_ts
    """)

    # ============================================================================
    # FACT_DIAGNOSTIC - Jointure bronze.diagnostics + séjours valides
    # ============================================================================
    logger.info("Construction fact_diagnostic...")
    client.execute("""
        INSERT INTO eds_silver.fact_diagnostic
        SELECT
            d.stay_id,
            s.patient_pseudo,
            d.code_cim10,
            d.type,
            d._source_date AS source_date
        FROM eds_bronze.diagnostics AS d
        INNER JOIN eds_bronze.sejours AS s ON d.stay_id = s.stay_id
        WHERE s.discharge_ts IS NULL
           OR s.discharge_ts >= s.admission_ts
    """)

    logger.info("Rejet diagnostics (séjour invalide)...")
    client.execute("""
        INSERT INTO eds_silver.rejets (domaine, regle, stay_id, patient_pseudo, source_date, detail)
        SELECT
            'diagnostics' AS domaine,
            'sejour_invalide' AS regle,
            d.stay_id,
            '' AS patient_pseudo,
            d._source_date,
            concat('code=', d.code_cim10, ' type=', d.type) AS detail
        FROM eds_bronze.diagnostics AS d
        LEFT ANTI JOIN
        (
            SELECT stay_id
            FROM eds_bronze.sejours
            WHERE discharge_ts IS NULL
               OR discharge_ts >= admission_ts
        ) AS v ON d.stay_id = v.stay_id
    """)

    # ============================================================================
    # FACT_MONITORING - Filtrer par plages physiologiques
    # ============================================================================
    logger.info("Rejet monitoring (valeurs hors plage)...")
    client.execute("""
        INSERT INTO eds_silver.rejets (domaine, regle, stay_id, patient_pseudo, source_date, detail)
        SELECT
            'monitoring' AS domaine,
            'valeur_hors_plage' AS regle,
            stay_id,
            '' AS patient_pseudo,
            _source_date,
            concat('hr=', toString(heart_rate), ' spo2=', toString(spo2), ' temp=', toString(temp_c)) AS detail
        FROM eds_bronze.monitoring
        WHERE heart_rate < 20 OR heart_rate > 250
           OR spo2 < 50 OR spo2 > 100
           OR temp_c < 30 OR temp_c > 45
    """)

    logger.info("Construction fact_monitoring...")
    client.execute("""
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
          AND temp_c BETWEEN 30 AND 45
    """)

    logger.info("Silver rebuilt successfully.")
