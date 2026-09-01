"""Chargement du lake vers bronze dans ClickHouse."""

from __future__ import annotations

from datetime import date


def _file(rel: str, fmt: str, select: str, dest: str, source_date: date) -> str:
    """Construit une requête INSERT...SELECT FROM file()."""
    return f"""
INSERT INTO {dest}
SELECT {select}, toDate('{source_date.isoformat()}'), now()
FROM file('{rel}', '{fmt}')
""".strip()


def load_patients(client, source_date: date) -> int:
    """Charge patients.csv depuis lake vers bronze."""
    rel = f"lake/patients/{source_date.isoformat()}/patients.csv"
    client.execute(
        _file(
            rel,
            "CSVWithNames",
            "patient_pseudo, toUInt16(birth_year), sex, region_code",
            "eds_bronze.patients",
            source_date,
        )
    )
    return client.execute(
        f"SELECT count() FROM eds_bronze.patients WHERE _source_date = '{source_date.isoformat()}'"
    )[0][0]


def load_sejours(client, source_date: date) -> int:
    """Charge séjours.csv depuis lake vers bronze."""
    rel = f"lake/sejours/{source_date.isoformat()}/sejours.csv"
    schema = (
        "stay_id String, patient_pseudo String, service_code String, "
        "admission_ts DateTime, discharge_ts Nullable(DateTime), "
        "admission_mode String, discharge_mode String"
    )
    client.execute(
        f"""
INSERT INTO eds_bronze.sejours
SELECT
    stay_id,
    patient_pseudo,
    service_code,
    admission_ts,
    discharge_ts,
    admission_mode,
    discharge_mode,
    toDate('{source_date.isoformat()}'),
    now()
FROM file('{rel}', CSVWithNames, '{schema}')
""".strip()
    )
    return client.execute(
        f"SELECT count() FROM eds_bronze.sejours WHERE _source_date = '{source_date.isoformat()}'"
    )[0][0]


def load_diagnostics(client, source_date: date) -> int:
    """Charge diagnostics.ndjson depuis lake vers bronze."""
    rel = f"lake/diagnostics/{source_date.isoformat()}/diagnostics.ndjson"
    client.execute(
        _file(
            rel,
            "JSONEachRow",
            "stay_id, code_cim10, type",
            "eds_bronze.diagnostics",
            source_date,
        )
    )
    return client.execute(
        f"SELECT count() FROM eds_bronze.diagnostics WHERE _source_date = '{source_date.isoformat()}'"
    )[0][0]


def load_monitoring(client, source_date: date) -> int:
    """Charge monitoring.parquet depuis lake vers bronze."""
    rel = f"lake/monitoring/{source_date.isoformat()}/monitoring.parquet"
    client.execute(
        _file(
            rel,
            "Parquet",
            "stay_id, ts, heart_rate, spo2, temp_c",
            "eds_bronze.monitoring",
            source_date,
        )
    )
    return client.execute(
        f"SELECT count() FROM eds_bronze.monitoring WHERE _source_date = '{source_date.isoformat()}'"
    )[0][0]


def load_referentiels(client, source_date: date) -> tuple[int, int]:
    """Charge services.csv et cim10.csv depuis lake vers bronze."""
    counts = 0, 0

    # Services
    rel = f"lake/referentiels/{source_date.isoformat()}/services.csv"
    try:
        client.execute(
            f"""
INSERT INTO eds_bronze.ref_services
SELECT service_code, service_label, toDate('{source_date.isoformat()}'), now()
FROM file('{rel}', CSVWithNames)
""".strip()
        )
        c = client.execute(
            f"SELECT count() FROM eds_bronze.ref_services WHERE _source_date = '{source_date.isoformat()}'"
        )[0][0]
        counts = (c, counts[1])
    except Exception:
        pass  # Ignoré si le fichier n'existe pas

    # CIM-10
    rel = f"lake/referentiels/{source_date.isoformat()}/cim10.csv"
    try:
        client.execute(
            f"""
INSERT INTO eds_bronze.ref_cim10
SELECT code_cim10, libelle, toDate('{source_date.isoformat()}'), now()
FROM file('{rel}', CSVWithNames)
""".strip()
        )
        c = client.execute(
            f"SELECT count() FROM eds_bronze.ref_cim10 WHERE _source_date = '{source_date.isoformat()}'"
        )[0][0]
        counts = (counts[0], c)
    except Exception:
        pass  # Ignoré si le fichier n'existe pas

    return counts
