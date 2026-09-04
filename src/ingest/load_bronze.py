from __future__ import annotations

from datetime import date


def _file(rel: str, fmt: str, select: str, dest: str, source_date: date) -> str:
    return f"""
INSERT INTO {dest}
SELECT {select}, toDate('{source_date.isoformat()}'), now()
FROM file('{rel}', '{fmt}')
""".strip()


def load_patients(client, source_date: date) -> int:
    rel = f"lake/patients/{source_date.isoformat()}/patients.csv"
    client.command(
        _file(
            rel,
            "CSVWithNames",
            "patient_pseudo, toUInt16(birth_year), sex, region_code",
            "eds_bronze.patients",
            source_date,
        )
    )
    return _count_bronze(client, "patients", source_date)


def load_sejours(client, source_date: date) -> int:
    rel = f"lake/sejours/{source_date.isoformat()}/sejours.csv"
    schema = (
        "stay_id String, patient_pseudo String, service_code String, "
        "admission_ts DateTime, discharge_ts Nullable(DateTime), "
        "admission_mode String, discharge_mode String"
    )
    client.command(
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
    return _count_bronze(client, "sejours", source_date)


def load_diagnostics(client, source_date: date) -> int:
    rel = f"lake/diagnostics/{source_date.isoformat()}/diagnostics.ndjson"
    client.command(
        _file(
            rel,
            "JSONEachRow",
            "stay_id, code_cim10, type",
            "eds_bronze.diagnostics",
            source_date,
        )
    )
    return _count_bronze(client, "diagnostics", source_date)


def load_monitoring(client, source_date: date) -> int:
    """Parquet lu par ClickHouse (`file()`)."""
    rel = f"lake/monitoring/{source_date.isoformat()}/monitoring.parquet"
    client.command(
        _file(
            rel,
            "Parquet",
            "stay_id, ts, heart_rate, spo2, temp_c",
            "eds_bronze.monitoring",
            source_date,
        )
    )
    return _count_bronze(client, "monitoring", source_date)


def load_actes(client, source_date: date) -> int:
    """Parquet lu par ClickHouse (`file()`)."""
    rel = f"lake/actes/{source_date.isoformat()}/actes.parquet"
    client.command(
        _file(
            rel,
            "Parquet",
            "stay_id, code_ccam, acte_ts",
            "eds_bronze.actes",
            source_date,
        )
    )
    return _count_bronze(client, "actes", source_date)


def load_ref_services(client, source_date: date) -> int:
    rel = f"lake/referentiels/{source_date.isoformat()}/services.csv"
    client.command(
        _file(
            rel,
            "CSVWithNames",
            "service_code, service_label",
            "eds_bronze.ref_services",
            source_date,
        )
    )
    result = client.query("SELECT count() FROM eds_bronze.ref_services")
    return int(result.first_row[0])


def load_ref_cim10(client, source_date: date) -> int:
    rel = f"lake/referentiels/{source_date.isoformat()}/cim10.csv"
    client.command(
        _file(
            rel,
            "CSVWithNames",
            "code_cim10, libelle",
            "eds_bronze.ref_cim10",
            source_date,
        )
    )
    result = client.query("SELECT count() FROM eds_bronze.ref_cim10")
    return int(result.first_row[0])


def load_ref_description_service(client, source_date: date) -> int:
    rel = f"lake/referentiels/{source_date.isoformat()}/description_service.csv"
    schema = (
        "service_code String, categorie String, capacite_lits Int32, pole String"
    )
    client.command(
        f"""
INSERT INTO eds_bronze.ref_description_service
SELECT
    service_code,
    categorie,
    capacite_lits,
    pole,
    toDate('{source_date.isoformat()}'),
    now()
FROM file('{rel}', CSVWithNames, '{schema}')
""".strip()
    )
    result = client.query("SELECT count() FROM eds_bronze.ref_description_service")
    return int(result.first_row[0])


def load_ref_ccam(client, source_date: date) -> int:
    rel = f"lake/referentiels/{source_date.isoformat()}/ccam.csv"
    schema = "code_ccam String, libelle String, tarif_euros Int32"
    client.command(
        f"""
INSERT INTO eds_bronze.ref_ccam
SELECT
    code_ccam,
    libelle,
    tarif_euros,
    toDate('{source_date.isoformat()}'),
    now()
FROM file('{rel}', CSVWithNames, '{schema}')
""".strip()
    )
    result = client.query("SELECT count() FROM eds_bronze.ref_ccam")
    return int(result.first_row[0])


def drop_bronze_partition(client, table: str, source_date: date) -> None:
    try:
        client.command(
            f"ALTER TABLE eds_bronze.{table} DROP PARTITION '{source_date.isoformat()}'"
        )
    except Exception as exc:  # noqa: BLE001
        if "partition" not in str(exc).lower() and "No partition" not in str(exc):
            raise


def _count_bronze(client, table: str, source_date: date) -> int:
    result = client.query(
        f"SELECT count() AS n FROM eds_bronze.{table} WHERE _source_date = {{d:Date}}",
        parameters={"d": source_date},
    )
    return int(result.first_row[0])


LOADERS = {
    "patients": ("patients", load_patients),
    "sejours": ("sejours", load_sejours),
    "diagnostics": ("diagnostics", load_diagnostics),
    "monitoring": ("monitoring", load_monitoring),
    "actes": ("actes", load_actes),
}

REF_LOADERS = {
    "services": load_ref_services,
    "cim10": load_ref_cim10,
    "description_service": load_ref_description_service,
    "ccam": load_ref_ccam,
}
