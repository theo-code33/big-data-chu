"""Étape 1 — filestorage CHU (lecture seule) → lake.

Règle simple :
- patients et séjours : on réécrit le CSV **sans identité** (hash + année de naissance)
- le reste : copie telle quelle (pas de PII)
"""
from __future__ import annotations

import csv
import shutil
from datetime import date
from pathlib import Path

from src.config import LAKE_ROOT, SOURCE_FILESTORAGE
from src.ingest.anonymize import patient_pseudo


def lake_path(domain: str, source_date: date, name: str) -> Path:
    return LAKE_ROOT / domain / source_date.isoformat() / name


def copy_patients(source_date: date, src: Path) -> tuple[Path, int]:
    """Supprime nir/nom/prénom, généralise la date → année, hashe l'IPP."""
    dest = lake_path("patients", source_date, "patients.csv")
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".tmp")
    n = 0
    with src.open(newline="", encoding="utf-8") as fin, tmp.open(
        "w", newline="", encoding="utf-8"
    ) as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(
            fout, fieldnames=["patient_pseudo", "birth_year", "sex", "region_code"]
        )
        writer.writeheader()
        for row in reader:
            writer.writerow(
                {
                    "patient_pseudo": patient_pseudo(row["patient_id"]),
                    "birth_year": (row.get("birth_date") or "")[:4],
                    "sex": (row.get("sex") or "").strip(),
                    "region_code": (row.get("region_code") or "").strip(),
                }
            )
            n += 1
    tmp.replace(dest)
    return dest, n


def copy_sejours(source_date: date, src: Path) -> tuple[Path, int]:
    """Même hash que les patients, pour que les jointures restent possibles plus tard."""
    dest = lake_path("sejours", source_date, "sejours.csv")
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".tmp")
    n = 0
    with src.open(newline="", encoding="utf-8") as fin, tmp.open(
        "w", newline="", encoding="utf-8"
    ) as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(
            fout,
            fieldnames=[
                "stay_id",
                "patient_pseudo",
                "service_code",
                "admission_ts",
                "discharge_ts",
                "admission_mode",
                "discharge_mode",
            ],
        )
        writer.writeheader()
        for row in reader:
            writer.writerow(
                {
                    "stay_id": row["stay_id"],
                    "patient_pseudo": patient_pseudo(row["patient_id"]),
                    "service_code": row["service_code"],
                    "admission_ts": row["admission_ts"],
                    "discharge_ts": (row.get("discharge_ts") or "").strip(),
                    "admission_mode": row.get("admission_mode") or "",
                    "discharge_mode": row.get("discharge_mode") or "",
                }
            )
            n += 1
    tmp.replace(dest)
    return dest, n


def copy_as_is(domain: str, source_date: date, src: Path) -> tuple[Path, int]:
    dest = lake_path(domain, source_date, src.name)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    return dest, 0


def discover_dates() -> list[date]:
    found: set[date] = set()
    for domain in ("patients", "sejours", "diagnostics", "monitoring"):
        folder = SOURCE_FILESTORAGE / domain
        if not folder.is_dir():
            continue
        for child in folder.iterdir():
            if child.is_dir():
                found.add(date.fromisoformat(child.name))
    return sorted(found)


def source_file(domain: str, source_date: date) -> Path | None:
    day = SOURCE_FILESTORAGE / domain / source_date.isoformat()
    names = {
        "patients": "patients.csv",
        "sejours": "sejours.csv",
        "diagnostics": "diagnostics.json",
        "monitoring": "monitoring.parquet",
    }
    if domain == "referentiels":
        return day if day.is_dir() else None
    path = day / names[domain]
    return path if path.is_file() else None
