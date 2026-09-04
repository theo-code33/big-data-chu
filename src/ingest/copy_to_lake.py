"""
Copy src/ingest/copy_to_lake.py — filestorage → lake.

Les patients et séjours sont réécrits sans identifiants directs.
Le JSON diagnostics est aplati en NDJSON (format d'ingestion, pas de règle métier).
Le monitoring Parquet et le flux actes sont recopiés tels quels (pas de PII, volume lu ensuite par ClickHouse).
"""
from __future__ import annotations

import csv
import hashlib
import json
import shutil
from datetime import date
from pathlib import Path

from src.config import LAKE_ROOT, SOURCE_FILESTORAGE
from src.ingest.anonymize import patient_pseudo


def file_checksum(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def lake_path(domain: str, source_date: date, name: str) -> Path:
    return LAKE_ROOT / domain / source_date.isoformat() / name


def _atomic_write_text(dest: Path, content: str) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(dest)


def copy_patients(source_date: date, src: Path) -> tuple[Path, int]:
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
            birth = (row.get("birth_date") or "")[:4]
            writer.writerow(
                {
                    "patient_pseudo": patient_pseudo(row["patient_id"]),
                    "birth_year": birth,
                    "sex": (row.get("sex") or "").strip(),
                    "region_code": (row.get("region_code") or "").strip(),
                }
            )
            n += 1
    tmp.replace(dest)
    return dest, n


def copy_sejours(source_date: date, src: Path) -> tuple[Path, int]:
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
            discharge = (row.get("discharge_ts") or "").strip()
            writer.writerow(
                {
                    "stay_id": row["stay_id"],
                    "patient_pseudo": patient_pseudo(row["patient_id"]),
                    "service_code": row["service_code"],
                    "admission_ts": row["admission_ts"],
                    "discharge_ts": "\\N" if not discharge else discharge,
                    "admission_mode": row.get("admission_mode") or "",
                    "discharge_mode": row.get("discharge_mode") or "",
                }
            )
            n += 1
    tmp.replace(dest)
    return dest, n


def copy_diagnostics(source_date: date, src: Path) -> tuple[Path, int]:
    dest = lake_path("diagnostics", source_date, "diagnostics.ndjson")
    data = json.loads(src.read_text(encoding="utf-8"))
    lines = []
    n = 0
    for item in data:
        stay_id = item["stay_id"]
        for diag in item.get("diagnostics") or []:
            lines.append(
                json.dumps(
                    {
                        "stay_id": stay_id,
                        "code_cim10": diag.get("code_cim10", ""),
                        "type": diag.get("type", ""),
                    },
                    ensure_ascii=False,
                )
            )
            n += 1
    _atomic_write_text(dest, "\n".join(lines) + ("\n" if lines else ""))
    return dest, n


def copy_as_is(domain: str, source_date: date, src: Path) -> tuple[Path, int]:
    dest = lake_path(domain, source_date, src.name)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    return dest, 0


def copy_referentiels(source_date: date, src_dir: Path) -> list[tuple[str, Path, Path, int]]:
    out = []
    for name in ("services.csv", "cim10.csv", "description_service.csv", "ccam.csv"):
        src = src_dir / name
        if src.exists():
            dest, _ = copy_as_is("referentiels", source_date, src)
            with src.open(newline="", encoding="utf-8") as fh:
                n = max(sum(1 for _ in fh) - 1, 0)
            out.append((name.replace(".csv", ""), src, dest, n))
    return out


def discover_dates() -> list[date]:
    """Dates pour lesquelles au moins un domaine métier a déposé un fichier."""
    found: set[date] = set()
    for domain in ("patients", "sejours", "diagnostics", "monitoring", "actes", "referentiels"):
        folder = SOURCE_FILESTORAGE / domain
        if not folder.is_dir():
            continue
        for child in folder.iterdir():
            if child.is_dir():
                try:
                    found.add(date.fromisoformat(child.name))
                except ValueError:
                    continue
    return sorted(found)


def source_file(domain: str, source_date: date) -> Path | None:
    day = SOURCE_FILESTORAGE / domain / source_date.isoformat()
    names = {
        "patients": "patients.csv",
        "sejours": "sejours.csv",
        "diagnostics": "diagnostics.json",
        "monitoring": "monitoring.parquet",
        "actes": "actes.parquet",
    }
    if domain == "referentiels":
        return day if day.is_dir() else None
    path = day / names[domain]
    return path if path.is_file() else None
