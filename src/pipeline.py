"""Étape 1 : remplir le lake (copie + anonymisation). Pas de ClickHouse."""
from __future__ import annotations

import argparse
from datetime import date

from src.config import LAKE_ROOT, SOURCE_FILESTORAGE
from src.ingest.copy_to_lake import (
    copy_as_is,
    copy_patients,
    copy_sejours,
    discover_dates,
    source_file,
)
from src.logging_setup import setup_logging

logger = setup_logging("eds.lake")


def ingest_day(source_date: date) -> None:
    logger.info("=== Jour %s ===", source_date)

    src = source_file("patients", source_date)
    if src:
        dest, n = copy_patients(source_date, src)
        logger.info("patients anonymisés → %s (%s lignes)", dest, n)

    src = source_file("sejours", source_date)
    if src:
        dest, n = copy_sejours(source_date, src)
        logger.info("séjours (patient hashé) → %s (%s lignes)", dest, n)

    src = source_file("diagnostics", source_date)
    if src:
        dest, _ = copy_as_is("diagnostics", source_date, src)
        logger.info("diagnostics copiés → %s", dest)

    src = source_file("monitoring", source_date)
    if src:
        dest, _ = copy_as_is("monitoring", source_date, src)
        logger.info("monitoring copié → %s", dest)

    refs = SOURCE_FILESTORAGE / "referentiels" / source_date.isoformat()
    if refs.is_dir():
        for f in sorted(refs.glob("*.csv")):
            dest, _ = copy_as_is("referentiels", source_date, f)
            logger.info("référentiel copié → %s", dest)


def run(dates: list[date] | None = None) -> None:
    logger.info("Source (lecture seule) : %s", SOURCE_FILESTORAGE)
    logger.info("Lake (notre copie)     : %s", LAKE_ROOT)

    todo = dates if dates is not None else discover_dates()
    if not todo:
        logger.warning("Aucune date trouvée dans le filestorage.")
        return

    for source_date in todo:
        ingest_day(source_date)

    logger.info("Terminé. Relancer le script écrase les mêmes fichiers (pas de doublons).")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Copie filestorage → lake avec anonymisation des patients."
    )
    parser.add_argument("--date", type=str, help="Un jour AAAA-MM-JJ (sinon tous)")
    args = parser.parse_args()
    dates = [date.fromisoformat(args.date)] if args.date else None
    run(dates)


if __name__ == "__main__":
    main()
