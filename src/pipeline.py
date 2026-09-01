"""Pipeline EDS : Lake → Bronze → Silver."""

from __future__ import annotations

import argparse
from datetime import date

from src.config import LAKE_ROOT, SOURCE_FILESTORAGE
from src.db import get_client
from src.ingest.build_silver import build_silver
from src.ingest.copy_to_lake import (
    copy_as_is,
    copy_diagnostics,
    copy_patients,
    copy_sejours,
    discover_dates,
    source_file,
)
from src.ingest.load_bronze import (
    load_diagnostics,
    load_monitoring,
    load_patients,
    load_referentiels,
    load_sejours,
)
from src.logging_setup import setup_logging

logger = setup_logging("eds.pipeline")


def ingest_lake(source_date: date) -> None:
    """Étape 1 : filestorage → lake (anonymisation des patients)."""
    logger.info("=== LAKE : %s ===", source_date)

    src = source_file("patients", source_date)
    if src:
        dest, n = copy_patients(source_date, src)
        logger.info("  patients anonymisés → %s (%s lignes)", dest, n)

    src = source_file("sejours", source_date)
    if src:
        dest, n = copy_sejours(source_date, src)
        logger.info("  séjours (patient hashé) → %s (%s lignes)", dest, n)

    src = source_file("diagnostics", source_date)
    if src:
        dest, n = copy_diagnostics(source_date, src)
        logger.info("  diagnostics transformés JSON→NDJSON → %s (%s lignes)", dest, n)

    src = source_file("monitoring", source_date)
    if src:
        dest, _ = copy_as_is("monitoring", source_date, src)
        logger.info("  monitoring copié → %s", dest)

    refs = SOURCE_FILESTORAGE / "referentiels" / source_date.isoformat()
    if refs.is_dir():
        for f in sorted(refs.glob("*.csv")):
            dest, _ = copy_as_is("referentiels", source_date, f)
            logger.info("  référentiel copié → %s", dest)


def ingest_bronze(client, source_date: date) -> None:
    """Étape 2 : lake → bronze (chargement ClickHouse)."""
    logger.info("=== BRONZE : %s ===", source_date)

    src = source_file("patients", source_date)
    if src:
        n = load_patients(client, source_date)
        logger.info("  patients chargés → eds_bronze (%s lignes)", n)

    src = source_file("sejours", source_date)
    if src:
        n = load_sejours(client, source_date)
        logger.info("  séjours chargés → eds_bronze (%s lignes)", n)

    src = source_file("diagnostics", source_date)
    if src:
        n = load_diagnostics(client, source_date)
        logger.info("  diagnostics chargés → eds_bronze (%s lignes)", n)

    src = source_file("monitoring", source_date)
    if src:
        n = load_monitoring(client, source_date)
        logger.info("  monitoring chargé → eds_bronze (%s lignes)", n)

    # Référentiels (une seule fois généralement)
    services, cim10 = load_referentiels(client, source_date)
    if services > 0:
        logger.info("  services chargés → eds_bronze (%s lignes)", services)
    if cim10 > 0:
        logger.info("  CIM-10 chargés → eds_bronze (%s lignes)", cim10)


def run(dates: list[date] | None = None, step: str = "all") -> None:
    """Exécute le pipeline EDS.

    Args:
        dates: Dates à traiter (sinon toutes les dates trouvées).
        step: "lake", "bronze", "silver", ou "all" (par défaut).
    """
    logger.info("Source (lecture seule) : %s", SOURCE_FILESTORAGE)
    logger.info("Lake (notre copie)     : %s", LAKE_ROOT)

    todo = dates if dates is not None else discover_dates()
    if not todo:
        logger.warning("Aucune date trouvée dans le filestorage.")
        return

    # Connexion ClickHouse (sauf pour lake)
    client = None
    if step != "lake":
        client = get_client()
        logger.info("Connecté à ClickHouse")

    # Étape 1 : Lake
    if step in ("lake", "all"):
        logger.info("\n▶ ÉTAPE 1 : LAKE (anonymisation)")
        for source_date in todo:
            ingest_lake(source_date)
        logger.info("Lake complété.\n")

    # Étape 2 : Bronze
    if step in ("bronze", "all"):
        if client is None:
            client = get_client()
        logger.info("▶ ÉTAPE 2 : BRONZE (chargement ClickHouse)")
        for source_date in todo:
            ingest_bronze(client, source_date)
        logger.info("Bronze complété.\n")

    # Étape 3 : Silver
    if step in ("silver", "all"):
        if client is None:
            client = get_client()
        logger.info("▶ ÉTAPE 3 : SILVER (transformation & modèle en étoile)")
        build_silver(client)
        logger.info("Silver complété.\n")

    logger.info("✓ Pipeline terminé.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline EDS : Lake → Bronze → Silver"
    )
    parser.add_argument("--date", type=str, help="Un jour AAAA-MM-JJ (sinon tous)")
    parser.add_argument(
        "--step",
        choices=["lake", "bronze", "silver", "all"],
        default="all",
        help="Étape à exécuter (défaut: all)",
    )
    args = parser.parse_args()
    dates = [date.fromisoformat(args.date)] if args.date else None
    run(dates, step=args.step)


if __name__ == "__main__":
    main()
