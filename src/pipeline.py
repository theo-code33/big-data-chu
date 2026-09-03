from __future__ import annotations

import argparse
import traceback
import uuid
from datetime import date, datetime
from pathlib import Path

from src.config import SQL_DIR, SOURCE_FILESTORAGE
from src.db import get_client
from src.ingest.copy_to_lake import (
    copy_diagnostics,
    copy_patients,
    copy_referentiels,
    copy_sejours,
    copy_as_is,
    discover_dates,
    file_checksum,
    source_file,
)
from src.ingest.load_bronze import (
    LOADERS,
    REF_LOADERS,
    drop_bronze_partition,
)
from src.logging_setup import setup_logging

logger = setup_logging("eds.pipeline")


def split_sql(sql: str) -> list[str]:
    statements: list[str] = []
    buf: list[str] = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        buf.append(line)
        if stripped.endswith(";"):
            stmt = "\n".join(buf).strip().rstrip(";").strip()
            if stmt:
                statements.append(stmt)
            buf = []
    tail = "\n".join(buf).strip().rstrip(";").strip()
    if tail:
        statements.append(tail)
    return statements


def apply_sql_bootstrap(client) -> None:
    """Schémas, ops et bronze uniquement. Silver/gold sont reconstruits après l'ingestion."""
    for name in ("00_init.sql", "01_ops.sql", "02_bronze.sql"):
        path = SQL_DIR / name
        logger.info("SQL %s", path.name)
        for stmt in split_sql(path.read_text(encoding="utf-8")):
            client.command(stmt)


def latest_statut(client, source_path: str) -> str | None:
    result = client.query(
        """
        SELECT statut
        FROM eds_ops.fichiers_traites
        WHERE source_path = {p:String}
        ORDER BY finished_at DESC
        LIMIT 1
        """,
        parameters={"p": source_path},
    )
    if result.row_count == 0:
        return None
    return str(result.first_row[0])


def record_file(
    client,
    source_path: str,
    domaine: str,
    source_date: date,
    checksum: str,
    nb_lignes: int,
    statut: str,
    message: str,
    started_at: datetime,
) -> None:
    client.insert(
        "eds_ops.fichiers_traites",
        [[
            source_path,
            domaine,
            source_date,
            checksum,
            nb_lignes,
            statut,
            message[:2000],
            started_at,
            datetime.now(),
        ]],
        column_names=[
            "source_path",
            "domaine",
            "source_date",
            "checksum",
            "nb_lignes",
            "statut",
            "message",
            "started_at",
            "finished_at",
        ],
    )


def record_run(
    client,
    run_id: str,
    couche: str,
    statut: str,
    message: str,
    started_at: datetime,
    source_date: date | None = None,
) -> None:
    client.insert(
        "eds_ops.runs",
        [[
            run_id,
            started_at,
            datetime.now(),
            source_date,
            couche,
            statut,
            message[:2000],
        ]],
        column_names=[
            "run_id",
            "started_at",
            "finished_at",
            "source_date",
            "couche",
            "statut",
            "message",
        ],
    )


def process_domain(
    client,
    domaine: str,
    source_date: date,
    force: bool,
    retry_errors: bool,
) -> bool:
    """Retourne True si une ingestion bronze a eu lieu (ou une erreur)."""
    src = source_file(domaine, source_date)
    if src is None:
        logger.info("Pas de fichier %s pour %s — skip", domaine, source_date)
        return False

    if domaine == "referentiels":
        return _process_referentiels(client, source_date, src, force, retry_errors)

    src_str = str(src)
    statut = latest_statut(client, src_str)
    if statut == "ok" and not force:
        logger.info("Déjà traité %s %s — skip", domaine, source_date)
        return False
    if statut == "erreur" and not retry_errors and not force:
        logger.warning("En erreur %s %s — utiliser --retry-errors", domaine, source_date)
        return False

    started = datetime.now()
    checksum = file_checksum(src) if src.is_file() else ""
    try:
        if domaine == "patients":
            dest, n_copy = copy_patients(source_date, src)
        elif domaine == "sejours":
            dest, n_copy = copy_sejours(source_date, src)
        elif domaine == "diagnostics":
            dest, n_copy = copy_diagnostics(source_date, src)
        elif domaine == "monitoring":
            dest, n_copy = copy_as_is("monitoring", source_date, src)
        elif domaine == "actes":
            dest, n_copy = copy_as_is("actes", source_date, src)
        else:
            raise ValueError(domaine)

        table, loader = LOADERS[domaine]
        if force and statut == "ok":
            drop_bronze_partition(client, table, source_date)
        n_load = loader(client, source_date)
        n = n_load or n_copy
        record_file(client, src_str, domaine, source_date, checksum, n, "ok", f"lake={dest}", started)
        logger.info("OK %s %s (%s lignes)", domaine, source_date, n)
        return True
    except Exception as exc:
        logger.exception("ERREUR %s %s", domaine, source_date)
        record_file(
            client,
            src_str,
            domaine,
            source_date,
            checksum,
            0,
            "erreur",
            f"{exc}\n{traceback.format_exc()}",
            started,
        )
        return True


def _process_referentiels(client, source_date: date, src_dir: Path, force: bool, retry_errors: bool) -> bool:
    did = False
    for name, src, dest, n_copy in copy_referentiels(source_date, src_dir):
        src_str = str(src)
        statut = latest_statut(client, src_str)
        if statut == "ok" and not force:
            logger.info("Déjà traité referentiel %s — skip", name)
            continue
        if statut == "erreur" and not retry_errors and not force:
            continue
        started = datetime.now()
        checksum = file_checksum(src)
        try:
            loader = REF_LOADERS[name]
            n = loader(client, source_date)
            record_file(client, src_str, "referentiels", source_date, checksum, n or n_copy, "ok", f"lake={dest}", started)
            logger.info("OK referentiel %s (%s lignes)", name, n or n_copy)
            did = True
        except Exception as exc:
            logger.exception("ERREUR referentiel %s", name)
            record_file(client, src_str, "referentiels", source_date, checksum, 0, "erreur", str(exc), started)
            did = True
    return did


def rebuild_curated(client, run_id: str) -> None:
    started = datetime.now()
    try:
        silver = SQL_DIR / "03_silver.sql"
        gold = SQL_DIR / "04_gold.sql"
        for path in (silver, gold):
            logger.info("Reconstruction %s", path.name)
            for stmt in split_sql(path.read_text(encoding="utf-8")):
                client.command(stmt)
        record_run(client, run_id, "silver_gold", "ok", "rebuild", started)
        logger.info("Silver + gold reconstruits")
    except Exception:
        logger.exception("Échec reconstruction silver/gold")
        record_run(client, run_id, "silver_gold", "erreur", traceback.format_exc(), started)
        raise


def run(
    dates: list[date] | None = None,
    force: bool = False,
    retry_errors: bool = False,
    bronze_only: bool = False,
) -> None:
    run_id = uuid.uuid4().hex[:12]
    started_all = datetime.now()
    logger.info("Run %s — source=%s", run_id, SOURCE_FILESTORAGE)
    client = get_client()
    apply_sql_bootstrap(client)

    todo = dates if dates is not None else discover_dates()
    if not todo:
        logger.warning("Aucune date découverte dans %s", SOURCE_FILESTORAGE)
        record_run(client, run_id, "pipeline", "ok", "aucune date", started_all)
        return

    for source_date in todo:
        logger.info("=== Jour %s ===", source_date)
        for domaine in ("referentiels", "patients", "sejours", "diagnostics", "monitoring", "actes"):
            process_domain(client, domaine, source_date, force, retry_errors)

    if not bronze_only:
        rebuild_curated(client, run_id)
    record_run(client, run_id, "pipeline", "ok", f"dates={','.join(d.isoformat() for d in todo)}", started_all)
    logger.info("Run %s terminé", run_id)


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline EDS CHU (incrémental)")
    parser.add_argument("--all", action="store_true", help="Toutes les dates du filestorage")
    parser.add_argument("--date", type=str, help="Une date AAAA-MM-JJ")
    parser.add_argument("--force", action="store_true", help="Ré-ingérer même si déjà ok")
    parser.add_argument("--retry-errors", action="store_true", help="Rejouer les fichiers en erreur")
    parser.add_argument(
        "--bronze-only",
        action="store_true",
        help="Ingestion bronze sans reconstruire silver/gold",
    )
    args = parser.parse_args()

    if args.date:
        dates = [date.fromisoformat(args.date)]
    else:
        dates = None
        if not args.all:
            # défaut pédagogique : tout traiter, en skippant l'already-ok
            dates = None
    run(dates=dates, force=args.force, retry_errors=args.retry_errors, bronze_only=args.bronze_only)


if __name__ == "__main__":
    main()
