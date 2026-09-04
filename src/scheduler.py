"""Boucle d'orchestration : surveille le filestorage et ingère les nouveaux dépôts."""
from __future__ import annotations

import argparse
import os
import signal
import threading
from datetime import datetime

from src.logging_setup import setup_logging
from src.pipeline import run

logger = setup_logging("eds.scheduler")


def run_scheduler(interval: int, retry_errors: bool = False, once: bool = False) -> None:
    stop_event = threading.Event()

    def handle_signal(sig, _frame):
        logger.info("Signal reçu (%s) — arrêt propre du scheduler...", sig)
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info(
        "Scheduler démarré [intervalle: %ds, retry_errors: %s, mode: %s]",
        interval,
        retry_errors,
        "once" if once else "daemon",
    )

    cycle = 0
    while not stop_event.is_set():
        cycle += 1
        logger.info(
            "--- Cycle scheduler #%d (%s) ---",
            cycle,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        try:
            # En mode scheduler : ne pas reconstruire silver/gold s'il n'y a aucun nouveau fichier
            n = run(force=False, retry_errors=retry_errors, rebuild_if_no_changes=False)
            if n > 0:
                logger.info(
                    "Cycle #%d : %d fichier(s) ingéré(s) et modèles reconstruits.",
                    cycle,
                    n,
                )
            else:
                logger.info(
                    "Cycle #%d : aucun nouveau fichier. En attente du prochain dépôt.",
                    cycle,
                )
        except Exception:
            logger.exception(
                "Échec du cycle scheduler #%d — nouvelle tentative au prochain cycle",
                cycle,
            )

        if once:
            logger.info("Mode --once : fin d'exécution.")
            break

        # Attente interruptible jusqu'au prochain cycle ou jusqu'à un signal d'arrêt
        stop_event.wait(timeout=interval)

    logger.info("Scheduler arrêté.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Scheduler d'orchestration EDS CHU")
    parser.add_argument(
        "--interval",
        type=int,
        default=int(os.getenv("SCHEDULER_INTERVAL_SEC", "60")),
        help="Intervalle entre chaque vérification en secondes (défaut: 60 ou $SCHEDULER_INTERVAL_SEC)",
    )
    parser.add_argument(
        "--retry-errors",
        action="store_true",
        default=os.getenv("SCHEDULER_RETRY_ERRORS", "false").lower() in ("1", "true", "yes"),
        help="Rejouer automatiquement les fichiers en erreur",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Exécuter un seul cycle d'orchestration puis quitter (mode batch/cron)",
    )
    args = parser.parse_args()

    run_scheduler(interval=args.interval, retry_errors=args.retry_errors, once=args.once)


if __name__ == "__main__":
    main()
