"""Boucle d'orchestration : rejoue le pipeline dès qu'un nouveau jour apparaît."""
from __future__ import annotations

import os
import time

from src.logging_setup import setup_logging
from src.pipeline import run

logger = setup_logging("eds.scheduler")


def main() -> None:
    interval = int(os.getenv("SCHEDULER_INTERVAL_SEC", "60"))
    logger.info("Scheduler démarré (toutes les %s s)", interval)
    while True:
        try:
            run()
        except Exception:
            logger.exception("Échec d'un cycle scheduler — on réessaiera")
        time.sleep(interval)


if __name__ == "__main__":
    main()
