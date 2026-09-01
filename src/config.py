from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


def _path(env_name: str, default: Path) -> Path:
    raw = os.getenv(env_name)
    if not raw:
        return default.resolve()
    p = Path(raw)
    return p.resolve() if p.is_absolute() else (ROOT / p).resolve()


# Filestorage CHU = lecture seule. Lake = notre copie de travail.
SOURCE_FILESTORAGE = _path("SOURCE_FILESTORAGE", ROOT / "source-filestorage")
LAKE_ROOT = _path("LAKE_ROOT", ROOT / "lake")
LOG_DIR = _path("LOG_DIR", ROOT / "logs")

# Sel de hash : même IPP → même pseudo tous les jours (jointures).
EDS_PSEUDO_SALT = os.getenv("EDS_PSEUDO_SALT", "chu-eds-dev-salt-changez-moi")
