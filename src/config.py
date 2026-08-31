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


SOURCE_FILESTORAGE = _path("SOURCE_FILESTORAGE", ROOT.parent / "source-filestorage")
LAKE_ROOT = _path("LAKE_ROOT", ROOT / "lake")
SQL_DIR = ROOT / "sql"
LOG_DIR = _path("LOG_DIR", ROOT / "logs")

EDS_PSEUDO_SALT = os.getenv("EDS_PSEUDO_SALT", "chu-eds-dev-salt-changez-moi")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_HTTP_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "eds_admin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "eds_admin")

DOMAINS = ("patients", "sejours", "diagnostics", "monitoring", "referentiels")
