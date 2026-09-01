"""Connexion et utilitaires ClickHouse."""

from __future__ import annotations

from typing import TYPE_CHECKING

from clickhouse_driver import Client

from src.config import (
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
)

if TYPE_CHECKING:
    from datetime import date


def get_client() -> Client:
    """Crée un client ClickHouse connecté."""
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
        settings={"use_numpy": False},
    )


def count_table(
    client: Client, db: str, table: str, source_date: date | None = None
) -> int:
    """Compte les lignes d'une table, optionnellement filtrées par source_date."""
    if source_date:
        query = f"SELECT count() FROM {db}.{table} WHERE _source_date = '{source_date.isoformat()}'"
    else:
        query = f"SELECT count() FROM {db}.{table}"
    return client.execute(query)[0][0]
