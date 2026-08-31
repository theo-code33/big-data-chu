from __future__ import annotations

import time

import clickhouse_connect

from src.config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_HTTP_PORT,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_USER,
)


def get_client(retries: int = 20, delay: float = 2.0):
    last = None
    for _ in range(retries):
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_HTTP_PORT,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
            )
            client.command("SELECT 1")
            return client
        except Exception as exc:  # noqa: BLE001 — retry until ClickHouse is up
            last = exc
            time.sleep(delay)
    raise RuntimeError(f"ClickHouse injoignable sur {CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}") from last
