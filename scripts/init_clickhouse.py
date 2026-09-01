#!/usr/bin/env python3
"""Initialise ClickHouse : crée les bases de données et les tables."""

import sys
from pathlib import Path

# Ajouter le répertoire parent au chemin pour les imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from clickhouse_driver import Client

from src.config import (
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
)


def init_clickhouse():
    """Crée les bases et les tables à partir des fichiers SQL."""
    try:
        client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE,
        )
        print("✓ Connexion à ClickHouse réussie")
    except Exception as e:
        print(f"✗ Erreur de connexion : {e}")
        sys.exit(1)

    sql_dir = Path(__file__).parent.parent / "sql"
    sql_files = ["00_init.sql", "02_bronze.sql", "03_silver.sql"]

    for sql_file in sql_files:
        path = sql_dir / sql_file
        if not path.exists():
            print(f"✗ Fichier introuvable : {path}")
            sys.exit(1)

        print(f"\n→ Exécution de {sql_file}...")
        with open(path, "r", encoding="utf-8") as f:
            sql = f.read()

        # Diviser les requêtes (simples split par ;, pas 100% robuste mais ok pour du SQL simple)
        queries = [q.strip() for q in sql.split(";") if q.strip()]

        for i, query in enumerate(queries, 1):
            try:
                client.execute(query)
                print(f"  [{i}/{len(queries)}] ✓")
            except Exception as e:
                print(f"  [{i}/{len(queries)}] ✗ {e}")
                # Continue anyway, peut-être que la table existe déjà
                if "already exists" not in str(e).lower():
                    sys.exit(1)

    print("\n✓ Initialisation complète !")


if __name__ == "__main__":
    init_clickhouse()
