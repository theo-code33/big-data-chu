#!/usr/bin/env python3
"""Script de validation du pipeline EDS."""

import sys
from datetime import date
from pathlib import Path

# Ajouter le répertoire parent au chemin pour les imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db import get_client


def check_connection():
    """Vérifie la connexion à ClickHouse."""
    print("\n=== Test de connexion ===")
    try:
        client = get_client()
        result = client.execute("SELECT 1")
        print(f"✓ Connexion réussie : {result}")
        return True
    except Exception as e:
        print(f"✗ Erreur de connexion : {e}")
        return False


def check_databases(client):
    """Vérifie l'existence des bases de données."""
    print("\n=== Vérification des bases de données ===")
    databases = ["eds_lake", "eds_bronze", "eds_silver", "eds_gold"]
    existing_dbs = client.execute("SELECT database FROM system.databases")
    existing_dbs = {row[0] for row in existing_dbs}

    for db in databases:
        if db in existing_dbs:
            print(f"✓ Base {db} trouvée")
        else:
            print(f"✗ Base {db} n'existe pas")
            return False
    return True


def check_tables(client):
    """Vérifie l'existence des tables importantes."""
    print("\n=== Vérification des tables ===")
    tables = [
        ("eds_bronze", "patients"),
        ("eds_bronze", "sejours"),
        ("eds_bronze", "diagnostics"),
        ("eds_bronze", "monitoring"),
        ("eds_bronze", "ref_services"),
        ("eds_bronze", "ref_cim10"),
        ("eds_silver", "rejets"),
        ("eds_silver", "dim_patient"),
        ("eds_silver", "dim_service"),
        ("eds_silver", "dim_cim10"),
        ("eds_silver", "fact_sejour"),
        ("eds_silver", "fact_diagnostic"),
        ("eds_silver", "fact_monitoring"),
    ]

    for db, table in tables:
        try:
            result = client.execute(f"SELECT count() FROM {db}.{table}")
            count = result[0][0]
            print(f"✓ {db}.{table} : {count} lignes")
        except Exception as e:
            print(f"✗ {db}.{table} : {e}")
            return False
    return True


def check_data_quality(client):
    """Vérifie la qualité des données chargées."""
    print("\n=== Vérification de la qualité des données ===")

    # Patients sans anomalies
    try:
        result = client.execute("SELECT count() FROM eds_silver.dim_patient")
        print(f"✓ Patients valides dans dim_patient : {result[0][0]}")
    except Exception as e:
        print(f"✗ Erreur dim_patient : {e}")
        return False

    # Rejets détectés
    try:
        result = client.execute("""
            SELECT domaine, regle, count() as cnt
            FROM eds_silver.rejets
            GROUP BY domaine, regle
            ORDER BY domaine, regle
        """)
        print(f"✓ Rejets détectés :")
        for row in result:
            print(f"    {row[0]} / {row[1]} : {row[2]} lignes")
    except Exception as e:
        print(f"✗ Erreur rejets : {e}")
        return False

    return True


def main():
    """Lance tous les tests de validation."""
    print("=" * 60)
    print("VALIDATION DU PIPELINE EDS")
    print("=" * 60)

    # Test 1: Connexion
    if not check_connection():
        sys.exit(1)

    client = get_client()

    # Test 2: Bases
    if not check_databases(client):
        sys.exit(1)

    # Test 3: Tables
    if not check_tables(client):
        sys.exit(1)

    # Test 4: Qualité
    if not check_data_quality(client):
        sys.exit(1)

    print("\n" + "=" * 60)
    print("✓ TOUS LES TESTS RÉUSSIS !")
    print("=" * 60)


if __name__ == "__main__":
    main()
