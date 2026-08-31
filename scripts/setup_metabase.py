#!/usr/bin/env python3
"""Configure Metabase : 2 connexions ClickHouse, 2 collections, 2 dashboards, 2 comptes."""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

BASE = os.getenv("METABASE_URL", "http://localhost:3000").rstrip("/")
ADMIN_EMAIL = os.getenv("METABASE_ADMIN_EMAIL", "admin@chu.local")
ADMIN_PASSWORD = os.getenv("METABASE_ADMIN_PASSWORD", "AdminEDS123!")
PILOTAGE_EMAIL = os.getenv("METABASE_PILOTAGE_EMAIL", "pilotage@chu.local")
PILOTAGE_PASSWORD = os.getenv("METABASE_PILOTAGE_PASSWORD", "Pilotage123!")
RECHERCHE_EMAIL = os.getenv("METABASE_RECHERCHE_EMAIL", "recherche@chu.local")
RECHERCHE_PASSWORD = os.getenv("METABASE_RECHERCHE_PASSWORD", "Recherche123!")
CH_HOST = os.getenv("METABASE_CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))


class Mb:
    def __init__(self) -> None:
        self.s = requests.Session()
        self.s.headers["Content-Type"] = "application/json"

    def wait_ready(self, timeout: int = 180) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                r = self.s.get(f"{BASE}/api/health", timeout=5)
                if r.ok and r.json().get("status") == "ok":
                    return
            except requests.RequestException:
                pass
            time.sleep(3)
        raise SystemExit("Metabase n'a pas démarré à temps")

    def setup_or_login(self) -> None:
        props = self.s.get(f"{BASE}/api/session/properties", timeout=30).json()
        already = props.get("has-user-setup") is True
        token = props.get("setup-token")
        if token and not already:
            payload = {
                "token": token,
                "user": {
                    "first_name": "Admin",
                    "last_name": "EDS",
                    "email": ADMIN_EMAIL,
                    "password": ADMIN_PASSWORD,
                    "site_name": "EDS CHU",
                },
                "prefs": {
                    "site_name": "EDS CHU",
                    "site_locale": "fr",
                    "allow_tracking": False,
                },
            }
            r = self.s.post(f"{BASE}/api/setup", json=payload, timeout=60)
            r.raise_for_status()
            print("Metabase initialisé (admin)")
            return
        r = self.s.post(
            f"{BASE}/api/session",
            json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
            timeout=30,
        )
        r.raise_for_status()
        print("Session admin OK")

    def get(self, path: str):
        r = self.s.get(f"{BASE}{path}", timeout=30)
        r.raise_for_status()
        return r.json()

    def post(self, path: str, payload: dict):
        r = self.s.post(f"{BASE}{path}", json=payload, timeout=60)
        if not r.ok:
            print("POST", path, r.status_code, r.text[:800])
        r.raise_for_status()
        return r.json() if r.content else {}

    def put(self, path: str, payload: dict):
        r = self.s.put(f"{BASE}{path}", json=payload, timeout=60)
        if not r.ok:
            print("PUT", path, r.status_code, r.text[:800])
        r.raise_for_status()
        return r.json() if r.content else {}

    def find_or_create_db(self, name: str, dbname: str, user: str, password: str) -> int:
        dbs = self.get("/api/database")
        items = dbs.get("data", dbs) if isinstance(dbs, dict) else dbs
        for db in items:
            if isinstance(db, dict) and db.get("name") == name:
                return int(db["id"])
        created = self.post(
            "/api/database",
            {
                "engine": "clickhouse",
                "name": name,
                "details": {
                    "host": CH_HOST,
                    "port": CH_PORT,
                    "user": user,
                    "password": password,
                    "dbname": dbname,
                    "ssl": False,
                    "scan-all-databases": False,
                },
                "is_full_sync": True,
                "auto_run_queries": True,
            },
        )
        return int(created["id"])

    def find_or_create_collection(self, name: str) -> int:
        cols = self.get("/api/collection")
        for c in cols:
            if c.get("name") == name and not c.get("archived"):
                return int(c["id"])
        created = self.post("/api/collection", {"name": name, "color": "#509EE3"})
        return int(created["id"])

    def find_card(self, name: str, collection_id: int) -> int | None:
        try:
            items = self.get(f"/api/collection/{collection_id}/items?models=card")
            for it in items.get("data", []):
                if it.get("name") == name:
                    return int(it["id"])
        except requests.HTTPError:
            pass
        return None

    def upsert_card(
        self,
        name: str,
        db_id: int,
        collection_id: int,
        sql: str,
        display: str,
        viz: dict,
    ) -> int:
        existing = self.find_card(name, collection_id)
        payload = {
            "name": name,
            "dataset_query": {
                "type": "native",
                "native": {"query": sql},
                "database": db_id,
            },
            "display": display,
            "visualization_settings": viz,
            "collection_id": collection_id,
        }
        if existing:
            self.put(f"/api/card/{existing}", payload)
            return existing
        created = self.post("/api/card", payload)
        return int(created["id"])

    def archive_card(self, name: str, collection_id: int) -> None:
        existing = self.find_card(name, collection_id)
        if not existing:
            return
        try:
            self.put(f"/api/card/{existing}", {"archived": True, "name": name, "collection_id": collection_id})
        except requests.HTTPError:
            pass

    def upsert_dashboard(self, name: str, collection_id: int, cards: list[tuple[int, int, int, int, int]]) -> int:
        """cards: (card_id, row, col, size_x, size_y)"""
        dash_id = None
        items = self.get(f"/api/collection/{collection_id}/items?models=dashboard")
        for it in items.get("data", []):
            if it.get("name") == name:
                dash_id = int(it["id"])
                break
        if dash_id is None:
            dash_id = int(self.post("/api/dashboard", {"name": name, "collection_id": collection_id})["id"])
        dashcards = []
        for i, (card_id, row, col, sx, sy) in enumerate(cards, start=1):
            dashcards.append(
                {
                    "id": -i,
                    "card_id": card_id,
                    "row": row,
                    "col": col,
                    "size_x": sx,
                    "size_y": sy,
                    "parameter_mappings": [],
                    "visualization_settings": {},
                }
            )
        dash = self.get(f"/api/dashboard/{dash_id}")
        payload = {
            "name": name,
            "description": dash.get("description"),
            "parameters": dash.get("parameters") or [],
            "dashcards": dashcards,
            "collection_id": collection_id,
        }
        self.put(f"/api/dashboard/{dash_id}", payload)
        return dash_id

    def find_or_create_group(self, name: str) -> int:
        groups = self.get("/api/permissions/group")
        for g in groups:
            if g.get("name") == name:
                return int(g["id"])
        created = self.post("/api/permissions/group", {"name": name})
        return int(created["id"])

    def find_or_create_user(self, email: str, password: str, first: str, last: str, group_id: int) -> None:
        users = self.get("/api/user")
        data = users.get("data", users) if isinstance(users, dict) else users
        for u in data:
            if u.get("email") == email:
                print(f"Utilisateur déjà présent : {email}")
                return
        payload = {
            "first_name": first,
            "last_name": last,
            "email": email,
            "password": password,
            "user_group_memberships": [{"id": 1, "is_group_manager": False}, {"id": group_id, "is_group_manager": False}],
        }
        try:
            self.post("/api/user", payload)
        except requests.HTTPError:
            payload.pop("user_group_memberships")
            payload["group_ids"] = [group_id]
            self.post("/api/user", payload)
        print(f"Utilisateur créé : {email}")

    def restrict_collections(self, group_p: int, group_r: int, col_p: int, col_r: int) -> None:
        try:
            graph = self.get("/api/collection/graph")
        except requests.HTTPError:
            print("Impossible de lire le graphe des collections — à faire à la main")
            return
        collections = self.get("/api/collection")
        extra_hide = [
            str(c["id"])
            for c in collections
            if c.get("name") in {"Examples"} and not c.get("is_personal")
        ]
        groups_meta = self.get("/api/permissions/group")
        admin_ids = {
            str(g["id"])
            for g in groups_meta
            if g.get("name") == "Administrators" or g.get("magic_group_type") == "admin"
        }
        groups = graph.get("groups", {})
        for gid, perms in list(groups.items()):
            if gid in admin_ids or not isinstance(perms, dict):
                continue
            perms[str(col_p)] = "none"
            perms[str(col_r)] = "none"
            for hid in extra_hide:
                perms[hid] = "none"
            groups[gid] = perms
        hide = {hid: "none" for hid in extra_hide}
        groups[str(group_p)] = {
            **(groups.get(str(group_p)) or {}),
            str(col_p): "read",
            str(col_r): "none",
            **hide,
        }
        groups[str(group_r)] = {
            **(groups.get(str(group_r)) or {}),
            str(col_r): "read",
            str(col_p): "none",
            **hide,
        }
        graph["groups"] = groups
        try:
            self.put("/api/collection/graph", graph)
            print("Permissions collections mises à jour")
        except requests.HTTPError:
            print("Échec PUT collection/graph — configurer les droits dans l'admin Metabase")

    def restrict_data(self, group_p: int, group_r: int, db_p: int, db_r: int) -> None:
        """All Users ne voit rien ; chaque groupe n'a que sa base gold."""
        try:
            graph = self.get("/api/permissions/graph")
        except requests.HTTPError:
            print("Impossible de lire le graphe data — à faire à la main")
            return
        none = {
            "view-data": "unrestricted",
            "create-queries": "no",
            "download": {"schemas": "none"},
        }
        native = {
            "view-data": "unrestricted",
            "create-queries": "query-builder-and-native",
            "download": {"schemas": "full"},
        }
        groups_meta = self.get("/api/permissions/group")
        admin_ids = {
            str(g["id"])
            for g in groups_meta
            if g.get("name") == "Administrators" or g.get("magic_group_type") == "admin"
        }
        all_users_id = next(
            str(g["id"]) for g in groups_meta if g.get("magic_group_type") == "all-internal-users"
        )
        groups = graph.get("groups", {})
        for gid, perms in list(groups.items()):
            if gid in admin_ids or not isinstance(perms, dict):
                continue
            perms[str(db_p)] = dict(none)
            perms[str(db_r)] = dict(none)
            groups[gid] = perms
        groups[all_users_id][str(db_p)] = dict(none)
        groups[all_users_id][str(db_r)] = dict(none)
        groups[str(group_p)][str(db_p)] = dict(native)
        groups[str(group_p)][str(db_r)] = dict(none)
        groups[str(group_r)][str(db_r)] = dict(native)
        groups[str(group_r)][str(db_p)] = dict(none)
        graph["groups"] = groups
        try:
            self.put("/api/permissions/graph", graph)
            print("Permissions data mises à jour")
        except requests.HTTPError:
            print("Échec PUT permissions/graph — configurer les droits data dans l'admin")


def viz_bar(dim: str, metric: str) -> dict:
    return {"graph.dimensions": [dim], "graph.metrics": [metric]}


def viz_line(dim: str, metric: str) -> dict:
    return {"graph.dimensions": [dim], "graph.metrics": [metric]}


def main() -> None:
    mb = Mb()
    mb.wait_ready()
    mb.setup_or_login()

    db_p = mb.find_or_create_db(
        "EDS Pilotage",
        "eds_gold_pilotage",
        os.getenv("CLICKHOUSE_PILOTAGE_USER", "pilotage"),
        os.getenv("CLICKHOUSE_PILOTAGE_PASSWORD", "pilotage"),
    )
    db_r = mb.find_or_create_db(
        "EDS Recherche",
        "eds_gold_recherche",
        os.getenv("CLICKHOUSE_RECHERCHE_USER", "recherche"),
        os.getenv("CLICKHOUSE_RECHERCHE_PASSWORD", "recherche"),
    )
    print("Bases Metabase", db_p, db_r)

    col_p = mb.find_or_create_collection("Pilotage")
    col_r = mb.find_or_create_collection("Recherche")

    cards_p = []
    cards_p.append(
        mb.upsert_card(
            "DMS par service (jours)",
            db_p,
            col_p,
            "SELECT service_label, dms_jours, nb_sejours_sortis FROM eds_gold_pilotage.dms_par_service ORDER BY dms_jours DESC",
            "bar",
            viz_bar("service_label", "dms_jours"),
        )
    )
    cards_p.append(
        mb.upsert_card(
            "Passages urgences par jour",
            db_p,
            col_p,
            "SELECT jour, nb_passages FROM eds_gold_pilotage.passages_urgences_jour ORDER BY jour",
            "line",
            viz_line("jour", "nb_passages"),
        )
    )
    cards_p.append(
        mb.upsert_card(
            "Taux de réadmission à 30 jours",
            db_p,
            col_p,
            "SELECT service_label, taux_pct, nb_sorties, nb_readmissions FROM eds_gold_pilotage.readmission_30j ORDER BY taux_pct DESC",
            "bar",
            viz_bar("service_label", "taux_pct"),
        )
    )
    cards_p.append(
        mb.upsert_card(
            "Alertes monitoring par jour",
            db_p,
            col_p,
            "SELECT jour, nb_releves, nb_alertes FROM eds_gold_pilotage.alertes_monitoring_jour ORDER BY jour",
            "line",
            viz_line("jour", "nb_alertes"),
        )
    )

    mb.upsert_dashboard(
        "Pilotage hospitalier",
        col_p,
        [
            (cards_p[0], 0, 0, 12, 6),
            (cards_p[1], 0, 12, 12, 6),
            (cards_p[2], 6, 0, 12, 6),
            (cards_p[3], 6, 12, 12, 6),
        ],
    )

    for obsolete in (
        "Activité par service",
        "Rejets qualité (traçabilité)",
        "Cohorte par pathologie, âge et sexe (n ≥ 5)",
    ):
        mb.archive_card(obsolete, col_p)
        mb.archive_card(obsolete, col_r)

    cards_r = []
    cards_r.append(
        mb.upsert_card(
            "Prévalence par pathologie (n ≥ 5)",
            db_r,
            col_r,
            "SELECT libelle, code_cim10, nb_patients, nb_sejours FROM eds_gold_recherche.prevalence_pathologie ORDER BY nb_patients DESC",
            "bar",
            viz_bar("libelle", "nb_patients"),
        )
    )
    cards_r.append(
        mb.upsert_card(
            "Cohorte : âge × sexe (n ≥ 5)",
            db_r,
            col_r,
            "SELECT tranche_age, sex, nb_patients FROM eds_gold_recherche.cohorte_age_sexe ORDER BY tranche_age, sex",
            "bar",
            {"graph.dimensions": ["tranche_age", "sex"], "graph.metrics": ["nb_patients"]},
        )
    )

    mb.upsert_dashboard(
        "Recherche clinique",
        col_r,
        [
            (cards_r[0], 0, 0, 16, 8),
            (cards_r[1], 0, 16, 8, 8),
        ],
    )

    group_p = mb.find_or_create_group("Groupe Pilotage")
    group_r = mb.find_or_create_group("Groupe Recherche")
    mb.find_or_create_user(PILOTAGE_EMAIL, PILOTAGE_PASSWORD, "Analyste", "Pilotage", group_p)
    mb.find_or_create_user(RECHERCHE_EMAIL, RECHERCHE_PASSWORD, "Chercheur", "Clinique", group_r)
    mb.restrict_collections(group_p, group_r, col_p, col_r)
    mb.restrict_data(group_p, group_r, db_p, db_r)

    print("\nDashboards prêts.")
    print(f"  Admin     {ADMIN_EMAIL} / (voir .env)")
    print(f"  Pilotage  {PILOTAGE_EMAIL} — collection Pilotage uniquement")
    print(f"  Recherche {RECHERCHE_EMAIL} — collection Recherche uniquement")
    print(f"  UI        {BASE}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(1)
