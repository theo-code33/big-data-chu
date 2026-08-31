# Entrepôt de Données de Santé (EDS) — CHU

Projet fil rouge Big Data (M2). Les données hospitalières arrivent chaque jour dans un filestorage hétérogène. Ce dépôt construit un EDS en architecture **médaillon**, pour deux usages qui ne doivent pas se mélanger : le **pilotage** et la **recherche clinique**.

Ce README justifie les choix. Le détail des formules, des contrôles et des limites est dans [`docs/partie1-dossier.md`](docs/partie1-dossier.md). Le **modèle Silver** (grain, clés, cardinalités) est dans [`docs/modele-silver.md`](docs/modele-silver.md). L’exploitation est dans [`docs/partie2-exploitation.md`](docs/partie2-exploitation.md).

## 1. Contexte et besoin

Le CHU veut un EDS. Aujourd’hui les données sont éparpillées (dossier patient, urgences, laboratoire / diagnostics, monitoring des chambres) et déposées **chaque jour** sous forme de fichiers, dans des formats différents.

Deux publics, deux questions :

| Public | Question type | Données visibles |
|---|---|---|
| Direction / DIM | Combien de lits, quelle DMS, quelles urgences, quelle qualité (réadmissions, alertes) ? | Activité agrégée, **pas** de pathologie individuelle |
| Recherche clinique | Quelle taille de cohorte pour tel diagnostic ? Quelle structure d’âge / sexe ? | Cohortes agrégées, **jamais** d’identité, **jamais** un effectif &lt; 5 |

Les données de santé sont une catégorie particulière (RGPD art. 9). La conformité n’est pas un chapitre collé à la fin : elle contraint **chaque couche** (ce qu’on copie, ce qu’on stocke, qui voit quoi).

## 2. Livrables

| Partie | Ce qui est livré |
|---|---|
| **Partie 1** | Chaîne lake → bronze → silver → gold, deux dashboards Metabase, cloisonnement des droits, dossier d’analyse |
| **Partie 2** | Pipeline incrémental rejouable, journalisation, gestion d’erreur, doc d’exploitation |
| **Bonus** | Pseudonymisation **à l’entrée du lake** : aucune identité n’atteint ClickHouse |

## 3. Sources (ce que le CHU dépose)

Lecture seule : [`../source-filestorage/`](../source-filestorage/). On **copie** vers notre lake, on ne écrit jamais dans le dépôt CHU.

| Famille | Format | Rythme observé | Rôle |
|---|---|---|---|
| `patients/` | CSV | Dump **complet** qui grossit (~4 800 → 5 400 → 6 000) | Identité + IPP. **PII** |
| `sejours/` | CSV | ~5 000 **nouveaux** séjours / jour | Passage hospitalier |
| `diagnostics/` | JSON imbriqué | 1 principal + 0..n associés / séjour | CIM-10 |
| `monitoring/` | Parquet | Relevés de constantes au chevet | Volume (principe Big Data) |
| `referentiels/` | CSV | Déposés **le premier jour seulement** | Libellés services et CIM-10 |

Jours fournis : `2026-08-26` à `2026-08-28`.

Observations qui ont guidé le modèle :

- Un patient revient tous les jours dans le dump → **déduplication** (on garde la version la plus récente).
- Un séjour `S00000006` a une sortie **antérieure** à l’entrée → anomalie à **écarter**, pas à « corriger ».
- `discharge_ts` vide = séjour en cours → **légitime**, on conserve.
- `patient_id`, `nir`, `nom`, `prenom` sont des identifiants directs → **interdits** dans l’entrepôt.

## 4. Architecture médaillon — pourquoi ces couches

```text
Filestorage CHU (lecture seule)
        │  Python : copie + pseudonymisation
        ▼
      Lake          ← fichiers de travail, déjà sans identité
        │  Python : INSERT / file()
        ▼
     Bronze         ← tables typées, peu transformées, historisées par jour
        │  SQL ClickHouse
        ▼
     Silver         ← nettoyé, dédupliqué, enrichi + table de rejets
        │  SQL ClickHouse
        ├──────────────────┐
        ▼                  ▼
 Gold Pilotage      Gold Recherche     ← KPI déjà agrégés, droits séparés
        │                  │
        └────────┬─────────┘
                 ▼
              Metabase
         (2 collections, 2 comptes)
```

| Couche | Rôle | Pourquoi elle existe |
|---|---|---|
| **Lake** | Copie de travail. Pour les patients / séjours : déjà pseudonymisée | Le CHU reste intouchable. Le bonus RGPD exige que l’identité **n’entre pas** dans notre zone |
| **Bronze** | Fichiers → tables typées (`Date`, `DateTime`, types numériques) + `_source_date` / `_ingested_at` | On peut **rejouer** silver/gold sans relire les fichiers. On sait d’où vient chaque ligne |
| **Silver** | Qualité, déduplication, jointures (service, CIM-10, patient) | Seule « table de vérité » métier. Les rejets sont **tracés**, pas silencieux |
| **Gold** | Indicateurs **déjà agrégés**, un schéma par usage | Le dashboard ne recalcule pas la DMS. Le cloisonnement se fait ici (GRANT), pas dans l’UI seule |

Pourquoi **ne pas** tout nettoyer en bronze ? Parce qu’une règle métier (bornes FC, séjour inversé) peut évoluer. Bronze reste proche du fichier ; on reconstruit silver sans ré-ingérer.

Pourquoi **deux** gold plutôt qu’une table avec un filtre ? Parce que le RGPD demande un **cloisonnement** : le compte recherche ne doit pas pouvoir `SELECT` la table de réadmission, même en SQL.

Anti-pattern du cours : charger le monitoring en pandas, transformer en mémoire, renvoyer le résultat. Ça ne passe pas à l’échelle. **Python pilote, ClickHouse transforme.**

## 5. Choix de stack (et ce qu’on écarte)

Le sujet propose une stack qui tient sur un laptop. On la suit, et on justifie.

| Brique | Rôle | Pourquoi |
|---|---|---|
| **ClickHouse** (Docker) | Entrepôt colonne | Parquet natif, SQL dans le moteur, UI `:8123/play`, agrégations d’indicateurs très rapides |
| **Python 3** | Orchestrateur | Recopie, hash RGPD, envoi du SQL, logs. Module `csv` / `json` stdlib pour réécrire les petits fichiers **sans pandas** |
| **Metabase** (Docker, ≥ 0.54, driver ClickHouse intégré) | Dashboards | Restitution sans code, collections et groupes pour la démo de cloisonnement |
| **Docker Compose** | Runtime | Un `up` suffit. Pas d’install ClickHouse divergente selon la machine |

Ce qu’on n’utilise **pas** (ce n’est pas un oubli) :

- **pandas / Spark** pour la transformation métier — anti-pattern ici ; le volume pédagogique est le monitoring, conçu pour rester dans le moteur.
- **Airflow / Prefect** — disproportionné pour 3 jours de fichiers et un laptop. Un scheduler Python + table de watermark fait l’incrémental et la reprise. En production on remplacerait ce scheduler par Airflow ; le SQL médaillon ne changerait pas.

## 6. RGPD — décisions de conception

| Principe | Mise en œuvre |
|---|---|
| **Pseudonymisation à l’entrée** | `patient_id` → SHA-256(`sel` + id), **stable** (les jointures séjours/patients restent possibles). Sel dans `.env`, jamais dans git |
| **Minimisation** | `nir`, `nom`, `prenom` **supprimés** dès la copie. Date de naissance **généralisée à l’année** (`birth_year`). Le lake ne contient plus ces colonnes |
| **Cloisonnement** | 2 bases gold, 2 users ClickHouse (`pilotage` / `recherche`), 2 collections Metabase, 2 comptes applicatifs. Un `SELECT` SQL hors périmètre est refusé par ClickHouse, pas seulement caché dans l’UI |
| **Petits effectifs** | En recherche, une cohorte avec **&lt; 5 patients** n’est pas exposée (`HAVING nb_patients >= 5`) |
| **Traçabilité** | `_source_date`, `_ingested_at`, table `eds_ops.fichiers_traites`, `eds_ops.runs`, table `eds_silver.rejets` (règle, séjour, jour source) |

Le hash est déterministe **avec sel** : sans le sel, on ne recompose pas l’IPP. Avec le même sel, le même patient a le même pseudo tous les jours.

Après anonymisation, on **perd** la possibilité de détecter une incohérence prénom/sexe : c’est le prix de la minimisation, noté en limite.

## 7. Qualité — on écarte, on ne « répare » pas

On n’est pas médecins. Le sujet demande de **détecter**, **écarter**, **tracer**. Exception unique : un séjour sans date de sortie n’est pas une erreur.

| Contrôle | Action |
|---|---|
| Doublons patients (dump quotidien) | Garder `_source_date` la plus récente |
| `discharge_ts < admission_ts` | Rejet `sejour_dates_inversees` |
| `discharge_ts` vide | Conservé, `est_en_cours = 1` |
| FC hors 20–250, SpO2 hors 50–100, temp hors 30–45 °C | Rejet monitoring |
| Sexe | Normalisé `M`/`F` ; autre valeur → rejet |
| Dates / horodatages invalides | Rejet |

`discharge_mode` vide alors que le séjour est sorti existe dans les fichiers : ce n’est **pas** dans la liste des rejets imposés, on conserve la ligne et on le signale dans le dossier.

## 8. Indicateurs (gold)

**Pilotage** (séjour sorti uniquement pour la DMS et la réadmission) :

- DMS par service (moyenne des durées d’hospitalisation)
- Passages aux urgences par jour = séjours dont `service_code = 'URGENCES'` (activité du service, pas le mode d’entrée `urgence` qui existe aussi en cardio)
- Taux de réadmission à 30 jours : parmi les séjours sortis, part de ceux suivis d’une **nouvelle admission du même patient** dans les 30 jours
- Relevés monitoring en alerte / jour — bornes **cliniques** plus strictes que le rejet qualité (ex. SpO2 &lt; 90 %), documentées dans le dossier
- Vue d’activité complémentaire : admissions par service

**Recherche** :

- Prévalence / taille de cohorte par diagnostic **principal** (libellé CIM-10), masquée si n &lt; 5
- Distribution par tranches d’âge et sexe (globale et par pathologie), même règle n &lt; 5

Chaque chiffre doit pouvoir se **rejouer** à partir de silver (SQL dans `sql/04_gold.sql`).

## 9. Incrémental

On n’ingère pas deux fois le même fichier.

1. Découverte des dates présentes dans le filestorage.
2. Pour chaque fichier : checksum + ligne dans `eds_ops.fichiers_traites`.
3. Statut `ok` → on passe. Statut `erreur` → on peut relancer (`--retry-errors`).
4. Bronze s’**append** par jour. Silver et gold sont **reconstruits** depuis bronze (volume faible, idempotent, règles qualité toujours à jour).

C’est volontaire : l’incrémental « lourd » est à l’ingestion (ne pas relire 6 mois de Parquet). Recalculer les KPI sur 3 jours (ou même quelques mois) dans ClickHouse est cheap.

## 10. Structure du dépôt

```text
rendu/
├── docker-compose.yml     # ClickHouse + Metabase + pipeline
├── Dockerfile             # image Python d’orchestration
├── .env.example           # sel et mots de passe — copier vers .env
├── sql/                   # transformation dans le moteur
├── src/                   # copie, anonymisation, orchestration
├── scripts/setup_metabase.py
├── docs/
│   ├── partie1-dossier.md
│   └── partie2-exploitation.md
├── lake/                  # gitignoré — notre copie de travail
└── logs/                  # gitignoré
```

## 11. Lancement rapide

Prérequis : Docker, Python 3.11+ (optionnel en local).

```bash
cp .env.example .env          # puis éventuellement changer EDS_PSEUDO_SALT
docker compose up -d clickhouse metabase
```

Attendre que ClickHouse soit healthy, puis :

```bash
docker compose --profile batch run --rm pipeline
# ou, depuis l’hôte :
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m src.pipeline --all
python scripts/setup_metabase.py
```

| Service | URL | Compte |
|---|---|---|
| ClickHouse Play | http://localhost:8123/play | `eds_admin` / voir `.env` |
| Metabase | http://localhost:3000 | `admin@chu.local` |
| Dashboard pilotage | collection Pilotage | `pilotage@chu.local` |
| Dashboard recherche | collection Recherche | `recherche@chu.local` |

Mots de passe : `.env` / `.env.example`. Pour démontrer le cloisonnement, ouvrir Metabase avec `pilotage@chu.local` puis avec `recherche@chu.local` : chaque compte ne voit **que** sa collection. En SQL, `pilotage` ne peut pas lire `eds_gold_recherche`.

## 12. Limites assumées

- **3 jours** de dépôt : les taux (réadmission, DMS) sont pédagogiques, pas une statistique hospitalière annuelle.
- **Âge approximatif** : année de naissance seulement (minimisation) → `année_courante - birth_year`.
- **Données d’exercice** (identités fictives, CIM-10 limité à 10 codes).
- Pas de cluster, pas d’Airflow : stack laptop, justifiée plus haut.
- Le sel de hash, s’il change, **casse** l’historique des jointures — ne pas le rotator sans règle de migration.

Ces limites et les recommandations associées sont développées dans le dossier Partie 1.
