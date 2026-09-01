# EDS CHU — étape 1 : le lake

Branche `bronze`. Pour l’instant on ne fait **que** ça :

1. Lire le filestorage CHU (**sans jamais l’écrire**)
2. Copier vers **notre** lake
3. **Anonymiser** dès cette copie (identités interdites dans le lake)

Pas de ClickHouse, pas de Silver, pas de dashboard. Ça vient après.

```text
source-filestorage/     ← le CHU dépose, lecture seule
        │
        │  Python copie
        │  patients + séjours : hash / année / plus de nom-NIR
        ▼
     lake/              ← notre zone de travail
```

## Étape A — Comprendre les sources

Ouvre `source-filestorage/` (3 jours : 26, 27, 28 août 2026) :

| Dossier | Fichier | PII ? |
|---|---|---|
| `patients/` | CSV | **oui** : `patient_id`, `nir`, `nom`, `prenom`, date de naissance complète |
| `sejours/` | CSV | `patient_id` (IPP) à hasher, le reste métier |
| `diagnostics/` | JSON | non (seulement `stay_id` + codes CIM-10) |
| `monitoring/` | Parquet | non |
| `referentiels/` | CSV | non (déposés le premier jour seulement) |

Le CHU est lecture seule : on ne corrige rien là-dedans.

## Étape B — Brancher le projet

```bash
cp .env.example .env
python3 -m venv .venv
source .venv/bin/activate   # Windows : .venv\Scripts\activate
pip install -r requirements.txt
```

Le sel `EDS_PSEUDO_SALT` dans `.env` doit rester **le même** d’un run à l’autre : c’est lui qui rend le hash stable (jointures patients ↔ séjours).

## Étape C — Remplir le lake

Tous les jours :

```bash
python -m src.pipeline
```

Un seul jour, pour voir ce qui se passe :

```bash
python -m src.pipeline --date 2026-08-26
```

Relancer n’ajoute **pas** de doublons : les fichiers du lake sont **écrasés**.

## Étape D — Vérifier l’anonymisation (à montrer au prof)

Compare une ligne source et une ligne lake :

```bash
head -n 2 source-filestorage/patients/2026-08-26/patients.csv
head -n 2 lake/patients/2026-08-26/patients.csv
```

Tu dois voir :

| Source | Lake |
|---|---|
| `patient_id` (IPP0000000) | `patient_pseudo` (64 caractères SHA-256) |
| `nir`, `nom`, `prenom` | **absents** |
| `birth_date` (1933-12-09) | `birth_year` (1933) |
| `sex`, `region_code` | inchangés |

Même chose sur les séjours : plus d’IPP, le même hash que le patient.

```bash
head -n 2 source-filestorage/sejours/2026-08-26/sejours.csv
head -n 2 lake/sejours/2026-08-26/sejours.csv
```

Diagnostics / monitoring / référentiels = copie identique (rien à cacher).

## Ce qu’il faut savoir dire

- **Pourquoi un lake ?** On ne traite pas dans le dépôt du CHU. Notre copie, nos règles.
- **Pourquoi anonymiser à la copie ?** RGPD : aucune identité, même pas sur notre disque.
- **Pourquoi un hash déterministe ?** `SHA-256(sel + IPP)`. Sans le sel on ne retrouve pas l’IPP. Avec le même sel, le même patient a le même pseudo le 26 et le 27, donc on pourra joindre plus tard.
- **Pourquoi hasher aussi les séjours ?** Le CSV séjours contient `patient_id`. Si on le laissait en clair, l’anonymisation patients ne servirait à rien.

Code utile : [`src/ingest/anonymize.py`](src/ingest/anonymize.py) (le hash) et [`src/ingest/copy_to_lake.py`](src/ingest/copy_to_lake.py) (qui copie quoi).

## Fichiers du dépôt

```text
source-filestorage/     données CHU (lecture seule)
src/pipeline.py         lance la copie
src/ingest/copy_to_lake.py
src/ingest/anonymize.py
src/config.py           chemins + sel
.env.example
lake/                   généré, gitignoré
```
