# Setup ClickHouse

## Prérequis

### Option 1 : Docker (recommandé pour dev)

```bash
docker-compose up -d
```

Cela lance un conteneur ClickHouse accessible sur :
- **Native protocol** : localhost:9000
- **HTTP API** : localhost:8123

### Option 2 : Installation locale

Télécharger et installer ClickHouse depuis https://clickhouse.com

## Configuration

Copier `.env.example` en `.env` et adapter les variables :

```bash
cp .env.example .env
```

Valeurs par défaut (Docker) :
```
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=default
```

## Initialisation

Une fois ClickHouse démarré, initialiser les bases et tables :

```bash
python scripts/init_clickhouse.py
```

Cela exécute dans l'ordre :
1. `sql/00_init.sql` → Création des databases (eds_lake, eds_bronze, eds_silver, eds_gold)
2. `sql/02_bronze.sql` → Tables de la couche Bronze
3. `sql/03_silver.sql` → Star schema et table des rejets

## Pipeline complet

```bash
# Exemple : charger et transformer les données du 2026-08-26
python -m src.pipeline --date 2026-08-26 --step all
```

Décomposition par étapes :
- `--step lake` : filestorage → lake (anonymisation)
- `--step bronze` : lake → ClickHouse bronze (typage)
- `--step silver` : bronze → ClickHouse silver (star schema + QA)
- `--step all` : tout (par défaut)

## Validation

### Vérifier la connexion

```bash
python -c "from src.db import get_client; c = get_client(); print(c.execute('SELECT 1'))"
# Résultat attendu : [(1,)]
```

### Compter les lignes

```bash
python -c "
from src.db import get_client
from datetime import date
c = get_client()
print('Patients Bronze :', c.execute('SELECT COUNT(*) FROM eds_bronze.patients WHERE _source_date = %s', [date(2026, 8, 26)]))
print('Silver Rejects :', c.execute('SELECT COUNT(*) FROM eds_silver.rejets'))
"
```

### Explorer les rejets

```bash
python -c "
from src.db import get_client
c = get_client()
results = c.execute('SELECT domaine, regle, COUNT(*) as cnt FROM eds_silver.rejets GROUP BY domaine, regle')
for r in results:
    print(f'{r[0]}: {r[1]} ({r[2]} lignes)')
"
```
