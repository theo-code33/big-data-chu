# EDS CHU — Silver Layer Implementation

## Quick Start

```bash
# Setup environment
cp .env.example .env
pip install -r requirements.txt

# Start ClickHouse (Docker)
make docker-up
sleep 10
make init

# Load full pipeline
make load-all

# Validate
make validate
```

## Pipeline Stages

### Stage 1: Lake (Anonymization)
```
filestorage/ → lake/
- Patients: Strip PII, hash IPP → patient_pseudo
- Sejours: Hash patient_id
- Diagnostics: JSON (nested) → NDJSON (flat)
- Monitoring: Copy as-is (Parquet)
- Referentiels: Copy as-is (CSV)
```

### Stage 2: Bronze (Ingestion)
```
lake/ → ClickHouse Bronze
- Read from lake files
- Use file() function for streaming (no Python buffer)
- Add _source_date partition + _ingested_at timestamp
- MergeTree tables for analytics
```

### Stage 3: Silver (Transformation + QA)

**Star Schema:**
- Dimensions: dim_patient (deduplicated), dim_service, dim_cim10
- Facts: fact_sejour, fact_diagnostic, fact_monitoring
- Rejets: Quality control violations

**Data Quality Rules:**
- Patients: sex in {M,F}, birth_year in [1900, today]
- Sejours: discharge_ts IS NULL OR discharge_ts >= admission_ts
- Diagnostics: Must have valid sejour
- Monitoring: HR in [20,250], SpO2 in [50,100], Temp in [30,45]

## Database Structure

### EDS_BRONZE

Tables with minimal transformation:

| Table        | Columns                                                                                                                       | Partition    | Purpose                         |
| ------------ | ----------------------------------------------------------------------------------------------------------------------------- | ------------ | ------------------------------- |
| patients     | patient_pseudo, birth_year, sex, region_code, _source_date, _ingested_at                                                      | _source_date | Anonymized patient master       |
| sejours      | stay_id, patient_pseudo, service_code, admission_ts, discharge_ts, admission_mode, discharge_mode, _source_date, _ingested_at | _source_date | Hospital stays                  |
| diagnostics  | stay_id, code_cim10, type, _source_date, _ingested_at                                                                         | _source_date | Diagnoses (flattened from JSON) |
| monitoring   | stay_id, ts, heart_rate, spo2, temp_c, _source_date, _ingested_at                                                             | _source_date | Vital signs                     |
| ref_services | service_code, service_label, _source_date, _ingested_at                                                                       | _source_date | Service reference               |
| ref_cim10    | code_cim10, libelle, _source_date, _ingested_at                                                                               | _source_date | CIM-10 diagnosis codes          |

### EDS_SILVER

Star schema for analytics:

#### Rejets (QA violations)
```
domaine | regle | stay_id | patient_pseudo | source_date | detail | rejected_at
patients | sexe_ou_naissance_invalide | - | pseudo | date | sex=X birth_year=Y | now()
sejours | discharge_avant_admission | stay_id | pseudo | date | admission=T1 discharge=T2 | now()
diagnostics | sejour_invalide | stay_id | - | date | code=X type=Y | now()
monitoring | valeur_hors_plage | stay_id | - | date | hr=X spo2=Y temp=Z | now()
```

#### Dimensions

**dim_patient** (deduplicated, latest per source_date)
```
patient_pseudo | birth_year | sex | region_code | age_approx | source_date
```

**dim_service** (service codes)
```
service_code | service_label
```

**dim_cim10** (diagnosis codes)
```
code_cim10 | libelle
```

#### Facts

**fact_sejour** (one row = one hospital stay)
```
stay_id | patient_pseudo | service_code | admission_ts | discharge_ts |
admission_mode | discharge_mode | duree_heures | est_en_cours | source_date
```

**fact_diagnostic** (one row = one diagnosis code)
```
stay_id | patient_pseudo | code_cim10 | type | source_date
```
Note: Self-contained (recopies patient_pseudo from bronze.sejours)

**fact_monitoring** (one row = one vital sign measurement)
```
stay_id | ts | heart_rate | spo2 | temp_c | source_date
```

## Command Reference

### Makefile

```bash
make help              # Show all commands
make docker-up         # Start ClickHouse container
make docker-down       # Stop ClickHouse
make init              # Initialize databases and tables
make validate          # Test connection and structure
make load-lake         # filestorage → lake
make load-bronze       # lake → ClickHouse Bronze
make load-silver       # Bronze → Silver
make load-all          # Complete pipeline (lake + bronze + silver)
make load-date DATE=2026-08-26  # Pipeline for specific date
make test-query        # Quick row count check
make clean             # Remove temp files
```

### Direct Python

```bash
# All steps
python -m src.pipeline --step all

# Specific date
python -m src.pipeline --date 2026-08-26 --step all

# By stage
python -m src.pipeline --step lake
python -m src.pipeline --step bronze
python -m src.pipeline --step silver
```

### Validation

```bash
# Full validation
python scripts/validate_pipeline.py

# Connection test
python -c "from src.db import get_client; c = get_client(); print(c.execute('SELECT 1'))"

# Row counts
python -c "
from src.db import get_client
c = get_client()
for table in ['patients', 'sejours', 'diagnostics', 'monitoring']:
    cnt = c.execute(f'SELECT count() FROM eds_bronze.{table}')[0][0]
    print(f'{table}: {cnt}')
"

# Rejets analysis
python -c "
from src.db import get_client
c = get_client()
rows = c.execute('''
    SELECT domaine, regle, COUNT(*) as cnt
    FROM eds_silver.rejets
    GROUP BY domaine, regle
    ORDER BY domaine, regle
''')
for domaine, regle, cnt in rows:
    print(f'{domaine} / {regle}: {cnt}')
"
```

## Configuration

### .env

```ini
# Anonymization salt (MUST be stable across runs)
EDS_PSEUDO_SALT=chu-eds-dev-salt-changez-moi

# Filesystem paths
SOURCE_FILESTORAGE=./source-filestorage
LAKE_ROOT=./lake
LOG_DIR=./logs

# ClickHouse connection
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=default
```

## Data Flow Diagram

```
source-filestorage/     (CHU, read-only)
    ├── patients/2026-08-26/patients.csv
    ├── sejours/2026-08-26/sejours.csv
    ├── diagnostics/2026-08-26/diagnostics.json (nested array)
    ├── monitoring/2026-08-26/monitoring.parquet
    └── referentiels/2026-08-26/{services,cim10}.csv
    
    ↓ Python anonymize + transform
    
lake/                   (our workspace)
    ├── patients/2026-08-26/patients.csv (anonymized)
    ├── sejours/2026-08-26/sejours.csv (anonymized)
    ├── diagnostics/2026-08-26/diagnostics.ndjson (flattened)
    ├── monitoring/2026-08-26/monitoring.parquet (copy)
    └── referentiels/2026-08-26/{services,cim10}.csv (copy)
    
    ↓ ClickHouse file() function (native protocol)
    
EDS_BRONZE             (typed, partitioned)
    ├── patients (rows, _source_date, _ingested_at)
    ├── sejours
    ├── diagnostics
    ├── monitoring
    ├── ref_services
    └── ref_cim10
    
    ↓ SQL transformation + quality gates
    
EDS_SILVER             (star schema + QA)
    ├── Rejets (violations: 15 types)
    ├── Dimensions (dim_patient, dim_service, dim_cim10)
    └── Facts (fact_sejour, fact_diagnostic, fact_monitoring)
```

## Key Design Decisions

### 1. Stable Anonymization
- `patient_pseudo = SHA-256(patient_id + EDS_PSEUDO_SALT)`
- Same salt across runs → same patient = same pseudo
- Enables day-to-day joins on patient_pseudo

### 2. Diagnostics Transformation
- Source: JSON with nested structure (one record per stay with array of diagnostics)
- Lake: NDJSON, one record per diagnosis code (flattened)
- Reason: ClickHouse file() function works better with flat format

### 3. Self-Contained Facts
- `fact_diagnostic` recopies patient_pseudo from bronze.sejours
- Not a foreign key to fact_sejour
- Benefit: Each fact table is independent, no complex joins needed at ETL time

### 4. Quality Gates in Silver
- All QA rules implemented as SQL predicates
- Violations inserted into rejets table
- Clean facts only contain valid records
- Rejets table enables audit trail and debugging

### 5. Dimension Deduplication
- `dim_patient` uses row_number() to keep latest per source_date
- Consolidates multiple versions of patient master data
- Maintains historical accuracy without duplicates

## Troubleshooting

### "Connection refused" (ClickHouse)
```bash
docker logs eds-clickhouse
docker ps | grep clickhouse
# If not running:
make docker-up
sleep 10
```

### "Table doesn't exist"
```bash
# Re-initialize everything
make docker-down
make docker-up
make init
```

### Different pseudo for same patient
```bash
# Check EDS_PSEUDO_SALT consistency
cat .env | grep EDS_PSEUDO_SALT
# Must be identical across runs
```

### Missing columns in CSV
```bash
# Verify source file format
head -n 1 source-filestorage/patients/2026-08-26/patients.csv
# Check lake transformation
head -n 1 lake/patients/2026-08-26/patients.csv
```

### Rejets table is empty
- Normal if data is clean
- Run diagnostic query to confirm:
  ```bash
  python -c "
  from src.db import get_client
  c = get_client()
  cnt = c.execute('SELECT COUNT(*) FROM eds_silver.rejets')[0][0]
  print(f'Total rejets: {cnt}')
  "
  ```

## Next Steps

1. Implement `sql/04_gold.sql` for KPI tables
2. Create `src/ingest/build_gold.py` for KPI calculations
3. Add orchestration (Airflow or cron)
4. Implement automated tests
5. Set up data quality monitoring
