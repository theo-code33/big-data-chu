CREATE TABLE IF NOT EXISTS eds_ops.fichiers_traites
(
    source_path   String,
    domaine       LowCardinality(String),
    source_date   Date,
    checksum      String,
    nb_lignes     UInt64,
    statut        LowCardinality(String),
    message       String,
    started_at    DateTime,
    finished_at   DateTime
)
ENGINE = MergeTree
ORDER BY (domaine, source_date, source_path);

CREATE TABLE IF NOT EXISTS eds_ops.runs
(
    run_id        String,
    started_at    DateTime,
    finished_at   Nullable(DateTime),
    source_date   Nullable(Date),
    couche        LowCardinality(String),
    statut        LowCardinality(String),
    message       String
)
ENGINE = MergeTree
ORDER BY (started_at, run_id);
