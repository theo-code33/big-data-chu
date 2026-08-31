-- Schémas + utilisateurs cloisonnés.
-- Idempotent : le pipeline ré-exécute ce fichier à chaque run.

CREATE DATABASE IF NOT EXISTS eds_ops;
CREATE DATABASE IF NOT EXISTS eds_bronze;
CREATE DATABASE IF NOT EXISTS eds_silver;
CREATE DATABASE IF NOT EXISTS eds_gold_pilotage;
CREATE DATABASE IF NOT EXISTS eds_gold_recherche;

CREATE USER IF NOT EXISTS pilotage IDENTIFIED BY 'pilotage';
CREATE USER IF NOT EXISTS recherche IDENTIFIED BY 'recherche';

GRANT SELECT ON eds_gold_pilotage.* TO pilotage;
GRANT SELECT ON eds_gold_recherche.* TO recherche;
