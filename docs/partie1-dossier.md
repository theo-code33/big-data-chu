# Partie 1 — Dossier d’analyse

Entrepôt de Données de Santé (EDS) du CHU. Ce dossier complète le [README](../README.md) : besoin, sources, architecture, traitements, indicateurs, restitution, limites.

## 1. Analyse du besoin

Le CHU veut **centraliser** des exports quotidiens aujourd’hui éparpillés, pour deux usages :

1. **Pilotage hospitalier** (direction, DIM) : activité, durées de séjour, urgences, qualité des soins, surveillance des constantes.
2. **Recherche clinique** : tailles de cohortes par pathologie, description (âge, sexe).

Contrainte transverse : données de santé (respect du RGPD art. 9). Ce n’est pas un filtre final, c’est une **contrainte de conception** : on ne stocke pas l’identité, on sépare les publics, on masque les petits effectifs, on trace.

Ce que l’hôpital n’a pas demandé, et que nous ne livrons pas : un DPI, un outil de prescription, un identifiant nominatif pour le clinicien de terrain. L’EDS est un entrepôt **secondaire**.

## 2. Sources

Accès en **lecture seule** au filestorage CHU (`source-filestorage/`). Trois jours : 26, 27 et 28 août 2026.

| Source | Volume observé | Caractère |
|---|---|---|
| `patients.csv` | 4 800 / 5 400 / 6 000 lignes | Dump **complet** cumulatif (+600 patients/jour). Identité en clair |
| `sejours.csv` | 5 000 lignes / jour, `stay_id` unique | Incrémental. ~44–50 dates inversées / jour, ~390–407 séjours en cours |
| `diagnostics.json` | 5 000 séjours, ~12 400 codes | 1 diagnostic principal obligatoire + associés |
| `monitoring.parquet` | 3 fichiers | Constantes ; ingéré **nativement** par ClickHouse |
| `services.csv`, `cim10.csv` | 8 services, 10 codes CIM-10 | Référentiels **J1 seulement** |

Les 10 codes CIM-10 du référentiel sont exactement ceux présents dans les diagnostics : pas de code orphelin attendu.

Le dump patients ne change pas les attributs d’un IPP déjà vu (contrôle effectué J1 vs J3). On déduplique quand même : c’est la règle demandée, et elle restera juste le jour où un reverse administratif corrigera une date de naissance.

## 3. Architecture

Patron **médaillon**, transformations **dans ClickHouse** (SQL). Python : copie, pseudonymisation, orchestration, logs.

Voir le schéma du README. Choix qui se défendent à l’oral :

- **Lake ≠ copie bit-à-bit des patients.** Le sujet dit « copie brute » *et* « aucune donnée identifiante dans l’entrepôt » *et* valorise le bonus « à l’entrée du lake ». On tranche : le filestorage CHU reste brut ; **notre** lake est déjà pseudonymisé. Sinon on stockerait NIR et nom sur le disque étudiant, ce qui est contraire à l’esprit RGPD du sujet.
- **Bronze peu transformé.** Typage + colonnes techniques `_source_date`, `_ingested_at`. On peut reconstruire silver si la règle FC change.
- **Silver = vérité métier**, en étoile : 3 faits + dimensions. Modèle : [`docs/modele-silver.md`](modele-silver.md).
- **Deux gold.** Cloisonnement réel (GRANT SQL), pas un simple onglet Metabase.
- **Pas de pandas** sur le monitoring : `file(..., Parquet)` dans ClickHouse.

Stack laptop : ClickHouse 24.8 + Metabase ≥ 0.54 (driver ClickHouse **embarqué**) + Compose. Airflow / Spark écartés comme disproportionnés ; le scheduler Python de la partie 2 joue le rôle d’un cron traçable.

## 4. Traitements

### 4.1 Entrée du lake (RGPD)

Sur `patients.csv` :

- `patient_id` → `SHA-256(sel | IPP)` = `patient_pseudo` (stable)
- `birth_date` → `birth_year`
- suppression de `nir`, `nom`, `prenom`

Sur `sejours.csv` : même hash de `patient_id` pour conserver les jointures. `discharge_ts` vide → `\N` (NULL ClickHouse).

`diagnostics.json` → NDJSON aplati (`stay_id`, `code_cim10`, `type`) : changement de **format d’ingestion**, pas de règle métier.

Monitoring et référentiels : copie telle quelle (pas de PII).

### 4.2 Bronze

Tables typées, partitionnées par `_source_date`. Ingestion **incrémentale** : un jour déjà `ok` n’est pas réinséré (`eds_ops.fichiers_traites`). `--force` droppe la partition du jour puis réinjecte.

### 4.3 Silver — contrôles

| Règle | Action | Table de rejets |
|---|---|---|
| Doublons patients | `row_number()` par pseudo, on garde `_source_date` max | — |
| Sexe ∉ {M,F} ou année aberrante | écarté | `sexe_ou_naissance_invalide` |
| `discharge_ts < admission_ts` | écarté | `discharge_avant_admission` |
| `discharge_ts` NULL | **conservé**, `est_en_cours = 1` | — |
| FC ∉ [20,250], SpO2 ∉ [50,100], temp ∉ [30,45] | écarté | `valeur_hors_plage` |
| Diagnostic d’un séjour rejeté | écarté | `sejour_absent_silver` |

On **n’impute pas** une date de sortie, on ne « corrige » pas une FC à 15 bpm. On n’est pas médecins.

`discharge_mode` vide sur un séjour **sorti** (~2 000 lignes sur 3 jours) : hors liste imposée, **conservé**. Signalé ici comme dette qualité (export incomplet), pas comme rejet silencieux.

Enrichissements : `duree_heures` et `est_en_cours` sur le **fait séjour** ; `age_approx` sur **`dim_patient`**. Les libellés restent dans `dim_service` / `dim_cim10` (pas recopiés dans les faits).

Le modèle (3 faits + dimensions) : [`modele-silver.md`](modele-silver.md).

### 4.4 Gold

Rebuild complet depuis silver à chaque run (cheap). Droits ré-appliqués après `DROP TABLE` (ClickHouse retire les GRANT au drop).

## 5. Indicateurs — formules

Toutes les requêtes sont dans [`sql/04_gold.sql`](../sql/04_gold.sql). Un chiffre du dashboard doit pouvoir se retrouver au `clickhouse-client`.

### Pilotage

**DMS par service** (séjours sortis uniquement) :

\[
\mathrm{DMS} = \mathrm{moyenne}\left(\frac{t_{\mathrm{sortie}} - t_{\mathrm{entrée}}}{24\,\mathrm{h}}\right)
\]

Un séjour en cours n’a pas de durée : l’inclure tirerait la DMS vers le bas ou imposerait une imputation. On l’exclut.

**Passages urgences / jour** : `count` des séjours avec `service_code = 'URGENCES'`, groupé par `toDate(admission_ts)`.

Ce n’est **pas** `admission_mode = 'urgence'`. Ce mode existe aussi en cardiologie (entrée non programmée dans un autre service). L’activité *des* urgences, au sens du besoin, est celle du **service** URGENCES.

**Réadmission 30 jours** : parmi les séjours sortis, part de ceux pour lesquels il existe un **autre** séjour du même `patient_pseudo` avec

\[
t_{\mathrm{entrée}}^{\mathrm{suivant}} \in \left( t_{\mathrm{sortie}} \;;\; t_{\mathrm{sortie}} + 30\,\mathrm{j} \right]
\]

Calculé **par service** (service du séjour index, celui dont on mesure la sortie).

**Alertes monitoring / jour** — bornes **d’alerte**, plus strictes que le rejet qualité :

| Signal | Rejet qualité (silver) | Alerte (gold) |
|---|---|---|
| FC (bpm) | hors 20–250 | &lt; 50 ou &gt; 120 |
| SpO2 (%) | hors 50–100 | &lt; 90 |
| Température (°C) | hors 30–45 | &lt; 36 ou &gt; 38,5 |

Un relevé hors bornes qualité n’entre pas en `fact_monitoring` : il ne peut donc pas « alerter ». L’alerte mesure l’instabilité **parmi les constantes déjà crédibles**.

Pas d’autre indicateur de pilotage (pas de vue « activité / décès / rejets » : hors §4).

### Recherche

**Prévalence** : `uniqExact(patient_pseudo)` par diagnostic **principal** (`fact_diagnostic` ⋈ `fact_sejour` ⋈ `dim_cim10`), `HAVING >= 5`.

**Âge × sexe** : patients ayant au moins un séjour, tranches 0–17 / 18–39 / 40–64 / 65+, même seuil de 5.

## 6. Restitution et cloisonnement

Deux dashboards Metabase, **uniquement** les graphes du §4 :

- **Pilotage hospitalier** — DMS, passages urgences, réadmission 30 j, relevés en alerte.
- **Recherche clinique** — prévalence par pathologie, distribution âge × sexe.

Démonstration du cloisonnement :

1. Connexion `pilotage@chu.local` → collection Recherche **absente**.
2. Connexion `recherche@chu.local` → collection Pilotage **absente**.
3. Dans ClickHouse Play, user `pilotage` :

```sql
SELECT * FROM eds_gold_recherche.prevalence_pathologie;
-- ACCESS_DENIED
```

Le cloisonnement n’est pas qu’un masque d’UI : le moteur refuse la requête.

## 7 bis. Chiffres obtenus (26–28 août 2026)

Ces volumes servent à **justifier** les KPI, pas à en tirer une conclusion médicale.

| Couche / contrôle | Effectif |
|---|---|
| Bronze patients (3 dumps) | 16 200 |
| Silver `dim_patient` (dédupliqués) | 6 000 |
| Bronze séjours | 15 000 |
| Silver `fact_sejour` | 14 864 |
| Rejets `discharge_avant_admission` | 136 (44+50+42, conforme à l’exploration brute) |
| Bronze monitoring | 66 677 |
| Silver `fact_monitoring` | 65 308 |
| Rejets constantes hors plage | 1 369 |
| Diagnostics orphelins (séjour rejeté) | 340 |
| DMS par service | ~6,0–6,2 jours (données d’exercice, peu de variance) |
| Passages URGENCES | 617 / 639 / 581 sur J1–J3 |
| Taux réadmission 30 j | 5,0–6,4 % selon le service (fenêtre de 3 jours : **sous-estimé**) |
| Alertes monitoring | ~6,5 % des relevés silver |
| Cohortes recherche | 10 pathologie, toutes n ≥ 1 250 — la règle n&lt;5 est en place mais non visible |

Le lake patients ne contient que `patient_pseudo, birth_year, sex, region_code` (vérifié). Le user ClickHouse `pilotage` reçoit `ACCESS_DENIED` sur `eds_gold_recherche.*`.

## 7. Limites et recommandations

| Limite | Conséquence | Recommandation |
|---|---|---|
| 3 jours de dépôt | DMS et réadmission 30 j sont **sous-estimées / instables** (beaucoup de séjours sortent après J+3, hors fenêtre) | Recalculer sur 6–12 mois avant tout usage DIM |
| Âge = année civile − année de naissance | Erreur jusqu’à 1 an | Si un usage clinique l’exige : mois de naissance, jamais le jour |
| 10 codes CIM-10 | Toutes les cohortes dépassent 5 patients : la règle k=5 est **implémentée mais peu visible** | Tester avec un filtre ou un code rare |
| Données d’exercice | Identités et constantes synthétiques | Ne pas extraire de conclusion médicale |
| Hash + sel unique | Rotation du sel = rupture d’historique | Prévoir une table de correspondance **hors lake**, HSM / coffre, en production |
| Scheduler ≠ Airflow | Pas de DAG visuel, pas de SLA multi-équipe | Garder le SQL ; remplacer le process Python par Airflow plus tard |
| Minimisation | Impossible de contrôler prénom vs sexe après le lake | Ce contrôle, s’il est utile, doit se faire **côté producteur** (CHU) |

Recommandations de gouvernance : journal d’accès Metabase, revue trimestrielle des GRANT, pas de `SELECT` gold recherche pour le DIM, DPO associé à toute nouvelle table individuelle (nous n’en avons aucune en gold).
