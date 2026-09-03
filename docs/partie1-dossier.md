# Partie 1 — Dossier d’analyse

Entrepôt de Données de Santé (EDS) du CHU. Ce dossier complète le [README](../README.md) : besoin, sources, architecture, traitements, indicateurs, restitution, limites.

## 1. Analyse du besoin

Le CHU veut **centraliser** des exports quotidiens aujourd’hui éparpillés, pour deux usages :

1. **Pilotage hospitalier** (direction, DIM) : activité, durées de séjour, urgences, qualité des soins, surveillance des constantes.
2. **Recherche clinique** : tailles de cohortes par pathologie, description (âge, sexe).

Contrainte transverse : données de santé (respect du RGPD art. 9). Ce n’est pas un filtre final, c’est une **contrainte de conception** : on ne stocke pas l’identité, on sépare les publics, on masque les petits effectifs, on trace.

Ce que l’hôpital n’a pas demandé, et que nous ne livrons pas : un DPI, un outil de prescription, un identifiant nominatif pour le clinicien de terrain. L’EDS est un entrepôt **secondaire**.

## 2. Sources

Accès en **lecture seule** au filestorage CHU (`source-filestorage/`). Un mois : 1er–28 août 2026, plus un dépôt d’évolution le **29** (actes + description des services + CCAM).

| Source | Volume observé | Caractère |
|---|---|---|
| `patients.csv` | 3 × 6 000 lignes | Dump **complet** (snapshots). Identité en clair |
| `sejours.csv` | 6 797 lignes au total | Incrémental quotidien |
| `diagnostics.json` | un fichier / jour | 1 diagnostic principal + associés |
| `monitoring.parquet` | 28 fichiers | Constantes ; ingéré **nativement** par ClickHouse |
| `actes.parquet` | 8 112 lignes (29/08) | Nouveau flux ; pas de `service_code` sur l’acte |
| `services.csv`, `cim10.csv` | 8 services, 13 codes CIM-10 | Référentiels **J1** |
| `description_service.csv`, `ccam.csv` | 7 services décrits, 8 actes | Référentiels **J29** — description **incomplète** (NEURO absent) |

Les 13 codes CIM-10 du référentiel sont ceux des diagnostics (dont E84, Q90, G12 : petits effectifs).

Le dump patients ne change pas les attributs d’un IPP déjà vu (contrôle effectué J1 vs J3). On déduplique quand même : c’est la règle demandée, et elle restera juste le jour où un reverse administratif corrigera une date de naissance.

## 3. Architecture

Patron **médaillon**, transformations **dans ClickHouse** (SQL). Python : copie, pseudonymisation, orchestration, logs.

Voir le schéma du README. Choix qui se défendent à l’oral :

- **Lake ≠ copie bit-à-bit des patients.** Le sujet dit « copie brute » *et* « aucune donnée identifiante dans l’entrepôt » *et* valorise le bonus « à l’entrée du lake ». On tranche : le filestorage CHU reste brut ; **notre** lake est déjà pseudonymisé. Sinon on stockerait NIR et nom sur le disque étudiant, ce qui est contraire à l’esprit RGPD du sujet.
- **Bronze peu transformé.** Typage + colonnes techniques `_source_date`, `_ingested_at`. On peut reconstruire silver si la règle FC change.
- **Silver = vérité métier**, 4 faits **sans lien entre eux** (chacun ne joint qu’aux dimensions). Modèle : [`docs/modele-silver.md`](modele-silver.md).
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

Monitoring, actes et référentiels : copie telle quelle (pas de PII).

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
| Diagnostic d’un séjour aux dates inversées | **conservé** dans `fact_diagnostic` (le séjour est écarté de `fact_sejour`) | — |
| Service sans ligne dans `description_service` | **conservé** dans `dim_service`, `categorie` / `pole` / `capacite_lits` = NULL | `service_sans_description` |
| Acte sans séjour | écarté | `sejour_introuvable` |
| Acte d’un séjour aux dates inversées | **conservé** dans `fact_acte` (le service est lu dans `bronze.sejours`) | — |

On **n’impute pas** une date de sortie, on ne « corrige » pas une FC à 15 bpm, on n’invente pas une catégorie pour NEURO. On n’est pas médecins.

`discharge_mode` vide sur un séjour **sorti** : hors liste imposée, **conservé**. Signalé ici comme dette qualité (export incomplet), pas comme rejet silencieux.

Enrichissements : `duree_heures` et `est_en_cours` sur le **fait séjour** ; `age_approx` sur **`dim_patient`** ; `service_code` recopié sur **`fact_acte`** depuis `bronze.sejours` (jamais depuis `fact_sejour`). Les libellés, tarifs et la hiérarchie restent dans les dimensions.

Le modèle (4 faits + dimensions) : [`modele-silver.md`](modele-silver.md).

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

**Passages urgences / jour** : `count` des séjours avec `service_code = 'URGENCES'`, groupé par `toDate(admission_ts)`, plus `nb_encore_presents` (sortie encore vide à la fin de la fenêtre) et `duree_moy_heures` (moyenne sur les séjours clos — `avg` ignore les NULL).

Ce n’est **pas** `admission_mode = 'urgence'`. Ce mode existe aussi en cardiologie (entrée non programmée dans un autre service). L’activité *des* urgences, au sens du besoin, est celle du **service** URGENCES.

**Réadmission 30 jours** : un séjour est une réadmission s’il existe un **autre** séjour du même `patient_pseudo` **sorti** tel que

\[
t_{\mathrm{entrée}}^{\mathrm{actuel}} \in \left( t_{\mathrm{sortie}}^{\mathrm{précédent}} \;;\; t_{\mathrm{sortie}}^{\mathrm{précédent}} + 30\,\mathrm{j} \right]
\]

Indicateur **global** (pas par service) : `nb_readmissions / nb_sejours_silver`.

**Alertes monitoring / jour** — bornes **d’alerte**, plus strictes que le rejet qualité :

| Signal | Rejet qualité (silver) | Alerte (gold) |
|---|---|---|
| FC (bpm) | hors 20–250 | &lt; 50 ou &gt; 100 |
| SpO2 (%) | hors 50–100 | &lt; 92 |
| Température (°C) | hors 30–45 | &gt; 38,5 |

Un relevé hors bornes qualité n’entre pas en `fact_monitoring` : il ne peut donc pas « alerter ». L’alerte mesure l’instabilité **parmi les constantes déjà crédibles**.

### Pilotage — évolution 2026-08-29

Toujours `fact ⋈ dimension`, jamais deux faits.

**Activité et DMS par catégorie** : `fact_sejour` ⋈ `dim_service`, `GROUP BY categorie`. `nb_sejours` = tous les séjours ; la DMS uniquement sur les sortis (`avgIf`). NEURO sans description → `non renseigne`.

**Actes par service** : `fact_acte` ⋈ `dim_service`. Moyenne = `count() / uniqExact(stay_id)` parmi les séjours **qui ont au moins un acte**.

**Actes par type** : `fact_acte` ⋈ `dim_ccam`.

**Densité actes / lit** : `nb_actes / capacite_lits`. Si la capacité est NULL (NEURO), la densité est NULL — on ne divise pas par 0 et on n’impute pas de lits.

**Montant T2A / service** : `sum(tarif_euros)` via `fact_acte` ⋈ `dim_ccam` ⋈ `dim_service`.

### Recherche

**Prévalence** : `uniqExact(patient_pseudo)` par code CIM-10 posé — principal **ou** associé (`fact_diagnostic` ⋈ `dim_cim10`). `nb_patients_diffusable` est NULL si `nb_patients < 5`.

**Âge × sexe** : diagnostic **principal** uniquement, tranches de 10 ans (`0-9` … `90-99`), même masque.

## 6. Restitution et cloisonnement

Deux dashboards Metabase historiques (non-régression) **plus** un dashboard d’évolution, tous dans la collection Pilotage sauf la recherche :

- **Pilotage hospitalier** — DMS, passages urgences, réadmission 30 j, relevés en alerte.
- **Pilotage — actes et T2A** — catégorie, actes / service, actes / type, densité / lit, montant T2A.
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

## 7 bis. Chiffres obtenus (1er–29 août 2026)

Ces volumes servent à **justifier** les KPI, pas à en tirer une conclusion médicale.

| Couche / contrôle | Effectif |
|---|---|
| Fichiers traités au total (`eds_ops.fichiers_traites`) | **92** (89 initiaux + 3 évolution 29/08) |
| Bronze patients (3 dumps) | 18 000 |
| Silver `dim_patient` (dédupliqués) | 6 000 |
| Bronze séjours | 6 797 |
| Silver `fact_sejour` | 6 729 |
| Rejets `discharge_avant_admission` | 68 |
| Bronze monitoring | 41 778 |
| Silver `fact_monitoring` | 40 920 |
| Rejets constantes hors plage | 858 |
| Bronze actes (`eds_bronze.actes`) | 8 112 |
| Silver actes (`eds_silver.fact_acte`) | 8 112 (0 rejet `sejour_introuvable`) |
| Référentiel CCAM (`dim_ccam`) | 8 actes |
| Description services (`dim_service`) | 8 services (7 décrits + NEURO attributs NULL) |
| Rejet référentiel `service_sans_description` | 1 (NEURO) |
| DMS (REA) | 9,05 j / 217,1 h (423 séjours clos) |
| DMS par catégorie (`non renseigne` / NEURO) | 7,06 j / 169,5 h (1 077 séjours clos) |
| Réadmission 30 j | 780 / 6 729 = **11,59 %** |
| Prévalence | N39 = 2 234 … G12 = 8 ; E84 = 4 et Q90 = 3 **masqués** (`< 5`) |
| Top acte CCAM | ZBQK001 (Radio thorax) : 1 043 actes |
| Densité actes / lit max (URGENCES) | 86,55 (1 731 actes / 20 lits) ; NEURO = NULL |
| T2A CARDIO | 521 655 € (1 935 actes) |
| Total facturé T2A CHU | **2 199 445 €** (8 112 actes) |

Le lake patients ne contient que `patient_pseudo, birth_year, sex, region_code` (vérifié). Le user ClickHouse `pilotage` reçoit `ACCESS_DENIED` sur `eds_gold_recherche.*` et inversement pour `recherche` sur `eds_gold_pilotage.*`.

## 7. Limites et recommandations

| Limite | Conséquence | Recommandation |
|---|---|---|
| Fenêtre d’un mois | La réadmission à 30 jours est calculable, mais les séjours encore ouverts en fin de mois n’ont pas de DMS | Recalculer sur 6–12 mois avant tout usage DIM |
| Âge = 2026 − année de naissance | Erreur jusqu’à 1 an ; tranches de 10 ans | Si un usage clinique l’exige : mois de naissance, jamais le jour |
| Petits effectifs | E84 (4) et Q90 (3) sont masqués ; G12 (8) est diffusé | La règle `n < 5` est visible sur ce jeu |
| Données d’exercice | Identités et constantes synthétiques | Ne pas extraire de conclusion médicale |
| Hash + sel unique | Rotation du sel = rupture d’historique | Prévoir une table de correspondance **hors lake**, HSM / coffre, en production |
| Scheduler ≠ Airflow | Pas de DAG visuel, pas de SLA multi-équipe | Garder le SQL ; remplacer le process Python par Airflow plus tard |
| Minimisation | Impossible de contrôler prénom vs sexe après le lake | Ce contrôle, s’il est utile, doit se faire **côté producteur** (CHU) |

Recommandations de gouvernance : journal d’accès Metabase, revue trimestrielle des GRANT, pas de `SELECT` gold recherche pour le DIM, DPO associé à toute nouvelle table individuelle (nous n’en avons aucune en gold).
