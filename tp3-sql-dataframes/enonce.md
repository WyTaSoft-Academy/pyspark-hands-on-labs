# TP 3 — Nettoyer, joindre, interroger

**Module 3 · Spark SQL et DataFrames** · ≈ 1 h 20 · TP tutoré, en binôme

## Objectif

Transformer les trajets bruts de Vélo'Cité en une table propre, typée et
enrichie, puis y répondre à quatre questions métier **deux fois** : avec l'API
DataFrame, puis en SQL. Le résultat est écrit en Parquet : les TP 4, 5 et 6
repartent de là.

## Point de départ

- Notebook : `depart.ipynb` (ou `depart.py`, le même contenu en script)
- Données : `../donnees/trajets.csv`, `stations.csv`, `meteo.csv`
  — générées par `python ../donnees/generer_donnees.py` si elles manquent
- Corrigé : projeté en fin de TP

## Étapes

### 1. Lire avec un schéma explicite

Déclarez le schéma des onze colonnes de `trajets.csv` (`StructType` ou chaîne
DDL), lisez le fichier avec, puis convertissez `debut` et `fin` en `timestamp`.
Vérifiez avec `printSchema()`.

> Attendu : **300 300 lignes brutes**.

### 2. Mesurer les défauts, puis les retirer

Comptez, **avant** de supprimer quoi que ce soit :

| Défaut | Attendu |
|---|---|
| Durée nulle ou négative | 300 |
| Station d'arrivée vide | 300 |
| Type d'usager vide | 150 |
| Doublons exacts | 300 |

Retirez-les, puis ajoutez trois colonnes : `heure` (`F.hour`), `jour_semaine`
(`F.dayofweek`, 1 = dimanche) et `week_end` (booléen). Mettez le résultat en
cache.

> Attendu : **299 251 lignes propres**.

Piège : une cellule vide d'un CSV est lue comme `null`, pas comme `""`.

### 3. Joindre les stations et la météo

- le **quartier de départ**, depuis `stations.csv` ;
- la **température** et la **pluie** du jour, depuis `meteo.csv` (clé : la date
  de `debut`, via `F.to_date`).

### 4. Quatre questions, deux façons

Pour chacune : une réponse en API DataFrame, et au moins deux des quatre en SQL
après `createOrReplaceTempView("trajets")`.

1. Combien de trajets partent de chaque quartier ? — *Gare en tête, 40 004.*
2. À quelle heure roulent les abonnés, et les occasionnels ? (`pivot`) —
   *pointes à 8 h et 17 h pour les abonnés, 15 h pour les occasionnels.*
3. La pluie fait-elle baisser le nombre de trajets par jour ? —
   *≈ 1 727 trajets par jour sec, ≈ 1 538 par pluie faible, ≈ 1 050 par pluie forte (3 jours seulement).*
4. Quels sont les cinq trajets (départ → arrivée, stations distinctes) les plus
   fréquents ? — *S013 → S005 en tête, 305 trajets.*

### 5. Une fonction de fenêtre

Pour chaque quartier, la station de départ la plus utilisée (`Window.partitionBy`
+ `F.rank`). — *Huit lignes, une par quartier, autour de 5 100 trajets chacune.*

### 6. La même question avec l'API pandas

Passez par `pandas_api()` et calculez la durée moyenne par type d'usager,
exactement comme on l'écrirait en pandas.
— *≈ 8,7 min pour les abonnés, ≈ 20,5 min pour les occasionnels.*

### 7. Écrire en Parquet

Écrivez la table **propre** (étape 2, sans les colonnes de jointure) dans
`../sorties/trajets-propres`, en mode `overwrite`. C'est le contrat des TP
suivants : les colonnes du CSV typées, plus `heure`, `jour_semaine`, `week_end`.

Comparez la taille du dossier Parquet à celle du CSV, puis **comptez les
fichiers produits**. Sans rien faire, le dédoublonnage laisse 200 partitions,
donc 200 fichiers de 60 ko : ajoutez `coalesce(4)` avant `write` et regardez la
différence. Un fichier par partition, toujours.

### 8. Comparer les lectures

Chronométrez la même agrégation, **lecture comprise**, depuis le CSV (avec
`inferSchema`) et depuis le Parquet. Mesurez **trois fois** et gardez la
meilleure : la première mesure paie le démarrage, et une mesure isolée sur un
poste occupé ne veut rien dire. Sur le poste de préparation, machine au repos :
**CSV 3,5 s, Parquet 0,65 s**. Sur la même machine chargée par d'autres
traitements : 6,8 s et 4,7 s, avec une première mesure du CSV à 47 s. Les
valeurs absolues ne veulent rien dire hors de leur contexte ; le rapport, si.

## Bonus

- Affichez le plan de la question 1 avec `explain()` : où le filtre et la
  jointure apparaissent-ils ?
- Écrivez les trajets partitionnés par mois (`partitionBy`) et vérifiez avec
  `explain()` qu'une lecture filtrée sur un mois ne lit qu'un dossier.
- Calculez le chiffre d'affaires par quartier et par mois, en SQL, avec une
  colonne « évolution par rapport au mois précédent » (`LAG`).
- **Une ligne illisible.** Créez un petit CSV de trois lignes dont l'une porte
  `N/A` dans `duree_min`, et lisez-le avec un schéma déclaré dans les trois modes
  `PERMISSIVE`, `DROPMALFORMED` et `FAILFAST` (`option("mode", …)`). Lequel
  voudriez-vous pour un traitement qui tourne la nuit ? — *3 lignes dont une
  `NULL`, 2 lignes, puis une erreur `MALFORMED_RECORD_IN_PARSING`.*
- **Sans Python.** Interrogez la table Parquet de l'étape 7 avec la console
  `spark-sql`, installée avec PySpark, à partir d'un fichier `requete.sql` :
  `SELECT type_usager, COUNT(*) FROM parquet.`<chemin>` GROUP BY type_usager`,
  puis `spark-sql --master "local[2]" -f requete.sql`. — *abonne 189 723,
  occasionnel 109 528.*

## Ce qui est évalué

Pas la vitesse : la capacité à **compter avant de supprimer**, à choisir entre
API DataFrame et SQL en connaissance de cause, et à produire une table que
quelqu'un d'autre peut réutiliser sans la relire.
