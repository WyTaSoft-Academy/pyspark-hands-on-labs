# TP 5 — Statistiques en temps réel et prédictions

**Module 5 · Analyser en temps réel** · ≈ 1 h 20 · individuel

## Objectif

Calculer en direct, sur le flux des trajets Vélo'Cité :

- le nombre de trajets par station, mis à jour toutes les 5 secondes ;
- le nombre de trajets par quartier et par tranche de 5 minutes, en tolérant les retardataires ;
- la part de trajets « occasionnels » **prédite** par le modèle du TP 4, comparée à la part réelle ;

puis vérifier qu'une requête arrêtée et relancée **reprend là où elle s'était arrêtée**, sans perte ni doublon.

## Fichiers

| Fichier | Rôle |
|---|---|
| `depart.ipynb` | Le notebook à compléter (source : `depart.py`) |
| `../flux/emetteur.py` | L'émetteur du flux |
| `../donnees/stations.csv` | La table statique des stations |
| `../sorties/modele-type-usager/` | Le modèle sauvegardé au TP 4 |

## Avant de commencer

1. Les données doivent être générées : `python donnees/generer_donnees.py` (depuis `pyspark-hands-on-labs/`).
2. **Dans un terminal à part**, depuis `pyspark-hands-on-labs/`, lancez l'émetteur et laissez-le tourner :

   ```
   python flux/emetteur.py --vider
   ```

   Il dépose un fichier `lot-….json` par seconde dans `flux/entree/` (20 trajets par fichier,
   3 % d'entre eux en retard de 2 à 15 minutes). Vérifiez que les fichiers apparaissent.
3. Ouvrez `depart.ipynb`.

Sous Windows, `PYSPARK_PYTHON` doit être défini (voir TP 1), sinon les exécuteurs ne démarrent pas.

## Étapes

### Étape 1 — Lire le flux, avec un schéma explicite

Une source fichier en streaming **exige** un schéma. Écrivez-le en DDL et lisez `flux/entree/`
avec `spark.readStream.schema(...).json(...)`. `trajets.isStreaming` doit valoir `True`.

### Étape 2 — Comptes courants par station de départ

`groupBy("station_depart").count()`, puits `memory` nommé `comptes_stations`, déclencheur
`processingTime="5 seconds"`. Choisissez le mode de sortie : un compte déjà écrit peut-il
encore changer ?

Interrogez la table trois fois, à dix secondes d'intervalle.

**Attendu** : des comptes qui augmentent à chaque lecture. Avec l'émetteur par défaut, de
l'ordre de 200 trajets de plus toutes les dix secondes, et une dizaine à une quarantaine
de trajets pour les stations en tête au bout de trente secondes. Les stations en tête
changent d'une lecture à l'autre : le flux est tiré au hasard.

### Étape 3 — Fenêtres de 5 minutes par quartier

1. Joignez le flux à `stations.csv` pour obtenir le `quartier` de la station de départ.
2. Ajoutez `heure`, `jour_semaine` et `week_end` calculées sur `debut` (le modèle de l'étape 4
   en a besoin, calculées **exactement** comme au TP 3).
3. Agrégez par `F.window("fin", "5 minutes")` et `quartier`, avec
   `withWatermark("fin", "10 minutes")` **avant** le `groupBy`.
4. Puits `memory` nommé `par_quartier`, mode `update`.

**Attendu** : pour la tranche en cours, les huit quartiers, de quelques dizaines de trajets
chacun, voire plus de 150 si la requête tourne longtemps ; une durée moyenne plus longue pour
Parc (≈ 15 min) que pour Centre ou Affaires (≈ 11–12 min). L'ordre des quartiers change d'une
exécution à l'autre : le flux est tiré au hasard. Quelques
lignes sur les tranches précédentes, avec 1 ou 2 trajets : ce sont les **retardataires**.

Question : dans `q.lastProgress["eventTime"]`, que vaut `watermark` ? Tant que la requête n'a
fait qu'un lot, il vaut `1970-01-01T00:00:00.000Z` : le watermark se calcule sur ce qu'un lot a
vu, et ne s'applique qu'au lot suivant. Ensuite, que vaut-il, et pourquoi est-il en UTC ?

### Étape 4 — Appliquer le modèle du TP 4 au fil de l'eau

1. `PipelineModel.load(MODELE)`. Si le TP 4 n'est pas terminé, appelez
   `modele_de_secours()`, fournie dans le notebook, qui entraîne le même pipeline que le TP 4 (régression
   logistique « au dépôt ») sur un échantillon, avec les mêmes colonnes d'entrée.
2. `modele.transform(enrichis)` : un DataFrame de flux, comme les autres. Le modèle attend sept
   colonnes (contrat du TP 4) : `heure`, `jour_semaine`, `week_end`, `electrique`, `quartier`,
   `duree_min`, `distance_km`. Les trois premières et `quartier` viennent de l'étape 3.
3. Colonne `reel` = 1.0 si le trajet est celui d'un occasionnel.
4. Par fenêtre de 5 minutes : `count`, `avg("prediction")`, `avg("reel")`.

**Attendu** : sur la tranche en cours, une part prédite proche de la part réelle, toutes deux
entre 0,25 et 0,45 — à la préparation, 0,387 prédite pour 0,409 réelle sur 751 trajets.
Les tranches plus anciennes ne contiennent que des retardataires : quelques trajets, donc des
parts qui sautent. Ne les comparez pas.

### Étape 5 — Point de reprise : arrêter, relancer, ne rien perdre

1. Une fonction `lancer_ecriture()` qui écrit `trajet_id`, `fin`, `quartier`, `type_usager`,
   `prediction` en Parquet dans `sorties/predictions-flux/`, mode `append`, avec
   `checkpointLocation` dans `flux/points-de-reprise/predictions`.
2. Lancez, attendez, arrêtez. Comptez les lignes écrites.
3. Attendez dix secondes (l'émetteur continue), relancez avec **le même** point de reprise.
4. Comptez à nouveau, et comptez les `trajet_id` distincts.

**Attendu** : le premier lot après la relance porte le numéro qui suit le dernier lot
validé (par exemple lot 3 après un arrêt au lot 2), et le nombre de lignes égale le nombre
de `trajet_id` distincts : **aucun doublon**.

### Fin

Arrêtez toutes les requêtes (`for q in spark.streams.active: q.stop()`) et l'émetteur (`Ctrl+C`).

## Bonus, pour ceux qui ont fini

1. **Fenêtres glissantes** : trajets par quartier sur 10 minutes, mises à jour toutes les
   5 minutes (`F.window("fin", "10 minutes", "5 minutes")`). Pourquoi la somme des fenêtres
   dépasse-t-elle le nombre de trajets ?
2. **Le retardataire perdu** : relancez l'émetteur avec `--retard 0.3`, puis surveillez
   `q.lastProgress["stateOperators"][0]["numRowsDroppedByWatermark"]` sur la requête de
   l'étape 3. Passez le watermark à 2 minutes : que devient ce compteur ?
3. **`foreachBatch`** : écrivez chaque lot des comptes par quartier dans un fichier CSV
   différent (`batch_df.write.csv(...)` dans la fonction). Quelle garantie perd-on ?
4. **`availableNow`** : arrêtez l'émetteur, puis relancez l'écriture Parquet avec
   `.trigger(availableNow=True)` et `q.awaitTermination()`. La requête traite ce qui reste
   et s'arrête d'elle-même.

## Pièges fréquents

| Symptôme | Cause |
|---|---|
| La table `memory` est vide | Le premier lot n'est pas terminé : attendez 10 secondes |
| `flux/entree/` est vide | L'émetteur n'est pas lancé, ou lancé depuis un autre dossier |
| `AnalysisException` au `start()` sur le mode `append` | Agrégation sans watermark : utilisez `update` ou `complete` |
| Erreur au redémarrage sur un point de reprise | La requête a changé : nouveau dossier de reprise |
| Les exécuteurs ne démarrent pas (Windows) | `PYSPARK_PYTHON` non défini |
| Des lignes `ERROR … InterruptedException` à l'arrêt d'une requête | Le lot en cours est interrompu par `stop()` : sans conséquence |
