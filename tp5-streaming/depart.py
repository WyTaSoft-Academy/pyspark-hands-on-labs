"""
TP 5 — Statistiques en temps réel et prédictions : notebook de départ.

Avant de commencer, dans un terminal à part, depuis le dossier pyspark-hands-on-labs/ :

    python flux/emetteur.py --vider

et laissez-le tourner pendant tout le TP.
"""
# %% [markdown]
# # TP 5 — Statistiques en temps réel et prédictions
#
# L'émetteur rejoue les trajets Vélo'Cité « maintenant » : un petit fichier
# JSON par seconde dans `flux/entree/`, dont 3 % d'événements en retard.
#
# **Avant d'exécuter la première cellule**, vérifiez que des fichiers
# `lot-….json` apparaissent dans `flux/entree/`.
#
# Énoncé complet : `enonce.md`.

# %%
import os
import shutil
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ICI = os.path.dirname(os.path.abspath(globals().get("__file__", "depart.py")))
TP = os.path.dirname(ICI)
DONNEES = os.path.join(TP, "donnees")
FLUX = os.path.join(TP, "flux")
ENTREE = os.path.join(FLUX, "entree")
REPRISE = os.path.join(FLUX, "points-de-reprise")
SORTIES = os.path.join(TP, "sorties")
MODELE = os.path.join(SORTIES, "modele-type-usager")
PREDICTIONS = os.path.join(SORTIES, "predictions-flux")

spark = (SparkSession.builder
         .master("local[2]")
         .appName("tp5-streaming")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Points de reprise d'une exécution précédente : on repart de zéro.
shutil.rmtree(REPRISE, ignore_errors=True)
shutil.rmtree(PREDICTIONS, ignore_errors=True)
print("fichiers dans le flux :", len(os.listdir(ENTREE)))

# %% [markdown]
# ## Étape 1 — Lire le flux, avec un schéma explicite
#
# Colonnes des fichiers JSON : `trajet_id`, `velo_id`, `station_depart`,
# `station_arrivee` (texte), `debut`, `fin` (horodatages), `duree_min`,
# `distance_km` (réels), `type_usager` (texte), `electrique` (entier),
# `tarif_eur` (réel).
#
# 1. Écrivez le schéma sous forme de chaîne DDL : `"trajet_id STRING, …"`.
# 2. Lisez `ENTREE` avec `spark.readStream`, au format JSON.
# 3. Vérifiez que `trajets.isStreaming` vaut `True`.

# %%
SCHEMA = """
    TODO
"""

trajets = None  # TODO

# %% [markdown]
# ## Étape 2 — Comptes courants par station de départ
#
# 1. Comptez les trajets par `station_depart`.
# 2. Écrivez le résultat dans le puits `memory`, sous le nom `comptes_stations`,
#    avec un déclencheur toutes les 5 secondes. Quel mode de sortie ?
# 3. Exécutez la cellule suivante trois fois, à dix secondes d'intervalle :
#    les nombres doivent augmenter.

# %%
comptes = None  # TODO

q_comptes = None  # TODO : comptes.writeStream …start()

# %%
spark.sql("""SELECT station_depart, count FROM comptes_stations
             ORDER BY count DESC LIMIT 5""").show()

# %%
q_comptes.stop()

# %% [markdown]
# ## Étape 3 — Fenêtres de 5 minutes par quartier
#
# 1. Lisez `stations.csv` (table statique) et joignez-la au flux pour donner à
#    chaque trajet le `quartier` de sa station de départ.
# 2. Ajoutez les colonnes `heure` (`F.hour`), `jour_semaine` (`F.dayofweek`)
#    et `week_end` (dimanche = 1, samedi = 7) calculées sur `debut` : le
#    modèle de l'étape 4 en a besoin.
# 3. Comptez les trajets et la durée moyenne par fenêtre fixe de 5 minutes
#    sur `fin` et par quartier, avec un watermark de 10 minutes.
# 4. Puits `memory` nommé `par_quartier`, mode `update`.

# %%
stations = spark.read.csv(os.path.join(DONNEES, "stations.csv"),
                          header=True, inferSchema=True)

enrichis = None  # TODO

par_quartier = None  # TODO

q_quartiers = None  # TODO

# %%
(spark.table("par_quartier")
 .groupBy("window", "quartier")
 .agg(F.max("trajets").alias("trajets"))
 .select(F.date_format("window.start", "HH:mm").alias("debut"),
         "quartier", "trajets")
 .orderBy(F.desc("debut"), F.desc("trajets"))
 .show(10))

# %%
q_quartiers.stop()

# %% [markdown]
# ## Étape 4 — Appliquer le modèle du TP 4 au fil de l'eau
#
# 1. Chargez le modèle `MODELE` avec `PipelineModel.load`. S'il n'existe pas
#    (TP 4 non terminé), appelez `modele_de_secours()`, fournie dans la
#    cellule ci-dessous : elle entraîne le même pipeline sur un échantillon.
# 2. Appliquez-le à `enrichis` avec `transform`.
# 3. Ajoutez une colonne `reel` : 1.0 si `type_usager` vaut `occasionnel`.
# 4. Par fenêtre de 5 minutes (watermark 10 minutes) : nombre de trajets,
#    moyenne de `prediction` (part prédite), moyenne de `reel` (part réelle).
# 5. Puits `memory` nommé `part_occasionnels`, mode `update`.

# %%
# Fourni, rien à compléter : le repli si le TP 4 n'a pas sauvegardé son modèle.
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import (OneHotEncoder, StandardScaler,
                                StringIndexer, VectorAssembler)

# Les colonnes d'entrée du modèle « au dépôt » du TP 4 (voir tp4-ml/enonce.md).
AU_DEPOT = ["heure", "jour_semaine", "week_end", "electrique",
            "quartier_vec", "duree_min", "distance_km"]


def modele_de_secours():
    """Le même pipeline que le TP 4, entraîné ici si le TP 4 n'a pas été fait."""
    print("modèle du TP 4 absent : entraînement du même pipeline, en réduit")
    historique = (spark.read.csv(os.path.join(DONNEES, "trajets.csv"),
                                 header=True, inferSchema=True)
                  .dropDuplicates()
                  .where("duree_min > 0 AND station_arrivee IS NOT NULL"
                         " AND type_usager IS NOT NULL")
                  .withColumn("heure", F.hour("debut"))
                  .withColumn("jour_semaine", F.dayofweek("debut"))
                  .withColumn("week_end", F.dayofweek("debut").isin(1, 7))
                  .join(stations.select(F.col("station_id").alias("station_depart"),
                                        "quartier"), "station_depart")
                  .withColumn("label", (F.col("type_usager") == "occasionnel")
                              .cast("double"))
                  .sample(0.2, seed=42))
    etapes = [
        StringIndexer(inputCol="quartier", outputCol="quartier_idx",
                      handleInvalid="keep"),
        OneHotEncoder(inputCols=["quartier_idx"], outputCols=["quartier_vec"]),
        VectorAssembler(inputCols=AU_DEPOT, outputCol="brutes"),
        StandardScaler(inputCol="brutes", outputCol="features"),
        LogisticRegression(labelCol="label", featuresCol="features"),
    ]
    return Pipeline(stages=etapes).fit(historique)


# %%
modele = None  # TODO

predits = None  # TODO

part_predite = None  # TODO

q_ml = None  # TODO

# %%
spark.table("part_occasionnels").orderBy(F.desc("trajets")).show(truncate=False)

# %%
q_ml.stop()

# %% [markdown]
# ## Étape 5 — Point de reprise : arrêter, relancer, ne rien perdre
#
# 1. Écrivez une fonction `lancer_ecriture()` qui envoie `trajet_id`, `fin`,
#    `quartier`, `type_usager` et `prediction` vers des fichiers Parquet dans
#    `PREDICTIONS`, mode `append`, avec un point de reprise dans
#    `os.path.join(REPRISE, "predictions")`.
# 2. Lancez-la, attendez 15 secondes, arrêtez-la. Comptez les lignes écrites.
# 3. Attendez encore 10 secondes, relancez-la avec le même point de reprise.
#    Quel est le numéro du premier lot après la relance ?
# 4. Vérifiez qu'il n'y a aucun doublon : autant de `trajet_id` distincts que
#    de lignes.

# %%
def lancer_ecriture():
    pass  # TODO


# %% [markdown]
# ## Fin — tout arrêter
#
# Arrêtez aussi l'émetteur dans son terminal (`Ctrl+C`).

# %%
for q in spark.streams.active:
    q.stop()
spark.stop()
