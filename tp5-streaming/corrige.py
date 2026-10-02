"""
TP 5 — Statistiques en temps réel et prédictions : corrigé.

    python corrige.py

Le script lance lui-même l'émetteur de flux en tâche de fond, fait tourner
chaque requête quelques dizaines de secondes, affiche les résultats et
s'arrête proprement. Durée totale : 3 à 5 minutes selon le poste.
"""
# %% [markdown]
# # TP 5 — Statistiques en temps réel et prédictions (corrigé)
#
# Le flux : les trajets Vélo'Cité rejoués « maintenant » par `flux/emetteur.py`,
# un petit fichier JSON par seconde dans `flux/entree/`.

# %%
import os
import shutil
import subprocess
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ICI = os.path.dirname(os.path.abspath(globals().get("__file__", "corrige.py")))
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
         .config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# On repart de zéro : flux vide, aucun point de reprise.
for dossier in (ENTREE, REPRISE, PREDICTIONS):
    shutil.rmtree(dossier, ignore_errors=True)
os.makedirs(ENTREE)

# %% [markdown]
# ## Étape 0 — Démarrer l'émetteur
#
# En salle, on le lance dans un terminal à part :
# `python flux/emetteur.py --vider`. Ici, le corrigé le démarre lui-même.

# %%
emetteur = subprocess.Popen(
    [sys.executable, os.path.join(FLUX, "emetteur.py"),
     "--debit", "40", "--retard", "0.05", "--silencieux"])
time.sleep(3)
print("fichiers déjà déposés :", len(os.listdir(ENTREE)))

# %% [markdown]
# ## Étape 1 — Lire le flux, avec un schéma explicite
#
# Une source fichier en streaming refuse de deviner le schéma : il faut le
# donner. C'est aussi ce qui protège d'un fichier mal formé.

# %%
SCHEMA = """
    trajet_id STRING, velo_id STRING,
    station_depart STRING, station_arrivee STRING,
    debut TIMESTAMP, fin TIMESTAMP,
    duree_min DOUBLE, distance_km DOUBLE,
    type_usager STRING, electrique INT, tarif_eur DOUBLE
"""

trajets = (spark.readStream
           .schema(SCHEMA)
           .option("maxFilesPerTrigger", 10)
           .json(ENTREE))
print("flux ?", trajets.isStreaming)


def patienter(requete, secondes):
    """Laisse tourner une requête quelques secondes, et au moins un lot.

    Pas de processAllAvailable() ici : l'émetteur dépose un fichier par
    seconde, il y a toujours du nouveau, et l'appel ne rendrait jamais la main.
    """
    time.sleep(secondes)
    while not requete.recentProgress:
        time.sleep(1)


# %% [markdown]
# ## Étape 2 — Comptes courants par station de départ
#
# Agrégation sans fenêtre : le résultat entier est réécrit à chaque lot,
# donc mode `complete`. Le puits `memory` expose le résultat comme une table.

# %%
comptes = (trajets.groupBy("station_depart").count())

q_comptes = (comptes.writeStream
             .outputMode("complete")
             .format("memory")
             .queryName("comptes_stations")
             .trigger(processingTime="5 seconds")
             .start())

for tour in range(3):
    patienter(q_comptes, 8)
    total = spark.sql("SELECT sum(count) AS n FROM comptes_stations").first()["n"]
    print(f"--- tour {tour + 1} : {total} trajets vus")
    spark.sql("""SELECT station_depart, count FROM comptes_stations
                 ORDER BY count DESC LIMIT 5""").show()
q_comptes.stop()

# %% [markdown]
# ## Étape 3 — Enrichir avec les stations, puis fenêtres de 5 minutes par quartier
#
# Jointure flux-statique : chaque trajet reçoit le quartier de sa station de
# départ. Puis fenêtres fixes de 5 minutes sur l'heure de l'événement (`fin`),
# avec 10 minutes de tolérance pour les retardataires.

# %%
stations = spark.read.csv(os.path.join(DONNEES, "stations.csv"),
                          header=True, inferSchema=True)

enrichis = (trajets
            .join(stations.select(F.col("station_id").alias("station_depart"),
                                  "quartier"),
                  "station_depart")
            .withColumn("heure", F.hour("debut"))
            .withColumn("jour_semaine", F.dayofweek("debut"))
            .withColumn("week_end", F.dayofweek("debut").isin(1, 7)))

par_quartier = (enrichis
                .withWatermark("fin", "10 minutes")
                .groupBy(F.window("fin", "5 minutes"), "quartier")
                .agg(F.count("*").alias("trajets"),
                     F.round(F.avg("duree_min"), 1).alias("duree_moy")))

q_quartiers = (par_quartier.writeStream
               .outputMode("update")
               .format("memory")
               .queryName("par_quartier")
               .trigger(processingTime="5 seconds")
               .start())

patienter(q_quartiers, 20)
# Le puits memory en mode update empile les versions successives d'une
# fenêtre : on garde la plus récente, c'est-à-dire le plus grand compte.
(spark.table("par_quartier")
 .groupBy("window", "quartier")
 .agg(F.max("trajets").alias("trajets"),
      F.max_by("duree_moy", "trajets").alias("duree_moy"))
 .select(F.date_format("window.start", "HH:mm").alias("debut"),
         F.date_format("window.end", "HH:mm").alias("fin"),
         "quartier", "trajets", "duree_moy")
 .orderBy(F.desc("debut"), F.desc("trajets"))
 .show(10))
q_quartiers.stop()

# %% [markdown]
# ## Étape 4 — Appliquer le modèle du TP 4 au fil de l'eau
#
# Un `PipelineModel` s'applique à un DataFrame de flux exactement comme à une
# table : `transform`. Si le TP 4 n'a pas été fait, on entraîne ici le même
# pipeline (régression logistique), sur un échantillon.

# %%
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


modele = PipelineModel.load(MODELE) if os.path.exists(MODELE) else modele_de_secours()

predits = (modele.transform(enrichis)
           .withColumn("reel", (F.col("type_usager") == "occasionnel")
                       .cast("double")))

part_predite = (predits
                .withWatermark("fin", "10 minutes")
                .groupBy(F.window("fin", "5 minutes"))
                .agg(F.count("*").alias("trajets"),
                     F.round(F.avg("prediction"), 3).alias("part_predite"),
                     F.round(F.avg("reel"), 3).alias("part_reelle")))

q_ml = (part_predite.writeStream
        .outputMode("update")
        .format("memory")
        .queryName("part_occasionnels")
        .trigger(processingTime="5 seconds")
        .start())

patienter(q_ml, 20)
(spark.table("part_occasionnels")
 .orderBy(F.desc("trajets"))
 .dropDuplicates(["window"])
 .select(F.date_format("window.start", "HH:mm").alias("debut"),
         "trajets", "part_predite", "part_reelle")
 .orderBy("debut")
 .show())
q_ml.stop()

# %% [markdown]
# ## Étape 5 — Point de reprise : arrêter, relancer, ne rien perdre
#
# Les prédictions ligne à ligne partent dans des fichiers Parquet (mode
# `append`). Le point de reprise mémorise quels fichiers d'entrée ont été
# lus : à la relance, la requête reprend où elle s'était arrêtée.

# %%
def lancer_ecriture():
    return (predits.select("trajet_id", "fin", "quartier", "type_usager",
                           "prediction")
            .writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", PREDICTIONS)
            .option("checkpointLocation", os.path.join(REPRISE, "predictions"))
            .trigger(processingTime="5 seconds")
            .start())


q_ecriture = lancer_ecriture()
patienter(q_ecriture, 10)
q_ecriture.stop()
dernier_lot = q_ecriture.lastProgress["batchId"]  # lu après l'arrêt : le lot en cours a pu finir
avant = spark.read.parquet(PREDICTIONS).count()
print(f"arrêt après le lot {dernier_lot} : {avant} prédictions écrites")

time.sleep(10)  # pendant l'arrêt, l'émetteur continue de déposer des fichiers

q_ecriture = lancer_ecriture()
patienter(q_ecriture, 10)
print("reprise au lot", q_ecriture.recentProgress[0]["batchId"])
q_ecriture.stop()

ecrits = spark.read.parquet(PREDICTIONS)
print(f"après relance : {ecrits.count()} prédictions,"
      f" {ecrits.select('trajet_id').distinct().count()} trajets distincts")

# %% [markdown]
# ## Fin — tout arrêter

# %%
for q in spark.streams.active:
    q.stop()
emetteur.terminate()
emetteur.wait()
spark.stop()
print("terminé")
