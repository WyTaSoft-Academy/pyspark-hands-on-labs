"""
TP 4 — Classification supervisée : abonné ou occasionnel ?  (corrigé)

    python corrige.py

Lit les trajets nettoyés au TP 3 (tp/sorties/trajets-propres). S'ils sont
absents, les reconstruit depuis le CSV : le TP tourne donc seul.
Écrit le modèle retenu dans tp/sorties/modele-type-usager, repris au TP 5.

Cible : PySpark 3.5.
"""

# %% [markdown]
# # TP 4 — Abonné ou occasionnel ?
#
# Corrigé complet. Chaque cellule correspond à une étape de l'énoncé.

# %%
import os

from pyspark.sql import SparkSession, functions as F

try:
    ICI = os.path.dirname(os.path.abspath(__file__))
except NameError:  # dans un notebook, __file__ n'existe pas
    ICI = os.getcwd()
TP = os.path.dirname(ICI)
DONNEES = os.path.join(TP, "donnees")
SORTIES = os.path.join(TP, "sorties")
PROPRES = os.path.join(SORTIES, "trajets-propres")
MODELE = os.path.join(SORTIES, "modele-type-usager")

spark = (SparkSession.builder
         .master("local[2]")
         .appName("tp4-ml")
         .config("spark.driver.memory", "2g")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# %% [markdown]
# ## Étape 1 — Charger les trajets propres, joindre le quartier
#
# Si le TP 3 n'a pas été fait, on refait ici le nettoyage minimal.

# %%
if os.path.exists(PROPRES):
    trajets = spark.read.parquet(PROPRES)
else:
    brut = spark.read.csv(os.path.join(DONNEES, "trajets.csv"),
                          header=True, inferSchema=True)
    trajets = (brut.dropDuplicates()
               .where("duree_min > 0")
               .where("station_arrivee IS NOT NULL AND type_usager IS NOT NULL")
               .withColumn("debut", F.to_timestamp("debut"))
               .withColumn("fin", F.to_timestamp("fin"))
               .withColumn("heure", F.hour("debut"))
               .withColumn("jour_semaine", F.dayofweek("debut"))
               .withColumn("week_end", F.dayofweek("debut").isin(1, 7)))

stations = spark.read.csv(os.path.join(DONNEES, "stations.csv"),
                          header=True, inferSchema=True)

donnees = (trajets
           .join(stations.select(F.col("station_id").alias("station_depart"),
                                 "quartier"),
                 "station_depart")
           .withColumn("label",
                       (F.col("type_usager") == "occasionnel").cast("double")))

donnees.groupBy("type_usager").count().show()

# %% [markdown]
# ## Étape 2 — Découper, et mesurer la référence naïve
#
# Un trajet va dans le test si le hachage de son identifiant, modulo 5, vaut 0 :
# 20 % des trajets, toujours les mêmes, quel que soit le partitionnement.
# (`randomSplit` tire au hasard *partition par partition* : même avec une
# graine, il change dès que les données sont découpées autrement.)
#
# Répondre toujours « abonné » : c'est le score à battre.

# %%
dans_test = F.pmod(F.hash("trajet_id"), 5) == 0
train = donnees.where(~dans_test).cache()
test = donnees.where(dans_test).cache()
print("entraînement :", train.count(), " test :", test.count())

part_abonnes = test.where("label = 0").count() / test.count()
print(f"référence « toujours abonné » : exactitude {part_abonnes:.3f}")

# %% [markdown]
# ## Étape 3 — Un modèle « au retrait » : régression logistique
#
# On prédit au moment où le vélo est retiré : on ne connaît encore ni la
# durée ni la distance.

# %%
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import (OneHotEncoder, StandardScaler,
                                StringIndexer, VectorAssembler)

AU_RETRAIT = ["heure", "jour_semaine", "week_end", "electrique",
              "quartier_vec"]

indexeur = StringIndexer(inputCol="quartier", outputCol="quartier_idx",
                         handleInvalid="keep")
encodeur = OneHotEncoder(inputCols=["quartier_idx"],
                         outputCols=["quartier_vec"])
assembleur = VectorAssembler(inputCols=AU_RETRAIT, outputCol="brutes")
normaliseur = StandardScaler(inputCol="brutes", outputCol="features")
reglog = LogisticRegression(labelCol="label", featuresCol="features")

modele_lr = Pipeline(stages=[indexeur, encodeur, assembleur,
                             normaliseur, reglog]).fit(train)
predictions_lr = modele_lr.transform(test)
predictions_lr.select("type_usager", "heure", "quartier", "probability",
                      "prediction").show(5, truncate=False)

# %% [markdown]
# ## Étape 4 — Évaluer : AUC, exactitude, matrice de confusion

# %%
from pyspark.ml.evaluation import (BinaryClassificationEvaluator,
                                   MulticlassClassificationEvaluator)

auc = BinaryClassificationEvaluator(labelCol="label",
                                    metricName="areaUnderROC")
exactitude = MulticlassClassificationEvaluator(labelCol="label",
                                               metricName="accuracy")


def evaluer(nom, predictions):
    print(f"{nom:24} AUC {auc.evaluate(predictions):.3f}"
          f"   exactitude {exactitude.evaluate(predictions):.3f}")


evaluer("logistique au retrait", predictions_lr)
(predictions_lr.groupBy("label").pivot("prediction", [0.0, 1.0])
 .count().orderBy("label").show())

# %% [markdown]
# ## Étape 5 — Une forêt aléatoire « au retrait », et ce qu'elle regarde

# %%
from pyspark.ml.classification import RandomForestClassifier

foret = RandomForestClassifier(labelCol="label", featuresCol="brutes",
                               numTrees=30, maxDepth=8, seed=42)
modele_rf = Pipeline(stages=[indexeur, encodeur, assembleur,
                             foret]).fit(train)
predictions_rf = modele_rf.transform(test)
evaluer("forêt au retrait", predictions_rf)

attrs = predictions_rf.schema["brutes"].metadata["ml_attr"]["attrs"]
noms = sorted((a["idx"], a["name"])
              for groupe in attrs.values() for a in groupe)
importances = modele_rf.stages[-1].featureImportances.toArray()
for (i, nom), imp in sorted(zip(noms, importances),
                            key=lambda x: -x[1])[:5]:
    print(f"{nom:22} {imp:.3f}")

# %% [markdown]
# ## Étape 6 — Un modèle « au dépôt », et on sauvegarde le meilleur
#
# Au dépôt du vélo, la durée et la distance sont connues. Le TP 5 applique
# le modèle aux trajets qui se terminent : c'est celui-ci qu'on sauvegarde.

# %%
AU_DEPOT = ["heure", "jour_semaine", "week_end", "electrique",
            "quartier_vec", "duree_min", "distance_km"]
assembleur_depot = VectorAssembler(inputCols=AU_DEPOT, outputCol="brutes")

depot_lr = Pipeline(stages=[indexeur, encodeur, assembleur_depot,
                            normaliseur, reglog]).fit(train)
depot_rf = Pipeline(stages=[indexeur, encodeur, assembleur_depot,
                            foret]).fit(train)
evaluer("logistique au dépôt", depot_lr.transform(test))
evaluer("forêt au dépôt", depot_rf.transform(test))

# %%
from pyspark.ml import PipelineModel

depot_lr.write().overwrite().save(MODELE)
recharge = PipelineModel.load(MODELE)
evaluer("modèle rechargé", recharge.transform(test))

# %% [markdown]
# ## Bonus 1 — Et si on ajoutait le tarif ?
#
# Le tarif est calculé *à partir* du type d'usager : c'est une fuite.

# %%
assembleur_fuite = VectorAssembler(inputCols=AU_RETRAIT + ["tarif_eur"],
                                   outputCol="brutes")
fuite = Pipeline(stages=[indexeur, encodeur, assembleur_fuite,
                         normaliseur, reglog]).fit(train)
evaluer("au retrait + tarif", fuite.transform(test))

# %% [markdown]
# ## Bonus 2 — Rattraper le déséquilibre des classes
#
# Le modèle « au retrait » manque 61 % des occasionnels : il penche vers la
# classe majoritaire. On donne plus de poids aux occasionnels — sans toucher
# aux données — et on regarde ce que devient le rappel.

# %%
rappel_occ = MulticlassClassificationEvaluator(
    labelCol="label", metricName="recallByLabel", metricLabel=1.0)
precision_occ = MulticlassClassificationEvaluator(
    labelCol="label", metricName="precisionByLabel", metricLabel=1.0)


def rappel(nom, predictions):
    print(f"{nom:24} rappel(occ) {rappel_occ.evaluate(predictions):.3f}"
          f"   précision(occ) {precision_occ.evaluate(predictions):.3f}"
          f"   AUC {auc.evaluate(predictions):.3f}")


part_occ = train.where("label = 1").count() / train.count()
train_pondere = train.withColumn(
    "poids", F.when(F.col("label") == 1, 1 - part_occ).otherwise(part_occ))

reglog_pondere = LogisticRegression(labelCol="label", featuresCol="features",
                                    weightCol="poids")
modele_pondere = Pipeline(stages=[indexeur, encodeur, assembleur,
                                  normaliseur, reglog_pondere]).fit(train_pondere)

rappel("sans pondération", predictions_lr)
rappel("avec pondération", modele_pondere.transform(test))

# %%
spark.stop()
