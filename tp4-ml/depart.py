"""
TP 4 — Classification supervisée : abonné ou occasionnel ?  (départ)

Complétez les cellules marquées TODO, dans l'ordre. L'énoncé détaillé est
dans enonce.md ; le corrigé sera projeté en fin de TP.
"""

# %% [markdown]
# # TP 4 — Abonné ou occasionnel ?
#
# À partir de ce qu'on sait d'un trajet (heure, jour, vélo électrique ou non,
# quartier de départ, puis durée et distance), peut-on deviner si c'est un
# **abonné** ou un **occasionnel** ?
#
# On entraîne un modèle sur les trajets dont on connaît la réponse, on
# l'évalue honnêtement, et on le sauvegarde pour le TP 5.

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
# ## Étape 1 — Charger les trajets propres
#
# Si le TP 3 a abouti, les trajets nettoyés sont dans `sorties/trajets-propres`.
# Sinon, cette cellule refait le nettoyage minimal depuis le CSV.

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

# %%
# TODO 1
# a. Joindre `trajets` et `stations` pour obtenir la colonne `quartier`
#    de la station de DÉPART (attention au nom de la clé de jointure).
# b. Ajouter la colonne `label` : 1.0 pour un occasionnel, 0.0 pour un abonné.
# c. Afficher le nombre de trajets par type d'usager.
donnees = ...

# %% [markdown]
# ## Étape 2 — Découper, et mesurer la référence naïve
#
# Un trajet va dans le test si le hachage de son identifiant, modulo 5, vaut 0.
# Tout le monde obtient ainsi le même découpage, quel que soit son poste.

# %%
dans_test = F.pmod(F.hash("trajet_id"), 5) == 0

# TODO 2
# a. `train` : les trajets hors test ; `test` : les autres. Les mettre en cache.
# b. Afficher le nombre de trajets de chacun.
# c. Calculer l'exactitude obtenue en répondant toujours « abonné » sur le test.
train, test = ..., ...

# %% [markdown]
# ## Étape 3 — Un modèle « au retrait » : régression logistique
#
# On veut deviner le type d'usager au moment où le vélo est retiré. À cet
# instant, on ne connaît ni la durée ni la distance du trajet.

# %%
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import (OneHotEncoder, StandardScaler,
                                StringIndexer, VectorAssembler)

AU_RETRAIT = ["heure", "jour_semaine", "week_end", "electrique",
              "quartier_vec"]

# TODO 3
# a. StringIndexer sur `quartier` (handleInvalid="keep"), puis OneHotEncoder
#    vers `quartier_vec`.
# b. VectorAssembler des colonnes AU_RETRAIT vers `brutes`,
#    puis StandardScaler vers `features`.
# c. LogisticRegression, puis un Pipeline des cinq étapes, entraîné sur `train`.
# d. Appliquer le modèle à `test` et afficher quelques prédictions.
modele_lr = ...
predictions_lr = ...

# %% [markdown]
# ## Étape 4 — Évaluer

# %%
from pyspark.ml.evaluation import (BinaryClassificationEvaluator,
                                   MulticlassClassificationEvaluator)

# TODO 4
# a. Calculer l'AUC (areaUnderROC) et l'exactitude (accuracy).
# b. Construire la matrice de confusion avec groupBy("label").pivot("prediction").
# c. Comparer à la référence naïve : de combien fait-on mieux ?

# %% [markdown]
# ## Étape 5 — Une forêt aléatoire « au retrait », et ce qu'elle regarde

# %%
from pyspark.ml.classification import RandomForestClassifier

# TODO 5
# a. Le même pipeline sans StandardScaler, terminé par RandomForestClassifier
#    (featuresCol="brutes", numTrees=30, maxDepth=8, seed=42).
# b. L'évaluer comme à l'étape 4. Mieux que la logistique ? Pourquoi ?
# c. Afficher les 5 variables les plus importantes
#    (featureImportances du dernier étage, noms dans les métadonnées de `brutes`).
modele_rf = ...

# %% [markdown]
# ## Étape 6 — Un modèle « au dépôt », et on sauvegarde le meilleur
#
# Au dépôt du vélo, la durée et la distance sont connues. Le TP 5 appliquera
# le modèle aux trajets qui se terminent.

# %%
AU_DEPOT = AU_RETRAIT + ["duree_min", "distance_km"]

# TODO 6
# a. Réentraîner la logistique et la forêt avec les colonnes AU_DEPOT.
# b. Les évaluer. Laquelle gagne, cette fois ?
# c. Sauvegarder la meilleure dans MODELE (en écrasant), la recharger avec
#    PipelineModel.load et vérifier qu'elle donne le même score.

# %% [markdown]
# ## Bonus 1 — Une colonne oubliée
#
# Il reste une colonne *numérique* des trajets qu'on n'a pas utilisée.
# Ajoutez-la aux variables du modèle « au retrait » et réentraînez.
# Que devient l'AUC ? Faut-il la garder ?

# %%
# TODO bonus 1

# %% [markdown]
# ## Bonus 2 — Rattraper le déséquilibre des classes
#
# Le modèle « au retrait » manque 61 % des occasionnels. Donnez-leur plus de
# poids, sans toucher aux données.

# %%
# TODO bonus 2
# a. Mesurer le rappel et la précision de la classe « occasionnel » sur le
#    modèle de l'étape 3 : MulticlassClassificationEvaluator avec
#    metricName="recallByLabel" (puis "precisionByLabel") et metricLabel=1.0.
# b. Ajouter à train une colonne `poids` : la part de la classe OPPOSÉE
#    (les occasionnels reçoivent 1 - part_occ, les abonnés part_occ), de sorte
#    que les deux classes pèsent autant au total.
# c. Réentraîner la logistique avec weightCol="poids", et comparer rappel,
#    précision et AUC. Qu'est-ce qui s'améliore, qu'est-ce qui se dégrade ?

# %%
spark.stop()
