"""
TP 1 — Mise en place de l'environnement et premier script : corrigé.

    python corrige.py
    spark-submit corrige.py

Se lance tel quel, en script ou en notebook (format « percent »).
"""

# %% [markdown]
# # TP 1 · Premier script Spark — corrigé
#
# On lit les trajets Vélo'Cité, on regarde ce que Spark en a compris, et on
# observe **quand** Spark travaille vraiment : c'est l'objet du TP.

# %%
import os
import sys

# Sous Windows, sans ces deux variables, les exécuteurs Python lancés par Spark
# peuvent ne pas trouver le bon interpréteur (« Accept timed out »).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

try:
    ICI = os.path.dirname(os.path.abspath(__file__))
except NameError:  # dans un notebook, __file__ n'existe pas
    ICI = os.getcwd()
DONNEES = os.path.join(os.path.dirname(ICI), "donnees")

# %% [markdown]
# ## Étape 1 — Créer la session
#
# La `SparkSession` est le point d'entrée unique. `local[2]` : Spark tourne
# sur ce poste, avec deux cœurs, donc deux tâches en parallèle.

# %%
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .master("local[2]")
         .appName("tp1-premier-script")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

print("Spark", spark.version)
print("Interface web :", spark.sparkContext.uiWebUrl)

# %% [markdown]
# ## Étape 2 — Lire les trajets et regarder le schéma
#
# `inferSchema=True` fait lire le fichier une première fois pour deviner les
# types : c'est déjà un job, visible dans l'interface web.

# %%
trajets = spark.read.csv(os.path.join(DONNEES, "trajets.csv"),
                         header=True, inferSchema=True)
trajets.printSchema()

# %%
print("Nombre de trajets :", trajets.count())  # 300 300
print("Partitions :", trajets.rdd.getNumPartitions())
trajets.show(5)

# %% [markdown]
# ## Étape 3 — Transformation ou action ?
#
# `select` et `filter` ne font rien : ils décrivent un calcul. `count` le
# déclenche. On le vérifie en comptant les jobs lancés.

# %%
suivi = spark.sparkContext.statusTracker()


def nb_jobs():
    return len(suivi.getJobIdsForGroup())


avant = nb_jobs()
longs = trajets.filter(F.col("duree_min") > 30).select("trajet_id", "type_usager")
print("Jobs lancés par filter + select :", nb_jobs() - avant)  # 0

avant = nb_jobs()
print("Trajets de plus de 30 min :", longs.count())  # 22 829
print("Jobs lancés par count :", nb_jobs() - avant)  # au moins 1

# %% [markdown]
# ## Étape 4 — Qui roule ?
#
# Nombre de trajets et durée moyenne par type d'usager.

# %%
(trajets.groupBy("type_usager")
        .agg(F.count("*").alias("trajets"),
             F.round(F.avg("duree_min"), 1).alias("duree_moy"))
        .orderBy(F.desc("trajets"))
        .show())
# abonne       190 269 trajets   8,6 min
# occasionnel  109 881 trajets  20,4 min
# NULL             150 trajets  (badge illisible : un défaut volontaire)

# %% [markdown]
# ## Étape 5 — Les stations les plus actives

# %%
(trajets.groupBy("station_depart")
        .count()
        .orderBy(F.desc("count"))
        .show(5))

# %% [markdown]
# ## Étape 6 — Le plan d'exécution
#
# `explain()` montre ce que Spark va faire. L'`Exchange` est le **shuffle** :
# les lignes d'une même station doivent se retrouver dans la même partition.

# %%
trajets.groupBy("station_depart").count().explain()

# %% [markdown]
# ## Bonus — Repartitionner
#
# Plus de partitions, c'est plus de tâches. Utile sur un cluster, souvent
# contre-productif sur un portable.

# %%
huit = trajets.repartition(8)
print("Partitions après repartition(8) :", huit.rdd.getNumPartitions())

# %%
spark.stop()
