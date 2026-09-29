"""
TP 1 — Mise en place de l'environnement et premier script : notebook de départ.

Complétez les cellules marquées TODO. L'énoncé complet est dans enonce.md.
"""

# %% [markdown]
# # TP 1 · Premier script Spark
#
# Avant tout : `python verifier_env.py` doit afficher « Poste prêt pour les TP ».
#
# On lit les trajets Vélo'Cité, on regarde ce que Spark en a compris, et on
# observe **quand** Spark travaille vraiment. Gardez l'interface web ouverte
# à côté (l'adresse s'affiche à l'étape 1) et regardez l'onglet **Jobs**
# après chaque cellule.

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
# Créez une `SparkSession` en mode local sur deux cœurs, nommée
# `tp1-premier-script`, puis affichez la version de Spark et l'adresse de
# l'interface web (`spark.sparkContext.uiWebUrl`).

# %%
from pyspark.sql import SparkSession, functions as F

# TODO : spark = SparkSession.builder ... .getOrCreate()

# %% [markdown]
# ## Étape 2 — Lire les trajets et regarder le schéma
#
# Lisez `trajets.csv` avec l'en-tête et l'inférence de types, puis affichez
# le schéma, le nombre de lignes, le nombre de partitions et cinq lignes.
#
# Question : combien de jobs la lecture seule a-t-elle lancés ? Pourquoi ?

# %%
# TODO : trajets = spark.read.csv(os.path.join(DONNEES, "trajets.csv"), ...)

# %%
# TODO : printSchema, count, rdd.getNumPartitions(), show(5)

# %% [markdown]
# ## Étape 3 — Transformation ou action ?
#
# Le `statusTracker` compte les jobs lancés. Mesurez combien de jobs lancent :
#
# 1. un `filter` sur `duree_min > 30` suivi d'un `select` de deux colonnes ;
# 2. le `count` de ce résultat.

# %%
suivi = spark.sparkContext.statusTracker()


def nb_jobs():
    return len(suivi.getJobIdsForGroup())


# TODO : mesurer nb_jobs() avant et après le filter + select, puis le count

# %% [markdown]
# ## Étape 4 — Qui roule ?
#
# Par type d'usager : nombre de trajets et durée moyenne arrondie à 0,1 min,
# triés par nombre de trajets décroissant.
#
# Que représente la ligne `NULL` ?

# %%
# TODO : groupBy("type_usager").agg(...)

# %% [markdown]
# ## Étape 5 — Les stations les plus actives
#
# Les cinq stations d'où partent le plus de trajets.

# %%
# TODO

# %% [markdown]
# ## Étape 6 — Le plan d'exécution
#
# Affichez le plan du comptage par station avec `explain()`. Repérez
# l'`Exchange` : c'est le shuffle. Retrouvez-le dans l'onglet **Stages**.

# %%
# TODO

# %% [markdown]
# ## Bonus
#
# - Repartitionnez les trajets en 8 partitions et vérifiez-le.
# - Exécutez ce notebook en script : `spark-submit --master "local[2]" depart.py`.

# %%
# TODO

# %%
spark.stop()
