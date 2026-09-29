"""
TP 3 — Nettoyer, joindre, interroger (notebook de départ).

L'énoncé complet est dans enonce.md. Remplacez chaque « TODO ».
"""

# %% [markdown]
# # TP 3 — Nettoyer, joindre, interroger
#
# Le fil : lire les trajets **avec un schéma explicite**, mesurer puis retirer
# les défauts, enrichir, interroger de deux façons, et écrire le résultat en
# Parquet pour les TP suivants.

# %%
import os
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)

try:
    ICI = os.path.dirname(os.path.abspath(__file__))
except NameError:  # dans un notebook, __file__ n'existe pas
    ICI = os.getcwd()
TP = os.path.dirname(ICI)
DONNEES = os.path.join(TP, "donnees")
SORTIES = os.path.join(TP, "sorties")

spark = (SparkSession.builder
         .master("local[2]")
         .appName("tp3-sql-dataframes")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# %% [markdown]
# ## Étape 1 — Lire avec un schéma explicite
#
# Les onze colonnes : `trajet_id`, `velo_id`, `station_depart`,
# `station_arrivee`, `debut`, `fin`, `duree_min`, `distance_km`,
# `type_usager`, `electrique`, `tarif_eur`.
#
# Attendu : 300 300 lignes brutes.

# %%
schema = StructType([
    StructField("trajet_id", StringType()),
    # TODO : les dix autres colonnes
])

brut = None  # TODO : spark.read … .schema(schema).csv(…), puis debut et fin en timestamp
# brut.printSchema()
# print("lignes brutes :", brut.count())

# %% [markdown]
# ## Étape 2 — Mesurer les défauts avant de les retirer
#
# Durées nulles ou négatives, station d'arrivée vide, type d'usager vide,
# doublons exacts. Attention : une cellule vide d'un CSV est lue comme `null`.

# %%
# TODO : un seul select avec trois F.sum(… .cast("int"))
# TODO : le nombre de doublons exacts

# %% [markdown]
# Retirez les défauts, ajoutez `heure`, `jour_semaine` (1 = dimanche) et
# `week_end`, puis mettez en cache. Attendu : 299 251 lignes propres.

# %%
propres = None  # TODO

# %% [markdown]
# ## Étape 3 — Joindre les stations et la météo
#
# Le quartier de la station de départ, et la météo du jour (clé : `F.to_date("debut")`).

# %%
stations = None  # TODO
meteo = None  # TODO
enrichis = None  # TODO

# %% [markdown]
# ## Étape 4 — Quatre questions, deux façons de les poser
#
# ### Q1. Combien de trajets partent de chaque quartier ?

# %%
# TODO : API DataFrame

# TODO : enrichis.createOrReplaceTempView("trajets"), puis la même en SQL

# %% [markdown]
# ### Q2. À quelle heure roulent les abonnés, et les occasionnels ?
#
# Indice : `groupBy("heure").pivot("type_usager", [...]).count()`

# %%
# TODO

# %% [markdown]
# ### Q3. La pluie fait-elle baisser le nombre de trajets par jour ?
#
# Trois classes : sec (0 mm), pluie faible (< 5 mm), pluie forte. Divisez le
# nombre de trajets par le nombre de jours distincts de chaque classe.

# %%
# TODO : en SQL, avec CASE WHEN

# %% [markdown]
# ### Q4. Quels sont les cinq trajets les plus fréquents ?
#
# Départ et arrivée distincts.

# %%
# TODO

# %% [markdown]
# ## Étape 5 — La station de départ la plus utilisée de chaque quartier

# %%
# TODO : Window.partitionBy("quartier").orderBy(...) et F.rank()

# %% [markdown]
# ## Étape 6 — La même question avec l'API pandas
#
# Durée moyenne par type d'usager, écrite comme en pandas.

# %%
# TODO : enrichis.select(...).pandas_api(), puis groupby

# %% [markdown]
# ## Étape 7 — Écrire en Parquet pour les TP suivants
#
# La table **propre** de l'étape 2, dans `../sorties/trajets-propres`,
# en mode `overwrite`. Comparez la taille au CSV, puis comptez les fichiers
# écrits : un par partition. `coalesce(4)` avant `write` évite d'en produire
# deux cents de 60 ko.

# %%
cible = os.path.join(SORTIES, "trajets-propres")
# TODO

# %% [markdown]
# ## Étape 8 — Comparer une lecture CSV et une lecture Parquet

# %%
# TODO : chronométrer lecture + agrégation, sur les deux formats.
# Mesurez trois fois et gardez la meilleure : la première paie le démarrage,
# et une mesure isolée sur un poste occupé ne veut rien dire.

# %%
spark.stop()
