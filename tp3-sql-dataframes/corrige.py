"""
TP 3 — Nettoyer, joindre, interroger (corrigé).

    python corrige.py

Lit les trajets bruts, les nettoie, les enrichit des stations et de la météo,
répond à quatre questions en API DataFrame et en SQL, puis écrit
../sorties/trajets-propres en Parquet pour les TP 4, 5 et 6.
"""

# %% [markdown]
# # TP 3 — Nettoyer, joindre, interroger (corrigé)
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
# `inferSchema` relit tout le fichier pour deviner les types. Un schéma écrit
# à la main ne coûte rien, et documente ce qu'on attend.

# %%
schema = StructType([
    StructField("trajet_id", StringType()),
    StructField("velo_id", StringType()),
    StructField("station_depart", StringType()),
    StructField("station_arrivee", StringType()),
    StructField("debut", StringType()),
    StructField("fin", StringType()),
    StructField("duree_min", DoubleType()),
    StructField("distance_km", DoubleType()),
    StructField("type_usager", StringType()),
    StructField("electrique", IntegerType()),
    StructField("tarif_eur", DoubleType()),
])

brut = (spark.read
        .option("header", True)
        .schema(schema)
        .csv(os.path.join(DONNEES, "trajets.csv"))
        .withColumn("debut", F.to_timestamp("debut"))
        .withColumn("fin", F.to_timestamp("fin")))
brut.printSchema()
print("lignes brutes :", brut.count())

# %% [markdown]
# ## Étape 2 — Mesurer les défauts avant de les retirer
#
# On ne supprime rien sans l'avoir compté : le compte va dans le rapport.

# %%
defauts = brut.select(
    F.sum((F.col("duree_min") <= 0).cast("int")).alias("duree_negative"),
    F.sum(F.col("station_arrivee").isNull().cast("int")).alias("sans_arrivee"),
    F.sum(F.col("type_usager").isNull().cast("int")).alias("sans_type"),
)
defauts.show()
print("doublons exacts :", brut.count() - brut.dropDuplicates().count())

# %% [markdown]
# Le CSV lit une cellule vide comme `null` : c'est pour cela que l'on teste
# `isNull()` et pas `== ""`.

# %%
propres = (brut
           .dropDuplicates()
           .filter(F.col("duree_min") > 0)
           .dropna(subset=["station_arrivee", "type_usager"])
           .withColumn("heure", F.hour("debut"))
           .withColumn("jour_semaine", F.dayofweek("debut"))
           .withColumn("week_end", F.col("jour_semaine").isin(1, 7)))
propres.cache()
print("lignes propres :", propres.count())

# %% [markdown]
# ## Étape 3 — Joindre les stations et la météo

# %%
stations = (spark.read.option("header", True).option("inferSchema", True)
            .csv(os.path.join(DONNEES, "stations.csv")))
meteo = (spark.read.option("header", True).option("inferSchema", True)
         .csv(os.path.join(DONNEES, "meteo.csv")))

enrichis = (propres
            .join(stations.select(F.col("station_id").alias("station_depart"),
                                  F.col("quartier")),
                  on="station_depart", how="left")
            .withColumn("date", F.to_date("debut"))
            .join(meteo, on="date", how="left"))
enrichis.select("trajet_id", "quartier", "date", "temperature_c",
                "pluie_mm").show(3)

# %% [markdown]
# ## Étape 4 — Quatre questions, deux façons de les poser
#
# ### Q1. Combien de trajets partent de chaque quartier ?

# %%
(enrichis.groupBy("quartier")
 .agg(F.count("*").alias("trajets"))
 .orderBy(F.desc("trajets"))
 .show())

enrichis.createOrReplaceTempView("trajets")
spark.sql("""
    SELECT quartier, COUNT(*) AS trajets
    FROM trajets
    GROUP BY quartier
    ORDER BY trajets DESC
""").show()

# %% [markdown]
# ### Q2. À quelle heure roulent les abonnés, et les occasionnels ?

# %%
(enrichis.groupBy("heure")
 .pivot("type_usager", ["abonne", "occasionnel"])
 .count()
 .orderBy("heure")
 .show(24))

# %% [markdown]
# ### Q3. La pluie fait-elle baisser le nombre de trajets ?

# %%
spark.sql("""
    SELECT CASE WHEN pluie_mm = 0 THEN 'sec'
                WHEN pluie_mm < 5 THEN 'pluie faible'
                ELSE 'pluie forte' END AS temps,
           COUNT(DISTINCT date) AS jours,
           ROUND(COUNT(*) / COUNT(DISTINCT date)) AS trajets_par_jour
    FROM trajets
    GROUP BY 1
    ORDER BY trajets_par_jour DESC
""").show()

# %% [markdown]
# ### Q4. Quels sont les cinq trajets les plus fréquents ?

# %%
(enrichis
 .filter(F.col("station_depart") != F.col("station_arrivee"))
 .groupBy("station_depart", "station_arrivee")
 .count()
 .orderBy(F.desc("count"))
 .show(5))

# %% [markdown]
# ## Étape 5 — Une fonction de fenêtre : le classement des stations par quartier

# %%
par_station = enrichis.groupBy("quartier", "station_depart").count()
fenetre = Window.partitionBy("quartier").orderBy(F.desc("count"))
(par_station
 .withColumn("rang", F.rank().over(fenetre))
 .filter("rang = 1")
 .orderBy("quartier")
 .show())

# %% [markdown]
# ## Étape 6 — La même question avec l'API pandas de Spark

# %%
psdf = enrichis.select("type_usager", "duree_min").pandas_api()
print(psdf.groupby("type_usager")["duree_min"].mean().sort_index())

# %% [markdown]
# ## Étape 7 — Écrire en Parquet pour les TP suivants
#
# Le contrat : les colonnes du CSV typées, plus `heure`, `jour_semaine`,
# `week_end`. Pas les colonnes de jointure : chaque TP joint ce dont il a besoin.

# %%
cible = os.path.join(SORTIES, "trajets-propres")
# coalesce(4) : sans lui, le dédoublonnage laisse 200 partitions, donc 200
# fichiers de 60 ko. Un fichier par partition, et des fichiers minuscules
# coûtent plus cher à lister qu'à lire. Quatre fichiers suffisent ici.
propres.coalesce(4).write.mode("overwrite").parquet(cible)


def taille_mo(chemin):
    if os.path.isfile(chemin):
        return os.path.getsize(chemin) / 1e6
    return sum(os.path.getsize(os.path.join(r, f))
               for r, _, fs in os.walk(chemin) for f in fs) / 1e6


print(f"CSV     : {taille_mo(os.path.join(DONNEES, 'trajets.csv')):5.1f} Mo")
print(f"Parquet : {taille_mo(cible):5.1f} Mo")

# %% [markdown]
# ## Étape 8 — Comparer une lecture CSV et une lecture Parquet
#
# Même question, deux formats, **lecture comprise** : avec `inferSchema`, le
# CSV est relu une fois de plus pour deviner les types. Chaque mesure est
# répétée trois fois et on garde la **meilleure** : la première paie le
# démarrage, et sur un poste partagé avec d'autres programmes, une mesure
# isolée ne veut rien dire.

# %%
def chrono(lire, n=3):
    temps = []
    for _ in range(n):
        t = time.perf_counter()
        lire().groupBy("type_usager").agg(F.avg("duree_min")).collect()
        temps.append(time.perf_counter() - t)
    return temps


def lire_csv():
    return (spark.read.option("header", True).option("inferSchema", True)
            .csv(os.path.join(DONNEES, "trajets.csv")))


for nom, lire in [("CSV", lire_csv), ("Parquet", lambda: spark.read.parquet(cible))]:
    temps = chrono(lire)
    detail = ", ".join(f"{t:.1f}" for t in temps)
    print(f"{nom:8}: {min(temps):.2f} s   (mesures : {detail})")

# %%
spark.stop()
