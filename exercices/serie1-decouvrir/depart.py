"""
Série 1 — Découvrir Spark (module 1) : notebook de départ.

Remplissez les None marqués TODO, exécutez chaque cellule puis sa
vérification. Le corrigé est projeté en séance.
"""

# %% [markdown]
# # Série 1 · Découvrir Spark
#
# **Module 1** · environ **60 minutes** · 8 exercices, du plus simple au plus
# difficile.
#
# À la fin de cette série, vous saurez :
#
# - créer une session Spark en mode local ;
# - lire un fichier CSV et vérifier ce que Spark en a compris ;
# - choisir des colonnes, filtrer, trier et compter par groupe ;
# - distinguer une **transformation** d'une **action**, en comptant les jobs ;
# - connaître et modifier le nombre de **partitions** d'un DataFrame.
#
# Mode d'emploi : remplissez les `None`, exécutez la cellule, puis la cellule de
# vérification qui la suit, et lisez la ligne ✔ ou ✘.

# %%
import os
import sys

# Sous Windows : le même Python pour le driver et les exécuteurs, et une adresse
# locale fixe (sans elle, Docker Desktop peut faire échouer les jobs).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

try:
    ICI = os.path.dirname(os.path.abspath(__file__))
except NameError:  # dans un notebook, __file__ n'existe pas
    ICI = os.getcwd()
TP = os.path.dirname(os.path.dirname(ICI))
DONNEES = os.path.join(TP, "donnees")
SORTIES = os.path.join(TP, "sorties", "exercices")


def verifier(libelle, obtenu, attendu, tolerance=None):
    """Affiche ✔ si le résultat est bon, ✘ sinon. Ne lève jamais d'erreur."""
    if obtenu is None:
        print(f"…  {libelle} : à compléter")
        return
    try:
        ok = (abs(obtenu - attendu) <= tolerance) if tolerance is not None else obtenu == attendu
    except TypeError:
        ok = False
    suite = "" if ok else f"   (attendu : {attendu!r})"
    print(f"{'✔' if ok else '✘'}  {libelle} : {obtenu!r}{suite}")


# %% [markdown]
# ## Exercice 1.1 — Créer la session  ★
#
# **Ce que vous apprenez** : la `SparkSession` est le point d'entrée unique de
# tout programme Spark.
#
# 1. Complétez le constructeur : master `"local[2]"` (Spark tourne sur ce poste,
#    avec deux cœurs), nom d'application `"exercices-serie1"`.
# 2. Rangez la version de Spark dans `version` et le master dans `master`.
# 3. Ouvrez dans un navigateur l'adresse affichée : c'est l'interface web de
#    Spark. Gardez-la ouverte pendant toute la série.
#
# *Indice* : `.master("local[2]")`, `.appName("exercices-serie1")`, puis
# `spark.version` et `spark.sparkContext.master`.

# %%
from pyspark.sql import SparkSession, functions as F

# TODO : complétez les deux lignes marquées « ... », puis retirez le « # »
#        au début de chaque ligne du bloc.
spark = None
# spark = (SparkSession.builder
#          .master(...)
#          .appName(...)
#          .config("spark.sql.shuffle.partitions", "4")
#          .config("spark.ui.showConsoleProgress", "false")
#          .getOrCreate())

version = None  # TODO
master = None   # TODO
if spark is not None:
    print("Interface web :", spark.sparkContext.uiWebUrl)

# %%
verifier("version de Spark (branche)", version[:3] if version else None, "3.5")
verifier("master", master, "local[2]")

# %%
# Filet de sécurité : sans session, rien de la suite ne tourne. Si l'exercice
# 1.1 n'est pas terminé, cette cellule crée la session à votre place.
if spark is None:
    print("Session créée par le filet de sécurité : revenez à l'exercice 1.1.")
    spark = (SparkSession.builder
             .master("local[2]")
             .appName("exercices-serie1")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# %% [markdown]
# ## Exercice 1.2 — Lire `stations.csv`  ★
#
# **Ce que vous apprenez** : `spark.read.csv` lit un fichier CSV ; les options
# `header` et `inferSchema` disent à Spark de prendre la première ligne comme
# noms de colonnes et de deviner les types.
#
# 1. Exécutez la première cellule : sans option, les colonnes s'appellent `_c0`,
#    `_c1`… et tout est du texte (`string`).
# 2. Relisez le fichier avec `header=True` et `inferSchema=True`, dans `stations`.
# 3. Affichez son schéma avec `printSchema()`.
# 4. Rangez la liste des colonnes dans `colonnes`, et le type de la colonne
#    `capacite` dans `type_capacite`.
#
# *Indice* : `spark.read.csv(CHEMIN_STATIONS, header=True, inferSchema=True)`,
# puis `stations.columns` et `dict(stations.dtypes)["capacite"]`.

# %%
CHEMIN_STATIONS = os.path.join(DONNEES, "stations.csv")

# Sans option : à observer seulement.
spark.read.csv(CHEMIN_STATIONS).printSchema()

# %%
stations = None       # TODO : spark.read.csv(CHEMIN_STATIONS, ...)
# TODO : afficher le schéma de stations

colonnes = None       # TODO
type_capacite = None  # TODO

# %%
verifier("colonnes", colonnes,
         ["station_id", "nom", "quartier", "latitude", "longitude", "capacite"])
verifier("type de capacite", type_capacite, "int")

# %% [markdown]
# ## Exercice 1.3 — Compter, afficher, choisir des colonnes  ★
#
# **Ce que vous apprenez** : `select` garde certaines colonnes ; `count` et
# `show` déclenchent le calcul et en rapportent le résultat.
#
# 1. Comptez les stations dans `nb_stations`.
# 2. Dans `fiches`, gardez seulement les colonnes `nom`, `quartier` et `capacite`.
# 3. Affichez les cinq premières lignes de `fiches`.
# 4. Rangez la liste des colonnes de `fiches` dans `colonnes_fiches`.
#
# *Indice* : `stations.count()`, `stations.select("nom", "quartier", "capacite")`,
# `fiches.show(5)`.

# %%
nb_stations = None      # TODO

fiches = None           # TODO : stations.select(...)
# TODO : afficher cinq lignes de fiches

colonnes_fiches = None  # TODO

# %%
verifier("nombre de stations", nb_stations, 60)
verifier("colonnes de fiches", colonnes_fiches, ["nom", "quartier", "capacite"])

# %% [markdown]
# ## Exercice 1.4 — Garder les grandes stations  ★★
#
# **Ce que vous apprenez** : `filter` garde les lignes qui vérifient une
# condition ; plusieurs conditions se combinent avec `&` (et) ou `|` (ou).
#
# 1. Dans `grandes`, gardez les stations dont la `capacite` vaut au moins 25.
# 2. Comptez-les dans `nb_grandes`.
# 3. Dans `nb_grandes_centre`, comptez celles qui, en plus, sont dans le
#    quartier `"Centre"`.
#
# *Indice* : `F.col("capacite") >= 25`. Avec deux conditions, mettez des
# parenthèses autour de chacune : `(… >= 25) & (F.col("quartier") == "Centre")`.
# Sans elles, Python évalue `&` en premier et l'erreur est déroutante.

# %%
grandes = None            # TODO
nb_grandes = None         # TODO

nb_grandes_centre = None  # TODO

# %%
verifier("stations d'au moins 25 places", nb_grandes, 33)
verifier("dont au Centre", nb_grandes_centre, 5)

# %% [markdown]
# ## Exercice 1.5 — Trier les stations  ★★
#
# **Ce que vous apprenez** : `orderBy` trie un DataFrame, dans l'ordre croissant
# par défaut, décroissant avec `F.desc(...)`.
#
# 1. Triez `stations` par `capacite` décroissante ; à capacité égale, par `nom`
#    dans l'ordre alphabétique. Rangez le résultat dans `triees`.
# 2. Affichez les cinq premières lignes (colonnes `nom`, `quartier`, `capacite`).
# 3. Dans `top3`, rangez la **liste Python** des noms des trois premières.
#
# *Indice* : `orderBy(F.desc("capacite"), "nom")`. Pour ramener trois lignes
# dans Python : `triees.limit(3).collect()` renvoie une liste de `Row` ;
# `ligne["nom"]` lit une colonne d'une ligne.

# %%
triees = None  # TODO
# TODO : afficher cinq lignes (nom, quartier, capacite)

top3 = None    # TODO : une liste de trois chaînes

# %%
verifier("trois plus grandes stations", top3,
         ["Arsenal", "Bibliothèque", "Piscine"])

# %% [markdown]
# ## Exercice 1.6 — Compter les stations par quartier  ★★
#
# **Ce que vous apprenez** : `groupBy(...).count()` regroupe les lignes qui ont
# la même valeur et compte chaque groupe, dans une colonne nommée `count`.
#
# 1. Dans `par_quartier`, comptez les stations de chaque quartier, triées du
#    quartier le plus fourni au moins fourni (à égalité, par nom de quartier).
# 2. Affichez `par_quartier`.
# 3. Rangez le nombre de quartiers dans `nb_quartiers`.
# 4. Rangez dans `quartier_max` le couple `(quartier, nombre)` de la première
#    ligne. Plusieurs quartiers sont à égalité en tête : c'est le second critère
#    de tri, l'ordre alphabétique, qui les départage.
#
# *Indice* : `groupBy("quartier").count()`, puis le tri de l'exercice 1.5 sur la
# colonne `count`. `first()` ramène la première ligne.

# %%
par_quartier = None  # TODO
# TODO : afficher par_quartier

nb_quartiers = None  # TODO
quartier_max = None  # TODO : un tuple (quartier, nombre)

# %%
verifier("nombre de quartiers", nb_quartiers, 8)
verifier("quartier le plus fourni", quartier_max, ("Affaires", 8))

# %% [markdown]
# ## Exercice 1.7 — Spark est paresseux : le prouver  ★★★
#
# **Ce que vous apprenez** : une **transformation** (`filter`, `select`,
# `orderBy`…) décrit un calcul sans le lancer ; une **action** (`count`, `show`,
# `first`…) le déclenche sous la forme d'un ou plusieurs **jobs**.
#
# La fonction `nb_jobs()` ci-dessous renvoie le nombre de jobs lancés depuis le
# début de la session. Mesurez la différence avant et après chaque étape.
#
# 1. Lisez `trajets.csv` dans `trajets` (mêmes options qu'à l'exercice 1.2), et
#    rangez dans `jobs_lecture` le nombre de jobs lancés par cette lecture.
# 2. Construisez `electriques` : les trajets à vélo électrique (`electrique`
#    vaut 1), colonnes `velo_id` et `distance_km`, du plus long au plus court.
#    Rangez dans `jobs_transfo` le nombre de jobs lancés.
# 3. Comptez `electriques` dans `nb_electriques`, et rangez dans `jobs_action`
#    le nombre de jobs lancés par ce `count`.
#
# *Indice* : `avant = nb_jobs()`, l'étape, puis `nb_jobs() - avant`.
#
# *Pour aller plus loin* : refaites la lecture **sans** `inferSchema`. Combien
# de jobs ? Pourquoi moins ? Regardez aussi l'onglet **Jobs** de l'interface web.

# %%
suivi = spark.sparkContext.statusTracker()


def nb_jobs():
    """Nombre de jobs lancés depuis le début de la session."""
    return len(suivi.getJobIdsForGroup())


# %%
CHEMIN_TRAJETS = os.path.join(DONNEES, "trajets.csv")

# 1. Lire trajets.csv
trajets = None       # TODO
jobs_lecture = None  # TODO

# 2. Filtrer, choisir, trier
electriques = None   # TODO
jobs_transfo = None  # TODO

# 3. Compter
nb_electriques = None  # TODO
jobs_action = None     # TODO

# %%
verifier("la lecture avec inferSchema lance des jobs",
         jobs_lecture >= 1 if jobs_lecture is not None else None, True)
verifier("jobs lancés par filter + select + orderBy", jobs_transfo, 0)
verifier("count lance au moins un job",
         jobs_action >= 1 if jobs_action is not None else None, True)
verifier("trajets à vélo électrique", nb_electriques, 112896)

# %% [markdown]
# ## Exercice 1.8 — Les partitions  ★★★
#
# **Ce que vous apprenez** : un DataFrame est découpé en **partitions**, traitées
# chacune par une tâche ; `repartition(n)` en change le nombre.
#
# 1. Rangez dans `nb_part_stations` et `nb_part_trajets` le nombre de partitions
#    de `stations` et de `trajets`.
# 2. Construisez `trajets6`, les trajets répartis en 6 partitions, et mesurez
#    dans `jobs_repartition` les jobs lancés : transformation ou action ?
# 3. Rangez dans `nb_part_trajets6` le nombre de partitions de `trajets6`.
#
# *Indice* : `df.rdd.getNumPartitions()`.
#
# *Pour aller plus loin* : `coalesce(1)` réduit le nombre de partitions sans
# tout redistribuer. Pourquoi `stations` n'a-t-il qu'une partition, et
# `trajets` deux ? (`spark.sparkContext.defaultParallelism` donne une piste.)

# %%
nb_part_stations = None  # TODO
nb_part_trajets = None   # TODO

trajets6 = None          # TODO
jobs_repartition = None  # TODO
nb_part_trajets6 = None  # TODO

# %%
verifier("partitions de stations", nb_part_stations, 1)
verifier("partitions de trajets", nb_part_trajets, 2)
verifier("jobs lancés par repartition(6)", jobs_repartition, 0)
verifier("partitions après repartition(6)", nb_part_trajets6, 6)

# %% [markdown]
# ## Ce que vous avez appris
#
# - `SparkSession.builder … getOrCreate()` ouvre la session ; l'interface web
#   montre chaque job.
# - `spark.read.csv(…, header=True, inferSchema=True)`, puis `printSchema()` pour
#   vérifier ce que Spark a compris.
# - `select`, `filter`, `orderBy`, `groupBy().count()` : des **transformations**,
#   qui ne lancent rien.
# - `count`, `show`, `first`, `collect` : des **actions**, qui lancent des jobs.
# - Un DataFrame est découpé en **partitions** ; `repartition` en change le
#   nombre.
#
# Place au TP du module : `tp1-environnement/` (énoncé dans
# `tp1-environnement/enonce.md`), qui combine ces gestes sur les trajets et
# ouvre le plan d'exécution.

# %%
spark.stop()
