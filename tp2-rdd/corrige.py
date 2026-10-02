"""
TP 2 · Analyser le journal brut des bornes, avec les RDD — corrigé.

    python corrige.py                       dans un notebook, cellule par cellule
    spark-submit --master "local[2]" corrige.py

Format « percent » : chaque « # %% » ouvre une cellule. Le notebook
corrige.ipynb est construit à partir de ce fichier par
outils/construire_notebooks.py.
"""

# %% [markdown]
# # TP 2 · Analyser le journal brut des bornes — corrigé
#
# Le journal `journal-bornes.log` est ce que la collecte reçoit des bornes,
# avant tout nettoyage : une ligne de texte par événement, trois types
# d'événements, et quelques lignes tronquées par des coupures réseau.
#
# On l'analyse avec l'API la plus basse de Spark, les **RDD** : pas de schéma,
# pas de colonnes, des fonctions Python appliquées ligne à ligne.

# %% [markdown]
# ## Étape 0 · Démarrer une session

# %%
import os
import re
import shutil
import time

from pyspark.sql import SparkSession

try:
    ICI = os.path.dirname(os.path.abspath(__file__))
except NameError:  # dans un notebook, __file__ n'existe pas
    ICI = os.getcwd()
TP = os.path.dirname(ICI)
JOURNAL = os.path.join(TP, "donnees", "journal-bornes.log")
STATIONS = os.path.join(TP, "donnees", "stations.csv")
SORTIES = os.path.join(TP, "sorties")

spark = (SparkSession.builder
         .master("local[2]")
         .appName("tp2-journal-bornes")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("ERROR")
print(sc.version, sc.master, sc.defaultParallelism)

# %% [markdown]
# ## Étape 1 · Charger le journal et le compter

# %%
lignes = sc.textFile(JOURNAL)
print("partitions :", lignes.getNumPartitions())
print("lignes     :", lignes.count())

# Aperçu. Sous Linux, macOS, WSL ou Docker : lignes.take(3).
# Sous Windows, take() sur une grosse partition de texte peut faire planter
# le worker Python (« Python worker exited unexpectedly ») : c'est un défaut
# de PySpark sous Windows, pas du code. L'aperçu passe alors par l'API
# DataFrame, qui lit le fichier sans passer par Python.
for r in spark.read.text(JOURNAL).limit(3).collect():
    print(r.value)

# %% [markdown]
# ## Étape 2 · Découper chaque ligne, compter les lignes invalides
#
# Une ligne valide : horodatage, station, type d'événement, puis des paires
# `clé=valeur`. Le message d'erreur est entre guillemets et contient des
# espaces : on découpe au plus trois fois, puis on lit les paires par
# expression régulière.

# %%
TYPES = {"RETRAIT": {"velo", "badge"},
         "DEPOT": {"velo", "duree"},
         "ERREUR": {"code", "message"}}
PAIRE = re.compile(r'(\w+)=("[^"]*"|\S+)')
STATION = re.compile(r"^S\d{3}$")

invalides = sc.accumulator(0)


def analyser(ligne):
    """Renvoie [événement] si la ligne est valide, [] sinon.

    Utilisé avec flatMap : une liste vide fait disparaître la ligne,
    sans avoir à enchaîner un map et un filter."""
    morceaux = ligne.split(" ", 3)
    if len(morceaux) < 4:
        invalides.add(1)
        return []
    horodatage, station, type_, reste = morceaux
    champs = {k: v.strip('"') for k, v in PAIRE.findall(reste)}
    if (len(horodatage) != 19 or not STATION.match(station)
            or type_ not in TYPES or not TYPES[type_] <= champs.keys()
            or reste.count('"') % 2):
        invalides.add(1)
        return []
    return [(horodatage, station, type_, champs)]


evenements = lignes.flatMap(analyser)
print("événements valides :", evenements.count())
print("lignes invalides   :", invalides.value)

# %% [markdown]
# ### Le piège de l'accumulateur
#
# `evenements` n'est pas mis en cache : chaque action relit le fichier et
# rappelle `analyser`. L'accumulateur est donc incrémenté **à nouveau**.

# %%
evenements.count()
print("après une deuxième action :", invalides.value)

# %% [markdown]
# ## Étape 3 · Mettre en cache, et mesurer le gain

# %%
def chrono(action):
    debut = time.perf_counter()
    action()
    return time.perf_counter() - debut


invalides = sc.accumulator(0)          # on repart de zéro
evenements = lignes.flatMap(analyser).cache()

sans_cache = chrono(lambda: lignes.flatMap(analyser).count())
premier = chrono(evenements.count)     # calcule ET remplit le cache
second = chrono(evenements.count)      # lit le cache
print(f"sans cache       : {sans_cache:.2f} s")
print(f"1re action cache : {premier:.2f} s")
print(f"2e action cache  : {second:.2f} s")
print("lignes invalides :", invalides.value)

# %% [markdown]
# `invalides` compte deux passages — l'appel sans cache et le remplissage du
# cache — mais plus aucun ensuite : les actions suivantes lisent le cache,
# `analyser` n'est plus rappelée. La valeur fiable est la moitié, et c'est
# pour cela qu'on ne se fie à un accumulateur posé dans une transformation
# qu'une fois le RDD en cache.

# %%
invalides = sc.accumulator(0)
evenements.unpersist()
evenements = lignes.flatMap(analyser).cache()
evenements.count()
evenements.count()
print("lignes invalides (fiable) :", invalides.value)

# %% [markdown]
# ## Étape 4 · Événements par type : le map/reduce

# %%
par_type = (evenements
            .map(lambda e: (e[2], 1))
            .reduceByKey(lambda a, b: a + b)
            .collect())
for type_, n in sorted(par_type):
    print(f"{type_:8} {n:>8}")

# %% [markdown]
# ## Étape 5 · Les dix stations qui tombent le plus en panne
#
# Le journal ne contient que l'identifiant de la station. Les noms sont dans
# `stations.csv`, soixante lignes : on les **diffuse** à tous les exécuteurs
# plutôt que de faire une jointure.

# %%
noms = (sc.textFile(STATIONS)
        .filter(lambda l: not l.startswith("station_id"))
        .map(lambda l: l.split(","))
        .map(lambda c: (c[0], c[1]))
        .collectAsMap())
noms_diffuses = sc.broadcast(noms)

top_pannes = (evenements
              .filter(lambda e: e[2] == "ERREUR")
              .map(lambda e: (e[1], 1))
              .reduceByKey(lambda a, b: a + b)
              # effectif décroissant, puis identifiant : un ex æquo sort
              # toujours dans le même ordre
              .sortBy(lambda kv: (-kv[1], kv[0]))
              .map(lambda kv: (kv[0], noms_diffuses.value[kv[0]], kv[1])))
for station, nom, n in top_pannes.take(10):
    print(f"{station} {nom:20} {n:>5}")

# %% [markdown]
# ## Étape 6 · Répartition des codes d'erreur

# %%
codes = (evenements
         .filter(lambda e: e[2] == "ERREUR")
         .map(lambda e: ((e[3]["code"], e[3]["message"]), 1))
         .reduceByKey(lambda a, b: a + b)
         .sortByKey()
         .collect())
for (code, message), n in codes:
    print(f"{code} {message:24} {n:>5}")

# %% [markdown]
# ## Étape 7 · Batterie moyenne au retrait des vélos électriques
#
# Une moyenne ne se réduit pas directement : la moyenne de deux moyennes est
# fausse dès que les effectifs diffèrent. On réduit des couples
# `(somme, effectif)`, et on divise à la fin.

# %%
somme, effectif = (evenements
                   .filter(lambda e: e[2] == "RETRAIT" and "batterie" in e[3])
                   .map(lambda e: (int(e[3]["batterie"]), 1))
                   .reduce(lambda a, b: (a[0] + b[0], a[1] + b[1])))
print(f"retraits électriques : {effectif}")
print(f"batterie moyenne     : {somme / effectif:.1f} %")

# %% [markdown]
# ## Étape 8 · Écrire les résultats
#
# `saveAsTextFile` écrit un **dossier**, avec un fichier par partition. On
# ne garde que les dix premières stations, puis ramène à une partition :
# dix lignes, un seul fichier.

# %%
cible = os.path.join(SORTIES, "tp2-top-pannes")
shutil.rmtree(cible, ignore_errors=True)   # saveAsTextFile refuse d'écraser
(sc.parallelize(top_pannes.take(10))       # les dix stations, pas les 60
 .map(lambda t: f"{t[0]};{t[1]};{t[2]}")
 .coalesce(1)
 .saveAsTextFile(cible))
print(sorted(os.listdir(cible)))

# %% [markdown]
# ## Étape 9 · Soumettre le script
#
# Ce fichier est aussi un script. Depuis le dossier du TP :
#
#     spark-submit --master "local[2]" corrige.py
#
# Sur un cluster, seule l'option `--master` change.

# %%
evenements.unpersist()
spark.stop()
