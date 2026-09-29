"""
Série 2 — Programmer avec les RDD (module 2) : notebook de départ.

    python depart.py

Se lance tel quel, en script ou en notebook (format « percent » : chaque
« # %% » ouvre une cellule).
"""

# %% [markdown]
# # Série 2 · Programmer avec les RDD
#
# **Module 2 — Programmer avec les RDD** · environ 1 h · 8 exercices, du plus
# simple au plus difficile.
#
# À la fin de la série, vous saurez :
#
# - créer un RDD à partir d'une liste Python ou d'un fichier texte ;
# - le transformer avec `map`, `filter`, `flatMap`, et le résumer avec
#   `collect`, `count`, `reduce` ;
# - compter par clé avec des paires `(clé, valeur)` et `reduceByKey` ;
# - trier et garder les premiers avec `sortBy` et `take` ;
# - regarder les partitions d'un RDD ;
# - compter « à côté » avec un accumulateur, et partager une petite table
#   avec une variable diffusée (broadcast).
#
# **Mode d'emploi** : remplissez les `None` marqués `# TODO`, exécutez la
# cellule, puis la cellule de vérification juste en dessous. Elle affiche ✔ si
# votre résultat est bon, ✘ sinon (avec la valeur attendue).

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
# ## Démarrer la session
#
# Les RDD ne passent pas par la `SparkSession` mais par le **SparkContext**,
# qu'elle contient : `sc = spark.sparkContext`. Toutes les méthodes des
# exercices (`parallelize`, `textFile`, `accumulator`, `broadcast`) sont des
# méthodes de `sc`.

# %%
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .master("local[2]")
         .appName("exercices-serie2")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

sc = spark.sparkContext
JOURNAL = os.path.join(DONNEES, "journal-bornes.log")
STATIONS = os.path.join(DONNEES, "stations.csv")
print("Spark", sc.version, "·", sc.master)

# %% [markdown]
# ## Exercice 2.1 — Créer un premier RDD  ★
#
# **Ce que vous apprenez** : `sc.parallelize` transforme une liste Python en
# RDD ; `collect` et `count` sont des **actions** qui ramènent un résultat.
#
# 1. Créez `nombres`, un RDD des entiers de 1 à 10, avec `sc.parallelize`.
# 2. Ramenez tout son contenu dans une liste Python `contenu`.
# 3. Comptez ses éléments dans `nb_nombres`.
#
# *Indice* : `sc.parallelize(range(1, 11))`, puis `nombres.collect()` et
# `nombres.count()`.

# %%
nombres = None      # TODO
contenu = None      # TODO
nb_nombres = None   # TODO

# %%
verifier("contenu de nombres", contenu, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
verifier("nombre d'éléments", nb_nombres, 10)

# %% [markdown]
# ## Exercice 2.2 — Transformer : `map` et `filter`  ★
#
# **Ce que vous apprenez** : `map` applique une fonction à chaque élément,
# `filter` ne garde que ceux qui vérifient une condition. Ce sont des
# **transformations** : elles renvoient un nouveau RDD, sans rien calculer
# tant qu'aucune action ne le demande.
#
# 1. À partir de `nombres`, construisez `carres`, le RDD des carrés.
# 2. À partir de `nombres`, construisez `pairs`, le RDD des nombres pairs.
# 3. Ramenez-les dans les listes `liste_carres` et `liste_pairs`.
#
# *Indice* : `nombres.map(lambda x: x * x)` et
# `nombres.filter(lambda x: x % 2 == 0)`, puis `.collect()`.

# %%
carres = None        # TODO
pairs = None         # TODO
liste_carres = None  # TODO
liste_pairs = None   # TODO

# %%
verifier("carrés", liste_carres, [1, 4, 9, 16, 25, 36, 49, 64, 81, 100])
verifier("pairs", liste_pairs, [2, 4, 6, 8, 10])

# %% [markdown]
# ## Exercice 2.3 — Résumer : `reduce`  ★
#
# **Ce que vous apprenez** : `reduce` combine les éléments deux à deux avec
# une fonction, jusqu'à n'en garder qu'un.
#
# 1. Calculez `somme_carres`, la somme des carrés de 1 à 10, avec `reduce`.
# 2. Calculez `somme_carres_pairs`, la somme des carrés des nombres pairs, en
#    enchaînant `filter`, `map` et `reduce` sur `nombres`.
#
# *Indice* : `carres.reduce(lambda a, b: a + b)`. Pour la seconde, partez de
# `nombres.filter(...)`.

# %%
somme_carres = None        # TODO
somme_carres_pairs = None  # TODO

# %%
verifier("somme des carrés", somme_carres, 385)
verifier("somme des carrés pairs", somme_carres_pairs, 220)

# %% [markdown]
# ## Exercice 2.4 — Compter les mots : `flatMap` et `reduceByKey`  ★★
#
# **Ce que vous apprenez** : un RDD de paires `(clé, valeur)` se réduit
# **par clé** avec `reduceByKey`. C'est le comptage de mots, le « bonjour le
# monde » du big data.
#
# 1. Découpez les `phrases` en mots avec `flatMap` (un élément par mot, et
#    non une liste par phrase) : le RDD `mots`. Comptez-les dans `nb_mots`.
# 2. Transformez chaque mot en paire `(mot, 1)`, puis additionnez les `1`
#    par mot : le RDD `comptes`.
# 3. Ramenez `comptes` au driver sous forme de dictionnaire `occurrences`.
#
# *Indice* : `phrases.flatMap(lambda p: p.split())` ; puis
# `.map(lambda m: (m, 1)).reduceByKey(lambda a, b: a + b)` ;
# `collectAsMap()` renvoie un dictionnaire.

# %%
phrases = sc.parallelize([
    "le vélo part de la gare",
    "le vélo arrive à la gare",
    "la borne de la gare est en panne",
])

# %%
mots = None         # TODO
nb_mots = None      # TODO
comptes = None      # TODO
occurrences = None  # TODO

# %%
verifier("nombre de mots", nb_mots, 20)
verifier("mots différents", len(occurrences) if occurrences is not None else None, 12)
verifier("occurrences de « gare »",
         occurrences.get("gare") if occurrences is not None else None, 3)
verifier("occurrences de « la »",
         occurrences.get("la") if occurrences is not None else None, 4)

# %% [markdown]
# ## Exercice 2.5 — Lire un fichier texte : `sc.textFile`  ★★
#
# **Ce que vous apprenez** : `sc.textFile` lit un fichier ligne à ligne ;
# chaque élément du RDD est une ligne, en texte brut.
#
# Le journal des bornes compte plus de 600 000 lignes, par exemple :
#
#     2026-01-01T06:03:49 S027 RETRAIT velo=V0696 badge=A-37535
#     2026-01-01T08:42:28 S014 ERREUR code=E42 message="batterie non détectée"
#
# 1. La lecture est fournie : `journal = sc.textFile(JOURNAL)`.
# 2. Comptez les lignes du journal dans `nb_lignes`.
# 3. Comptez dans `nb_erreurs` les lignes qui contiennent le mot `ERREUR`.
#
# *Indice* : `journal.filter(lambda ligne: "ERREUR" in ligne).count()`.
#
# *Attention, sous Windows* : évitez `journal.take(3)` ou `journal.first()`
# pour « jeter un œil » au fichier, ils peuvent faire tomber le worker Python
# sur une grosse partition de texte. Préférez `count()`, ou un `take` sur un
# petit RDD déjà réduit (exercice 2.7).

# %%
journal = sc.textFile(JOURNAL)

# %%
nb_lignes = None   # TODO
nb_erreurs = None  # TODO

# %%
verifier("lignes du journal", nb_lignes, 608411)
verifier("lignes ERREUR", nb_erreurs, 9001)

# %% [markdown]
# ## Exercice 2.6 — Voir les partitions : `getNumPartitions` et `glom`  ★★
#
# **Ce que vous apprenez** : un RDD est découpé en **partitions**, traitées
# en parallèle. `glom()` regroupe chaque partition en une liste, ce qui permet
# de voir le découpage.
#
# 1. Créez `trois_parts`, les entiers de 1 à 10 répartis en 3 partitions
#    (second argument de `parallelize`).
# 2. Ramenez dans `decoupage` la liste des partitions, chacune sous forme de
#    liste.
# 3. Pour le journal : son nombre de partitions dans `nb_partitions_journal`,
#    et le nombre de lignes de chaque partition dans `lignes_par_partition`.
#
# *Indice* : `sc.parallelize(range(1, 11), 3)`, puis `.glom().collect()`.
# Pour le journal, ne ramenez pas les lignes elles-mêmes :
# `journal.glom().map(len).collect()`.

# %%
trois_parts = None            # TODO
decoupage = None              # TODO
nb_partitions_journal = None  # TODO
lignes_par_partition = None   # TODO

# %%
verifier("découpage en 3 partitions", decoupage, [[1, 2, 3], [4, 5, 6], [7, 8, 9, 10]])
verifier("partitions du journal", nb_partitions_journal, 2)
verifier("lignes par partition", lignes_par_partition, [304210, 304201])
verifier("total des partitions",
         sum(lignes_par_partition) if lignes_par_partition is not None else None,
         608411)

# %% [markdown]
# ## Exercice 2.7 — Les 5 stations les plus en erreur : `sortBy` et `take`  ★★
#
# **Ce que vous apprenez** : `sortBy` trie un RDD selon une clé calculée,
# `take(n)` ramène ses `n` premiers éléments.
#
# Dans une ligne, les champs sont séparés par des espaces ; le deuxième est
# l'identifiant de la station (`S014`).
#
# 1. Gardez les lignes `ERREUR` du journal.
# 2. Transformez chacune en paire `(station, 1)`, puis comptez par station.
# 3. Triez par nombre d'erreurs décroissant et gardez les 5 premières dans la
#    liste `top5`, de la forme `[("S0xx", n), ...]`.
#
# *Indice* : `ligne.split()[1]` donne la station ;
# `.sortBy(lambda kv: -kv[1])` trie du plus grand au plus petit.
# Ici `take` ne pose pas de problème : après `reduceByKey`, le RDD ne compte
# plus que 60 éléments.

# %%
top5 = None  # TODO : journal.filter(...).map(...).reduceByKey(...).sortBy(...).take(5)
print(top5)

# %%
verifier("top 5 des stations en erreur", top5,
         [("S016", 173), ("S014", 172), ("S008", 171), ("S033", 170), ("S044", 168)])

# %% [markdown]
# ## Exercice 2.8 — Lignes mal formées et noms des stations  ★★★
#
# **Ce que vous apprenez** : un **accumulateur** compte « à côté » pendant un
# traitement ; une variable **diffusée** (broadcast) envoie une fois pour
# toutes une petite table à tous les exécuteurs.
#
# Certaines lignes du journal sont tronquées (coupures réseau) : elles ont
# moins de 3 champs, par exemple `2026-01-01T07:54:29 S`.
#
# 1. Construisez `noms`, le dictionnaire `station_id → nom` lu dans
#    `stations.csv` (sans la ligne d'en-tête), puis diffusez-le dans
#    `noms_diffuses`.
# 2. Créez l'accumulateur `mal_formees`, à 0.
# 3. Écrivez une fonction `decouper(ligne)` qui renvoie `[champs]` si la ligne
#    a au moins 3 champs, et sinon ajoute 1 à l'accumulateur et renvoie `[]`.
#    Utilisez-la avec `flatMap` : les lignes mal formées disparaissent.
# 4. Sur ce résultat, gardez les erreurs (troisième champ `ERREUR`), comptez-
#    les par **nom** de station (lu dans `noms_diffuses.value`), et gardez les
#    5 premiers dans `top5_noms`, de la forme `[("nom", n), ...]`.
# 5. Relevez `nb_mal_formees = mal_formees.value`, **après** l'action.
#
# *Indice* : `sc.broadcast(noms)`, `sc.accumulator(0)`, et
# `collectAsMap()` pour le dictionnaire.
#
# *Pour aller plus loin* : relancez le calcul de `top5_noms` sans recréer
# l'accumulateur. Que vaut `mal_formees.value` ? Pourquoi ? Le grand TP y
# revient, avec `cache()`.

# %%
noms = None           # TODO : dictionnaire station_id -> nom
noms_diffuses = None  # TODO : sc.broadcast(...)

mal_formees = None    # TODO : sc.accumulator(0)


def decouper(ligne):
    """Renvoie [champs] pour une ligne complète, [] (et compte) sinon."""
    champs = ligne.split()
    # TODO : si moins de 3 champs, ajouter 1 à mal_formees et renvoyer []
    return [champs]


top5_noms = None       # TODO : journal.flatMap(decouper)...take(5)
nb_mal_formees = None  # TODO : mal_formees.value, après l'action
print(top5_noms)


# %%
verifier("stations dans le dictionnaire", len(noms) if noms is not None else None, 60)
verifier("lignes mal formées", nb_mal_formees, 1145)
verifier("top 5 par nom", top5_noms,
         [("Roseraie", 173), ("Bibliothèque", 172), ("Jardin des Plantes", 171),
          ("Parvis", 170), ("Vieux Pont", 168)])

# %% [markdown]
# ## Ce que vous avez appris
#
# - Un RDD se crée avec `sc.parallelize` (liste Python) ou `sc.textFile`
#   (fichier, une ligne par élément).
# - `map`, `filter`, `flatMap`, `reduceByKey`, `sortBy` sont des
#   transformations : paresseuses, elles décrivent le calcul.
# - `collect`, `count`, `reduce`, `take`, `collectAsMap` sont des actions :
#   elles le déclenchent et ramènent un résultat au driver.
# - Compter par clé : paires `(clé, 1)`, puis `reduceByKey`.
# - `getNumPartitions` et `glom` montrent le découpage en partitions.
# - Un accumulateur compte pendant un traitement, une variable diffusée
#   partage une petite table.
#
# **La suite** : le TP 2 (`tp2-rdd/`) combine ces gestes pour analyser le
# journal des bornes en profondeur : découpage complet des lignes, mise en
# cache, codes d'erreur, batterie des vélos électriques.

# %%
spark.stop()
