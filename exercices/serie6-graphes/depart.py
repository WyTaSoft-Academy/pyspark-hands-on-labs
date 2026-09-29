"""
Série 6 — Théorie des graphes (module 6) : notebook de départ.

Remplissez les None marqués TODO, dans l'ordre, et lisez les lignes ✔ ou ✘.
Se lance tel quel, en script ou en notebook (format « percent »).
Aucune dépendance à GraphFrames : tout se fait avec des DataFrames.
"""

# %% [markdown]
# # Série 6 · Théorie des graphes
#
# **Module 6 · Théorie des graphes** — durée ≈ 60 minutes, huit exercices.
#
# Un graphe, ce sont deux tables : les **nœuds** et les **arêtes**. Avec les
# DataFrames seuls, sans bibliothèque de graphes, vous saurez à la fin :
#
# - calculer les degrés entrant et sortant d'un nœud ;
# - trouver les voisins d'un nœud, puis les nœuds à deux sauts, par jointure ;
# - mesurer la longueur du plus court chemin par un parcours en largeur ;
# - construire le graphe du réseau Vélo'Cité à partir des trajets ;
# - écrire quelques itérations de PageRank.
#
# Mode d'emploi : remplissez les `None`, exécutez la cellule, puis la cellule
# de vérification, et lisez la ligne ✔ ou ✘.

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


# %%
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .master("local[2]")
         .appName("exercices-serie6")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# %% [markdown]
# ## Exercice 6.1 — Un graphe, c'est deux DataFrames  ★
#
# **Ce que vous apprenez** : un graphe orienté se range dans deux tables, les
# nœuds (`id`) et les arêtes (`src` → `dst`).
#
# Voici un petit réseau de six stations fictives, reliées par huit liaisons à
# sens unique :
#
# ```
#     A ─────► B
#     ▲ ╲      │
#     │  ╲     ▼
#     │   ╰──► C ◄───── F
#     ╰─────── │        ▲
#              ▼        │
#              D ─────► E
# ```
#
# Autrement dit : A→B, A→C, B→C, C→A, C→D, D→E, E→F, F→C.
#
# 1. Exécutez la cellule qui crée `noeuds` et `aretes` (elle est fournie).
# 2. Comptez les nœuds dans `nb_noeuds` et les arêtes dans `nb_aretes`.
#
# *Indice* : `noeuds.count()`.

# %%
NOEUDS = [("A", "Arsenal"), ("B", "Beffroi"), ("C", "Cathédrale"),
          ("D", "Docks"), ("E", "Esplanade"), ("F", "Faubourg")]
ARETES = [("A", "B"), ("A", "C"), ("B", "C"), ("C", "A"),
          ("C", "D"), ("D", "E"), ("E", "F"), ("F", "C")]

# .cache() : ces petites tables, resservies à chaque exercice, restent en mémoire.
noeuds = spark.createDataFrame(NOEUDS, "id string, nom string").cache()
aretes = spark.createDataFrame(ARETES, "src string, dst string").cache()
noeuds.show()
aretes.show()

# %%
nb_noeuds = None  # TODO
nb_aretes = None  # TODO

# %%
verifier("nombre de nœuds", nb_noeuds, 6)
verifier("nombre d'arêtes", nb_aretes, 8)

# %% [markdown]
# ## Exercice 6.2 — Le degré sortant  ★
#
# **Ce que vous apprenez** : le degré sortant d'un nœud est le nombre d'arêtes
# qui en partent ; c'est un simple `groupBy("src").count()`.
#
# 1. Groupez `aretes` par `src` et comptez : c'est `degres_sortants`.
# 2. Renommez la colonne `count` en `degre_sortant`.
# 3. Rapatriez le résultat, trié par `src`, sous forme de liste de couples
#    `(src, degre_sortant)` dans `liste_sortants`.
#
# *Indice* :
# `aretes.groupBy("src").count().withColumnRenamed("count", "degre_sortant")`,
# puis `[(r["src"], r["degre_sortant"]) for r in df.orderBy("src").collect()]`.

# %%
degres_sortants = None  # TODO : aretes.groupBy(...).count()...
liste_sortants = None   # TODO : liste de couples (src, degre_sortant), triée par src

# %%
verifier("degrés sortants", liste_sortants,
         [("A", 2), ("B", 1), ("C", 2), ("D", 1), ("E", 1), ("F", 1)])

# %% [markdown]
# ## Exercice 6.3 — Le nœud le plus « visé »  ★
#
# **Ce que vous apprenez** : le degré entrant se compte de la même façon, en
# groupant cette fois sur `dst` ; un tri décroissant donne le nœud le plus
# visé.
#
# 1. Calculez `degres_entrants` : colonnes `dst` et `degre_entrant`.
# 2. Triez par `degre_entrant` décroissant et prenez la première ligne.
# 3. Rangez son identifiant dans `plus_vise` et son degré dans
#    `degre_max`.
#
# *Indice* : `degres_entrants.orderBy(F.desc("degre_entrant")).first()`
# renvoie une ligne ; `ligne["dst"]` en lit une colonne.

# %%
degres_entrants = None  # TODO : colonnes dst, degre_entrant
plus_vise = None        # TODO : l'identifiant du nœud le plus visé
degre_max = None        # TODO : son degré entrant

# %%
verifier("nœud au plus fort degré entrant", plus_vise, "C")
verifier("son degré entrant", degre_max, 3)

# %% [markdown]
# ## Exercice 6.4 — Les voisins directs, avec leur nom  ★★
#
# **Ce que vous apprenez** : une jointure entre `aretes` et `noeuds` remplace
# un identifiant par les informations du nœud.
#
# 1. Gardez les arêtes qui partent de `C` : ce sont les voisins directs de C.
# 2. Joignez-les à `noeuds`, en reliant `dst` à `id`, pour obtenir le nom de
#    chaque voisin.
# 3. Rangez la liste des noms, triée par ordre alphabétique, dans
#    `voisins_de_c`.
#
# *Indice* : `aretes.where(...).join(noeuds, aretes.dst == noeuds.id)` ;
# pour la liste, `sorted(r["nom"] for r in df.collect())`.

# %%
voisins = None       # TODO : arêtes qui partent de C, jointes à noeuds
voisins_de_c = None  # TODO : liste triée des noms

# %%
verifier("voisins directs de C", voisins_de_c, ["Arsenal", "Docks"])

# %% [markdown]
# ## Exercice 6.5 — À deux sauts  ★★
#
# **Ce que vous apprenez** : joindre `aretes` avec elle-même enchaîne deux
# arêtes, donc deux sauts. Il faut des **alias** pour distinguer les deux
# copies de la table.
#
# 1. Prenez deux copies : `a1 = aretes.alias("a1")`, `a2 = aretes.alias("a2")`.
# 2. Joignez-les là où l'arrivée de la première est le départ de la seconde :
#    `a1.dst == a2.src`.
# 3. Gardez les chemins qui partent de `A`, sélectionnez `a1.src`, `a1.dst`
#    (le nœud intermédiaire) et `a2.dst` (l'arrivée).
# 4. Rangez les arrivées **distinctes**, triées, dans `deux_sauts_de_a`.
#
# *Indice* : `F.col("a1.dst") == F.col("a2.src")` dans la jointure, puis
# `.select(F.col("a2.dst").alias("arrivee")).distinct()`.
#
# Remarquez dans le résultat que A revient sur lui-même, et que C est à la
# fois à un saut et à deux sauts : « à deux sauts » ne veut pas dire « au plus
# court à deux sauts ». L'exercice suivant règle ce point.

# %%
a1 = aretes.alias("a1")
a2 = aretes.alias("a2")
chemins2 = None         # TODO : a1.join(a2, ...).where(...).select(...)
deux_sauts_de_a = None  # TODO : arrivées distinctes, triées

# %%
verifier("arrivées à deux sauts de A", deux_sauts_de_a, ["A", "C", "D"])

# %% [markdown]
# ## Exercice 6.6 — Le plus court chemin, par un parcours en largeur  ★★
#
# **Ce que vous apprenez** : répéter la jointure dans une boucle fait avancer
# une « frontière » d'un saut par tour ; une jointure `left_anti` écarte les
# nœuds déjà visités.
#
# On cherche le nombre minimal de sauts pour aller de `A` à `F`.
#
# 1. `vus` et `frontiere` contiennent au départ le seul nœud `A` (colonne
#    `id`) ; ils sont fournis.
# 2. À chaque tour : joignez `frontiere` à `aretes` (`id` = `src`), gardez les
#    `dst` renommés en `id`, sans doublon, puis retirez ceux déjà dans `vus`
#    avec `.join(vus, "id", "left_anti")`. C'est la nouvelle frontière.
# 3. Si la nouvelle frontière contient `F`, rangez le numéro du tour dans
#    `distance_a_f` et sortez de la boucle (`break`).
# 4. Sinon, ajoutez la frontière à `vus` (`unionByName`) et recommencez.
# 5. Garde-fou : au plus `MAX_TOURS` tours, pour ne jamais boucler sans fin.
#
# *Indice* : pour tester la présence de F,
# `frontiere.where(F.col("id") == "F").count() > 0`.

# %%
MAX_TOURS = 10
vus = noeuds.where(F.col("id") == "A").select("id")
frontiere = vus
distance_a_f = None

for tour in range(1, MAX_TOURS + 1):
    # TODO : la nouvelle frontière (jointure avec aretes, dst -> id, distinct,
    #        puis .join(vus, "id", "left_anti"))
    # TODO : si elle contient F, distance_a_f = tour, puis break
    # TODO : si elle est vide, break (plus rien à explorer)
    # TODO : sinon, vus = vus.unionByName(frontiere)
    break  # à retirer une fois la boucle écrite

# %%
verifier("plus court chemin de A à F (en sauts)", distance_a_f, 4)

# %% [markdown]
# ## Exercice 6.7 — Le graphe du réseau Vélo'Cité  ★★
#
# **Ce que vous apprenez** : un graphe se construit à partir de données brutes
# en agrégeant : une arête par couple (départ, arrivée), avec un seuil pour ne
# garder que les liaisons régulières.
#
# 1. `trajets` est lu pour vous.
# 2. Écartez les trajets dont la station de départ ou d'arrivée est vide, et
#    les boucles (départ = arrivée).
# 3. Groupez par (`station_depart`, `station_arrivee`), comptez, et gardez les
#    couples d'**au moins 100 trajets**. Renommez les colonnes en `src`, `dst`,
#    `trajets` : c'est `aretes_velo`. Comptez-les dans `nb_aretes_velo`.
# 4. Réutilisez le geste de l'exercice 6.3 : le degré entrant maximal va dans
#    `degre_entrant_max`, et le nombre de stations qui l'atteignent dans
#    `nb_stations_max`.
#
# *Indice* : `F.col("station_arrivee").isNotNull()`, puis
# `.groupBy("station_depart", "station_arrivee").count()` et
# `.where(F.col("count") >= 100)`. Pour le maximum :
# `df.agg(F.max("degre_entrant")).first()[0]`.
#
# Ici, on part des trajets **bruts**, sans les nettoyer. Le TP 6, lui, part
# des trajets nettoyés au TP 3 et ne trouve pas tout à fait les mêmes
# chiffres : le nettoyage change le graphe.

# %%
trajets = spark.read.csv(os.path.join(DONNEES, "trajets.csv"),
                         header=True, inferSchema=True)

# %%
aretes_velo = None        # TODO : colonnes src, dst, trajets (au moins 100)
nb_aretes_velo = None     # TODO

entrants_velo = None      # TODO : colonnes dst, degre_entrant
degre_entrant_max = None  # TODO
nb_stations_max = None    # TODO

# %%
verifier("liaisons d'au moins 100 trajets", nb_aretes_velo, 914)
verifier("degré entrant maximal", degre_entrant_max, 59)
verifier("stations qui l'atteignent", nb_stations_max, 8)

# %% [markdown]
# ## Exercice 6.8 — PageRank, en quelques itérations  ★★★
#
# **Ce que vous apprenez** : PageRank s'écrit comme une boucle de jointures et
# d'agrégations, où chaque tour part du résultat du tour précédent.
#
# Retour au petit graphe de l'exercice 6.1, où chaque nœud a au moins une
# arête sortante. Le principe : chaque nœud partage son rang, à parts égales,
# entre ses arêtes sortantes ; chaque nœud additionne ce qu'il reçoit ; puis
#
#     rang = 0,15 / n + 0,85 × reçu      (n = nombre de nœuds)
#
# 1. Au départ, tous les nœuds ont le rang `1 / n` : DataFrame `rangs`,
#    colonnes `id` et `rang`.
# 2. Préparez `liens` : les arêtes, avec le degré sortant de leur `src`
#    (exercice 6.2).
# 3. Faites 20 tours : joignez `liens` et `rangs`, sommez `rang / degre_sortant`
#    par `dst`, puis appliquez la formule à tous les nœuds.
# 4. Rangez l'identifiant du nœud au plus fort rang dans `premier_pr`, et la
#    somme des rangs dans `somme_pr`.
#
# La cellule de vérification compare ensuite vos rangs à une référence
# calculée en Python pur (et à `networkx.pagerank` s'il est installé).
#
# *Indice* : `F.lit(1.0 / n)` ; `F.sum(F.col("rang") / F.col("degre_sortant"))`.
#
# *Pour aller plus loin* : que se passerait-il si un nœud n'avait aucune arête
# sortante ? Où son rang irait-il ? (Le TP 6 le rencontre.)

# %%
n = noeuds.count()
AMORTISSEMENT = 0.85
liens = None   # TODO : src, dst, degre_sortant
rangs = None   # TODO : id, rang = 1 / n

# TODO : 20 tours ; à chaque tour
#   - recu  : liens joints à rangs, somme de rang / degre_sortant par dst
#   - rangs : tous les nœuds, rang = (1 - AMORTISSEMENT) / n + AMORTISSEMENT * reçu
#             (F.coalesce("recu", F.lit(0.0)) pour un nœud qui ne reçoit rien)
#   - rangs = rangs.localCheckpoint()  (le plan ne s'allonge pas à chaque tour)

premier_pr = None  # TODO : id du nœud au plus fort rang
somme_pr = None    # TODO : somme des rangs

# %%
def pagerank_python(noeuds_py, aretes_py, amortissement=0.85, tours=200):
    """Référence en Python pur, poussée jusqu'à la convergence."""
    ids = [i for i, _ in noeuds_py]
    sortants = {i: sum(1 for s, _ in aretes_py if s == i) for i in ids}
    r = {i: 1 / len(ids) for i in ids}
    for _ in range(tours):
        recu = {i: 0.0 for i in ids}
        for s, d in aretes_py:
            recu[d] += r[s] / sortants[s]
        r = {i: (1 - amortissement) / len(ids) + amortissement * recu[i] for i in ids}
    return r


reference = pagerank_python(NOEUDS, ARETES)
try:
    import networkx as nx
    G = nx.DiGraph(ARETES)
    reference = nx.pagerank(G, alpha=0.85)
    print("Référence : networkx.pagerank")
except ImportError:
    print("Référence : PageRank en Python pur (networkx n'est pas installé)")

ecart_max = None
if rangs is not None:
    ecart_max = max(abs(r["rang"] - reference[r["id"]]) for r in rangs.collect())

verifier("nœud au plus fort PageRank", premier_pr, "C")
verifier("somme des rangs", somme_pr, 1.0, tolerance=1e-6)
verifier("écart maximal avec la référence", ecart_max, 0.0, tolerance=1e-3)

# %% [markdown]
# ## Ce que vous avez appris
#
# - Un graphe orienté = une table de nœuds + une table d'arêtes `src` → `dst`.
# - Degrés : `groupBy("src")` ou `groupBy("dst")`, puis `count()`.
# - Voisins : une jointure ; deux sauts : `aretes` jointe à elle-même, avec
#   des alias.
# - Plus court chemin : une boucle de jointures, une jointure `left_anti` pour
#   écarter les nœuds vus, et un garde-fou sur le nombre de tours.
# - Un graphe réel se construit en agrégeant les données brutes, avec un seuil.
# - PageRank : une boucle de jointures et d'agrégations, et `localCheckpoint`
#   pour que le plan ne grossisse pas à chaque tour.
#
# Le TP 6 (`tp6-graphes/`) combine ces gestes sur le vrai réseau : PageRank
# des 60 stations, plus court chemin entre deux stations, connexité et carte.

# %%
spark.stop()
