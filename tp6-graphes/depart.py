"""
TP 6 · PageRank, plus court chemin et visualisation — point de départ.

Complétez les cellules marquées TODO, dans l'ordre. L'énoncé détaillé est
dans enonce.md ; le corrigé est projeté en fin de TP.
"""

# %% [markdown]
# # TP 6 · PageRank, plus court chemin et visualisation
#
# Le réseau Vélo'Cité vu comme un graphe : les **stations** sont les nœuds,
# les **trajets agrégés** sont les arêtes, orientées (départ → arrivée) et
# pondérées (nombre de trajets).

# %% [markdown]
# ## Étape 0 · La session, avec ou sans GraphFrames
#
# La cellule est fournie. Elle essaie de charger GraphFrames depuis Maven
# Central ; si le réseau ou le proxy l'en empêche, elle démarre une session
# ordinaire et vous travaillerez avec les DataFrames seuls.

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
os.makedirs(SORTIES, exist_ok=True)

PAQUET = "io.graphframes:graphframes-spark3_2.12:0.12.2"


def session(avec_graphframes):
    b = (SparkSession.builder.master("local[2]").appName("tp6-graphes")
         .config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.shuffle.partitions", "4"))
    if avec_graphframes:
        b = b.config("spark.jars.packages", PAQUET)
    return b.getOrCreate()


try:
    if os.environ.get("SANS_GRAPHFRAMES"):  # pour tester le plan B
        raise RuntimeError("SANS_GRAPHFRAMES est positionné")
    spark = session(avec_graphframes=True)
    from graphframes import GraphFrame
    GRAPHFRAMES = True
except Exception as e:
    print("GraphFrames indisponible, bascule sur les DataFrames :", str(e)[:120])
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
    spark = session(avec_graphframes=False)
    GRAPHFRAMES = False

spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.setCheckpointDir(os.path.join(SORTIES, "points-de-reprise-graphes"))
print("Spark", spark.version, "· GraphFrames :", GRAPHFRAMES)

# %% [markdown]
# ## Étape 1 · Les trajets propres
#
# Lisez `sorties/trajets-propres` (Parquet, produit au TP 3) s'il est complet,
# c'est-à-dire s'il contient le fichier `_SUCCESS`. Sinon, relisez
# `donnees/trajets.csv` et appliquez le nettoyage minimal : doublons, durées
# négatives ou nulles, arrivée ou type d'usager manquant.

# %%
PROPRES = os.path.join(SORTIES, "trajets-propres")
trajets = None  # TODO
print(trajets.count(), "trajets")

# %% [markdown]
# ## Étape 2 · Les nœuds et les arêtes
#
# - `noeuds` : `stations.csv`, avec la colonne `station_id` renommée `id`.
# - `toutes` : une ligne par couple (départ, arrivée), colonnes `src`, `dst`,
#   `trajets` (le nombre) et `distance_km` (la moyenne). Écartez les boucles.
# - `aretes` : seulement les liaisons d'au moins 100 trajets.

# %%
noeuds = None  # TODO
toutes = None  # TODO
SEUIL = 100
aretes = None  # TODO
print(noeuds.count(), "nœuds ·", toutes.count(), "arêtes ·", aretes.count(), "liaisons régulières")

# %% [markdown]
# ## Étape 3 · Les degrés
#
# Pour chaque station : degré entrant et degré sortant, avec son nom.
# Quelles stations reçoivent le plus de liaisons ? Lesquelles le moins ?

# %%
# TODO

# %% [markdown]
# ## Étape 4 · PageRank
#
# Avec GraphFrames : `GraphFrame(noeuds, aretes).pageRank(resetProbability=0.15, maxIter=20)`.
# Sans GraphFrames : complétez la fonction ci-dessous. À chaque itération,
# chaque station partage son rang, à parts égales, entre ses liaisons
# sortantes ; puis `rang = 0,15 / n + 0,85 × reçu`.

# %%
def pagerank_dataframes(noeuds, aretes, iterations=20, amortissement=0.85):
    n = noeuds.count()
    sortie = aretes.groupBy("src").agg(F.count("*").alias("nb_sorties"))
    liens = aretes.select("src", "dst").join(sortie, "src")
    rangs = noeuds.select("id", F.lit(1.0 / n).alias("rang"))
    for i in range(iterations):
        recu = None   # TODO : joindre liens et rangs, sommer rang / nb_sorties par dst
        rangs = None  # TODO : repartir de tous les nœuds, appliquer la formule
        if i % 5 == 4:
            rangs = rangs.localCheckpoint()
    return rangs


rangs = None  # TODO : colonnes id, rang (somme des rangs = 1)
# TODO : les 10 premières stations, avec leur nom et leur quartier

# %% [markdown]
# ## Étape 5 · Vérifier avec networkx
#
# Rapatriez nœuds et arêtes dans un `nx.DiGraph` (60 nœuds : ça tient), puis
# comparez vos rangs à `nx.pagerank(G, alpha=0.85, weight=None)`.

# %%
import networkx as nx

G = nx.DiGraph()
# TODO : ajouter les nœuds (avec nom, quartier, pos=(longitude, latitude))
# TODO : ajouter les arêtes (avec trajets et distance)

# %% [markdown]
# ## Étape 6 · Le plus court chemin
#
# De la station « Roseraie » à la station « Docks », en nombre de liaisons.
# Parcours en largeur : la frontière avance d'un saut par tour, et on écarte
# les stations déjà vues (`left_anti`). Vérifiez avec
# `nx.shortest_path_length`.

# %%
# TODO

# %% [markdown]
# ## Étape 7 · La connexité
#
# Combien de composantes faiblement et fortement connexes au seuil de 100 ?
# Et aux seuils de 150 et 200 ?

# %%
# TODO

# %% [markdown]
# ## Étape 8 · La visualisation
#
# Chaque station à sa position (longitude, latitude), colorée par quartier,
# de taille proportionnelle à son PageRank. Enregistrez l'image dans
# `sorties/graphe-stations.png`.

# %%
import matplotlib.pyplot as plt

# TODO

# %% [markdown]
# ## Bonus · Le chemin le plus court en kilomètres
#
# Dijkstra, avec networkx, sur le poids `distance`. Est-ce le même chemin
# que celui qui a le moins de sauts ?

# %%
# TODO

# %%
spark.stop()
