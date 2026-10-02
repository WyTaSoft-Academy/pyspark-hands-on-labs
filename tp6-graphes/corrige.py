"""
TP 6 · PageRank, plus court chemin et visualisation — corrigé.

    python corrige.py

Deux chemins, selon ce que le poste permet :

  - GraphFrames, si le paquet se télécharge (il vient de Maven Central au
    démarrage de la session : un proxy d'entreprise peut le bloquer) ;
  - DataFrames seuls sinon : PageRank et parcours en largeur écrits à la
    main, en quelques jointures. Même résultat, aucun paquet à installer.

Le script essaie le premier et bascule seul sur le second.
"""

# %% [markdown]
# # TP 6 · PageRank, plus court chemin et visualisation — corrigé
#
# Le réseau Vélo'Cité vu comme un graphe : les **stations** sont les nœuds,
# les **trajets agrégés** sont les arêtes, orientées (départ → arrivée) et
# pondérées (nombre de trajets).

# %% [markdown]
# ## Étape 0 · La session, avec ou sans GraphFrames

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

# La coordonnée Maven dépend de la version de Spark : celle-ci vaut pour
# Spark 3.5 (Scala 2.12). Pour Spark 4.x : graphframes-spark4_2.13.
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
except Exception as e:  # proxy, pas de réseau, paquet Python absent
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
# Produits par le TP 3. S'ils n'existent pas, on les reconstruit ici avec le
# nettoyage minimal.

# %%
PROPRES = os.path.join(SORTIES, "trajets-propres")
# _SUCCESS n'est écrit qu'à la fin d'une écriture complète : un dossier sans
# lui est un TP 3 interrompu, illisible.
if os.path.exists(os.path.join(PROPRES, "_SUCCESS")):
    trajets = spark.read.parquet(PROPRES)
else:
    trajets = (spark.read.csv(os.path.join(DONNEES, "trajets.csv"),
                              header=True, inferSchema=True)
               .dropDuplicates()
               .where("duree_min > 0")
               .where("station_arrivee IS NOT NULL AND type_usager IS NOT NULL"))
print(trajets.count(), "trajets")

# %% [markdown]
# ## Étape 2 · Les nœuds et les arêtes
#
# GraphFrames impose deux noms de colonnes : `id` pour les nœuds, `src` et
# `dst` pour les arêtes. Les boucles (retour à la station de départ) sont
# mises de côté : elles ne relient rien.

# %%
noeuds = (spark.read.csv(os.path.join(DONNEES, "stations.csv"),
                         header=True, inferSchema=True)
          .withColumnRenamed("station_id", "id"))

toutes = (trajets
          .where("station_depart <> station_arrivee")
          .groupBy(F.col("station_depart").alias("src"),
                   F.col("station_arrivee").alias("dst"))
          .agg(F.count("*").alias("trajets"),
               F.round(F.avg("distance_km"), 2).alias("distance_km")))

print(noeuds.count(), "nœuds ·", toutes.count(), "arêtes")
print(trajets.where("station_depart = station_arrivee").count(), "boucles écartées")

# %% [markdown]
# 60 nœuds, 3 540 arêtes : 60 × 59, **toutes les paires existent**. Un graphe
# complet ne dit rien : tout le monde est à un saut de tout le monde. On garde
# les **liaisons régulières**, au moins 100 trajets sur six mois.

# %%
SEUIL = 100
aretes = toutes.where(F.col("trajets") >= SEUIL).cache()
print(aretes.count(), "liaisons régulières")

# %% [markdown]
# ## Étape 3 · Les degrés
#
# Degré sortant : le nombre de liaisons régulières qui partent d'une station.
# Degré entrant : celles qui y arrivent.

# %%
entrants = aretes.groupBy(F.col("dst").alias("id")).agg(F.count("*").alias("degre_entrant"))
sortants = aretes.groupBy(F.col("src").alias("id")).agg(F.count("*").alias("degre_sortant"))
degres = (noeuds.select("id", "nom", "quartier")
          .join(entrants, "id", "left").join(sortants, "id", "left")
          .fillna(0))
degres.orderBy(F.desc("degre_entrant")).show(5, truncate=False)
degres.orderBy("degre_entrant").show(3, truncate=False)

# %% [markdown]
# ## Étape 4 · PageRank
#
# Une station est importante si des stations importantes y mènent. Facteur
# d'amortissement 0,85 : à chaque pas, 15 % de chances de repartir d'une
# station au hasard.

# %%
def pagerank_dataframes(noeuds, aretes, iterations=20, amortissement=0.85):
    """PageRank non pondéré, écrit avec des jointures : chaque station
    partage son rang, à parts égales, entre ses liaisons sortantes."""
    n = noeuds.count()
    sortie = aretes.groupBy("src").agg(F.count("*").alias("nb_sorties"))
    liens = aretes.select("src", "dst").join(sortie, "src")
    rangs = noeuds.select("id", F.lit(1.0 / n).alias("rang"))
    for i in range(iterations):
        recu = (liens.join(rangs, liens.src == rangs.id)
                .groupBy("dst")
                .agg(F.sum(F.col("rang") / F.col("nb_sorties")).alias("recu")))
        rangs = (noeuds.select("id")
                 .join(recu, noeuds.id == recu.dst, "left")
                 .select("id", ((1 - amortissement) / n
                                + amortissement * F.coalesce("recu", F.lit(0.0)))
                         .alias("rang")))
        if i % 5 == 4:
            rangs = rangs.localCheckpoint()  # coupe un plan qui s'allonge
    return rangs


if GRAPHFRAMES:
    g = GraphFrame(noeuds, aretes)
    pr = g.pageRank(resetProbability=0.15, maxIter=20).vertices
    # GraphFrames normalise la somme des rangs à n, et non à 1
    rangs = pr.select("id", (F.col("pagerank") / noeuds.count()).alias("rang"))
else:
    rangs = pagerank_dataframes(noeuds, aretes)

classement = (rangs.join(noeuds, "id")
              .select("id", "nom", "quartier", F.round("rang", 4).alias("rang"))
              .orderBy(F.desc("rang")))
classement.show(10, truncate=False)

# %% [markdown]
# ## Étape 5 · Vérifier avec networkx
#
# 60 nœuds : le graphe tient dans la mémoire du driver. On le rapatrie, et on
# compare avec une implémentation de référence.

# %%
import networkx as nx

G = nx.DiGraph()
for r in noeuds.collect():
    G.add_node(r["id"], nom=r["nom"], quartier=r["quartier"],
               pos=(r["longitude"], r["latitude"]))
for r in aretes.collect():
    G.add_edge(r["src"], r["dst"], trajets=r["trajets"], distance=r["distance_km"])

reference = nx.pagerank(G, alpha=0.85, weight=None)
ecart = max(abs(reference[r["id"]] - r["rang"]) for r in rangs.collect())
print(f"écart maximal avec networkx : {ecart:.6f}")

# %% [markdown]
# ## Étape 6 · Le plus court chemin
#
# Parcours en largeur : on part d'une station, on avance d'une liaison à
# chaque tour, et on garde le premier chemin qui atteint chaque station.

# %%
def id_de(nom):
    return noeuds.where(F.col("nom") == nom).first()["id"]


DEPART, ARRIVEE = id_de("Roseraie"), id_de("Docks")


def plus_court_chemin(aretes, depart, arrivee, max_sauts=10):
    """BFS en DataFrames : la frontière avance d'un saut par tour."""
    vus = spark.createDataFrame([(depart, [depart])], "id string, chemin array<string>")
    frontiere = vus
    for _ in range(max_sauts):
        suivants = (frontiere.join(aretes, frontiere.id == aretes.src)
                    .select(F.col("dst").alias("id"),
                            F.concat("chemin", F.array("dst")).alias("chemin"))
                    .join(vus.select("id"), "id", "left_anti")
                    .dropDuplicates(["id"]))
        if suivants.isEmpty():
            return None
        trouve = suivants.where(F.col("id") == arrivee).first()
        if trouve:
            return trouve["chemin"]
        vus = vus.unionByName(suivants).localCheckpoint()
        frontiere = suivants
    return None


chemin = plus_court_chemin(aretes, DEPART, ARRIVEE)
noms = dict(noeuds.select("id", "nom").collect())
print(len(chemin) - 1, "sauts :", " → ".join(noms[s] for s in chemin))

if GRAPHFRAMES:
    g.shortestPaths(landmarks=[ARRIVEE]).where(F.col("id") == DEPART).show(truncate=False)

print("networkx :", nx.shortest_path_length(G, DEPART, ARRIVEE), "sauts")

# %% [markdown]
# ## Étape 7 · La connexité
#
# Un graphe orienté est **fortement connexe** si chaque station peut
# atteindre chacune des autres en suivant le sens des liaisons.

# %%
for seuil in (100, 150, 200):
    H = nx.DiGraph()
    H.add_nodes_from(G.nodes)
    H.add_edges_from((r["src"], r["dst"]) for r in
                     toutes.where(F.col("trajets") >= seuil).collect())
    print(f"seuil {seuil:>3} · {H.number_of_edges():>4} liaisons · "
          f"{nx.number_weakly_connected_components(H):>2} composantes faibles · "
          f"{nx.number_strongly_connected_components(H):>2} fortes")

if GRAPHFRAMES:
    cc = g.stronglyConnectedComponents(maxIter=10)
    print("GraphFrames, seuil 100 :", cc.select("component").distinct().count(),
          "composante(s) forte(s)")

# %% [markdown]
# ## Étape 8 · La visualisation
#
# Chaque station à sa position réelle, colorée par quartier, de taille
# proportionnelle à son PageRank.

# %%
import matplotlib
matplotlib.use("Agg")  # pas d'écran nécessaire ; retirer dans un notebook
import matplotlib.pyplot as plt

rang_de = dict(rangs.select("id", "rang").collect())
quartiers = sorted({d["quartier"] for _, d in G.nodes(data=True)})
palette = plt.get_cmap("tab10")
couleur = {q: palette(i) for i, q in enumerate(quartiers)}
pos = nx.get_node_attributes(G, "pos")

fig, ax = plt.subplots(figsize=(11, 8))
nx.draw_networkx_edges(G, pos, ax=ax, alpha=0.12, arrows=False)
nx.draw_networkx_nodes(G, pos, ax=ax,
                       node_size=[rang_de[n] * 30000 for n in G.nodes],
                       node_color=[couleur[G.nodes[n]["quartier"]] for n in G.nodes])
for n in sorted(rang_de, key=rang_de.get, reverse=True)[:10]:
    x, y = pos[n]
    ax.annotate(G.nodes[n]["nom"], (x, y), fontsize=8,
                xytext=(6, 6), textcoords="offset points")
for q in quartiers:
    ax.scatter([], [], color=couleur[q], label=q)
ax.legend(title="Quartier", loc="lower left", fontsize=8)
ax.set_title(f"Vélo'Cité · liaisons d'au moins {SEUIL} trajets · taille = PageRank")
ax.set_axis_off()
IMAGE = os.path.join(SORTIES, "graphe-stations.png")
fig.savefig(IMAGE, dpi=120, bbox_inches="tight")
print("image écrite :", IMAGE)

# %% [markdown]
# ## Bonus · Le chemin le plus court en kilomètres
#
# Le plus petit nombre de sauts n'est pas le plus court en distance :
# Dijkstra, sur le poids `distance`.

# %%
km = nx.shortest_path(G, DEPART, ARRIVEE, weight="distance")
print(" → ".join(noms[s] for s in km),
      f"· {nx.shortest_path_length(G, DEPART, ARRIVEE, weight='distance'):.2f} km")

# %%
spark.stop()
