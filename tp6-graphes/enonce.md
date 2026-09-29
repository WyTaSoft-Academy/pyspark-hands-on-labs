# TP 6 · PageRank, plus court chemin et visualisation

**Module 6 · Théorie des graphes** — durée ≈ 1 h 15, dont 10 min de restitution.

## Objectif

Lire le réseau Vélo'Cité comme un **graphe** : les stations sont les nœuds,
les trajets agrégés sont les arêtes, orientées (départ → arrivée) et pondérées
(nombre de trajets). Trouver les stations qui structurent le réseau, le plus
court chemin entre deux stations, vérifier que le réseau « tient », et en
dessiner la carte.

## Fichiers

| Fichier | Rôle |
|---|---|
| `depart.ipynb` | Le notebook à compléter (source : `depart.py`) |
| `../donnees/stations.csv`, `../donnees/trajets.csv` | Les données du fil rouge |
| `../sorties/trajets-propres/` | Les trajets nettoyés au TP 3, s'ils existent |

## Avant de commencer : GraphFrames ou pas

La cellule 0 est **fournie**. Elle tente de charger GraphFrames, qui n'est pas
livré avec Spark :

- le module Python : `pip install graphframes-py` (et non `graphframes`, une
  ancienne version) ;
- le JAR, téléchargé depuis Maven Central au démarrage de la session :
  `io.graphframes:graphframes-spark3_2.12:0.12.2` pour Spark 3.5, la version
  de la formation. Sur Spark 4.x, ce serait la variante `graphframes-spark4_2.13`.

Si le téléchargement échoue (proxy, pas de réseau), la cellule démarre une
session ordinaire et affiche `GraphFrames : False`. **Ce n'est pas bloquant** :
tout le TP se fait aussi avec les DataFrames seuls. Pour tester ce chemin
volontairement : `SANS_GRAPHFRAMES=1` dans l'environnement.

Le premier démarrage avec GraphFrames prend plus de temps : le JAR est
téléchargé, puis gardé en cache dans `~/.ivy2`.

## Étapes

### 1. Les trajets propres

Lire `sorties/trajets-propres` (Parquet), s'il est complet : il contient alors
un fichier `_SUCCESS`, que Spark n'écrit qu'à la fin d'une écriture réussie.
Sinon, relire
`donnees/trajets.csv` et appliquer le nettoyage minimal : doublons, durées
négatives ou nulles, arrivée ou type d'usager manquant.

**Attendu** : 299 251 trajets.

### 2. Les nœuds et les arêtes

- `noeuds` : `stations.csv`, avec `station_id` renommée en `id` (nom imposé par
  GraphFrames).
- `toutes` : une ligne par couple (départ, arrivée), colonnes `src`, `dst`,
  `trajets` (nombre) et `distance_km` (moyenne). **Écarter les boucles**
  (départ = arrivée).
- `aretes` : seulement les liaisons d'au moins **100 trajets**.

**Attendu** : 60 nœuds, 3 540 arêtes, 28 796 boucles écartées, 914 liaisons
régulières.

> 3 540 = 60 × 59 : toutes les paires existent. Sans seuil, le graphe est
> complet et chaque station est à un saut de toutes les autres.

### 3. Les degrés

Pour chaque station : degré entrant, degré sortant, avec son nom.

**Attendu** : dix stations ont un degré entrant de **58 ou 59** (huit à 59, deux à 58) —
presque toutes les autres stations y mènent régulièrement (Place du Marché, Hôtel de Ville, Gare Centrale,
Quai Sud, Opéra…). Les moins desservies en ont **6** (Campus Nord, Roseraie,
Bibliothèque…).

### 4. PageRank

- Avec GraphFrames : `GraphFrame(noeuds, aretes).pageRank(resetProbability=0.15, maxIter=20)`.
  Attention : la somme des rangs vaut *n*, pas 1. Diviser par le nombre de
  nœuds.
- Sans GraphFrames : compléter `pagerank_dataframes`. À chaque itération,
  chaque station partage son rang, à parts égales, entre ses liaisons
  sortantes ; puis `rang = 0,15 / n + 0,85 × reçu`.

**Attendu** : les dix stations-carrefours en tête, entre 0,0549 et 0,0555
(Quai Sud et Gare Centrale à 0,0555). Les cinquante autres autour de 0,009.

### 5. Vérifier avec networkx

Rapatrier nœuds et arêtes dans un `nx.DiGraph` (60 nœuds : le driver les
tient sans peine), puis comparer à `nx.pagerank(G, alpha=0.85, weight=None)`.

**Attendu** : écart maximal inférieur à 10⁻⁶.

### 6. Le plus court chemin

De « Roseraie » à « Docks », en nombre de liaisons. Parcours en largeur : la
frontière avance d'un saut par tour, et on écarte les stations déjà vues avec
une jointure `left_anti`. Vérifier avec `nx.shortest_path_length`, et avec
`g.shortestPaths(landmarks=[...])` si GraphFrames est disponible.

**Attendu** : 2 sauts, Roseraie → Opéra → Docks.

### 7. La connexité

Nombre de composantes **faiblement** et **fortement** connexes, aux seuils de
100, 150 et 200 trajets.

**Attendu** :

| Seuil | Liaisons | Composantes faibles | Composantes fortes |
|---|---|---|---|
| 100 | 914 | 1 | 1 |
| 150 | 306 | 1 | 36 |
| 200 | 48 | 18 | 59 |

Question à se poser : pourquoi, au seuil de 150, le réseau est-il encore d'un
seul morceau, mais éclaté en 36 composantes fortes ?

### 8. La visualisation

Chaque station à sa position réelle (longitude, latitude), colorée par
quartier, de taille proportionnelle à son PageRank, les dix premières
nommées. Enregistrer l'image dans `sorties/graphe-stations.png`.

Dans un notebook, retirer la ligne `matplotlib.use("Agg")` du code pour
voir l'image s'afficher.

## Bonus

1. **Le chemin le plus court en kilomètres** : Dijkstra avec networkx, sur le
   poids `distance`. *Attendu : Roseraie → Planétarium → Opéra → Docks, 6,00 km
   — un saut de plus que le chemin en sauts.*
2. **Les allers-retours** (GraphFrames) : `g.find("(a)-[e1]->(b); (b)-[e2]->(a)")`.
   *Attendu : 239 couples reliés dans les deux sens, et 436 liaisons sans retour
   régulier (motif `"(a)-[]->(b); !(b)-[]->(a)"`).*
3. **Le seuil** : refaire les étapes 4 et 6 au seuil de 150. Que devient le
   plus court chemin ?

## Restitution

Projeter deux ou trois cartes `graphe-stations.png`, et répondre en une phrase :
**qu'est-ce que le graphe montre, que le tableau des trajets ne montrait pas ?**
