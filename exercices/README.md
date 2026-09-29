# Exercices guidés : Spark Python

Six séries d'exercices, une par module, pour apprendre les gestes **un par un** avant de
les combiner dans le grand TP du module. Elles sont pensées pour des débutants : on part
de presque tout le code fourni, et l'aide diminue d'un exercice à l'autre.

**50 exercices, environ 6 h 15 en tout**, sur les mêmes données Vélo'Cité que les TP.

## Comment travailler

1. Générez les données une fois, si ce n'est pas fait : `python ../donnees/generer_donnees.py`.
2. Ouvrez le `depart.ipynb` de la série, dans Jupyter ou VS Code.
3. Pour chaque exercice : lisez la consigne, **remplacez les `None`** de la cellule de code,
   exécutez, puis exécutez la cellule de vérification juste en dessous.
4. Lisez la ligne qui s'affiche :

   | Ligne | Sens |
   |---|---|
   | `✔  … : 23` | C'est juste, passez à la suite |
   | `✘  … : 21   (attendu : 23)` | Relisez la consigne et l'indice, corrigez, réexécutez |
   | `…  … : à compléter` | La variable vaut encore `None` |

5. Bloqué plus de dix minutes ? Relisez l'indice, puis demandez au formateur : les corrigés
   sont projetés en séance.

Les valeurs attendues sont les vraies : le jeu de données est déterministe, tout le monde
obtient les mêmes chiffres. Elles ont été relevées par exécution, en
`local[2]` ; un autre nombre de cœurs peut faire varier les résultats des tirages
aléatoires (série 4), et seulement ceux-là.

### Les niveaux

| Repère | Ce qu'on attend |
|---|---|
| ★ | Presque tout est donné : on complète une ligne, on regarde le résultat |
| ★★ | La méthode est indiquée, on écrit l'instruction soi-même |
| ★★★ | Plusieurs gestes de la série à combiner, avec peu d'aide |

Chaque exercice n'introduit **qu'une notion nouvelle**, annoncée sous son titre
(« Ce que vous apprenez »), et réutilise les précédents.

## Les six séries

| Dossier | Module | Durée | Exercices |
|---|---|---|---|
| [`serie1-decouvrir/`](serie1-decouvrir/depart.py) | 1 · Découvrir Spark | ≈ 60 min | 8 |
| [`serie2-rdd/`](serie2-rdd/depart.py) | 2 · RDD | ≈ 60 min | 8 |
| [`serie3-dataframes/`](serie3-dataframes/depart.py) | 3 · Spark SQL et DataFrames | ≈ 65 min | 9 |
| [`serie4-ml/`](serie4-ml/depart.py) | 4 · MLlib | ≈ 70 min | 9 |
| [`serie5-streaming/`](serie5-streaming/depart.py) | 5 · Structured Streaming | ≈ 60 min | 8 |
| [`serie6-graphes/`](serie6-graphes/depart.py) | 6 · Graphes | ≈ 60 min | 8 |

### Série 1 · Découvrir Spark

| | Exercice | Niveau |
|---|---|---|
| 1.1 | Créer la session | ★ |
| 1.2 | Lire `stations.csv` | ★ |
| 1.3 | Compter, afficher, choisir des colonnes | ★ |
| 1.4 | Garder les grandes stations | ★★ |
| 1.5 | Trier les stations | ★★ |
| 1.6 | Compter les stations par quartier | ★★ |
| 1.7 | Spark est paresseux : le prouver | ★★★ |
| 1.8 | Les partitions | ★★★ |

### Série 2 · Programmer avec les RDD

| | Exercice | Niveau |
|---|---|---|
| 2.1 | Créer un premier RDD | ★ |
| 2.2 | Transformer : `map` et `filter` | ★ |
| 2.3 | Résumer : `reduce` | ★ |
| 2.4 | Compter les mots : `flatMap` et `reduceByKey` | ★★ |
| 2.5 | Lire un fichier texte : `sc.textFile` | ★★ |
| 2.6 | Voir les partitions : `getNumPartitions` et `glom` | ★★ |
| 2.7 | Les 5 stations les plus en erreur : `sortBy` et `take` | ★★ |
| 2.8 | Lignes mal formées et noms des stations (accumulateur, broadcast) | ★★★ |

### Série 3 · Spark SQL et DataFrames

| | Exercice | Niveau |
|---|---|---|
| 3.1 | Lire les trajets avec un schéma déclaré | ★ |
| 3.2 | Ajouter des colonnes calculées | ★ |
| 3.3 | Durée moyenne par type d'usager | ★ |
| 3.4 | Classer les trajets : court, moyen, long | ★★ |
| 3.5 | Le quartier de départ, par une jointure | ★★ |
| 3.6 | La même question en SQL | ★★ |
| 3.7 | Compter les défauts avant de les retirer | ★★ |
| 3.8 | La station d'arrivée préférée de chaque quartier (fenêtre) | ★★★ |
| 3.9 | Écrire en Parquet, relire, recompter | ★★★ |

### Série 4 · Machine learning avec MLlib

| | Exercice | Niveau |
|---|---|---|
| 4.1 | Poser la cible `label` | ★ |
| 4.2 | Découper en entraînement et test | ★ |
| 4.3 | Le score à battre | ★ |
| 4.4 | Assembler les variables en un vecteur | ★★ |
| 4.5 | Transformer un texte en indice : `StringIndexer` | ★★ |
| 4.6 | Une régression logistique, et son AUC | ★★ |
| 4.7 | Tout ranger dans un `Pipeline` | ★★ |
| 4.8 | Matrice de confusion, précision, rappel | ★★★ |
| 4.9 | Bonus : une petite forêt aléatoire | ★★★ |

La série travaille sur un échantillon de 20 % des trajets, pour que chaque entraînement
réponde vite.

### Série 5 · Analyser en temps réel

| | Exercice | Niveau |
|---|---|---|
| 5.1 | Lot ou flux ? | ★ |
| 5.2 | Un flux exige un schéma | ★ |
| 5.3 | Une première requête continue | ★ |
| 5.4 | Un fichier arrive, le compte augmente | ★★ |
| 5.5 | Le mode append : seulement les nouvelles lignes | ★★ |
| 5.6 | Compter par tranches de 5 minutes | ★★ |
| 5.7 | Plusieurs requêtes à la fois, et tout arrêter | ★★ |
| 5.8 | Watermark : le retardataire accepté et le retardataire écarté | ★★★ |

Pas d'émetteur ici : la série dépose elle-même quelques fichiers de trajets choisis à la
main, pour que les résultats soient les mêmes pour tous. Les heures sont en UTC.

### Série 6 · Théorie des graphes

| | Exercice | Niveau |
|---|---|---|
| 6.1 | Un graphe, c'est deux DataFrames | ★ |
| 6.2 | Le degré sortant | ★ |
| 6.3 | Le nœud le plus « visé » | ★ |
| 6.4 | Les voisins directs, avec leur nom | ★★ |
| 6.5 | À deux sauts | ★★ |
| 6.6 | Le plus court chemin, par un parcours en largeur | ★★ |
| 6.7 | Le graphe du réseau Vélo'Cité | ★★ |
| 6.8 | PageRank, en quelques itérations | ★★★ |

Aucune dépendance à GraphFrames : tout se fait en DataFrames. Si `networkx` est installé,
l'exercice 6.8 s'en sert comme référence.

## Quand les faire

Chaque série est un **échauffement** : on la fait avant le TP de son module, ou en
autonomie le soir. En salle, les ★ se font ensemble en quelques minutes ; les ★★ et ★★★
occupent ceux qui avancent vite pendant que le groupe finit le TP.

| Série | Avant le TP |
|---|---|
| 1 | [`tp1-environnement/`](../tp1-environnement/enonce.md) |
| 2 | [`tp2-rdd/`](../tp2-rdd/enonce.md) |
| 3 | [`tp3-sql-dataframes/`](../tp3-sql-dataframes/enonce.md) |
| 4 | [`tp4-ml/`](../tp4-ml/enonce.md) |
| 5 | [`tp5-streaming/`](../tp5-streaming/enonce.md) |
| 6 | [`tp6-graphes/`](../tp6-graphes/enonce.md) |
