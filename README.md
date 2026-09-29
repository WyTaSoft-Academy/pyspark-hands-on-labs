# PySpark Hands-on Labs

Travaux pratiques et exercices guidés de la formation **Spark Python — développer des applications pour le big data**, WyTaSoft Academy.

Six TP, un par module, sur un seul jeu de données : **Vélo'Cité**, un réseau fictif
de vélos en libre-service. Tout vise **Spark 3.5.4** et tourne en mode local, sur le
poste du participant.

## Installer le poste

```
python -m venv .venv
.venv\Scripts\activate                 (Windows)   ·   source .venv/bin/activate
pip install "pyspark==3.5.4" "numpy<2" "pandas<2.3" pyarrow jupyter
pip install networkx matplotlib scipy  (TP 6)
pip install graphframes-py==0.12.2     (TP 6, facultatif)

python tp1-environnement/verifier_env.py
```

`verifier_env.py` contrôle Python, Java, PySpark, les bibliothèques, les pièges
Windows et les données, puis lance un vrai job. Il sort en code 1 si le poste
n'est pas prêt, et dit quoi faire.

| Prérequis | Pourquoi |
|---|---|
| Python 3.8 à 3.11 | Plage la plus sûre pour Spark 3.5 |
| Java **17** (ou 11, 8) — **pas 21** | Spark 3.5 n'est pas prévu pour Java 21 |
| `numpy<2`, `pandas<2.3` | Avec numpy 2, `import pyspark.pandas` échoue en 3.5 (`np.NaN was removed`) |

### Sous Windows

Cinq pièges, tous rencontrés sur le poste de préparation :

| Symptôme | Remède |
|---|---|
| Le premier job échoue sur `Accept timed out` | `PYSPARK_PYTHON` et `PYSPARK_DRIVER_PYTHON` = chemin de `python.exe` |
| `'JavaPackage' object is not callable` | Un `SPARK_HOME` pointe vers une autre version : le supprimer |
| L'écriture (Parquet, CSV) échoue | `winutils.exe` et `hadoop.dll` dans `C:\hadoop\bin`, `HADOOP_HOME=C:\hadoop` |
| `UnsatisfiedLinkError … NativeIO$POSIX.stat` au chargement d'un modèle (TP 4, TP 5) | `winutils.exe` et `hadoop.dll` d'une **autre version** que Hadoop 3.3 : les remplacer par ceux d'un Hadoop 3.3.x |
| Jobs anormalement lents, messages *heartbeat* | Docker Desktop installé : `SPARK_LOCAL_IP=127.0.0.1` |

Un sixième se contourne dans le TP 2 : `take()` ou `first()` sur une grosse
partition de texte peut faire tomber le processus Python. `count()`, `collect()`
et `show()` ne sont pas concernés.

Repli si un poste résiste : WSL, ou une image Docker. `apache/spark:3.5.4-java17-python3`
embarque exactement la version de la session ; `quay.io/jupyter/pyspark-notebook:spark-3.5.3`
apporte Jupyter, mais en 3.5.3 (il n'existe pas d'étiquette 3.5.4 pour cette image). Voir
`tp1-environnement/enonce.md`. Aucune des deux n'a été lancée sur le poste de préparation.

## Générer les données

```
python donnees/generer_donnees.py                  ≈ 1 minute, ≈ 60 Mo
python donnees/generer_donnees.py --trajets 20000  version réduite pour un poste lent
```

Bibliothèque standard uniquement, **aucun téléchargement** : le script tourne sur
un poste verrouillé. Le tirage est déterministe : tout le monde obtient les mêmes
chiffres.

| Fichier | Contenu | Module |
|---|---|---|
| `stations.csv` | 60 stations, 8 quartiers, position, capacité | tous |
| `meteo.csv` | Température et pluie, du 1er janvier au 30 juin 2026 | 3, 4 |
| `trajets.csv` | 300 300 lignes, dont ≈ 0,5 % de défauts volontaires | 1, 3, 4, 6 |
| `journal-bornes.log` | 608 411 lignes de texte brut, quelques-unes tronquées | 2 |

Les fichiers générés ne sont pas versionnés.

## Les six TP

| Dossier | Module | Ce qu'on fait |
|---|---|---|
| [`tp1-environnement/`](tp1-environnement/enonce.md) | 1 · Découvrir Spark | Installer, vérifier, premier script, compter les jobs |
| [`tp2-rdd/`](tp2-rdd/enonce.md) | 2 · RDD | Analyser le journal des bornes : map/reduce, accumulateur, broadcast, cache, `spark-submit` |
| [`tp3-sql-dataframes/`](tp3-sql-dataframes/enonce.md) | 3 · Spark SQL | Nettoyer, joindre, interroger en API et en SQL, API pandas, écrire en Parquet |
| [`tp4-ml/`](tp4-ml/enonce.md) | 4 · MLlib | Classer abonné / occasionnel, évaluer, démasquer une fuite de la cible, sauvegarder le modèle |
| [`tp5-streaming/`](tp5-streaming/enonce.md) | 5 · Streaming | Fenêtres, watermark, jointure, prédictions en direct, reprise sur point de contrôle |
| [`tp6-graphes/`](tp6-graphes/enonce.md) | 6 · Graphes | PageRank, plus court chemin, connexité, carte du réseau |

Chaque dossier contient :

| Fichier | Rôle |
|---|---|
| `enonce.md` | Les consignes, les résultats attendus, les étapes bonus |
| `depart.ipynb` | Le notebook à compléter |

Les `.py` sont la source : format « percent » (`# %%`), lisible dans un diff et
exécutable tel quel (`python depart.py`). Les notebooks s'en déduisent :

```
python outils/construire_notebooks.py
```

### Les exercices guidés

Avant chaque TP, une série d'exercices pas à pas, pour débutants : **50 exercices**
en six séries, du ★ au ★★★, chacun avec sa vérification ✔ / ✘. Voir
[`exercices/`](exercices/README.md).

### Ce qui passe d'un TP à l'autre

```
TP 3 ──► sorties/trajets-propres/      (Parquet)  ──► TP 4, TP 6
TP 4 ──► sorties/modele-type-usager/   (PipelineModel) ──► TP 5
```

**Aucun TP n'est bloquant pour le suivant** : si une sortie manque, le TP qui en a
besoin la reconstruit lui-même. Un participant absent une demi-journée reprend au
TP suivant sans rien rattraper.

### Deux TP ont un mode de repli

- **TP 5** a besoin d'un flux : `python flux/emetteur.py --vider` le produit, en
  déposant de petits fichiers JSON dans `flux/entree/`.
- **TP 6** utilise GraphFrames s'il se charge (`--packages
  io.graphframes:graphframes-spark3_2.12:0.12.2`, téléchargé depuis Maven Central).
  Si le proxy du client le bloque, le TP bascule sur un PageRank et un plus court
  chemin écrits en DataFrames, aux résultats identiques. `SANS_GRAPHFRAMES=1`
  force ce chemin, pour le répéter avant la session.
