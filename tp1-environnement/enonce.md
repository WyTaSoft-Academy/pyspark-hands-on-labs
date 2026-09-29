# TP 1 — Mise en place de l'environnement et premier script

**Durée :** ≈ 1 h, dont 10 min de restitution · **Individuel** · Module 1

## Objectif

Faire tourner Spark sur votre poste, générer le jeu de données Vélo'Cité, et
observer de vos propres yeux **quand** Spark calcule : une transformation ne
lance rien, une action lance un job.

## Fichiers

| Fichier | Rôle |
|---|---|
| `verifier_env.py` | Contrôle le poste : Python, Java, PySpark, variables Windows, données, un vrai job |
| `depart.ipynb` | Le notebook à compléter (source : `depart.py`) |

## Étape 0 — Installer

La session utilise **Spark 3.5.4**. Toutes les sorties attendues de ce TP et
des suivants ont été obtenues avec cette version exacte.

1. **Python 3.8 à 3.11** : ce sont les versions les plus sûres avec Spark 3.5.
   `python --version`.
2. **Java 17** de préférence ; Java 11 et 8 conviennent aussi. **Pas Java 21** :
   Spark 3.5 n'est pas prévu pour. `java -version`, puis définir `JAVA_HOME`
   sur le dossier du JDK.
3. **PySpark 3.5.4**, dans un environnement virtuel de préférence :

   ```
   python -m venv .venv
   .venv\Scripts\activate          # Windows
   source .venv/bin/activate       # macOS, Linux
   pip install "pyspark==3.5.4" "numpy<2" "pandas<2.3" pyarrow jupyter
   ```

   `pip install pyspark` installe aussi le moteur Spark : rien d'autre à
   télécharger. Les deux bornes sur numpy et pandas ne sont pas décoratives :
   avec numpy 2, `import pyspark.pandas` échoue sur Spark 3.5
   (« `np.NaN` was removed in the NumPy 2.0 release », observé sur un poste
   de préparation). On s'en sert au module 3.

4. **Vérifier** : depuis ce dossier, `python verifier_env.py`. Chaque ligne
   doit être `OK`, et la dernière dire « Poste prêt pour les TP ». Un
   `ATTENTION` n'est pas bloquant aujourd'hui ; un `ÉCHEC` l'est.

### Deux façons d'installer Spark

| | **Voie A — `pip` seul** (celle du TP) | **Voie B — la distribution Spark** |
|---|---|---|
| Ce qu'on installe | `pip install "pyspark==3.5.4"` : la bibliothèque **et** le moteur | L'archive `spark-3.5.4-bin-hadoop3.tgz` décompressée, puis `pip install "pyspark==3.5.4"` |
| Variables | Aucune, hormis `JAVA_HOME` (et `HADOOP_HOME` sous Windows) | `SPARK_HOME` sur le dossier décompressé, son `bin` dans le `PATH` |
| Quand la choisir | Développer et tester sur son poste | Démarrer un cluster standalone, utiliser `spark-shell` ou `spark-sql`, reproduire l'installation d'un serveur |
| Piège | — | La version de l'archive et celle de PySpark **doivent être identiques**, sinon « `'JavaPackage' object is not callable` » |

On n'a besoin que de la voie A cette semaine. La voie B sert à l'annexe « cluster
standalone » du module 1. Le paquet `findspark`, qu'on croise dans de vieux
tutoriels, ne sert qu'à la voie B : il ajoute `SPARK_HOME` au chemin de Python.
Avec la voie A, il est inutile.

### Travailler dans VS Code plutôt que dans Jupyter Lab

Les notebooks du TP s'ouvrent aussi dans VS Code :

1. Installer les extensions **Python** et **Jupyter** de Microsoft.
2. Ouvrir le dossier `pyspark-hands-on-labs/`, puis `tp1-environnement/depart.ipynb`.
3. En haut à droite, **Select Kernel** → **Python Environments** → choisir le
   `.venv` créé plus haut. Si on prend l'interpréteur système, `import pyspark`
   échoue alors que tout marche en ligne de commande.
4. Le fichier `depart.py` s'exécute aussi cellule par
   cellule dans VS Code : les lignes `# %%` y apparaissent comme des cellules
   (**Run Cell**).

Les variables d'environnement (`PYSPARK_PYTHON`, `SPARK_LOCAL_IP`…) doivent être
définies **avant** de lancer VS Code : un terminal ouvert après les avoir
définies dans Windows ne suffit pas si VS Code tournait déjà.

## Étape 1 — Générer les données

Depuis le dossier `pyspark-hands-on-labs/` :

```
python donnees/generer_donnees.py
```

Moins d'une minute, sans Spark ni connexion. Résultat attendu :

```
stations.csv               60 lignes
meteo.csv                 181 lignes
trajets.csv            300300 lignes
journal-bornes.log     608411 lignes
```

Sur un poste lent : `python donnees/generer_donnees.py --trajets 20000`
(les chiffres ci-dessous ne correspondront plus).

## Étape 2 — Le premier script

Ouvrez `depart.ipynb` dans Jupyter (`jupyter lab`, depuis `pyspark-hands-on-labs/`) et
complétez les cellules `TODO` dans l'ordre. Gardez l'interface web de Spark
ouverte dans un onglet : son adresse s'affiche à la création de la session
(`http://localhost:4040`, ou 4041, 4042… si une autre session tourne déjà).

| Étape | Ce qu'on fait | Résultat attendu |
|---|---|---|
| 1 | Créer la session `local[2]` | `Spark 3.5.4` et une adresse d'interface web |
| 2 | Lire `trajets.csv` avec `header=True, inferSchema=True` | 11 colonnes, `debut` et `fin` en `timestamp`, 300 300 lignes, 2 partitions |
| 3 | Compter les jobs d'un `filter` + `select`, puis d'un `count` | **0 job**, puis au moins 1 ; 22 829 trajets de plus de 30 min |
| 4 | Trajets et durée moyenne par type d'usager | abonné 190 269 · 8,6 min ; occasionnel 109 881 · 20,4 min ; `NULL` 150 |
| 5 | Les cinq stations de départ les plus actives | S052 (5 143), S051, S006, S023, S041 |
| 6 | `explain()` du comptage par station | un `Exchange hashpartitioning` : le shuffle |

### Questions pour la restitution

1. Combien de jobs la **lecture** a-t-elle lancés ? *(Deux : `inferSchema`
   oblige Spark à parcourir le fichier pour deviner les types.)*
2. Combien de jobs pour `filter` + `select` ? Et pour `count` ? Pourquoi ?
3. Que représente la ligne `NULL` de l'étape 4 ? *(150 badges illisibles : un
   défaut volontaire du jeu, qu'on nettoiera au module 3.)*
4. Dans l'onglet **Stages**, combien d'étapes pour le comptage par station, et
   qu'est-ce qui les sépare ?

## Bonus

- Repartitionner les trajets en 8 partitions (`repartition(8)`) et le vérifier.
- Lancer votre notebook en script : `spark-submit --master "local[2]" depart.py`.
- **Un mini-cluster standalone**, en trois terminaux ouverts dans ce dossier
  (annexe du module 1) :

  ```
  spark-class org.apache.spark.deploy.master.Master --host 127.0.0.1 --port 7078 --webui-port 8090
  spark-class org.apache.spark.deploy.worker.Worker spark://127.0.0.1:7078 --cores 2 --memory 2g
  spark-submit --master spark://127.0.0.1:7078 --total-executor-cores 2 job_standalone.py
  ```

  Le job affiche `master : spark://127.0.0.1:7078` et les mêmes comptes qu'à
  l'étape 4 ; l'interface du master (`http://localhost:8090`) montre le worker
  et l'application. Puis soumettez votre `depart.py` complété de la même façon : il tourne,
  mais aucune application n'apparaît dans l'interface du master. Pourquoi ? *(Un master fixé dans le code l'emporte sur
  `--master`. C'est pour cela que `job_standalone.py` ne le fixe pas.)*
  Lancez `spark-submit` depuis ce dossier, avec un chemin de script relatif :
  un chemin absolu de la forme `C:/…` a échoué à la soumission sur le poste de
  préparation.
- Refaire l'étape 2 **sans** `inferSchema` : quel est le type des colonnes ?
  Combien de jobs lance la lecture ? *(Toutes en `string`, et un seul job :
  Spark lit encore la première ligne pour trouver les noms de colonnes.)*

## Dépannage

Les trois premières pannes ont été observées sur un poste Windows de préparation.

| Symptôme | Cause | Remède |
|---|---|---|
| Le premier job échoue sur `Accept timed out` | Les exécuteurs Python ne trouvent pas le bon interpréteur | Définir `PYSPARK_PYTHON` (et `PYSPARK_DRIVER_PYTHON`) sur le chemin de `python.exe`. Les notebooks du TP le font eux-mêmes |
| `'JavaPackage' object is not callable` à la création de la session | `SPARK_HOME` pointe vers une autre version de Spark que le PySpark installé | Supprimer `SPARK_HOME` de l'environnement, ou le faire pointer vers la même version |
| La lecture marche, l'écriture (Parquet, CSV) échoue sous Windows | `winutils.exe` absent | Placer `winutils.exe` et `hadoop.dll` **d'un Hadoop 3.3.x** dans `C:\hadoop\bin` et définir `HADOOP_HOME=C:\hadoop`. Les vieux tutoriels pointent vers Hadoop 2.7 ou 3.0 : ils ne conviennent pas à Spark 3.5 (voir la ligne suivante). **Nécessaire dès le TP 2**, qui écrit ses résultats |
| `UnsatisfiedLinkError … NativeIO$POSIX.stat` en relisant un dossier (`textFile` sur un dossier, `PipelineModel.load`) | `hadoop.dll` d'une autre version que le Hadoop 3.3 de Spark 3.5 : tout le reste marche, d'où la surprise au module 4 | Remplacer `winutils.exe` et `hadoop.dll` par ceux d'un Hadoop 3.3.x. `verifier_env.py` le détecte (« Écrire puis relire ») |
| `UnsupportedClassVersionError` ou erreur Java au démarrage | Java incompatible avec Spark 3.5 (trop ancien, ou 21 et plus) | Installer un JDK 17 et définir `JAVA_HOME` |
| `np.NaN was removed in the NumPy 2.0 release` à `import pyspark.pandas` | numpy 2 installé | `pip install "numpy<2" "pandas<2.3"` |
| Jobs anormalement lents, messages `heartbeat` ou `Executor heartbeat timed out`, interface web annoncée sur `host.docker.internal` | Docker Desktop est installé : Spark prend son nom réseau comme adresse du driver et perd le contact avec ses propres exécuteurs | Définir `SPARK_LOCAL_IP=127.0.0.1` avant de lancer Python ou Jupyter |
| `getOrCreate()` ignore un changement de configuration | Une session existe déjà | `spark.stop()`, puis relancer la cellule |

### Repli : Docker

Si un poste ne s'en sort pas, deux images officielles, à télécharger **avant
la session** (le proxy du client peut bloquer Docker Hub et Quay) :

| Image | Contenu | Pour quoi |
|---|---|---|
| `quay.io/jupyter/pyspark-notebook:spark-3.5.3` | Python, Java, PySpark **3.5.3** et Jupyter | Suivre les TP dans un navigateur. Il n'existe pas d'étiquette `spark-3.5.4` pour cette image : la 3.5.3 est la plus proche, et les sorties des TP sont les mêmes |
| `apache/spark:3.5.4-java17-python3` | Spark **3.5.4** exact, Java 17, Python, sans Jupyter | `spark-submit` et le shell `pyspark` à la version exacte |

```
docker run -it --rm -p 8888:8888 -p 4040:4040 -v "%cd%":/home/jovyan/tp quay.io/jupyter/pyspark-notebook:spark-3.5.3
```

(`$(pwd)` au lieu de `%cd%` sous macOS et Linux.) Dans le conteneur,
`python verifier_env.py` signale la version 3.5.3 par un `ATTENTION` : ce
n'est pas bloquant. Ne pas prendre `latest`, qui suit la dernière version de
Spark, donc la branche 4.
