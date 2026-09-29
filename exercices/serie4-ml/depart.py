"""
Série 4 — Machine learning avec MLlib (module 4) : notebook de départ.

Complétez les cellules marquées TODO : remplacez chaque None par votre code.

    python depart.py

Se lance tel quel, en script ou en notebook (format « percent »).
Lit donnees/trajets.csv et donnees/stations.csv ; n'écrit rien.
"""

# %% [markdown]
# # Série 4 · Machine learning avec MLlib
#
# **Module 4** · environ 1 h 10 · neuf exercices, du plus simple au plus difficile.
#
# La question du module : un trajet Vélo'Cité a eu lieu ; est-ce un **abonné**
# ou un **occasionnel** ? À la fin de la série, vous saurez :
#
# - préparer une table avec une colonne cible `label` ;
# - découper en entraînement et test, et mesurer le score à battre ;
# - assembler des colonnes en vecteur, transformer un texte en indice ;
# - entraîner une régression logistique, l'évaluer (AUC), la ranger dans un `Pipeline` ;
# - lire une matrice de confusion : précision et rappel.
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
         .appName("exercices-serie4")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# %%
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler

# %% [markdown]
# ## Exercice 4.1 — Poser la cible `label`  ★
#
# **Ce que vous apprenez** : MLlib attend la réponse à deviner dans une colonne
# numérique, `label` par défaut.
#
# La table est déjà presque prête : trajets propres (durée positive, type
# d'usager renseigné), heure de départ, quartier de la station de départ
# (jointure avec `stations.csv`). Pour que chaque cellule réponde en quelques
# secondes, on travaille sur un **échantillon de 20 %** des trajets
# (`sample(fraction=0.2, seed=42)`).
#
# 1. Dans `donnees`, ajoutez la colonne `label` : `1.0` pour un occasionnel,
#    `0.0` pour un abonné.
# 2. Comptez les lignes de `donnees` dans `nb_trajets`.
# 3. Comptez les lignes où `label` vaut 1.0 dans `nb_occasionnels`.
#
# *Indice* : `(F.col("type_usager") == "occasionnel").cast("double")` donne
# 1.0 ou 0.0 ; à placer dans `base.withColumn("label", ...)`.

# %%
trajets = spark.read.csv(os.path.join(DONNEES, "trajets.csv"),
                         header=True, inferSchema=True)
stations = spark.read.csv(os.path.join(DONNEES, "stations.csv"),
                          header=True, inferSchema=True)
quartiers = stations.select(F.col("station_id").alias("station_depart"), "quartier")

base = (trajets
        .filter(F.col("duree_min") > 0)
        .filter(F.col("type_usager").isNotNull())
        .withColumn("heure", F.hour("debut"))
        .join(quartiers, "station_depart")
        .select("trajet_id", "station_depart", "quartier", "heure",
                "duree_min", "distance_km", "electrique", "type_usager")
        .sample(fraction=0.2, seed=42))

donnees = None  # TODO : base.withColumn("label", ...)
nb_trajets = None  # TODO
nb_occasionnels = None  # TODO

if donnees is not None:
    donnees.show(5)

# %%
verifier("trajets dans l'échantillon", nb_trajets, 60431)
verifier("occasionnels", nb_occasionnels, 21924)

# %%
# Filet de sécurité : si l'exercice précédent n'est pas terminé, la suite part
# quand même d'un objet correct. Ne modifiez pas cette cellule.
if donnees is None:
    donnees = base.withColumn("label",
                              (F.col("type_usager") == "occasionnel").cast("double"))

# %% [markdown]
# ## Exercice 4.2 — Découper en entraînement et test  ★
#
# **Ce que vous apprenez** : on n'évalue jamais un modèle sur les lignes qui ont
# servi à l'entraîner ; `randomSplit` met 80 % des lignes d'un côté, 20 % de l'autre.
#
# 1. Découpez `donnees` avec `randomSplit([0.8, 0.2], seed=42)` en `train` et `test`.
# 2. Mettez les deux en cache (`.cache()`) : on va s'en servir souvent.
# 3. Comptez-les dans `nb_train` et `nb_test`.
#
# *Indice* : `train, test = donnees.randomSplit([0.8, 0.2], seed=42)`.
#
# La graine rend le découpage reproductible **sur ce poste, en `local[2]`** :
# `randomSplit` tire partition par partition, donc un autre nombre de
# partitions donnerait d'autres lignes. Le TP 4 utilise pour cela un hachage
# de l'identifiant.

# %%
train, test = None, None  # TODO : randomSplit, puis .cache() sur chacun
nb_train = None  # TODO
nb_test = None  # TODO

# %%
verifier("lignes d'entraînement", nb_train, 48454)
verifier("lignes de test", nb_test, 11977)

# %%
# Filet de sécurité : si l'exercice précédent n'est pas terminé, la suite part
# quand même d'un objet correct. Ne modifiez pas cette cellule.
if train is None or test is None:
    train, test = donnees.randomSplit([0.8, 0.2], seed=42)
    train, test = train.cache(), test.cache()

# %% [markdown]
# ## Exercice 4.3 — Le score à battre  ★
#
# **Ce que vous apprenez** : la référence naïve, c'est l'exactitude d'un
# « modèle » qui répond toujours la classe la plus fréquente.
#
# 1. Sur `test`, comptez les trajets par `label` (`groupBy("label").count()`) :
#    quelle classe est majoritaire ?
# 2. Calculez `part_majoritaire` : la part de cette classe dans `test`
#    (un nombre entre 0 et 1).
#
# *Indice* : `test.filter(F.col("label") == 0.0).count() / nb_test`.

# %%
# TODO : afficher le nombre de trajets par label dans test

part_majoritaire = None  # TODO

# %%
verifier("part de la classe majoritaire", part_majoritaire, 0.6467, tolerance=0.001)

# %% [markdown]
# ## Exercice 4.4 — Assembler les variables en un vecteur  ★★
#
# **Ce que vous apprenez** : un modèle MLlib lit **une seule** colonne de
# variables, de type vecteur ; `VectorAssembler` la fabrique à partir de
# colonnes numériques.
#
# 1. Créez `assembleur`, un `VectorAssembler` qui lit les colonnes de
#    `COLONNES` et écrit dans `features`.
# 2. Appliquez-le à `train` avec `transform` : c'est `train_vec`.
# 3. Récupérez le vecteur de la première ligne dans `premier`
#    (`train_vec.select("features").first()[0]`).
#
# *Indice* : `VectorAssembler(inputCols=COLONNES, outputCol="features")`.
# Un assembleur n'apprend rien : il n'a pas de `fit`, seulement `transform`.

# %%
COLONNES = ["distance_km", "electrique", "heure"]

assembleur = None  # TODO
train_vec = None  # TODO
premier = None  # TODO

# %%
verifier("taille du vecteur", premier.size if premier is not None else None, 3)
verifier("valeurs du premier vecteur",
         premier.toArray().tolist() if premier is not None else None,
         [0.3, 0.0, 13.0])

# %%
# Filet de sécurité : si l'exercice précédent n'est pas terminé, la suite part
# quand même d'un objet correct. Ne modifiez pas cette cellule.
if assembleur is None:
    assembleur = VectorAssembler(inputCols=COLONNES, outputCol="features")
if train_vec is None:
    train_vec = assembleur.transform(train)

# %% [markdown]
# ## Exercice 4.5 — Transformer un texte en indice : `StringIndexer`  ★★
#
# **Ce que vous apprenez** : un *estimateur* apprend quelque chose avec `fit`
# et rend un *modèle*, qui transforme avec `transform`.
#
# Le quartier est un texte ; un modèle ne sait lire que des nombres.
# `StringIndexer` donne à chaque quartier un indice : 0.0 pour le plus
# fréquent, 1.0 pour le suivant, etc. Il doit donc d'abord **compter** :
# c'est son `fit`.
#
# 1. Créez `indexeur`, un `StringIndexer` de `quartier` vers `quartier_idx`.
# 2. Entraînez-le sur `train` : `modele_indexeur = indexeur.fit(train)`.
# 3. Regardez `modele_indexeur.labels` (les quartiers, dans l'ordre des
#    indices), puis appliquez `modele_indexeur.transform(train)` et affichez
#    quelques lignes.
# 4. Rangez le nombre de quartiers dans `nb_quartiers` et le quartier
#    d'indice 0.0 dans `quartier_0`.
#
# *Indice* : `StringIndexer(inputCol="quartier", outputCol="quartier_idx")`.
# Avec `handleInvalid="keep"`, un quartier inconnu à l'entraînement reçoit un
# indice de plus au lieu de faire échouer `transform`.

# %%
indexeur = None  # TODO
modele_indexeur = None  # TODO
# TODO : afficher modele_indexeur.labels et quelques lignes transformées

nb_quartiers = None  # TODO
quartier_0 = None  # TODO

# %%
verifier("nombre de quartiers", nb_quartiers, 8)
verifier("quartier le plus fréquent (indice 0.0)", quartier_0, "Centre")

# %%
# Filet de sécurité : si l'exercice précédent n'est pas terminé, la suite part
# quand même d'un objet correct. Ne modifiez pas cette cellule.
if indexeur is None:
    indexeur = StringIndexer(inputCol="quartier", outputCol="quartier_idx",
                             handleInvalid="keep")

# %% [markdown]
# ## Exercice 4.6 — Une régression logistique, et son AUC  ★★
#
# **Ce que vous apprenez** : entraîner un classifieur avec `fit`, prédire avec
# `transform`, noter avec `BinaryClassificationEvaluator`.
#
# 1. Créez `reglog = LogisticRegression(featuresCol="features", labelCol="label")`.
# 2. Entraînez-la sur `train_vec` : c'est `modele_lr`.
# 3. Prédisez sur le test **assemblé de la même façon** :
#    `predictions = modele_lr.transform(assembleur.transform(test))`.
#    Affichez `label`, `probability` et `prediction` pour quelques lignes.
# 4. Calculez l'AUC dans `auc_lr` :
#    `BinaryClassificationEvaluator(labelCol="label").evaluate(predictions)`.
#
# *Indice* : l'AUC vaut 0,5 pour un tirage à pile ou face, 1 pour un modèle
# parfait. Elle ne dépend pas du seuil de décision, contrairement à l'exactitude.

# %%
reglog = None  # TODO
modele_lr = None  # TODO
predictions = None  # TODO
# TODO : afficher label, probability, prediction

evaluateur = None  # TODO : BinaryClassificationEvaluator(labelCol="label")
auc_lr = None  # TODO

# %%
verifier("AUC de la régression logistique", auc_lr, 0.7388, tolerance=0.001)

# %%
# Filet de sécurité : si l'exercice précédent n'est pas terminé, la suite part
# quand même d'un objet correct. Ne modifiez pas cette cellule.
if reglog is None:
    reglog = LogisticRegression(featuresCol="features", labelCol="label")
if evaluateur is None:
    evaluateur = BinaryClassificationEvaluator(labelCol="label")

# %% [markdown]
# ## Exercice 4.7 — Tout ranger dans un `Pipeline`  ★★
#
# **Ce que vous apprenez** : un `Pipeline` enchaîne les étapes ; un seul `fit`
# sur les données brutes, un seul `transform` sur le test.
#
# À l'exercice 4.6, il fallait penser à assembler le test « de la même façon ».
# Le `Pipeline` s'en charge, et permet d'ajouter le quartier sans effort.
#
# 1. Créez `assembleur_q`, un `VectorAssembler` qui lit `COLONNES` **plus**
#    `quartier_idx`, vers `features`.
# 2. Créez `pipeline` avec les étapes `[indexeur, assembleur_q, reglog]`.
# 3. Entraînez-le sur `train` (brut !) : `modele_pipeline`.
# 4. Prédisez sur `test` dans `predictions_p`, et calculez l'AUC dans `auc_pipeline`.
#
# *Indice* : `Pipeline(stages=[...])`, puis `.fit(train)` et `.transform(test)` ;
# `evaluateur` de l'exercice 4.6 sert tel quel.
#
# *Remarque* : un indice de quartier n'est pas une grandeur (« Gare » = 2 n'est
# pas le double de « Centre » = 0…). Le TP 4 ajoute pour cela un `OneHotEncoder`.

# %%
assembleur_q = None  # TODO
pipeline = None  # TODO
modele_pipeline = None  # TODO
predictions_p = None  # TODO
auc_pipeline = None  # TODO

# %%
verifier("AUC du pipeline", auc_pipeline, 0.7518, tolerance=0.001)

# %%
# Filet de sécurité : si l'exercice précédent n'est pas terminé, la suite part
# quand même d'un objet correct. Ne modifiez pas cette cellule.
if assembleur_q is None:
    assembleur_q = VectorAssembler(inputCols=COLONNES + ["quartier_idx"],
                                   outputCol="features")
if predictions_p is None:
    predictions_p = (Pipeline(stages=[indexeur, assembleur_q, reglog])
                     .fit(train).transform(test))

# %% [markdown]
# ## Exercice 4.8 — Matrice de confusion, précision, rappel  ★★★
#
# **Ce que vous apprenez** : compter les quatre sortes de réponses, et en tirer
# la précision et le rappel sur la classe « occasionnel ».
#
# 1. Sur `predictions_p`, comptez les lignes par couple (`label`, `prediction`)
#    et affichez le résultat trié.
# 2. Rangez les trois comptes utiles dans des entiers :
#    `vp` (occasionnel prédit occasionnel), `fp` (abonné prédit occasionnel),
#    `fn` (occasionnel prédit abonné).
# 3. Calculez `precision = vp / (vp + fp)` : quand le modèle dit
#    « occasionnel », a-t-il raison ?
# 4. Calculez `rappel = vp / (vp + fn)` : quelle part des occasionnels trouve-t-il ?
#
# *Indice* : `groupBy("label", "prediction").count()`, puis `collect()` ; un
# dictionnaire `{(ligne["label"], ligne["prediction"]): ligne["count"] ...}`
# se lit ensuite facilement.
#
# *Pour aller plus loin* : l'exactitude vaut `(vp + vn) / nb_test`. Comparez-la
# à la référence naïve de l'exercice 4.3.

# %%
confusion = None  # TODO : groupBy("label", "prediction").count() sur predictions_p

vp = None  # TODO
fp = None  # TODO
fn = None  # TODO
precision = None  # TODO
rappel = None  # TODO

# %%
verifier("précision (occasionnels)", precision, 0.6420, tolerance=0.001)
verifier("rappel (occasionnels)", rappel, 0.4933, tolerance=0.001)

# %% [markdown]
# ## Exercice 4.9 — Bonus : une petite forêt aléatoire  ★★★
#
# **Ce que vous apprenez** : dans un `Pipeline`, changer de modèle, c'est
# changer une seule étape.
#
# 1. Créez `foret`, une `RandomForestClassifier` de 20 arbres, profondeur 5,
#    graine 42, qui lit `features` et `label`.
# 2. Construisez un pipeline identique à celui de l'exercice 4.7, avec `foret`
#    à la place de `reglog` ; entraînez-le sur `train`.
# 3. Calculez son AUC sur `test` dans `auc_foret`. Qui gagne ?
#
# *Indice* : `numTrees`, `maxDepth`, `seed`.
#
# *Pour aller plus loin* : la logistique ne peut que dire « plus l'heure est
# grande, plus (ou moins) c'est un occasionnel ». Un arbre, lui, découpe
# l'heure en tranches (le matin, l'après-midi…). Regardez
# `modele_foret.stages[-1].featureImportances` : quelle variable compte le plus ?

# %%
foret = None  # TODO
modele_foret = None  # TODO
auc_foret = None  # TODO

# %%
verifier("AUC de la forêt", auc_foret, 0.8349, tolerance=0.005)

# %% [markdown]
# ## Ce que vous avez appris
#
# - la cible dans `label`, en 0.0 / 1.0, et une référence naïve à battre ;
# - `randomSplit` pour garder un test jamais vu à l'entraînement ;
# - les *transformateurs* (`VectorAssembler`), les *estimateurs* (`StringIndexer`,
#   `LogisticRegression`) et leur `fit` qui rend un modèle ;
# - l'AUC avec `BinaryClassificationEvaluator` ;
# - le `Pipeline`, qui applique au test exactement les mêmes étapes qu'à l'entraînement ;
# - la matrice de confusion, la précision et le rappel.
#
# Place au **TP 4** (dossier `tp4-ml`) : sur toutes les données, vous comparerez
# un modèle « au retrait » et un modèle « au dépôt », encoderez proprement le
# quartier, rattraperez le déséquilibre des classes et sauvegarderez le meilleur
# modèle pour le TP 5.

# %%
spark.stop()
