"""
Série 3 — Spark SQL et DataFrames (module 3) : notebook de départ.

Remplacez chaque `None` marqué TODO, exécutez, puis lisez la ligne ✔ ou ✘.
Se lance tel quel, en script ou en notebook (format « percent »).
"""

# %% [markdown]
# # Série 3 · Spark SQL et DataFrames
#
# **Module 3** · environ 1 h 05 · neuf exercices, du plus simple au plus difficile.
#
# À la fin de cette série, vous saurez :
#
# - lire un CSV avec un **schéma déclaré** plutôt que deviné ;
# - ajouter des colonnes calculées (`withColumn`, `F.when`) ;
# - agréger (`groupBy().agg()`) et joindre deux tables (`join`) ;
# - poser la même question en **SQL** et vérifier que la réponse est identique ;
# - compter les défauts **avant** de les retirer ;
# - classer à l'intérieur d'un groupe avec une **fonction de fenêtre** ;
# - écrire en **Parquet** et relire.
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
from pyspark.sql import SparkSession, Window, functions as F

spark = (SparkSession.builder
         .master("local[2]")
         .appName("exercices-serie3")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# %% [markdown]
# ## Exercice 3.1 — Lire les trajets avec un schéma déclaré  ★
#
# **Ce que vous apprenez** : un schéma DDL dit à Spark le type de chaque colonne,
# au lieu de le laisser deviner (`inferSchema` relit tout le fichier pour cela).
#
# 1. Le schéma est fourni ci-dessous, dans `SCHEMA_TRAJETS`.
# 2. Lisez `trajets.csv` avec `header=True` et ce schéma, dans `trajets`.
# 3. Mettez dans `nb_trajets` le nombre de lignes, et dans `type_debut` le type
#    de la colonne `debut` (`dict(trajets.dtypes)["debut"]`).
#
# *Indice* : `spark.read.csv(os.path.join(DONNEES, "trajets.csv"), header=True, schema=SCHEMA_TRAJETS)`.

# %%
SCHEMA_TRAJETS = """
    trajet_id STRING, velo_id STRING, station_depart STRING, station_arrivee STRING,
    debut TIMESTAMP, fin TIMESTAMP, duree_min DOUBLE, distance_km DOUBLE,
    type_usager STRING, electrique INT, tarif_eur DOUBLE
"""

trajets = None  # TODO : spark.read.csv(..., header=True, schema=SCHEMA_TRAJETS)
nb_trajets = None  # TODO
type_debut = None  # TODO

# %%
verifier("lignes lues", nb_trajets, 300300)
verifier("type de debut", type_debut, "timestamp")

# %% [markdown]
# ## Exercice 3.2 — Ajouter des colonnes calculées  ★
#
# **Ce que vous apprenez** : `withColumn(nom, expression)` ajoute une colonne
# (ou remplace celle qui porte déjà ce nom) ; le DataFrame d'origine ne change pas.
#
# 1. Partez de `trajets`.
# 2. Ajoutez `heure`, l'heure de départ : `F.hour("debut")`.
# 3. Ajoutez `duree_h`, la durée en heures : `F.col("duree_min") / 60`.
# 4. Rangez le résultat dans `trajets_h`.
# 5. Comptez dans `nb_8h` les trajets partis entre 8 h et 8 h 59 (`heure == 8`).
#
# *Indice* : `trajets.withColumn("heure", F.hour("debut")).withColumn("duree_h", ...)`,
# puis `trajets_h.filter(F.col("heure") == 8).count()`.

# %%
trajets_h = None  # TODO : deux withColumn
nb_8h = None  # TODO

# %%
verifier("colonnes de trajets_h", len(trajets_h.columns) if trajets_h is not None else None, 13)
verifier("trajets partis à 8 h", nb_8h, 28267)

# %% [markdown]
# ## Exercice 3.3 — Durée moyenne par type d'usager  ★
#
# **Ce que vous apprenez** : `groupBy(...).agg(...)` calcule plusieurs
# agrégats d'un coup, chacun nommé avec `.alias(...)`.
#
# 1. Groupez `trajets` par `type_usager`.
# 2. Calculez `nb` (`F.count("*")`) et `duree_moy`, la moyenne de `duree_min`
#    arrondie à une décimale (`F.round(F.avg("duree_min"), 1)`).
# 3. Triez par `type_usager` et rangez le résultat dans `par_type`.
# 4. Mettez dans `lignes_type` la liste des tuples `(type_usager, nb, duree_moy)`.
#
# *Indice* : `[tuple(r) for r in par_type.collect()]`. Une ligne a `None` pour
# type, triée en premier : ce sont les trajets au type vide (une cellule vide
# d'un CSV est lue comme `null`). Le TP 3 les retirera.

# %%
par_type = None  # TODO : groupBy(...).agg(..., ...).orderBy(...)
lignes_type = None  # TODO

# %%
verifier("durée moyenne par type", lignes_type,
         [(None, 150, 12.2), ("abonne", 190269, 8.6), ("occasionnel", 109881, 20.4)])

# %% [markdown]
# ## Exercice 3.4 — Classer les trajets : court, moyen, long  ★★
#
# **Ce que vous apprenez** : `F.when(condition, valeur).when(...).otherwise(...)`
# est le « si… sinon si… sinon » des colonnes, l'équivalent de `CASE WHEN` en SQL.
#
# 1. Partez de `trajets_h` et ajoutez une colonne `classe` :
#    `"court"` sous 10 minutes, `"moyen"` de 10 à moins de 30 minutes, `"long"` au-delà.
# 2. Rangez le résultat dans `trajets_c`.
# 3. Comptez les trajets par `classe`, triez par `classe`, et mettez dans
#    `lignes_classe` la liste des tuples `(classe, count)`.
#
# *Indice* : les conditions sont testées dans l'ordre ; la première vraie gagne.
# Le tri par `classe` est alphabétique : court, long, moyen.

# %%
trajets_c = None  # TODO : trajets_h.withColumn("classe", F.when(...).when(...).otherwise(...))
lignes_classe = None  # TODO

# %%
verifier("trajets par classe", lignes_classe,
         [("court", 149752), ("long", 22979), ("moyen", 127569)])

# %% [markdown]
# ## Exercice 3.5 — Le quartier de départ, par une jointure  ★★
#
# **Ce que vous apprenez** : `join` rapproche deux tables sur une clé ; ici
# `station_depart` des trajets et `station_id` des stations.
#
# 1. Lisez `stations.csv` (`header=True, inferSchema=True`) dans `stations`.
# 2. Joignez `trajets_c` aux stations pour ajouter le `quartier` de départ,
#    dans `trajets_q`. Ne gardez des stations que `station_id` et `quartier`.
# 3. Comptez les trajets par `quartier`, du plus grand nombre au plus petit
#    (puis par nom de quartier), dans `par_quartier`.
# 4. Mettez dans `lignes_quartier` la liste des tuples `(quartier, count)`.
#
# *Indice* : `trajets_c.join(stations.select(...), trajets_c.station_depart == stations.station_id)`.

# %%
stations = None  # TODO
trajets_q = None  # TODO : join, puis .drop("station_id")
par_quartier = None  # TODO
lignes_quartier = None  # TODO

# %%
verifier("lignes après la jointure", trajets_q.count() if trajets_q is not None else None, 300300)
verifier("trajets par quartier", lignes_quartier,
         [("Gare", 40133), ("Centre", 40104), ("Université", 40034),
          ("Affaires", 39969), ("Faubourg", 35326), ("Vieille Ville", 35078),
          ("Berges", 35068), ("Parc", 34588)])

# %% [markdown]
# ## Exercice 3.6 — La même question en SQL  ★★
#
# **Ce que vous apprenez** : `createOrReplaceTempView` donne un nom de table à
# un DataFrame, que `spark.sql(...)` peut ensuite interroger ; les deux
# écritures produisent le même plan d'exécution.
#
# 1. Enregistrez `trajets_q` comme vue `trajets`.
# 2. Écrivez en SQL le nombre de trajets par quartier, trié comme à
#    l'exercice 3.5, dans `par_quartier_sql`.
# 3. Mettez dans `meme_resultat` le booléen : la liste de tuples obtenue en SQL
#    est-elle égale à `lignes_quartier` ?
#
# *Indice* : `SELECT quartier, COUNT(*) AS count FROM trajets GROUP BY quartier ORDER BY ...`

# %%
# TODO : trajets_q.createOrReplaceTempView("trajets")
par_quartier_sql = None  # TODO : spark.sql("""SELECT ... """)
meme_resultat = None  # TODO

# %%
verifier("SQL et DataFrame donnent le même résultat", meme_resultat, True)

# %% [markdown]
# ## Exercice 3.7 — Compter les défauts avant de les retirer  ★★
#
# **Ce que vous apprenez** : on mesure un défaut **avant** de le supprimer, pour
# savoir combien de lignes on perd et pourquoi.
#
# 1. Dans `nb_duree_ko`, comptez les trajets de `trajets_q` dont la durée est
#    nulle ou négative.
# 2. Dans `nb_doublons`, comptez les doublons exacts : le nombre de lignes moins
#    le nombre de lignes après `dropDuplicates()`.
# 3. Dans `propres`, retirez les deux défauts (filtre, puis `dropDuplicates()`),
#    et mettez son nombre de lignes dans `nb_propres`.
#
# *Indice* : `trajets_q.count() - trajets_q.dropDuplicates().count()`.
#
# Le TP 3 traite aussi les stations d'arrivée et les types vides : ici, deux
# défauts suffisent pour le geste.

# %%
nb_duree_ko = None  # TODO
nb_doublons = None  # TODO
propres = None  # TODO
nb_propres = None  # TODO

# %%
verifier("durées nulles ou négatives", nb_duree_ko, 300)
verifier("doublons exacts", nb_doublons, 300)
verifier("lignes propres", nb_propres, 299700)

# %% [markdown]
# ## Exercice 3.8 — La station d'arrivée préférée de chaque quartier  ★★★
#
# **Ce que vous apprenez** : une fonction de fenêtre numérote les lignes
# **à l'intérieur de chaque groupe**, sans fusionner les lignes comme le fait
# `groupBy`.
#
# 1. À partir de `propres` et de `stations`, comptez les trajets par quartier
#    **d'arrivée** et par `station_arrivee` (une nouvelle jointure, cette fois
#    sur `station_arrivee` ; les stations d'arrivée vides disparaissent).
# 2. Numérotez les stations de chaque quartier d'arrivée, de la plus fréquentée
#    à la moins fréquentée (à égalité, par `station_arrivee`), avec `F.row_number`.
# 3. Gardez le numéro 1 : une ligne par quartier, dans `preferees`.
# 4. Mettez dans `lignes_preferees` la liste des tuples
#    `(quartier, station_arrivee, count)`, triée par quartier.
#
# *Indice* : `Window.partitionBy("quartier").orderBy(F.desc("count"), "station_arrivee")`.
#
# *Pour aller plus loin* : remplacez `F.row_number` par `F.rank`. Quand deux
# stations sont à égalité, combien de lignes obtient-on pour leur quartier ?

# %%
arrivees = None  # TODO : jointure sur station_arrivee, puis groupBy(...).count()
fenetre = None  # TODO
preferees = None  # TODO
lignes_preferees = None  # TODO

# %%
verifier("station d'arrivée préférée par quartier", lignes_preferees,
         [("Affaires", "S003", 9089), ("Berges", "S006", 9686),
          ("Centre", "S001", 9333), ("Faubourg", "S008", 9043),
          ("Gare", "S002", 9172), ("Parc", "S005", 9688),
          ("Université", "S004", 9283), ("Vieille Ville", "S007", 9387)])

# %% [markdown]
# ## Exercice 3.9 — Écrire en Parquet, relire, recompter  ★★★
#
# **Ce que vous apprenez** : Parquet garde le schéma avec les données ; on le
# relit sans rien déclarer, et on vérifie qu'on retrouve toutes ses lignes.
#
# 1. Écrivez `propres` en Parquet dans `os.path.join(SORTIES, "serie3-trajets")`,
#    en mode `overwrite`.
# 2. Relisez ce dossier dans `relus`.
# 3. Mettez dans `nb_relus` son nombre de lignes, et dans `type_debut_relu`
#    le type de la colonne `debut` après relecture.
#
# *Indice* : `df.write.mode(...).parquet(chemin)`, puis `spark.read.parquet(chemin)`.
#
# *Pour aller plus loin* : comptez les fichiers `.parquet` du dossier
# (`os.listdir`). D'où vient ce nombre ?

# %%
cible = os.path.join(SORTIES, "serie3-trajets")
# TODO : écrire propres en Parquet dans cible
relus = None  # TODO
nb_relus = None  # TODO
type_debut_relu = None  # TODO

# %%
verifier("lignes relues", nb_relus, 299700)
verifier("type de debut après relecture", type_debut_relu, "timestamp")

# %% [markdown]
# ## Ce que vous avez appris
#
# - Un schéma DDL déclaré évite de relire le fichier et fixe les types.
# - `withColumn` et `F.when` ajoutent des colonnes ; `groupBy().agg()` résume.
# - `join` rapproche deux tables ; la même question s'écrit en SQL sur une vue.
# - On compte les défauts avant de les retirer.
# - Une fenêtre (`Window.partitionBy().orderBy()`) classe dans chaque groupe.
# - Parquet conserve schéma et données : on relit, on recompte.
#
# **Suite** : le TP 3 (`tp3-sql-dataframes/`) combine ces gestes : nettoyage
# complet, jointure avec la météo, quatre questions métier en DataFrame et en
# SQL, et la table Parquet dont repartent les TP suivants.

# %%
spark.stop()
