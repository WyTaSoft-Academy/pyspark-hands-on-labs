"""
Série 5 — Analyser en temps réel (module 5, Structured Streaming) : notebook de départ.

    python depart.py

Complétez les cellules marquées TODO. Se lance tel quel, en script ou en notebook (format « percent »). Les données
du flux sont quelques trajets écrits à la main par la série elle-même : tout le
monde obtient les mêmes chiffres. Durée d'exécution : une à trois minutes.
"""

# %% [markdown]
# # Série 5 · Analyser en temps réel
#
# Module 5 · Structured Streaming · environ 1 heure · 8 exercices.
#
# À la fin de la série, vous saurez lire un dossier **en flux**, lancer une
# requête continue vers une table en mémoire, la voir se mettre à jour quand
# un fichier arrive, choisir le mode de sortie, compter par tranches de
# 5 minutes sur l'heure de l'événement, tolérer (ou non) les retardataires
# avec un watermark, et arrêter proprement toutes les requêtes.
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
         .appName("exercices-serie5")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.showConsoleProgress", "false")
         # Les heures des trajets sont lues et affichées en UTC : ainsi, tout
         # le monde voit les mêmes heures, quel que soit le fuseau du poste.
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# %% [markdown]
# ## Les données de la série (cellule fournie)
#
# Pas d'émetteur ici : la cellule ci-dessous écrit elle-même **dix trajets**
# choisis à la main, du lundi 2 mars 2026 entre 8 h et 8 h 25, dans deux
# petits fichiers JSON (un trajet par ligne) du dossier `ENTREE`. Tout le
# monde a donc exactement les mêmes chiffres.
#
# | trajet | station | fin | durée | usager | électrique |
# |---|---|---|---|---|---|
# | T001 | S001 | 08:01 | 12,0 | abonne | 0 |
# | T002 | S002 | 08:03 | 8,5 | occasionnel | 1 |
# | T003 | S001 | 08:04 | 15,0 | abonne | 1 |
# | T004 | S003 | 08:06 | 6,0 | abonne | 0 |
# | T005 | S002 | 08:09 | 22,0 | occasionnel | 0 |
# | T006 | S001 | 08:11 | 9,0 | abonne | 1 |
# | T007 | S003 | 08:13 | 11,5 | abonne | 0 |
# | T008 | S001 | 08:16 | 7,0 | occasionnel | 1 |
# | T009 | S002 | 08:18 | 18,0 | abonne | 0 |
# | T010 | S001 | 08:22 | 13,0 | occasionnel | 1 |
#
# Deux fonctions servent dans toute la série :
#
# - `deposer(dossier, nom, trajets)` dépose un fichier dans un dossier, comme
#   le ferait l'émetteur du TP 5 (écrit sous un nom caché, puis renommé : Spark
#   ne voit jamais un fichier à moitié écrit) ;
# - `reprise(nom)` rend un dossier de **point de reprise** (checkpoint) vide,
#   sous `SORTIES`. Chaque requête en flux y note où elle en est ; le vider à
#   chaque lancement permet de réexécuter une cellule autant de fois que voulu.

# %%
import json
import shutil
import time

SERIE = os.path.join(SORTIES, "serie5")
ENTREE = os.path.join(SERIE, "entree")
shutil.rmtree(SERIE, ignore_errors=True)  # on repart de zéro à chaque fois

# Le schéma des trajets du flux, en DDL.
SCHEMA = ("trajet_id STRING, station_depart STRING, fin TIMESTAMP, "
          "duree_min DOUBLE, type_usager STRING, electrique INT")


def trajet(trajet_id, station, fin, duree, usager, electrique):
    """Un trajet du lundi 2 mars 2026 ; fin au format HH:MM."""
    return {"trajet_id": trajet_id, "station_depart": station,
            "fin": f"2026-03-02T{fin}:00", "duree_min": duree,
            "type_usager": usager, "electrique": electrique}


LOT_1 = [trajet("T001", "S001", "08:01", 12.0, "abonne", 0),
         trajet("T002", "S002", "08:03", 8.5, "occasionnel", 1),
         trajet("T003", "S001", "08:04", 15.0, "abonne", 1),
         trajet("T004", "S003", "08:06", 6.0, "abonne", 0),
         trajet("T005", "S002", "08:09", 22.0, "occasionnel", 0)]
LOT_2 = [trajet("T006", "S001", "08:11", 9.0, "abonne", 1),
         trajet("T007", "S003", "08:13", 11.5, "abonne", 0),
         trajet("T008", "S001", "08:16", 7.0, "occasionnel", 1),
         trajet("T009", "S002", "08:18", 18.0, "abonne", 0),
         trajet("T010", "S001", "08:22", 13.0, "occasionnel", 1)]


def deposer(dossier, nom, trajets):
    """Dépose un fichier JSON (un trajet par ligne) dans le dossier."""
    os.makedirs(dossier, exist_ok=True)
    cache = os.path.join(dossier, "." + nom)  # Spark ignore les noms en « . »
    with open(cache, "w", encoding="utf-8") as f:
        for t in trajets:
            f.write(json.dumps(t) + "\n")
    os.replace(cache, os.path.join(dossier, nom))


def reprise(nom):
    """Un dossier de point de reprise vide, sous SORTIES."""
    dossier = os.path.join(SERIE, "reprises", nom)
    shutil.rmtree(dossier, ignore_errors=True)
    return dossier


deposer(ENTREE, "lot-01.json", LOT_1)
deposer(ENTREE, "lot-02.json", LOT_2)
print(sorted(os.listdir(ENTREE)))

# %% [markdown]
# ## Exercice 5.1 — Lot ou flux ?  ★
#
# **Ce que vous apprenez** : `isStreaming` dit si un DataFrame est une table
# figée (lecture en lot) ou un flux sans fin.
#
# 1. Lisez le dossier `ENTREE` **en lot** : `spark.read.json(ENTREE)`, dans
#    `lot`.
# 2. Lisez le même dossier **en flux** : `spark.readStream.schema(SCHEMA).json(ENTREE)`,
#    dans `flux`.
# 3. Rangez `lot.isStreaming` dans `lot_en_flux` et `flux.isStreaming` dans
#    `flux_en_flux`.
#
# *Indice* : les deux lectures ne diffèrent que d'un mot, `read` ou
# `readStream` (plus le schéma, voir l'exercice suivant).

# %%
lot = None   # TODO : lecture en lot du dossier ENTREE
flux = None  # TODO : lecture en flux, avec le schéma SCHEMA

lot_en_flux = None   # TODO
flux_en_flux = None  # TODO

# %%
verifier("la lecture en lot est-elle un flux ?", lot_en_flux, False)
verifier("la lecture en flux est-elle un flux ?", flux_en_flux, True)

# %% [markdown]
# ## Exercice 5.2 — Un flux exige un schéma  ★
#
# **Ce que vous apprenez** : une source fichier en flux ne devine pas le
# schéma : il faut le lui donner.
#
# La cellule suivante (fournie) tente deux choses interdites et affiche le
# message d'erreur de Spark : lire en flux **sans** schéma, puis compter un
# flux avec `count()`. Lisez les deux messages.
#
# 1. Rangez dans `colonnes` la liste des colonnes de `flux` (`flux.columns`).
# 2. Rangez dans `type_fin` le type de la colonne `fin` : `dict(flux.dtypes)["fin"]`.
#
# *Indice* : le type vient du schéma `SCHEMA` que vous avez donné, pas des
# fichiers ; en lot, sans schéma, `spark.read.json` aurait lu `fin` comme une
# simple chaîne.

# %%
try:
    spark.readStream.json(ENTREE)
except Exception as e:
    print("Sans schéma  :", str(e).splitlines()[0][:110])
try:
    spark.readStream.schema(SCHEMA).json(ENTREE).count()
except Exception as e:
    print("Avec count() :", str(e).splitlines()[0][:110])
print("En lot, sans schéma, fin est lue comme :", dict(spark.read.json(ENTREE).dtypes)["fin"])

# %%
colonnes = None  # TODO
type_fin = None  # TODO

# %%
verifier("colonnes du flux", colonnes,
         ["trajet_id", "station_depart", "fin", "duree_min", "type_usager", "electrique"])
verifier("type de la colonne fin", type_fin, "timestamp")

# %% [markdown]
# ## Exercice 5.3 — Une première requête continue  ★
#
# **Ce que vous apprenez** : `writeStream ... start()` lance une requête qui
# tourne en arrière-plan ; le puits `memory` expose son résultat comme une
# table que l'on interroge en SQL.
#
# 1. Comptez les trajets par `type_usager` : `flux.groupBy("type_usager").count()`.
# 2. Lancez la requête dans `q_usagers` (code presque complet dans l'indice).
# 3. Appelez `q_usagers.processAllAvailable()` : cette méthode attend que tous
#    les fichiers déjà présents soient traités.
# 4. Lisez le compte des abonnés dans `nb_abonnes` avec `spark.sql`.
# 5. Arrêtez la requête : `q_usagers.stop()`. Une requête oubliée tourne
#    jusqu'à la fin de la session.
#
# *Indice* :
#
# ```python
# q_usagers = (par_usager.writeStream
#              .outputMode("complete")          # toute la table à chaque lot
#              .format("memory")
#              .queryName("usagers")            # le nom de la table
#              .option("checkpointLocation", reprise("usagers"))
#              .start())
# ```
#
# puis `spark.sql("SELECT count FROM usagers WHERE type_usager = 'abonne'").first()["count"]`.

# %%
par_usager = None  # TODO : compte par type_usager
q_usagers = None   # TODO : lancer la requête, puis processAllAvailable()
nb_abonnes = None  # TODO : lire le compte des abonnés avec spark.sql
# TODO : arrêter la requête

# %%
verifier("trajets d'abonnés", nb_abonnes, 6)
verifier("la requête tourne-t-elle encore ?",
         q_usagers.isActive if q_usagers is not None else None, False)

# %% [markdown]
# ## Exercice 5.4 — Un fichier arrive, le compte augmente  ★★
#
# **Ce que vous apprenez** : une requête en flux traite **chaque nouveau
# fichier** qui arrive dans le dossier, sans être relancée.
#
# La cellule de préparation (fournie) crée un dossier `ENTREE_5_4` avec les
# deux fichiers de départ. Réexécutez-la si vous voulez recommencer
# l'exercice.
#
# 1. Lisez `ENTREE_5_4` en flux, comptez les trajets par `station_depart`,
#    et lancez la requête `q_stations` (mode `complete`, puits `memory`,
#    table `stations_5_4`).
# 2. `processAllAvailable()`, puis rangez le **total** des trajets dans
#    `total_avant` : `SELECT sum(count) AS n FROM stations_5_4`.
# 3. Exécutez la cellule fournie qui dépose `lot-03.json` (trois trajets).
# 4. Sans relancer la requête : `processAllAvailable()`, puis `total_apres`,
#    et le compte de la station `S003` dans `compte_s003`.
# 5. Arrêtez la requête.
#
# *Indice* : même forme de requête qu'à l'exercice 5.3 ;
# `spark.sql("SELECT sum(count) AS n FROM stations_5_4").first()["n"]`.

# %%
ENTREE_5_4 = os.path.join(SERIE, "entree-5-4")
shutil.rmtree(ENTREE_5_4, ignore_errors=True)
deposer(ENTREE_5_4, "lot-01.json", LOT_1)
deposer(ENTREE_5_4, "lot-02.json", LOT_2)

# %%
flux_5_4 = None     # TODO : lire ENTREE_5_4 en flux
q_stations = None   # TODO : lancer la requête (table stations_5_4), puis processAllAvailable()
total_avant = None  # TODO

# %%
# Cellule fournie : trois trajets arrivent pendant que la requête tourne.
deposer(ENTREE_5_4, "lot-03.json",
        [trajet("T011", "S003", "08:24", 10.0, "abonne", 0),
         trajet("T012", "S003", "08:26", 5.5, "occasionnel", 1),
         trajet("T013", "S001", "08:27", 16.0, "abonne", 0)])

# %%
# TODO : processAllAvailable() sur q_stations
total_apres = None  # TODO
compte_s003 = None  # TODO
# TODO : arrêter la requête

# %%
verifier("trajets vus avant le nouveau fichier", total_avant, 10)
verifier("trajets vus après le nouveau fichier", total_apres, 13)
verifier("trajets partis de S003", compte_s003, 4)

# %% [markdown]
# ## Exercice 5.5 — Le mode append : seulement les nouvelles lignes  ★★
#
# **Ce que vous apprenez** : sans agrégation, chaque ligne est définitive dès
# qu'elle arrive ; le mode `append` n'écrit que les lignes nouvelles.
#
# 1. À partir de `flux`, gardez les trajets en vélo électrique
#    (`electrique == 1`) et sélectionnez `trajet_id`, `fin` et `duree_min`.
# 2. Lancez la requête `q_electriques` en mode `append`, puis `memory`, table
#    `electriques`.
# 3. `processAllAvailable()`, puis rangez dans `ids_electriques` la liste
#    **triée** des `trajet_id` de la table.
# 4. Arrêtez la requête.
#
# *Indice* : `[r["trajet_id"] for r in spark.sql("SELECT trajet_id FROM electriques ORDER BY trajet_id").collect()]`.
# Essayez aussi `outputMode("complete")` sur cette requête : Spark refuse,
# car sans agrégation il n'y a pas de « table entière » à réécrire.

# %%
electriques = None      # TODO : filtre + select
q_electriques = None    # TODO : mode append, table electriques, puis processAllAvailable()
ids_electriques = None  # TODO : liste triée des trajet_id
# TODO : arrêter la requête

# %%
verifier("trajets électriques", ids_electriques,
         ["T002", "T003", "T006", "T008", "T010"])

# %% [markdown]
# ## Exercice 5.6 — Compter par tranches de 5 minutes  ★★
#
# **Ce que vous apprenez** : `F.window("fin", "5 minutes")` range chaque trajet
# dans une tranche de 5 minutes selon l'heure de **l'événement** (la fin du
# trajet), et non selon l'heure où Spark le reçoit.
#
# 1. À partir de `flux`, groupez par `F.window("fin", "5 minutes")` et comptez.
# 2. Lancez la requête `q_tranches` (mode `complete`, `memory`, table
#    `tranches`), puis `processAllAvailable()`.
# 3. La colonne `window` est une structure à deux champs, `start` et `end`.
#    Rangez dans `par_tranche` la liste des couples (début de tranche au
#    format `HH:mm`, nombre de trajets), triée par heure.
# 4. Arrêtez la requête.
#
# *Indice* :
#
# ```python
# lignes = spark.sql("""SELECT date_format(window.start, 'HH:mm') AS debut, count
#                       FROM tranches ORDER BY debut""").collect()
# par_tranche = [(r["debut"], r["count"]) for r in lignes]
# ```

# %%
q_tranches = None   # TODO : compte par F.window("fin", "5 minutes"), table tranches
par_tranche = None  # TODO : liste de couples (début HH:mm, nombre de trajets)
# TODO : arrêter la requête

# %%
verifier("trajets par tranche de 5 minutes", par_tranche,
         [("08:00", 3), ("08:05", 2), ("08:10", 2), ("08:15", 2), ("08:20", 1)])

# %% [markdown]
# ## Exercice 5.7 — Plusieurs requêtes à la fois, et tout arrêter  ★★
#
# **Ce que vous apprenez** : `spark.streams.active` liste les requêtes qui
# tournent ; on les arrête toutes d'une boucle.
#
# La source `rate` fabrique des lignes toutes seules (un compteur `value` et
# un `timestamp`) et **ne s'arrête jamais** : `processAllAvailable()` ne
# rendrait jamais la main sur elle. Le puits `console` affiche chaque lot.
# Dans un notebook, cet affichage part dans le **terminal** qui a lancé
# Jupyter, pas sous la cellule.
#
# 1. Lancez `q_horloge` : source `rate` à 2 lignes par seconde, puits
#    `console`, nom `horloge` (code dans l'indice).
# 2. Lancez `q_tranches_bis` : la même requête qu'à l'exercice 5.6, table
#    `tranches_bis`.
# 3. Attendez quelques secondes (`time.sleep(4)`), puis rangez dans
#    `noms_actifs` la liste **triée** des noms des requêtes actives
#    (`q.name` pour chaque `q` de `spark.streams.active`).
# 4. Arrêtez-les toutes avec une boucle `for`, puis rangez dans `nb_actives`
#    le nombre de requêtes encore actives.
#
# *Indice* :
#
# ```python
# q_horloge = (spark.readStream.format("rate").option("rowsPerSecond", 2).load()
#              .writeStream.format("console").queryName("horloge")
#              .option("checkpointLocation", reprise("horloge"))
#              .start())
# ```
#
# puis `for q in spark.streams.active: q.stop()`.
#
# Des lignes `ERROR … aborting` ou `Aborting task` peuvent s'afficher à l'arrêt :
# `stop()` interrompt le lot en cours. Elles sont sans conséquence.

# %%
q_horloge = None       # TODO : source rate, puits console, nom horloge
q_tranches_bis = None  # TODO : comme à l'exercice 5.6, table tranches_bis
# TODO : time.sleep(4)
noms_actifs = None     # TODO : noms triés des requêtes de spark.streams.active

# TODO : tout arrêter avec une boucle for
nb_actives = None      # TODO

# %%
verifier("requêtes actives avant l'arrêt", noms_actifs, ["horloge", "tranches_bis"])
verifier("requêtes actives après l'arrêt", nb_actives, 0)

# %% [markdown]
# ## Exercice 5.8 — Watermark : le retardataire accepté et le retardataire écarté  ★★★
#
# **Ce que vous apprenez** : `withWatermark` fixe combien de retard on
# tolère ; au-delà, une ligne en retard est écartée.
#
# La règle, exacte :
#
# - le **watermark** vaut l'heure d'événement la plus récente déjà vue, moins
#   le délai toléré. Il est calculé à la fin d'un lot et sert au lot suivant ;
# - une ligne en retard est **écartée** quand la **fin de sa tranche** est
#   inférieure ou égale au watermark : sa tranche est close. Sinon, elle est
#   comptée, même si sa propre heure est déjà passée sous le watermark.
#
# Scénario : le dossier `ENTREE_5_8` contient les dix trajets de départ (le
# plus récent finit à 08:22). On tolère 10 minutes de retard. Arrivent ensuite
# deux retardataires : R001 fini à 08:11, et R002 fini à 08:08.
#
# Avant de coder, prédisez : que vaudra le watermark après le premier lot ?
# Lequel des deux retardataires sera compté ?
#
# 1. Lisez `ENTREE_5_8` en flux ; ajoutez un watermark de 10 minutes sur `fin`
#    **avant** le `groupBy` ; comptez par tranche de 5 minutes.
# 2. Lancez `q_retards` en mode `update`, `memory`, table `retards`.
#    `processAllAvailable()`, puis rangez le watermark dans `watermark`
#    (voir l'indice).
# 3. Exécutez la cellule fournie qui dépose les retardataires.
# 4. `processAllAvailable()`, puis rangez :
#    - dans `nb_ecartes`, le nombre de lignes écartées par le watermark ;
#    - dans `compte_0805` et `compte_0810`, le nombre de trajets **final** des
#      tranches 08:05 et 08:10.
# 5. Arrêtez la requête.
#
# *Indice* : `q_retards.lastProgress["eventTime"]["watermark"]` (une chaîne en
# UTC). Le nombre de lignes écartées figure dans chaque rapport de lot :
# `p["stateOperators"][0]["numRowsDroppedByWatermark"]` ; additionnez-le sur
# `q_retards.recentProgress`. En mode `update`, le puits `memory` **ajoute**
# à chaque lot les tranches modifiées : une tranche peut donc y figurer
# plusieurs fois, et son compte final est le plus grand.
#
# *Pour aller plus loin* : relancez avec un watermark de 15 minutes. Que
# devient `nb_ecartes` ? Et avec le mode `append`, quelles tranches la table
# contient-elle après le premier lot ?

# %%
ENTREE_5_8 = os.path.join(SERIE, "entree-5-8")
shutil.rmtree(ENTREE_5_8, ignore_errors=True)
deposer(ENTREE_5_8, "lot-01.json", LOT_1)
deposer(ENTREE_5_8, "lot-02.json", LOT_2)

# %%
tolerants = None  # TODO : lecture de ENTREE_5_8, watermark, groupBy par tranche, count
q_retards = None  # TODO : mode update, table retards, puis processAllAvailable()
watermark = None  # TODO

# %%
# Cellule fournie : deux retardataires arrivent.
deposer(ENTREE_5_8, "lot-retards.json",
        [trajet("R001", "S002", "08:11", 7.5, "abonne", 0),        # tranche 08:10-08:15
         trajet("R002", "S003", "08:08", 12.0, "occasionnel", 1)])  # tranche 08:05-08:10

# %%
# TODO : processAllAvailable() sur q_retards
nb_ecartes = None   # TODO
compte_0805 = None  # TODO
compte_0810 = None  # TODO
# TODO : arrêter la requête

# %%
verifier("watermark après le premier lot", watermark, "2026-03-02T08:12:00.000Z")
verifier("lignes écartées par le watermark", nb_ecartes, 1)
verifier("trajets de la tranche 08:05 (R002 écarté)", compte_0805, 2)
verifier("trajets de la tranche 08:10 (R001 compté)", compte_0810, 3)

# %% [markdown]
# ## Ce que vous avez appris
#
# - `readStream` au lieu de `read`, avec un **schéma** obligatoire ;
#   `isStreaming` pour savoir à quoi l'on a affaire ;
# - `writeStream ... start()` lance une requête continue ; le puits `memory`
#   se lit en SQL ; `processAllAvailable()` attend la fin des fichiers
#   présents ; un fichier qui arrive est traité sans relancer la requête ;
# - les modes de sortie : `complete` pour une agrégation réécrite en entier,
#   `append` pour des lignes définitives, `update` pour les seules lignes
#   modifiées ;
# - `F.window` compte par tranche sur l'heure de l'événement, et
#   `withWatermark` écarte une ligne dont la tranche est déjà close ;
# - `spark.streams.active` et `q.stop()` : on arrête toujours ses requêtes.
#
# Au **TP 5** (dossier `tp5-streaming`), le flux vient de l'émetteur, sans
# fin : vous combinerez ces gestes avec une jointure sur les stations, le
# modèle du TP 4, et la reprise d'une requête arrêtée sur son point de reprise.

# %%
spark.stop()
