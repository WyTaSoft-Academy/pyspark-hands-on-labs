# TP 2 · Analyser le journal brut des bornes

**Module 2 — Programmer avec les RDD** · ≈ 1 h 30, dont 15 min de restitution · seul ou en binôme

## Objectif

Le fichier `donnees/journal-bornes.log` est ce que la collecte reçoit des bornes
Vélo'Cité, avant tout nettoyage : une ligne de texte par événement. Vous allez
répondre à une question d'exploitation — **quelles bornes tombent le plus en
panne ?** — en n'utilisant que l'API RDD : pas de schéma, pas de colonnes,
des fonctions Python appliquées ligne à ligne.

## Avant de commencer

- Les données sont générées (TP 1) : `python donnees/generer_donnees.py` depuis `pyspark-hands-on-labs/`.
- Ouvrez `tp2-rdd/depart.ipynb` dans Jupyter, ou `depart.py` dans votre éditeur.
- Sous Windows, `PYSPARK_PYTHON` doit être positionné (voir TP 1).

## Le format du journal

Trois types d'événements, puis des paires `clé=valeur` :

```
2026-01-01T06:03:49 S027 RETRAIT velo=V0696 badge=A-37535
2026-01-01T06:10:59 S030 RETRAIT velo=V0169 badge=A-71688 batterie=47
2026-01-01T06:08:49 S050 DEPOT velo=V0696 duree=5.0
2026-01-01T08:42:28 S012 ERREUR code=E27 message="lecteur de badge muet"
```

- `batterie` n'apparaît que pour les vélos électriques ;
- le message d'erreur est entre guillemets et **contient des espaces** ;
- environ 0,2 % des lignes sont **tronquées** par des coupures réseau.

## Étapes

### 1. Charger et compter

Créez une `SparkSession` en `local[2]`, récupérez le `SparkContext`, lisez le
journal avec `sc.textFile`. Affichez le nombre de partitions et de lignes.

> **Sous Windows**, `lignes.take(3)` peut échouer avec *Python worker exited
> unexpectedly* : c'est un défaut connu de PySpark sous Windows quand une action
> s'arrête avant d'avoir lu une partition volumineuse, pas une erreur de votre
> code. Pour l'aperçu, utilisez `spark.read.text(chemin).show(3, truncate=False)`.
> Les actions qui lisent tout (`count`, `collect` après réduction) ne sont pas
> concernées.

### 2. Analyser chaque ligne, compter les invalides

Écrivez une fonction `analyser(ligne)` qui renvoie `[(horodatage, station, type, champs)]`
pour une ligne valide, et `[]` sinon — `champs` étant un dictionnaire des paires
`clé=valeur`. Appliquez-la avec `flatMap`.

Une ligne est valide si : horodatage de 19 caractères, station de la forme
`S` + trois chiffres, type connu, champs obligatoires présents (`velo` et `badge`
pour un retrait, `velo` et `duree` pour un dépôt, `code` et `message` pour une
erreur), guillemets appariés.

Comptez les lignes écartées avec un **accumulateur**. Appelez deux actions de
suite sur le RDD : que devient l'accumulateur ? Expliquez.

*Indice :* `ligne.split(" ", 3)` découpe au plus trois fois ; une expression
régulière comme `(\w+)=("[^"]*"|\S+)` lit ensuite les paires.

### 3. Mettre en cache, et mesurer

Mesurez avec `time.perf_counter()` la durée d'un `count()` sans cache, puis de
deux `count()` successifs sur le RDD mis en cache. Vérifiez qu'avec le cache,
l'accumulateur ne compte plus qu'une fois.

### 4. Événements par type

Par map/reduce : nombre d'événements `RETRAIT`, `DEPOT` et `ERREUR`.

### 5. Les dix stations les plus en panne

Comptez les erreurs par station, triez par nombre décroissant (à égalité, par
identifiant), et affichez les dix premières **avec leur nom**. Les noms sont dans
`donnees/stations.csv` : chargez-les en dictionnaire et diffusez-les avec
`sc.broadcast` plutôt que de faire une jointure.

### 6. Répartition des codes d'erreur

Nombre d'erreurs par couple `(code, message)`, trié par code.

### 7. Batterie moyenne au retrait

Moyenne du champ `batterie` sur les retraits de vélos électriques.
Attention : la moyenne n'est pas associative. Réduisez des couples
`(somme, effectif)` et divisez à la fin.

### 8. Écrire le résultat

Écrivez les dix stations dans `sorties/tp2-top-pannes`, une ligne
`identifiant;nom;nombre` par station, **en un seul fichier**. `saveAsTextFile`
refuse d'écraser un dossier existant : supprimez-le d'abord.

### 9. Soumettre le script

Enregistrez votre notebook en script (`.py`), et lancez-le depuis le dossier du TP :

```
spark-submit --master "local[2]" mon_script.py
```

## Résultats attendus

| Mesure | Valeur |
|---|---|
| Partitions du journal | 2 |
| Lignes | 608 411 |
| Événements valides | 607 165 |
| Lignes invalides | 1 246 (2 492 après une seconde action sans cache) |
| `RETRAIT` / `DEPOT` / `ERREUR` | 299 081 / 299 083 / 9 001 |
| Station la plus en panne | S016 Roseraie, 173 erreurs |
| Codes d'erreur | E12 2 275 · E27 2 159 · E42 2 258 · E51 2 309 |
| Retraits de vélos électriques | 112 427 |
| Batterie moyenne au retrait | 57,0 % |

Les durées dépendent de votre poste ; l'ordre de grandeur attendu est un second
`count()` sur le RDD en cache **nettement plus rapide** que sans cache (≈ 3 s contre
≈ 5 s sur le poste de préparation, et l'écart grandit avec le volume).

## Pour aller plus loin

- **Bonus 1.** Batterie moyenne **par station**, avec `aggregateByKey`.
- **Bonus 2.** Heure de la journée où les erreurs sont les plus fréquentes.
- **Bonus 3.** Nombre de vélos distincts retirés au moins une fois, avec `distinct()`.
  Transformation étroite ou large ?
- **Bonus 4.** Vérifiez que chaque `DEPOT` a un `RETRAIT` du même vélo avant lui :
  regroupez par vélo, triez ses événements par horodatage. Pourquoi est-ce l'un
  des rares cas où `groupByKey` se justifie ?
- **Bonus 5.** Refaites l'étape 5 **avec une jointure** au lieu du broadcast : chargez
  `stations.csv` en RDD de paires `(station_id, nom)` et joignez-le au comptage par
  station. Même résultat ; comparez le nombre d'étapes dans l'interface web (onglet
  *Stages*) et dans `toDebugString()`. Sur 60 stations, lequel préférez-vous, et à
  partir de quelle taille de table l'autre devient-il nécessaire ?

## Corrigé

Projeté en fin de TP.
