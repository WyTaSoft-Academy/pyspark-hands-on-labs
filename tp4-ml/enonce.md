# TP 4 — Classification supervisée : abonné ou occasionnel ?

**Module 4 · Machine learning avec MLlib** · ≈ 1 h 30, dont 15 min de restitution · seul ou en binôme

## Objectif

Un trajet Vélo'Cité vient d'avoir lieu. À partir de ce qu'on en sait, peut-on deviner
si c'est un **abonné** ou un **occasionnel** ?

Vous allez entraîner et comparer quatre modèles, les évaluer honnêtement, et
**sauvegarder le meilleur** : le TP 5 l'appliquera demain à un flux de trajets en temps réel.

## Fichiers

| Fichier | Rôle |
|---|---|
| `depart.ipynb` | Le notebook de départ : une cellule `TODO` par étape |
| `depart.py` | Le même, en script (format « percent », lisible par VS Code et PyCharm) |

**Entrée** : `sorties/trajets-propres` (Parquet, écrit par le TP 3). S'il n'existe pas, la
première cellule reconstruit les trajets depuis `donnees/trajets.csv` : rien à faire de votre côté.
**Sortie** : `sorties/modele-type-usager`.

## Étapes

### 1. Charger les trajets, joindre le quartier, poser la cible

- Joindre les trajets à `stations.csv` pour obtenir le `quartier` de la station **de départ**.
  Renommer `station_id` en `station_depart` avant la jointure évite une colonne en double.
- Ajouter `label` : `1.0` pour un occasionnel, `0.0` pour un abonné.

Attendu : 189 723 abonnés, 109 528 occasionnels.

### 2. Découper, et mesurer la référence naïve

- Un trajet va dans le test si `F.pmod(F.hash("trajet_id"), 5) == 0` : 20 % des trajets,
  toujours les mêmes. Mettre les deux parties en `cache()`.
- Calculer l'exactitude obtenue en répondant **toujours « abonné »** sur le test.

Attendu : 238 961 trajets d'entraînement, 60 290 de test, référence **0,637**.
C'est le score à battre.

> Pourquoi pas `randomSplit([0.8, 0.2], seed=42)` ? Il tire au hasard *partition par partition* :
> la graine ne suffit pas, le découpage change dès que les données sont partitionnées autrement
> (autre nombre de cœurs, Parquet réécrit). Le hachage de l'identifiant donne le même découpage
> sur tous les postes, donc les mêmes chiffres que cet énoncé.

### 3. Un modèle « au retrait » : régression logistique

On veut deviner le type d'usager **au moment où le vélo est retiré** : on ne connaît encore
ni la durée ni la distance. Variables :

```python
AU_RETRAIT = ["heure", "jour_semaine", "week_end", "electrique", "quartier_vec"]
```

Pipeline : `StringIndexer` (quartier, `handleInvalid="keep"`) → `OneHotEncoder` →
`VectorAssembler` (vers `brutes`) → `StandardScaler` (vers `features`) → `LogisticRegression`.

### 4. Évaluer

- AUC avec `BinaryClassificationEvaluator`, exactitude avec `MulticlassClassificationEvaluator`.
- Matrice de confusion : `groupBy("label").pivot("prediction", [0.0, 1.0]).count()`.

Attendu : AUC **0,706**, exactitude **0,677**.

| | prédit abonné | prédit occasionnel |
|---|---|---|
| **vrai abonné** | 32 178 | 6 218 |
| **vrai occasionnel** | 13 284 | 8 610 |

Question : quelle part des occasionnels le modèle manque-t-il ? *(≈ 61 %.)*

### 5. Une forêt aléatoire « au retrait », et ce qu'elle regarde

- Le même pipeline **sans** `StandardScaler`, terminé par
  `RandomForestClassifier(featuresCol="brutes", numTrees=30, maxDepth=8, seed=42)`.
- Lire les importances : `modele.stages[-1].featureImportances`, et les noms des variables dans
  les métadonnées de la colonne `brutes` (`df.schema["brutes"].metadata["ml_attr"]["attrs"]`).

Attendu : AUC **0,809**, exactitude **0,742**. Variable dominante : `heure` (≈ 0,58), puis `electrique` et `week_end`.

Question : pourquoi la forêt fait-elle nettement mieux que la logistique ici ?
*(L'heure ne pousse pas toujours dans le même sens : 8 h et 18 h sont des heures d'abonnés,
15 h une heure d'occasionnels. Un arbre la découpe en tranches ; la logistique ne le peut pas.)*

### 6. Un modèle « au dépôt », et on sauvegarde le meilleur

Au dépôt, la durée et la distance sont connues :

```python
AU_DEPOT = AU_RETRAIT + ["duree_min", "distance_km"]
```

- Réentraîner la logistique et la forêt avec ces colonnes, les évaluer.
- Sauvegarder la meilleure dans `sorties/modele-type-usager` (`write().overwrite().save(...)`),
  la recharger avec `PipelineModel.load` et vérifier le score.

Attendu : logistique AUC **0,992** (exactitude 0,966), forêt AUC **0,985** (exactitude 0,943).
C'est la **logistique** qu'on sauvegarde.

## Le contrat du modèle sauvegardé

Le TP 5 applique ce modèle à un flux. Pour prédire, un DataFrame doit porter ces colonnes :

| Colonne | Type | D'où elle vient |
|---|---|---|
| `heure` | int | `hour(debut)` |
| `jour_semaine` | int | `dayofweek(debut)` : 1 = dimanche, 7 = samedi |
| `week_end` | boolean | `jour_semaine` vaut 1 ou 7 |
| `electrique` | int | 0 ou 1 |
| `quartier` | string | **jointure** sur `stations.csv`, par la station de départ |
| `duree_min` | double | minutes |
| `distance_km` | double | kilomètres |

Ni `type_usager`, ni `label`, ni `tarif_eur` : le modèle ne s'en sert pas. Il ajoute les colonnes
`probability` (P(abonné), P(occasionnel)) et `prediction` (1.0 = occasionnel).

## Bonus

Les bonus 1 et 2 sont corrigés en séance ; les deux autres sont laissés à faire.

1. **Une colonne oubliée.** Il reste une colonne *numérique* des trajets qu'on n'a pas utilisée.
   Ajoutez-la au modèle « au retrait ». Que devient l'AUC ? Faut-il la garder ?
   *(C'est `tarif_eur` : l'AUC passe de 0,706 à 1,000. Le tarif est calculé d'après le type
   d'usager — c'est une **fuite de cible**. On ne la garde jamais.)*
2. **Rattraper le déséquilibre.** Le modèle « au retrait » manque 61 % des occasionnels.
   Mesurez d'abord son rappel et sa précision sur cette classe
   (`MulticlassClassificationEvaluator(metricName="recallByLabel", metricLabel=1.0)`,
   puis `"precisionByLabel"`). Ajoutez ensuite une colonne `poids` valant la part de la
   classe **opposée** — `1 - part_occ` pour les occasionnels, `part_occ` pour les abonnés —
   et réentraînez la logistique avec `weightCol="poids"`.
   *(Rappel 0,393 → **0,637**, précision 0,581 → 0,504, AUC inchangée à 0,706 : on n'a pas
   un meilleur modèle, on a déplacé le curseur entre les deux erreurs.)*
3. **Le seuil.** `LogisticRegression(threshold=0.3)` : que deviennent les occasionnels manqués,
   et les fausses alertes ? *(Avec `threshold=0.367`, la part d'occasionnels, on retrouve
   presque le résultat de la pondération : rappel 0,630, précision 0,498.)*
4. **La régression.** Prédire `duree_min` avec `LinearRegression` à partir de `distance_km`,
   `electrique` et du type d'usager. Attendu : RMSE ≈ 7,7 min, R² ≈ 0,51.

## Pièges fréquents

- `IllegalArgumentException: features does not exist` : l'assembleur ou le normaliseur manque
  dans le pipeline, ou les noms de colonnes ne s'enchaînent pas.
- `Column quartier not found` : la jointure avec `stations.csv` a été oubliée.
- Des chiffres légèrement différents à la 3ᵉ décimale : normal pour la forêt, dont le tirage des
  arbres dépend du nombre de partitions (donc de cœurs) ; normal aussi si votre TP 3 a nettoyé
  un peu autrement.
- Sous Windows, `PipelineModel.load` échoue en `UnsatisfiedLinkError` : le `hadoop.dll` et le
  `winutils.exe` de `HADOOP_HOME` ne correspondent pas au Hadoop 3.3 embarqué par Spark 3.5.
  Installer ceux d'un Hadoop 3.3.x (voir le TP 1).
