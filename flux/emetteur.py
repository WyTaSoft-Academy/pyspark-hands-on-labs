"""
Émetteur de flux du module 5 : rejoue les trajets Vélo'Cité comme s'ils
arrivaient maintenant.

    python emetteur.py                          20 trajets/s, sans fin
    python emetteur.py --debit 50 --duree 120   50 trajets/s pendant 2 minutes
    python emetteur.py --retard 0.10            10 % d'événements en retard
    python emetteur.py --vider                  vide le dossier d'entrée avant

Chaque intervalle, un petit fichier JSON (un trajet par ligne) est déposé dans
flux/entree/. C'est la source « fichier » de Structured Streaming : elle
surveille le dossier et lit chaque nouveau fichier une fois, et une seule.

L'heure de l'événement est réécrite : un trajet se termine « maintenant »
(colonne fin), et son début est recalé d'autant. Une part des trajets est
émise EN RETARD : leur fin est antérieure de 2 à 15 minutes à l'heure
d'émission, comme un vélo déposé pendant une coupure réseau et remonté plus
tard. C'est ce qui rend le watermark nécessaire.

Écriture atomique : le fichier est écrit sous un nom caché (.tmp), puis
renommé. Spark ne voit donc jamais un fichier à moitié écrit.

Bibliothèque standard uniquement. Le script tourne sans Spark.
"""
import argparse
import csv
import json
import os
import random
import time
from datetime import datetime, timedelta

ICI = os.path.dirname(os.path.abspath(__file__))
TRAJETS = os.path.join(os.path.dirname(ICI), "donnees", "trajets.csv")
FORMAT = "%Y-%m-%d %H:%M:%S"


def trajets_propres(chemin, rng):
    """Parcourt trajets.csv en boucle, en sautant les lignes sales et en
    tirant un trajet sur cinq : le flux ne rejoue pas l'ordre du fichier."""
    while True:
        with open(chemin, encoding="utf-8", newline="") as f:
            for ligne in csv.DictReader(f):
                if not ligne["station_arrivee"] or not ligne["type_usager"]:
                    continue
                if float(ligne["duree_min"]) <= 0 or rng.random() > 0.2:
                    continue
                yield ligne


def evenement(ligne, fin):
    duree = float(ligne["duree_min"])
    debut = fin - timedelta(minutes=duree)
    return {
        "trajet_id": ligne["trajet_id"],
        "velo_id": ligne["velo_id"],
        "station_depart": ligne["station_depart"],
        "station_arrivee": ligne["station_arrivee"],
        "debut": debut.strftime(FORMAT),
        "fin": fin.strftime(FORMAT),
        "duree_min": duree,
        "distance_km": float(ligne["distance_km"]),
        "type_usager": ligne["type_usager"],
        "electrique": int(ligne["electrique"]),
        "tarif_eur": float(ligne["tarif_eur"]),
    }


def main():
    p = argparse.ArgumentParser(description="Rejoue les trajets Vélo'Cité en flux.")
    p.add_argument("--debit", type=float, default=20, help="trajets par seconde (défaut : 20)")
    p.add_argument("--intervalle", type=float, default=1.0, help="secondes entre deux fichiers (défaut : 1)")
    p.add_argument("--retard", type=float, default=0.03, help="part des événements en retard (défaut : 0.03)")
    p.add_argument("--duree", type=float, default=0, help="durée d'émission en secondes, 0 = sans fin")
    p.add_argument("--sortie", default=os.path.join(ICI, "entree"), help="dossier surveillé par Spark")
    p.add_argument("--graine", type=int, default=None, help="graine du tirage, pour rejouer à l'identique")
    p.add_argument("--vider", action="store_true", help="supprime les fichiers déjà présents")
    p.add_argument("--silencieux", action="store_true", help="n'affiche rien")
    args = p.parse_args()

    if not os.path.exists(TRAJETS):
        raise SystemExit("trajets.csv introuvable : lancez d'abord  python donnees/generer_donnees.py")
    os.makedirs(args.sortie, exist_ok=True)
    if args.vider:
        for nom in os.listdir(args.sortie):
            os.remove(os.path.join(args.sortie, nom))

    rng = random.Random(args.graine)
    source = trajets_propres(TRAJETS, rng)
    par_fichier = max(1, round(args.debit * args.intervalle))
    depart, n_fichiers, n_trajets, n_retard = time.time(), 0, 0, 0
    try:
        while not args.duree or time.time() - depart < args.duree:
            maintenant = datetime.now().replace(microsecond=0)
            lignes = []
            for _ in range(par_fichier):
                fin = maintenant
                if rng.random() < args.retard:
                    fin -= timedelta(seconds=rng.randint(120, 900))
                    n_retard += 1
                lignes.append(json.dumps(evenement(next(source), fin), ensure_ascii=False))
            n_fichiers += 1
            n_trajets += len(lignes)
            nom = f"lot-{maintenant:%Y%m%d-%H%M%S}-{n_fichiers:05d}.json"
            tmp = os.path.join(args.sortie, "." + nom + ".tmp")
            with open(tmp, "w", encoding="utf-8", newline="\n") as f:
                f.write("\n".join(lignes) + "\n")
            os.replace(tmp, os.path.join(args.sortie, nom))
            if not args.silencieux:
                print(f"\r{n_fichiers} fichiers · {n_trajets} trajets · {n_retard} en retard", end="", flush=True)
            # cadence fixe : on vise l'instant du prochain fichier, pas « intervalle après celui-ci »
            time.sleep(max(0.0, depart + n_fichiers * args.intervalle - time.time()))
    except KeyboardInterrupt:
        pass
    if not args.silencieux:
        print()


if __name__ == "__main__":
    main()
