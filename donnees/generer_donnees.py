"""
Génère le jeu de données du fil rouge : Vélo'Cité, un réseau fictif de vélos
en libre-service.

    python generer_donnees.py                  jeu complet (≈ 300 000 trajets)
    python generer_donnees.py --trajets 20000  jeu réduit, pour un poste lent

Bibliothèque standard uniquement : le script tourne sans Spark, sans pandas,
et sans connexion — condition nécessaire sur un poste client verrouillé.
Le tirage est déterministe (graine fixe) : tous les participants ont les mêmes
chiffres, et les résultats attendus des énoncés tombent juste.

Fichiers produits, dans ce dossier :

    stations.csv          60 stations, 8 quartiers
    meteo.csv             une ligne par jour, du 1er janvier au 30 juin 2026
    trajets.csv           un trajet par ligne, avec quelques lignes sales
    journal-bornes.log    le journal brut des bornes, texte non structuré

Les défauts sont volontaires et en petit nombre (≈ 0,5 %) : durées négatives,
valeurs manquantes, doublons exacts, lignes de journal tronquées. Ils servent
aux modules 2 et 3 — un jeu propre n'apprendrait rien sur le nettoyage.
"""
import argparse
import csv
import math
import os
import random
from datetime import date, datetime, timedelta

GRAINE = 2026
ICI = os.path.dirname(os.path.abspath(__file__))
DEBUT = date(2026, 1, 1)
FIN = date(2026, 6, 30)

QUARTIERS = [
    # nom, centre (lat, lon), profil : part des trajets de loisir
    ("Centre", (45.7600, 4.8350), 0.30),
    ("Gare", (45.7605, 4.8590), 0.20),
    ("Affaires", (45.7640, 4.8520), 0.10),
    ("Université", (45.7480, 4.8420), 0.25),
    ("Parc", (45.7770, 4.8530), 0.70),
    ("Berges", (45.7530, 4.8300), 0.60),
    ("Vieille Ville", (45.7620, 4.8270), 0.55),
    ("Faubourg", (45.7450, 4.8650), 0.20),
]

NOMS = [
    "Place du Marché", "Hôtel de Ville", "Gare Centrale", "Quai Sud", "Opéra",
    "Halles", "Pont Neuf", "Jardin des Plantes", "Musée", "Cathédrale",
    "Tour Horizon", "Esplanade", "Campus Nord", "Bibliothèque", "Stade",
    "Roseraie", "Grande Serre", "Lac", "Belvédère", "Canal",
    "Ancien Port", "Écluse", "Traboules", "Fontaine", "Théâtre",
    "Marché Couvert", "Hôpital", "Mairie Annexe", "Piscine", "Patinoire",
    "Conservatoire", "Planétarium", "Parvis", "Arsenal", "Manufacture",
    "Filature", "Docks", "Passerelle", "Cours Lafayette", "Rue Neuve",
    "Carré d'Or", "Terrasses", "Colline", "Vieux Pont", "Porte Est",
    "Porte Ouest", "Clinique", "Lycée", "Médiathèque", "Cinéma",
    "Pépinière", "Observatoire", "Chapelle", "Palais", "Forum",
    "Atelier", "Verrière", "Kiosque", "Belle Allée", "Rotonde",
]

TYPES_JOURNAL_ERREUR = [
    ("E12", "verrou bloqué"),
    ("E27", "lecteur de badge muet"),
    ("E42", "batterie non détectée"),
    ("E51", "écran figé"),
]


def distance_km(a, b):
    """Distance à vol d'oiseau, formule de haversine."""
    lat1, lon1 = map(math.radians, a)
    lat2, lon2 = map(math.radians, b)
    h = (math.sin((lat2 - lat1) / 2) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2)
    return 6371 * 2 * math.asin(math.sqrt(h))


def generer_stations(rng):
    stations = []
    for i, nom in enumerate(NOMS):
        quartier, (lat, lon), loisir = QUARTIERS[i % len(QUARTIERS)]
        stations.append({
            "station_id": f"S{i + 1:03d}",
            "nom": nom,
            "quartier": quartier,
            "latitude": round(lat + rng.gauss(0, 0.004), 5),
            "longitude": round(lon + rng.gauss(0, 0.006), 5),
            "capacite": rng.choice([15, 20, 20, 25, 30, 40]),
            "_loisir": loisir,
        })
    return stations


def generer_meteo(rng):
    meteo = []
    jour = DEBUT
    while jour <= FIN:
        n = (jour - DEBUT).days
        # de 4 °C début janvier à 26 °C fin juin, avec du bruit
        temperature = round(4 + 22 * n / 180 + rng.gauss(0, 3), 1)
        pluie = round(max(0.0, rng.gauss(-1.5, 4)), 1)
        meteo.append({"date": jour.isoformat(), "temperature_c": temperature, "pluie_mm": pluie})
        jour += timedelta(days=1)
    return meteo


def poids_heure(heure, week_end, abonne):
    """Profil horaire : pointes domicile-travail pour les abonnés en semaine,
    une bosse l'après-midi pour les occasionnels et le week-end."""
    if abonne and not week_end:
        return (0.2 + 3.0 * math.exp(-((heure - 8.3) ** 2) / 1.2)
                + 2.6 * math.exp(-((heure - 18) ** 2) / 1.8)
                + 0.8 * math.exp(-((heure - 12.5) ** 2) / 1.0))
    return 0.1 + 1.8 * math.exp(-((heure - 15.5) ** 2) / 8)


def tirer_heure(rng, week_end, abonne):
    heures = [h + 0.5 for h in range(6, 24)]
    poids = [poids_heure(h, week_end, abonne) for h in heures]
    h = rng.choices(heures, weights=poids)[0]
    return h - 0.5 + rng.random()


def generer_trajets(rng, stations, meteo, nb):
    par_quartier = {}
    for s in stations:
        par_quartier.setdefault(s["quartier"], []).append(s)
    meteo_du_jour = {m["date"]: m for m in meteo}
    jours = [DEBUT + timedelta(days=i) for i in range((FIN - DEBUT).days + 1)]
    # plus de trajets quand il fait beau et sec
    poids_jours = []
    for j in jours:
        m = meteo_du_jour[j.isoformat()]
        p = 1.0 + 0.04 * m["temperature_c"] - 0.08 * min(m["pluie_mm"], 10)
        poids_jours.append(max(0.2, p))

    trajets = []
    velos = [f"V{i:04d}" for i in range(1, 1201)]
    for n in range(nb):
        jour = rng.choices(jours, weights=poids_jours)[0]
        week_end = jour.weekday() >= 5
        depart = rng.choice(stations)
        # un occasionnel part plus souvent d'un quartier de loisir
        p_occasionnel = 0.15 + 0.45 * depart["_loisir"] + (0.2 if week_end else 0)
        abonne = rng.random() > p_occasionnel
        heure = tirer_heure(rng, week_end, abonne)

        if abonne:
            arrivee = rng.choice(stations)
        else:
            # les occasionnels restent souvent dans le même quartier, ou reviennent au point de départ
            arrivee = depart if rng.random() < 0.18 else rng.choice(par_quartier[depart["quartier"]] + stations[:10])
        dist = distance_km((depart["latitude"], depart["longitude"]),
                           (arrivee["latitude"], arrivee["longitude"]))
        dist = round(max(0.3, dist * rng.uniform(1.15, 1.45)), 2)  # on ne roule pas à vol d'oiseau
        electrique = rng.random() < (0.45 if abonne else 0.25)
        vitesse = rng.gauss(16 if abonne else 11, 2.5) + (4 if electrique else 0)
        duree = dist / max(6, vitesse) * 60
        if not abonne:
            duree += rng.expovariate(1 / 12)  # flânerie
        duree = round(duree, 1)

        debut = datetime.combine(jour, datetime.min.time()) + timedelta(hours=heure)
        fin = debut + timedelta(minutes=duree)
        if abonne:
            tarif = 0.0 if duree <= 30 else round(1.0 * math.ceil((duree - 30) / 30), 2)
        else:
            tarif = round(1.0 + 0.05 * duree, 2)
        if electrique:
            tarif = round(tarif + 0.5, 2)

        trajets.append({
            "trajet_id": f"T{n + 1:07d}",
            "velo_id": rng.choice(velos),
            "station_depart": depart["station_id"],
            "station_arrivee": arrivee["station_id"],
            "debut": debut.strftime("%Y-%m-%d %H:%M:%S"),
            "fin": fin.strftime("%Y-%m-%d %H:%M:%S"),
            "duree_min": duree,
            "distance_km": dist,
            "type_usager": "abonne" if abonne else "occasionnel",
            "electrique": int(electrique),
            "tarif_eur": tarif,
        })

    # Défauts volontaires, ≈ 0,5 % au total.
    for t in rng.sample(trajets, nb // 1000):
        t["duree_min"] = -t["duree_min"]          # horloge de borne décalée
    for t in rng.sample(trajets, nb // 1000):
        t["station_arrivee"] = ""                 # vélo jamais redéposé
    for t in rng.sample(trajets, nb // 2000):
        t["type_usager"] = ""                     # badge illisible
    trajets.sort(key=lambda t: t["debut"])
    doublons = rng.sample(trajets, nb // 1000)    # double remontée du même trajet
    for d in doublons:
        trajets.insert(rng.randrange(len(trajets)), dict(d))
    return trajets


def ecrire_journal(rng, trajets, chemin):
    """Le journal brut des bornes : ce que la collecte reçoit avant tout
    nettoyage. Une ligne par événement, format libre, et quelques lignes
    tronquées par des coupures réseau."""
    lignes = []
    for t in trajets:
        if not t["station_arrivee"] or t["duree_min"] < 0:
            continue
        badge = ("A-" if t["type_usager"] == "abonne" else "O-") + f"{rng.randrange(10000, 99999)}"
        batterie = f" batterie={rng.randrange(15, 100)}" if t["electrique"] else ""
        lignes.append((t["debut"], f'{t["debut"].replace(" ", "T")} {t["station_depart"]} RETRAIT velo={t["velo_id"]} badge={badge}{batterie}'))
        lignes.append((t["fin"], f'{t["fin"].replace(" ", "T")} {t["station_arrivee"]} DEPOT velo={t["velo_id"]} duree={t["duree_min"]}'))
        if rng.random() < 0.03:
            code, message = rng.choice(TYPES_JOURNAL_ERREUR)
            lignes.append((t["debut"], f'{t["debut"].replace(" ", "T")} {t["station_depart"]} ERREUR code={code} message="{message}"'))
    lignes.sort(key=lambda x: x[0])
    with open(chemin, "w", encoding="utf-8", newline="\n") as f:
        for _, ligne in lignes:
            if rng.random() < 0.002:
                ligne = ligne[: rng.randrange(8, max(9, len(ligne) // 2))]  # ligne tronquée
            f.write(ligne + "\n")
    return len(lignes)


def ecrire_csv(chemin, lignes, colonnes):
    with open(chemin, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=colonnes, extrasaction="ignore")
        w.writeheader()
        w.writerows(lignes)


def main():
    parser = argparse.ArgumentParser(description="Génère le jeu de données Vélo'Cité.")
    parser.add_argument("--trajets", type=int, default=300_000, help="nombre de trajets (défaut : 300 000)")
    parser.add_argument("--sortie", default=ICI, help="dossier de sortie (défaut : ce dossier)")
    args = parser.parse_args()

    rng = random.Random(GRAINE)
    os.makedirs(args.sortie, exist_ok=True)
    stations = generer_stations(rng)
    meteo = generer_meteo(rng)
    trajets = generer_trajets(rng, stations, meteo, args.trajets)

    ecrire_csv(os.path.join(args.sortie, "stations.csv"), stations,
               ["station_id", "nom", "quartier", "latitude", "longitude", "capacite"])
    ecrire_csv(os.path.join(args.sortie, "meteo.csv"), meteo, ["date", "temperature_c", "pluie_mm"])
    ecrire_csv(os.path.join(args.sortie, "trajets.csv"), trajets,
               ["trajet_id", "velo_id", "station_depart", "station_arrivee", "debut", "fin",
                "duree_min", "distance_km", "type_usager", "electrique", "tarif_eur"])
    n_journal = ecrire_journal(rng, trajets, os.path.join(args.sortie, "journal-bornes.log"))

    print(f"stations.csv        {len(stations):>9} lignes")
    print(f"meteo.csv           {len(meteo):>9} lignes")
    print(f"trajets.csv         {len(trajets):>9} lignes")
    print(f"journal-bornes.log  {n_journal:>9} lignes")


if __name__ == "__main__":
    main()
