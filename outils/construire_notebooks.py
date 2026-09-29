"""
Construit les notebooks Jupyter des TP à partir de leurs sources Python.

    python outils/construire_notebooks.py

Les sources sont écrites au format « percent » : une cellule commence par
une ligne « # %% » (code) ou « # %% [markdown] » (texte, chaque ligne
préfixée par « # »). C'est le format que VS Code, PyCharm et Jupytext lisent
directement, et c'est un fichier Python ordinaire : il se relit dans un diff,
et il s'exécute avec « python » ou « spark-submit ».

Pour chaque fichier tpN-*/depart.py et tpN-*/corrige.py, ainsi que
exercices/serieN-*/depart.py et corrige.py, le script écrit le .ipynb du
même nom à côté. Bibliothèque standard uniquement : pas besoin de
nbformat ni de Jupytext sur le poste.
"""
import glob
import json
import os
import re

ICI = os.path.dirname(os.path.abspath(__file__))
TP = os.path.dirname(ICI)
MARQUE = re.compile(r"^# %%(.*)$")


def cellules(source):
    """Découpe une source « percent » en cellules (type, lignes)."""
    courant, lignes, resultat = None, [], []
    for ligne in source.splitlines():
        m = MARQUE.match(ligne)
        if m:
            if courant is not None:
                resultat.append((courant, lignes))
            courant = "markdown" if "[markdown]" in m.group(1) else "code"
            lignes = []
            continue
        if courant is None:
            # En-tête avant la première cellule : docstring du fichier, ignorée.
            continue
        if courant == "markdown":
            ligne = re.sub(r"^# ?", "", ligne)
        lignes.append(ligne)
    if courant is not None:
        resultat.append((courant, lignes))

    propres = []
    for type_, lignes in resultat:
        while lignes and not lignes[0].strip():
            lignes.pop(0)
        while lignes and not lignes[-1].strip():
            lignes.pop()
        propres.append((type_, lignes))
    return propres


def notebook(cells):
    nb_cells = []
    for type_, lignes in cells:
        texte = [l + "\n" for l in lignes]
        if texte:
            texte[-1] = texte[-1].rstrip("\n")
        cell = {"cell_type": type_, "metadata": {}, "source": texte}
        if type_ == "code":
            cell.update({"execution_count": None, "outputs": []})
        nb_cells.append(cell)
    return {
        "cells": nb_cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def main():
    sources = sorted(glob.glob(os.path.join(TP, "tp*", "depart.py"))
                     + glob.glob(os.path.join(TP, "tp*", "corrige.py"))
                     + glob.glob(os.path.join(TP, "exercices", "serie*", "depart.py"))
                     + glob.glob(os.path.join(TP, "exercices", "serie*", "corrige.py")))
    for chemin in sources:
        with open(chemin, encoding="utf-8") as f:
            nb = notebook(cellules(f.read()))
        cible = chemin[:-3] + ".ipynb"
        with open(cible, "w", encoding="utf-8", newline="\n") as f:
            json.dump(nb, f, ensure_ascii=False, indent=1)
            f.write("\n")
        print(f"{os.path.relpath(cible, TP):45} {len(nb['cells']):>3} cellules")


if __name__ == "__main__":
    main()
