"""
Vérifie que le poste est prêt pour les TP Spark.

    python verifier_env.py

Contrôle, dans l'ordre où les problèmes se rencontrent : Python, Java,
PySpark, les variables d'environnement qui piègent sous Windows, les données
du fil rouge, et enfin un vrai petit job Spark. Chaque ligne dit OK, ATTENTION
ou ÉCHEC, et, en cas de problème, ce qu'il faut faire.

Sort en code 1 si un contrôle bloquant échoue.
"""
import os
import re
import shutil
import subprocess
import sys

ICI = os.path.dirname(os.path.abspath(__file__))
DONNEES = os.path.join(os.path.dirname(ICI), "donnees")
WINDOWS = os.name == "nt"
echecs = 0


def ligne(etat, sujet, detail=""):
    global echecs
    if etat == "ÉCHEC":
        echecs += 1
    print(f"  [{etat:^9}] {sujet:<22} {detail}")


def version_java():
    """Renvoie (version majeure, chemin) du java qui sera utilisé par Spark."""
    java_home = os.environ.get("JAVA_HOME")
    java = None
    if java_home:
        candidat = os.path.join(java_home, "bin", "java.exe" if WINDOWS else "java")
        if os.path.exists(candidat):
            java = candidat
    java = java or shutil.which("java")
    if not java:
        return None, None
    sortie = subprocess.run([java, "-version"], capture_output=True, text=True).stderr
    m = re.search(r'version "(\d+)(?:\.(\d+))?', sortie)
    if not m:
        return None, java
    majeure = int(m.group(1))
    if majeure == 1:  # Java 8 s'annonce « 1.8 »
        majeure = int(m.group(2))
    return majeure, java


print("\nPython")
v = sys.version_info
if v < (3, 8):
    ligne("ÉCHEC", "Python", f"{v.major}.{v.minor} : Spark 3.5 demande Python 3.8 ou plus")
elif v >= (3, 12):
    ligne("ATTENTION", "Python", f"{v.major}.{v.minor} : Spark 3.5 est le plus sûr en 3.8 à 3.11")
else:
    ligne("OK", "Python", f"{v.major}.{v.minor}.{v.micro} — {sys.executable}")

print("\nPySpark")
try:
    import pyspark
    version_spark = tuple(int(x) for x in pyspark.__version__.split(".")[:2])
    if pyspark.__version__ == "3.5.4":
        ligne("OK", "PySpark", f"{pyspark.__version__} — {os.path.dirname(pyspark.__file__)}")
    else:
        # Les TP visent la 3.5.4 : une autre 3.5.x tourne, une autre branche
        # aussi le plus souvent, mais les sorties attendues et les prérequis
        # Java ne sont plus garantis.
        ligne("ATTENTION", "PySpark", f"{pyspark.__version__} : la session utilise Spark 3.5.4, "
              'pip install "pyspark==3.5.4"')
except ImportError:
    version_spark = None
    ligne("ÉCHEC", "PySpark", 'introuvable : pip install "pyspark==3.5.4"')

print("\nJava")
majeure_java, java = version_java()
if majeure_java is None:
    ligne("ÉCHEC", "Java", "introuvable : installer un JDK 17 (ou 11, 8) et définir JAVA_HOME")
elif majeure_java in (8, 11, 17):
    ligne("OK", "Java", f"{majeure_java} — {java}")
else:
    # Java 21 et plus : Spark 3.5 n'est pas prévu pour, utiliser le JDK 17.
    ligne("ÉCHEC", "Java", f"{majeure_java} — {java} : Spark 3.5 demande Java 8, 11 ou 17")
if majeure_java is not None and not os.environ.get("JAVA_HOME"):
    ligne("ATTENTION", "JAVA_HOME", "non défini : Spark prendra le java du PATH")

print("\nBibliothèques pour pandas API on Spark")
try:
    import numpy
    if int(numpy.__version__.split(".")[0]) >= 2:
        # Observé : pyspark.pandas 3.5 échoue à l'import, « np.NaN was removed ».
        ligne("ATTENTION", "numpy", f'{numpy.__version__} : pyspark.pandas 3.5 exige numpy 1.x, '
              'pip install "numpy<2"')
    else:
        ligne("OK", "numpy", numpy.__version__)
except ImportError:
    ligne("ATTENTION", "numpy", 'absent : pip install "numpy<2"')
try:
    import pandas
    if tuple(int(x) for x in pandas.__version__.split(".")[:2]) >= (2, 3):
        ligne("ATTENTION", "pandas", f'{pandas.__version__} : trop récent pour Spark 3.5, '
              'pip install "pandas<2.3"')
    else:
        ligne("OK", "pandas", pandas.__version__)
except ImportError:
    ligne("ATTENTION", "pandas", 'absent : pip install "pandas<2.3"')
try:
    import pyarrow
    ligne("OK", "pyarrow", pyarrow.__version__)
except ImportError:
    ligne("ATTENTION", "pyarrow", "absent : pip install pyarrow")

print("\nVariables d'environnement")
spark_home = os.environ.get("SPARK_HOME")
if spark_home:
    # Un SPARK_HOME qui pointe vers une autre version que le PySpark installé
    # donne « 'JavaPackage' object is not callable » dès la création de la session.
    ligne("ATTENTION", "SPARK_HOME", f"{spark_home} : doit correspondre à PySpark "
          f"{getattr(pyspark, '__version__', '?') if version_spark else '?'}, sinon le supprimer")
else:
    ligne("OK", "SPARK_HOME", "non défini : PySpark utilise sa propre copie de Spark")

import socket
try:
    nom_driver = socket.getfqdn()
except OSError:
    nom_driver = ""
if os.environ.get("SPARK_LOCAL_IP"):
    ligne("OK", "SPARK_LOCAL_IP", os.environ["SPARK_LOCAL_IP"])
elif "docker" in nom_driver.lower() or shutil.which("docker"):
    # Observé sur le poste de test : avec Docker Desktop, le driver s'annonce
    # « host.docker.internal », perd les battements de cœur de ses exécuteurs,
    # et un TP de quelques secondes prend des dizaines de minutes.
    ligne("ATTENTION", "SPARK_LOCAL_IP", "Docker détecté : définir SPARK_LOCAL_IP=127.0.0.1 "
          "si les jobs sont anormalement lents")

if WINDOWS:
    pyspark_python = os.environ.get("PYSPARK_PYTHON")
    if pyspark_python and os.path.exists(pyspark_python):
        ligne("OK", "PYSPARK_PYTHON", pyspark_python)
    else:
        # Sans elle, les exécuteurs cherchent « python » dans le PATH, tombent
        # parfois sur l'alias du Microsoft Store, et le job échoue sur
        # « Accept timed out ». Le script la pose pour lui-même, plus bas.
        ligne("ATTENTION", "PYSPARK_PYTHON", f"non défini : à fixer à {sys.executable}")
    hadoop_home = os.environ.get("HADOOP_HOME")
    winutils = hadoop_home and os.path.exists(os.path.join(hadoop_home, "bin", "winutils.exe"))
    if winutils:
        ligne("OK", "HADOOP_HOME", f"{hadoop_home} (winutils.exe présent)")
    else:
        ligne("ATTENTION", "HADOOP_HOME", "winutils.exe absent : la lecture marche, "
              "l'écriture de fichiers (Parquet…) échouera")

print("\nDonnées du fil rouge")
for nom in ["stations.csv", "meteo.csv", "trajets.csv", "journal-bornes.log"]:
    chemin = os.path.join(DONNEES, nom)
    if os.path.exists(chemin):
        taille = os.path.getsize(chemin)
        ligne("OK", nom, f"{taille / 1e6:.1f} Mo" if taille >= 1e6 else f"{taille / 1e3:.0f} ko")
    else:
        ligne("ÉCHEC", nom, "absent : python ../donnees/generer_donnees.py")

print("\nUn vrai job Spark")
if version_spark is None or majeure_java is None:
    ligne("ÉCHEC", "Job", "impossible sans PySpark et Java")
else:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    try:
        from pyspark.sql import SparkSession
        spark = (SparkSession.builder.master("local[2]").appName("verifier-env")
                 .config("spark.ui.showConsoleProgress", "false").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        # Une somme calculée par les exécuteurs Python : c'est ce qui échoue
        # quand PYSPARK_PYTHON est mal réglé, pas la simple création de session.
        total = spark.sparkContext.parallelize(range(1, 101), 4).map(lambda x: x * x).sum()
        etat = "OK" if total == 338350 else "ÉCHEC"
        ligne(etat, "Job RDD", f"somme des carrés de 1 à 100 = {total}")
        n = spark.range(1_000_000).selectExpr("sum(id)").first()[0]
        ligne("OK" if n == 499999500000 else "ÉCHEC", "Job DataFrame", f"somme de 0 à 999 999 = {n}")
        ligne("OK", "Interface web", spark.sparkContext.uiWebUrl or "désactivée")
        # Écrire un dossier puis le relire comme le fait PipelineModel.load :
        # c'est ce chemin, et lui seul, qui appelle les fonctions natives de
        # hadoop.dll. Observé sur le poste de préparation : un hadoop.dll d'une
        # autre version que le Hadoop 3.3 de Spark 3.5 laisse tout passer, sauf
        # ceci, en « UnsatisfiedLinkError … NativeIO$POSIX.stat ».
        if WINDOWS:
            import tempfile
            dossier = os.path.join(tempfile.mkdtemp(prefix="verifier-env-"), "texte")
            try:
                spark.sparkContext.parallelize(["a", "b", "c"], 2).saveAsTextFile(dossier)
                relu = spark.sparkContext.textFile(dossier).count()
                ligne("OK" if relu == 3 else "ÉCHEC", "Écrire puis relire", f"{relu} lignes relues")
            except Exception as e:
                message = str(e)
                if "UnsatisfiedLinkError" in message:
                    ligne("ÉCHEC", "Écrire puis relire", "hadoop.dll incompatible : installer "
                          "winutils.exe et hadoop.dll d'un Hadoop 3.3.x dans HADOOP_HOME\\bin")
                else:
                    ligne("ÉCHEC", "Écrire puis relire", message.strip().splitlines()[0][:110])
        spark.stop()
    except Exception as e:  # on veut le message, pas la pile Java
        premiere = str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__
        ligne("ÉCHEC", "Job", premiere[:110])

print()
if echecs:
    print(f"{echecs} contrôle(s) en échec : voir enonce.md, section « Dépannage ».")
    sys.exit(1)
print("Poste prêt pour les TP.")
