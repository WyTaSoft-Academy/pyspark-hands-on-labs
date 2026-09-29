"""
Bonus du TP 1 : un job à soumettre à un mini-cluster standalone.

    spark-submit --master spark://127.0.0.1:7078 --total-executor-cores 2 job_standalone.py

Contrairement au notebook du TP, ce script N'ÉCRIT PAS le master dans le code : un
master fixé par .master(...) l'emporte sur l'option --master de spark-submit,
et le job tournerait en local sans rien dire. C'est la raison pour laquelle un
job de production ne fixe jamais son master lui-même.

Le chemin des données est un argument : sur un vrai cluster, il doit désigner
un stockage que toutes les machines voient (HDFS, S3, partage réseau).
"""
import os
import sys

from pyspark.sql import SparkSession

ICI = os.path.dirname(os.path.abspath(__file__))
chemin = sys.argv[1] if len(sys.argv) > 1 else os.path.join(ICI, "..", "donnees", "trajets.csv")

spark = SparkSession.builder.appName("tp1-job-standalone").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("master :", spark.sparkContext.master)
trajets = spark.read.csv(chemin, header=True)
trajets.groupBy("type_usager").count().orderBy("type_usager").show()

spark.stop()
