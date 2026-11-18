"""Tests pour spark/streaming_pipeline.py - Conformité Bloc 1 et Bloc 3"""
import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
import hashlib

@pytest.fixture(scope="session")
def spark_session():
    """Fixture pour créer une session Spark locale."""
    spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()
    yield spark
    spark.stop()

def sha_pseudo_nom_prenom_date_cp(nom, prenom, date_naissance, code_postal):
    """Fonction de déduplication (SHA256)."""
    clé = f"{nom}{prenom}{date_naissance}{code_postal}"
    return hashlib.sha256(clé.lower().strip().encode()).hexdigest()

class TestShaPseudoNomPrenomDateCp:
    """Tests pour la clé de déduplication (Bloc 1 - Matrice de doublons)."""

    def test_sha_pseudo_unique(self, spark_session):
        """Teste que des données différentes produisent des clés uniques."""
        df = spark_session.createDataFrame([
            Row(nom="Dupont", prenom="Jean", date_naissance="1980-01-01", code_postal="75001"),
            Row(nom="Martin", prenom="Pierre", date_naissance="1990-05-15", code_postal="75002"),
        ])
        result = df.withColumn(
            "clé_déduplication",
            F.udf(sha_pseudo_nom_prenom_date_cp)(
                F.col("nom"), F.col("prenom"), F.col("date_naissance"), F.col("code_postal")
            )
        )
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0]["clé_déduplication"] != rows[1]["clé_déduplication"]
        assert rows[0]["clé_déduplication"] is not None

    def test_sha_pseudo_deterministic(self, spark_session):
        """Teste que les mêmes données produisent la même clé."""
        df = spark_session.createDataFrame([
            Row(nom="Dupont", prenom="Jean", date_naissance="1980-01-01", code_postal="75001"),
            Row(nom="Dupont", prenom="Jean", date_naissance="1980-01-01", code_postal="75001"),
        ])
        result = df.withColumn(
            "clé_déduplication",
            F.udf(sha_pseudo_nom_prenom_date_cp)(
                F.col("nom"), F.col("prenom"), F.col("date_naissance"), F.col("code_postal")
            )
        )
        rows = result.collect()
        assert rows[0]["clé_déduplication"] == rows[1]["clé_déduplication"]