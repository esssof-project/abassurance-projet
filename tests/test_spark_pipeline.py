"""Tests pour spark/streaming_pipeline.py"""
import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.appName("test").master("local[2]").getOrCreate()
    yield spark
    spark.stop()


class TestShaPseudoNomPrenomDateCp:
    def test_sha_pseudo(self, spark_session):
        from spark.streaming_pipeline import sha_pseudo_nom_prenom_date_cp
        df = spark_session.createDataFrame([
            Row(nom="Dupont", prenom="Jean", date_naissance="1980-01-01", code_postal="75001"),
            Row(nom="Martin", prenom="Pierre", date_naissance="1990-05-15", code_postal="75002"),
        ])
        result = df.withColumn(
            "clé_déduplication",
            sha_pseudo_nom_prenom_date_cp(
                F.col("nom"), F.col("prenom"),
                F.col("date_naissance"), F.col("code_postal")
            )
        )
        rows = result.collect()
        assert len(rows) == 2
        # Vérifier que les clés sont différentes
        assert rows[0]["clé_déduplication"] != rows[1]["clé_déduplication"]
        # Vérifier que les clés ne sont pas NULL
        assert rows[0]["clé_déduplication"] is not None
        assert rows[1]["clé_déduplication"] is not None
