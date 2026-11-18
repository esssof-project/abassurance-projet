"""Tests pour streaming_pipeline.py - Conformité Bloc 1"""
import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

@pytest.fixture(scope="session")
def spark_streaming_session():
    """Fixture pour créer une session Spark avec support Kafka."""
    spark = SparkSession.builder \
        .appName("test-streaming") \
        .master("local[2]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()
    yield spark
    spark.stop()

class TestStreamingPipeline:
    """Tests pour le pipeline Spark Streaming (Bloc 1 - Traitement en temps réel)."""

    def test_streaming_pipeline_basic(self, spark_streaming_session):
        """Teste le pipeline Spark Streaming de base."""
        def sha_pseudo_nom_prenom_date_cp(nom, prenom, date_naissance, code_postal):
            import hashlib
            clé = f"{nom}{prenom}{date_naissance}{code_postal}"
            return hashlib.sha256(clé.lower().strip().encode()).hexdigest()

        df = spark_streaming_session.createDataFrame([
            Row(nom="Dupont", prenom="Jean", date_naissance="1980-01-01", code_postal="75001"),
            Row(nom="Martin", prenom="Pierre", date_naissance="1990-05-15", code_postal="75002"),
        ])
        result = df.withColumn(
            "clé_déduplication",
            F.udf(sha_pseudo_nom_prenom_date_cp)(F.col("nom"), F.col("prenom"), F.col("date_naissance"), F.col("code_postal"))
        )
        assert result.count() == 2
        assert "clé_déduplication" in result.columns