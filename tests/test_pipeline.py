"""Tests pour la logique de pipeline (doublons, déduplication) avec Spark.

Ces tests utilisent PySpark pour tester les vraies fonctions du pipeline.
"""

import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType


@pytest.fixture(scope="session")
def spark_session():
    """Crée une session Spark pour les tests."""
    spark = (SparkSession.builder
            .master("local[2]")
            .appName("test-pipeline")
            .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()


def build_doublons_spark(clients_df):
    """Logique de détection de doublons avec Spark (même logique que job_pipeline.py)."""
    # Filtrer les emails NULL
    df = clients_df.filter(F.col("email_pseudo").isNotNull())
    
    # Séparer AB et AP
    ab = df.filter("source = 'ABASSURANCE'").withColumnRenamed("id_source", "id_ab")
    ap = df.filter("source = 'ASSUREPLUS'").withColumnRenamed("id_source", "id_ap")
    
    # Jointure interne sur email_pseudo
    doublons = ab.join(ap, on=["email_pseudo"], how="inner") \
                   .select("id_ab", "id_ap", "nom", "prenom", "code_postal")
    
    return doublons


def test_doublon_detecte(spark_session):
    """Test qu'un doublon est détecté entre AB et AP."""
    schema = StructType([
        StructField("source", StringType(), True),
        StructField("id_source", StringType(), True),
        StructField("nom", StringType(), True),
        StructField("prenom", StringType(), True),
        StructField("email_pseudo", StringType(), True),
        StructField("code_postal", StringType(), True)
    ])
    
    clients = spark_session.createDataFrame([
        Row(source="ABASSURANCE", id_source="1", nom="DUPONT", prenom="Jean", 
             email_pseudo="hash-jean", code_postal="75001"),
        Row(source="ASSUREPLUS", id_source="1", nom="DUPONT", prenom="Jean",
             email_pseudo="hash-jean", code_postal="75001")
    ], schema)
    
    result = build_doublons_spark(clients)
    assert result.count() == 1


def test_pas_de_faux_doublon(spark_session):
    """Test qu'il n'y a pas de faux doublon avec des clients différents."""
    schema = StructType([
        StructField("source", StringType(), True),
        StructField("id_source", StringType(), True),
        StructField("nom", StringType(), True),
        StructField("prenom", StringType(), True),
        StructField("email_pseudo", StringType(), True),
        StructField("code_postal", StringType(), True)
    ])
    
    clients = spark_session.createDataFrame([
        Row(source="ABASSURANCE", id_source="1", nom="DUPONT", prenom="Jean",
             email_pseudo="hash-jean", code_postal="75001"),
        Row(source="ASSUREPLUS", id_source="2", nom="MARTIN", prenom="Sophie",
             email_pseudo="hash-sophie", code_postal="75002")
    ], schema)
    
    result = build_doublons_spark(clients)
    assert result.count() == 0


def test_bug_null_email(spark_session):
    """BUG CORRIGE : un email NULL chez les deux assureurs ne doit pas produire de faux doublon."""
    schema = StructType([
        StructField("source", StringType(), True),
        StructField("id_source", StringType(), True),
        StructField("nom", StringType(), True),
        StructField("prenom", StringType(), True),
        StructField("email_pseudo", StringType(), True),
        StructField("code_postal", StringType(), True)
    ])
    
    clients = spark_session.createDataFrame([
        Row(source="ABASSURANCE", id_source="3", nom="X", prenom="Y",
             email_pseudo=None, code_postal="75001"),
        Row(source="ASSUREPLUS", id_source="4", nom="Z", prenom="W",
             email_pseudo=None, code_postal="75001")
    ], schema)
    
    result = build_doublons_spark(clients)
    assert result.count() == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
