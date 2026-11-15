"""Tests pour les pipelines Spark (job_pipeline.py et streaming_pipeline.py).

Ces tests testent les fonctions des modules Spark. Ils nécessitent pyspark.
Si pyspark n'est pas disponible, les tests sont ignorés (skip).
"""

import pytest
from unittest.mock import MagicMock, patch

# Vérifie si pyspark est disponible
try:
    from pyspark.sql import SparkSession, Row
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


# Fixture pour la session Spark (uniquement si pyspark est disponible)
@pytest.fixture(scope="session")
def spark_session():
    """Crée une session Spark pour les tests."""
    if not PYSPARK_AVAILABLE:
        pytest.skip("pyspark not available")
    
    spark = (SparkSession.builder
            .master("local[2]")
            .appName("test-spark-pipeline")
            .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()


# Marqueur pour ignorer les tests si pyspark n'est pas disponible
pytestmark = pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark not installed")


class TestJobPipeline:
    """Tests pour job_pipeline.py."""
    
    def test_read_topic_schema(self, spark_session):
        """Test la définition des schemas dans job_pipeline."""
        from spark.job_pipeline import SCHEMAS
        
        # Vérifie que les schemas sont définis
        assert "topic-clients" in SCHEMAS
        assert "topic-contrats" in SCHEMAS
        assert "topic-sinistres" in SCHEMAS
        assert "topic-paiements" in SCHEMAS
        
        # Vérifie la structure du schema clients
        client_schema = SCHEMAS["topic-clients"]
        field_names = [f.name for f in client_schema.fields]
        required_fields = ["source", "id_source", "nom", "prenom", "date_naissance", 
                          "email_pseudo", "telephone_pseudo", "code_postal", "statut"]
        for field in required_fields:
            assert field in field_names
    
    def test_data_cleaning(self, spark_session):
        """Test la fonction de nettoyage des données."""
        from spark.job_pipeline import SCHEMAS
        
        # Crée des données de test
        test_data = [
            {
                "source": "ABASSURANCE",
                "id_source": "1",
                "nom": "  DUPONT  ",
                "prenom": "Jean",
                "date_naissance": "1980-01-01",
                "email_pseudo": "hash1",
                "telephone_pseudo": "hash2",
                "code_postal": "75 001",
                "statut": "ACTIF"
            },
            {
                "source": "ABASSURANCE",
                "id_source": "1",  # Doublon intra-source
                "nom": "DUPONT",
                "prenom": "Jean",
                "date_naissance": "1980-01-01",
                "email_pseudo": "hash1",
                "telephone_pseudo": "hash2",
                "code_postal": "75001",
                "statut": "ACTIF"
            }
        ]
        
        # Simule un topic Kafka avec des données JSON
        schema = SCHEMAS["topic-clients"]
        rows = [Row(**d) for d in test_data]
        df = spark_session.createDataFrame(rows, schema)
        
        # Applique le nettoyage : dropDuplicates + nettoyage code postal
        cleaned = df.dropDuplicates(["source", "id_source"])
        cleaned = cleaned.withColumn("code_postal", F.regexp_replace("code_postal", "\\s", ""))
        
        # Vérifie que le doublon a été supprimé
        assert cleaned.count() == 1
        
        # Vérifie que le code postal a été nettoyé
        result = cleaned.filter(cleaned.code_postal == "75001").count()
        assert result == 1
    
    def test_doublons_detection(self, spark_session):
        """Test la détection des doublons entre AB et AP."""
        from spark.job_pipeline import SCHEMAS
        
        # Crée des données avec un doublon
        ab_data = [
            {
                "source": "ABASSURANCE",
                "id_source": "ab_1",
                "nom": "DUPONT",
                "prenom": "Jean",
                "date_naissance": "1980-01-01",
                "email_pseudo": "hash-jean@email.fr",
                "telephone_pseudo": "hash-tel",
                "code_postal": "75001",
                "statut": "ACTIF"
            },
            {
                "source": "ABASSURANCE",
                "id_source": "ab_2",
                "nom": "MARTIN",
                "prenom": "Sophie",
                "date_naissance": "1990-01-01",
                "email_pseudo": "hash-sophie@email.fr",
                "telephone_pseudo": "hash-tel2",
                "code_postal": "75002",
                "statut": "ACTIF"
            }
        ]
        
        ap_data = [
            {
                "source": "ASSUREPLUS",
                "id_source": "ap_1",
                "nom": "DUPONT",
                "prenom": "Jean",
                "date_naissance": "1980-01-01",
                "email_pseudo": "hash-jean@email.fr",  # Même email = doublon
                "telephone_pseudo": "hash-tel3",
                "code_postal": "75001",
                "statut": "ACTIF"
            },
            {
                "source": "ASSUREPLUS",
                "id_source": "ap_2",
                "nom": "DURAND",
                "prenom": "Pierre",
                "date_naissance": "1995-01-01",
                "email_pseudo": "hash-pierre@email.fr",
                "telephone_pseudo": "hash-tel4",
                "code_postal": "75003",
                "statut": "ACTIF"
            }
        ]
        
        schema = SCHEMAS["topic-clients"]
        
        # Crée les DataFrames
        ab_df = spark_session.createDataFrame([Row(**d) for d in ab_data], schema)
        ap_df = spark_session.createDataFrame([Row(**d) for d in ap_data], schema)
        
        # Applique la logique du pipeline
        ab = (ab_df
              .filter(F.col("email_pseudo").isNotNull())
              .withColumnRenamed("id_source", "id_ab"))
        
        ap = (ap_df
              .filter(F.col("email_pseudo").isNotNull())
              .withColumnRenamed("id_source", "id_ap"))
        
        doublons = ab.join(ap, on=["email_pseudo"], how="inner") \
                       .select("id_ab", "id_ap", "nom", "prenom", "code_postal")
        
        # Vérifie qu'un seul doublon a été détecté
        assert doublons.count() == 1
        
        # Vérifie que c'est le bon doublon
        result = doublons.first()
        assert result.id_ab == "ab_1"
        assert result.id_ap == "ap_1"
        assert result.nom == "DUPONT"
        assert result.prenom == "Jean"
    
    def test_no_false_doublon_with_null_email(self, spark_session):
        """Test qu'il n'y a pas de faux doublons avec des emails NULL."""
        from spark.job_pipeline import SCHEMAS
        
        # Données avec emails NULL
        ab_data = [
            {
                "source": "ABASSURANCE",
                "id_source": "ab_1",
                "nom": "X",
                "prenom": "Y",
                "date_naissance": "1980-01-01",
                "email_pseudo": None,
                "telephone_pseudo": None,
                "code_postal": "75001",
                "statut": "ACTIF"
            }
        ]
        
        ap_data = [
            {
                "source": "ASSUREPLUS",
                "id_source": "ap_1",
                "nom": "Z",
                "prenom": "W",
                "date_naissance": "1980-01-01",
                "email_pseudo": None,
                "telephone_pseudo": None,
                "code_postal": "75001",
                "statut": "ACTIF"
            }
        ]
        
        schema = SCHEMAS["topic-clients"]
        
        ab_df = spark_session.createDataFrame([Row(**d) for d in ab_data], schema)
        ap_df = spark_session.createDataFrame([Row(**d) for d in ap_data], schema)
        
        # Applique la logique du pipeline avec filtrage NULL
        ab = (ab_df
              .filter(F.col("email_pseudo").isNotNull())
              .withColumnRenamed("id_source", "id_ab"))
        
        ap = (ap_df
              .filter(F.col("email_pseudo").isNotNull())
              .withColumnRenamed("id_source", "id_ap"))
        
        doublons = ab.join(ap, on=["email_pseudo"], how="inner") \
                       .select("id_ab", "id_ap", "nom", "prenom", "code_postal")
        
        # Aucun doublon ne doit être détecté
        assert doublons.count() == 0


class TestStreamingPipeline:
    """Tests pour streaming_pipeline.py."""
    
    def test_sha_pseudo(self, spark_session):
        """Test la fonction de pseudonymisation SHA-256."""
        from spark.streaming_pipeline import sha_pseudo
        
        # Crée un DataFrame de test
        data = [Row(value=None), Row(value=""), Row(value="test@email.fr"), Row(value="TEST@EMAIL.FR")]
        schema = StructType([StructField("value", StringType(), True)])
        df = spark_session.createDataFrame(data, schema)
        
        # Applique la fonction
        result = df.withColumn("pseudo", sha_pseudo(F.col("value")))
        
        # Vérifie que NULL et vide restent NULL
        null_rows = result.filter(result.pseudo.isNull()).count()
        assert null_rows == 2  # NULL et vide
        
        # Vérifie que la pseudonymisation est deterministe
        test_result = result.filter(result.value == "test@email.fr").first().pseudo
        test_upper_result = result.filter(result.value == "TEST@EMAIL.FR").first().pseudo
        assert test_result == test_upper_result
        
        # Vérifie que le hash fait 64 caractères
        assert len(test_result) == 64
    
    def test_epoch_days_str(self, spark_session):
        """Test la conversion de jours depuis 1970 vers date string."""
        from spark.streaming_pipeline import epoch_days_str
        
        # Crée un DataFrame de test
        data = [Row(days=0), Row(days=18262)]
        schema = StructType([StructField("days", IntegerType(), True)])
        df = spark_session.createDataFrame(data, schema)
        
        result = df.withColumn("date_str", epoch_days_str(F.col("days")))
        
        # Vérifie les résultats
        row1 = result.filter(result.days == 0).first()
        assert row1.date_str == "1970-01-01"
        
        row2 = result.filter(result.days == 18262).first()
        assert row2.date_str == "2020-01-01"
    
    def test_union_or_none(self, spark_session):
        """Test la fonction union_or_none."""
        from spark.streaming_pipeline import union_or_none
        
        # Crée des DataFrames de test
        schema = StructType([
            StructField("source", StringType(), True),
            StructField("id", StringType(), True)
        ])
        
        df1 = spark_session.createDataFrame([Row(source="A", id="1")], schema)
        df2 = spark_session.createDataFrame([Row(source="B", id="2")], schema)
        
        # Test union de deux DataFrames
        result = union_or_none(df1, df2)
        assert result.count() == 2
        
        # Test avec None
        result1 = union_or_none(df1, None)
        assert result1.count() == 1
        
        result2 = union_or_none(None, df2)
        assert result2.count() == 1
        
        result3 = union_or_none(None, None)
        assert result3 is None


class TestDeduplicationKey:
    """Tests pour la clé de déduplication."""
    
    def test_dedup_key_creation(self, spark_session):
        """Test la création de la clé de déduplication."""
        from pyspark.sql import functions as F
        
        # Crée des données de test
        data = [
            {
                "source": "ABASSURANCE",
                "id_source": "1",
                "nom": "DUPONT",
                "prenom": "Jean",
                "date_naissance": "1980-01-01",
                "code_postal": "75001",
            },
            {
                "source": "ASSUREPLUS",
                "id_source": "2",
                "nom": "DUPONT",
                "prenom": "Jean",
                "date_naissance": "1980-01-01",
                "code_postal": "75001",
            },
            {
                "source": "ABASSURANCE",
                "id_source": "3",
                "nom": "MARTIN",
                "prenom": "Sophie",
                "date_naissance": "1990-01-01",
                "code_postal": "75002",
            }
        ]
        
        schema = StructType([
            StructField("source", StringType(), True),
            StructField("id_source", StringType(), True),
            StructField("nom", StringType(), True),
            StructField("prenom", StringType(), True),
            StructField("date_naissance", StringType(), True),
            StructField("code_postal", StringType(), True)
        ])
        
        df = spark_session.createDataFrame([Row(**d) for d in data], schema)
        
        # Normalisation
        df_normalized = (df
            .withColumn("nom", F.upper(F.trim(F.col("nom"))))
            .withColumn("prenom", F.upper(F.trim(F.col("prenom"))))
            .withColumn("code_postal", F.regexp_replace("code_postal", "\\s", ""))
        )
        
        # Création de la clé de déduplication
        df_with_key = df_normalized.withColumn(
            "dedup_key",
            F.concat_ws("|", "nom", "prenom", "date_naissance", "code_postal")
        )
        
        # Compte les doublons
        from pyspark.sql.window import Window
        w = Window.partitionBy("dedup_key")
        df_with_count = df_with_key.withColumn("count", F.count("*").over(w))
        
        # Vérifie que DUPONT Jean apparaît 2 fois
        dupont_count = df_with_count.filter("nom = 'DUPONT'").first().count
        assert dupont_count == 2
        
        # Vérifie que MARTIN Sophie apparaît 1 fois
        martin_count = df_with_count.filter("nom = 'MARTIN'").first().count
        assert martin_count == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
