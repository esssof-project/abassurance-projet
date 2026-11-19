"""Fixtures pour les tests."""
import pytest
from pyspark.sql import SparkSession

from spark.streaming_pipeline import bronze


@pytest.fixture(scope="session")
def hdfs_accessible():
    """Vérifie que HDFS est accessible."""
    from hdfs import InsecureClient
    try:
        client = InsecureClient("http://namenode:9870", user_name="hadoop")
        client.list("/")
        yield True
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"HDFS non accessible: {e}")
        
def test_bronze_ab_client(self, spark_session, hdfs_accessible):
    """Teste la lecture des clients AbAssurance depuis HDFS."""
    if not hdfs_accessible:
        pytest.skip("HDFS non accessible")
    df = bronze(spark_session, "dbab_ab_ab_client")
    if df is not None:
        assert df.count() > 0
    else:
        pytest.skip("Aucune donnée AbAssurance disponible dans HDFS")

@pytest.fixture(scope="session")
def spark_session():
    """Session Spark pour les tests."""
    spark = SparkSession.builder \
        .appName("test-streaming-pipeline") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()