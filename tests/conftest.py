"""Fixtures pour les tests."""
import os

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """Session Spark pour les tests."""
    spark = (
        SparkSession.builder
        .appName("test-streaming-pipeline")
        .master("local[2]")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def hdfs_accessible():
    """Vérifie que HDFS est accessible."""
    from hdfs import InsecureClient
    webhdfs_url = os.environ.get("HDFS_WEB_URL", "http://namenode:9870")
    try:
        client = InsecureClient(webhdfs_url, user_name="hadoop")
        client.list("/")
        yield True
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"HDFS non accessible: {e}")


def test_bronze_ab_client(spark_session, hdfs_accessible):
    """Teste la lecture des clients AbAssurance depuis HDFS."""
    if not hdfs_accessible:
        pytest.skip("HDFS non accessible")
    
    # Import lazy pour éviter ModuleNotFoundError si pyspark n'est pas installé
    from spark.streaming_pipeline import bronze
    
    df = bronze(spark_session, "dbab_ab_ab_client")
    if df is not None:
        assert df.count() > 0
    else:
        pytest.skip("Aucune donnée AbAssurance disponible dans HDFS")
