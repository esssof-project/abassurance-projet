"""Fixtures pour les tests."""
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    """Session Spark pour les tests."""
    spark = SparkSession.builder \
        .appName("test-streaming-pipeline") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()