
import pytest

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

from spark import streaming_pipeline as pipeline


def get_bronze(spark, table):
    df = pipeline.bronze(spark, table)

    if df is None:
        pytest.skip(f"Table Bronze indisponible : {table}")

    if df.limit(1).count() == 0:
        pytest.skip(f"Table Bronze vide : {table}")

    return df


def test_clean_string_hdfs(spark_session):
    df = get_bronze(spark_session, "dbab_ab_ab_client")

    result = df.select(
        pipeline.clean_string(
            F.col("ab_nom"), "oracle"
        ).alias("nom_nettoye")
    )

    assert isinstance(
        result.schema["nom_nettoye"].dataType,
        StringType
    )

    rows = result.limit(20).collect()

    assert len(rows) > 0
    assert all(row["nom_nettoye"] is not None for row in rows)


def test_clean_numeric_hdfs(spark_session):
    df = get_bronze(spark_session, "dbab_ab_ab_paiement")

    result = df.select(
        pipeline.clean_numeric("ab_montant").alias("montant")
    )

    assert isinstance(
        result.schema["montant"].dataType,
        DoubleType
    )

    rows = result.limit(20).collect()

    assert len(rows) > 0
    assert all(row["montant"] is not None for row in rows)


def test_clean_date_hdfs(spark_session):
    df = get_bronze(spark_session, "dbap_ap_ap_claims")

    result = df.select(
        pipeline.clean_date("ap_incident_date").alias("date")
    )

    assert isinstance(
        result.schema["date"].dataType,
        StringType
    )

    rows = result.limit(20).collect()

    assert len(rows) > 0


def test_clean_string_null_and_spaces(spark_session):
    df = spark_session.createDataFrame(
        [(None,), ("   Jean   Dupont   ",), ("",), ("NaN",)],
        ["valeur"]
    )

    result = df.select(
        pipeline.clean_string("valeur").alias("nettoye")
    ).collect()

    assert result[0]["nettoye"] == ""
    assert result[1]["nettoye"] == "Jean Dupont"
    assert result[2]["nettoye"] == ""
    assert result[3]["nettoye"] == ""


def test_clean_numeric_invalid_value(spark_session):
    df = spark_session.createDataFrame(
        [("125.50",), ("abc",), (None,)],
        ["valeur"]
    )

    result = df.select(
        pipeline.clean_numeric("valeur").alias("montant")
    ).collect()

    assert result[0]["montant"] == 125.50
    assert result[1]["montant"] == 0.0
    assert result[2]["montant"] == 0.0


def test_clean_date_formats(spark_session):
    df = spark_session.createDataFrame(
        [
            ("2024-01-15",),
            ("15/01/2024",),
            ("15-01-2024",),
            ("2024/01/15",),
        ],
        ["date_source"]
    )

    result = df.select(
        pipeline.clean_date("date_source").alias("date")
    ).collect()

    assert all(row["date"] == "2024-01-15" for row in result)
