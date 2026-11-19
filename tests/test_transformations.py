
from pyspark.sql import functions as F

from spark import streaming_pipeline as pipeline


def test_sha_pseudo(spark_session):
    df = spark_session.createDataFrame(
        [
            ("jean@example.com",),
            ("JEAN@example.com",),
            (None,),
            ("",)
        ],
        ["email"]
    )

    result = df.withColumn(
        "email_hash",
        pipeline.sha_pseudo(F.col("email"))
    ).collect()

    # Le hachage est insensible à la casse.
    assert result[0]["email_hash"] == result[1]["email_hash"]

    # Les valeurs nulles et vides ne sont pas hachées.
    assert result[2]["email_hash"] is None
    assert result[3]["email_hash"] is None

    assert len(result[0]["email_hash"]) == 64


def test_union_or_none(spark_session):
    df1 = spark_session.createDataFrame(
        [(1, "AB")],
        ["id", "source"]
    )

    df2 = spark_session.createDataFrame(
        [(2, "AP")],
        ["id", "source"]
    )

    assert pipeline.union_or_none(None, df1).count() == 1
    assert pipeline.union_or_none(df1, None).count() == 1
    assert pipeline.union_or_none(None, None) is None

    result = pipeline.union_or_none(df1, df2)

    assert result.count() == 2


def test_latest_hdfs(spark_session):
    df = pipeline.bronze(
        spark_session,
        "dbab_ab_ab_client"
    )

    if df is None or df.limit(1).count() == 0:
        import pytest
        pytest.skip("Table clients Bronze indisponible ou vide")

    result = pipeline.latest(df, "ab_client_id")

    assert "event_ts" not in result.columns
    assert "op" not in result.columns
    assert "_rn" not in result.columns

    assert result.groupBy("ab_client_id").count() \
        .filter(F.col("count") > 1).limit(1).count() == 0


def test_bronze_all_tables(spark_session):
    tables = [
        "dbab_ab_ab_client",
        "dbab_ab_ab_contrat",
        "dbab_ab_ab_paiement",
        "dbab_ab_ab_sinistre",
        "dbap_ap_ap_users",
        "dbap_ap_ap_contracts",
        "dbap_ap_ap_payments",
        "dbap_ap_ap_claims",
    ]

    for table in tables:
        df = pipeline.bronze(spark_session, table)

        if df is not None:
            assert len(df.columns) > 0
