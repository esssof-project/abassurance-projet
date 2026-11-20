
"""Tests unitaires complémentaires de spark/streaming_pipeline.py.

Ces tests complètent les tests déjà présents dans tests/.
Ils n'utilisent ni Kafka ni HDFS : les DataFrames sont locaux à Spark.
"""
import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark import streaming_pipeline as pipeline


@pytest.fixture(scope="session")
def spark_session():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("abassurance-streaming-pipeline-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def cdc_df(spark, rows, schema):
    """Construit un DataFrame Bronze minimal avec les métadonnées CDC."""
    full_schema = T.StructType(
        list(schema.fields) + [
            T.StructField("event_ts", T.TimestampType(), True),
            T.StructField("op", T.StringType(), True),
        ]
    )
    values = []
    for row in rows:
        values.append(tuple(row) + (
            datetime.datetime(2024, 1, 1, 12, 0, 0),
            "u",
        ))
    return spark.createDataFrame(values, full_schema)


def test_clean_string_handles_null_nan_and_whitespace(spark_session):
    df = spark_session.createDataFrame(
        [("  Jean   Dupont  ",), (None,), ("NaN",), ("",)],
        ["v"],
    )
    result = [r.v for r in df.select(pipeline.clean_string("v", "sqlserver").alias("v")).collect()]
    assert result == ["Jean Dupont", "", "", ""]


def test_clean_string_normalizes_curly_quotes(spark_session):
    df = spark_session.createDataFrame([("“L’été”",)], ["v"])
    value = df.select(pipeline.clean_string("v", "sqlserver").alias("v")).first().v
    assert value == '"L\'été"'


def test_clean_numeric_handles_currency_and_invalid_values(spark_session):
    df = spark_session.createDataFrame([("1 234,50 €",), ("abc",), (None,), ("-12.5",)], ["v"])
    values = [r.v for r in df.select(pipeline.clean_numeric("v").alias("v")).collect()]
    # La fonction retire les caractères non numériques ; la virgule n'est pas un séparateur décimal.
    assert values[0] == 123450.0
    assert values[1:] == [0.0, 0.0, -12.5]


def test_clean_date_accepts_supported_formats(spark_session):
    df = spark_session.createDataFrame(
        [("2024-02-03",), ("03/02/2024",), ("2024/02/04 10:11:12",), ("invalid",)],
        ["v"],
    )
    values = [r.v for r in df.select(pipeline.clean_date("v").alias("v")).collect()]
    assert values == ["2024-02-03", "2024-02-03", "2024-02-04", None]


def test_after_contains_eight_topics():
    assert len(pipeline.AFTER) == 8
    assert "dbab.ab.ab_client" in pipeline.AFTER
    assert "dbap.ap.ap_payments" in pipeline.AFTER


def test_envelope_schema_has_debezium_fields():
    schema = pipeline.envelope_schema(T.StructType([T.StructField("id", T.IntegerType())]))
    assert set(schema.fieldNames()) == {"op", "after", "before"}


def test_sha_pseudo_hashes_nonempty_and_returns_null_for_empty(spark_session):
    df = spark_session.createDataFrame([(" Alice ",), ("",), (None,)], ["v"])
    rows = df.select(pipeline.sha_pseudo(F.col("v")).alias("h")).collect()
    assert len(rows[0].h) == 64
    assert rows[1].h is None
    assert rows[2].h is None


def test_latest_keeps_most_recent_record(spark_session):
    schema = T.StructType([
        T.StructField("id", T.IntegerType()),
        T.StructField("value", T.StringType()),
        T.StructField("event_ts", T.TimestampType()),
        T.StructField("op", T.StringType()),
    ])
    df = spark_session.createDataFrame([
        (1, "old", datetime.datetime(2024, 1, 1), "u"),
        (1, "new", datetime.datetime(2024, 1, 2), "u"),
        (2, "only", datetime.datetime(2024, 1, 1), "c"),
    ], schema)
    rows = {r.id: r.value for r in pipeline.latest(df, "id").collect()}
    assert rows == {1: "new", 2: "only"}


def test_epoch_days_str(spark_session):
    df = spark_session.createDataFrame([(0,), (1,), (None,)], ["days"])
    values = [r.v for r in df.select(pipeline.epoch_days_str("days").alias("v")).collect()]
    assert values == ["1970-01-01", "1970-01-02", None]


def test_epoch_ms_and_us_are_formatted_as_timestamps(spark_session):
    df = spark_session.createDataFrame([(1704067200000, 1704067200000000)], ["ms", "us"])
    row = df.select(
        pipeline.epoch_ms_str("ms").alias("ms"),
        pipeline.epoch_us_str("us").alias("us"),
    ).first()
    assert row.ms == "2024-01-01 00:00:00"
    assert row.us == "2024-01-01 00:00:00"


def test_union_or_none_handles_all_cases(spark_session):
    a = spark_session.createDataFrame([(1,)], ["id"])
    b = spark_session.createDataFrame([(2,)], ["id"])
    assert pipeline.union_or_none(None, None) is None
    assert pipeline.union_or_none(a, None).count() == 1
    assert pipeline.union_or_none(None, b).count() == 1
    assert sorted(r.id for r in pipeline.union_or_none(a, b).collect()) == [1, 2]


def test_bronze_returns_none_when_read_fails(spark_session, monkeypatch):
    class Reader:
        def parquet(self, path):
            raise OSError("not found")
    class FakeSpark:
        read = Reader()
    assert pipeline.bronze(FakeSpark(), "missing") is None


def test_build_clients_returns_none_when_bronze_missing(spark_session, monkeypatch):
    monkeypatch.setattr(pipeline, "bronze", lambda spark, name: None)
    assert pipeline.build_clients(spark_session) is None


def test_build_clients_ab_source(spark_session, monkeypatch):
    schema = T.StructType([
        T.StructField("ab_client_id", T.IntegerType()),
        T.StructField("ab_nom", T.StringType()),
        T.StructField("ab_prenom", T.StringType()),
        T.StructField("ab_date_naissance", T.IntegerType()),
        T.StructField("ab_email", T.StringType()),
        T.StructField("ab_telephone", T.StringType()),
        T.StructField("ab_code_postal", T.StringType()),
        T.StructField("ab_statut_client", T.StringType()),
    ])
    ab = cdc_df(spark_session, [(7, "DUPONT", "Jean", 0, "j@example.com", "0600000000", "75001", "ACTIF")], schema)
    monkeypatch.setattr(pipeline, "bronze", lambda spark, name: ab if name == "dbab_ab_ab_client" else None)
    row = pipeline.build_clients(spark_session).first()
    assert row.source == "ABASSURANCE"
    assert row.id_source == "7"
    assert row.date_naissance == "1970-01-01"
    assert row.email_pseudo and len(row.email_pseudo) == 64


def test_build_clients_ap_source(spark_session, monkeypatch):
    schema = T.StructType([
        T.StructField("ap_user_id", T.IntegerType()),
        T.StructField("ap_full_name", T.StringType()),
        T.StructField("ap_birth_date", T.StringType()),
        T.StructField("ap_mail_address", T.StringType()),
        T.StructField("ap_phone_number", T.StringType()),
        T.StructField("ap_zip_code", T.StringType()),
        T.StructField("ap_customer_status", T.StringType()),
    ])
    ap = cdc_df(spark_session, [(9, "Marie Martin", "02/03/1990", "m@example.com", "0611111111", "69000", "ACTIVE")], schema)
    monkeypatch.setattr(pipeline, "bronze", lambda spark, name: ap if name == "dbap_ap_ap_users" else None)
    row = pipeline.build_clients(spark_session).first()
    assert row.source == "ASSUREPLUS"
    assert row.nom == "Marie"
    assert row.prenom == "Martin"
    assert row.date_naissance == "1990-03-02"


def test_build_doublons_returns_none_for_none():
    assert pipeline.build_doublons(None) is None


def test_build_doublons_matches_same_identity(spark_session):
    df = spark_session.createDataFrame([
        ("ABASSURANCE", "1", "DUPONT", "Jean", "1980-01-01", "75001"),
        ("ASSUREPLUS", "2", "DUPONT", "Jean", "1980-01-01", "75001"),
    ], ["source", "id_source", "nom", "prenom", "date_naissance", "code_postal"])
    rows = pipeline.build_doublons(df).collect()
    assert len(rows) == 1
    assert rows[0].id_ab == "1"
    assert rows[0].id_ap == "2"


def test_build_doublons_does_not_match_different_postcodes(spark_session):
    df = spark_session.createDataFrame([
        ("ABASSURANCE", "1", "DUPONT", "Jean", "1980-01-01", "75001"),
        ("ASSUREPLUS", "2", "DUPONT", "Jean", "1980-01-01", "69000"),
    ], ["source", "id_source", "nom", "prenom", "date_naissance", "code_postal"])
    assert pipeline.build_doublons(df).count() == 0


def test_recompute_gold_skips_missing_tables(spark_session, monkeypatch):
    monkeypatch.setattr(pipeline, "build_clients", lambda spark: None)
    monkeypatch.setattr(pipeline, "build_contrats", lambda spark: None)
    monkeypatch.setattr(pipeline, "build_sinistres", lambda spark: None)
    monkeypatch.setattr(pipeline, "build_paiements", lambda spark: None)
    pipeline.recompute_gold(spark_session)


def test_recompute_gold_reports_builder_failure(spark_session, monkeypatch):
    monkeypatch.setattr(pipeline, "build_clients", lambda spark: (_ for _ in ()).throw(ValueError("boom")))
    monkeypatch.setattr(pipeline, "build_contrats", lambda spark: None)
    monkeypatch.setattr(pipeline, "build_sinistres", lambda spark: None)
    monkeypatch.setattr(pipeline, "build_paiements", lambda spark: None)
    with pytest.raises(RuntimeError, match="clients"):
        pipeline.recompute_gold(spark_session)
