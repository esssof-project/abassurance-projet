
from unittest.mock import MagicMock

import pytest
from pyspark.sql import functions as F

from spark import streaming_pipeline as p

# =========================================================
# 1. CLEANING
# =========================================================

def test_clean_string_oracle(spark_session):
    df = spark_session.createDataFrame(
        [
            ("Jean\u2019Dupont",),
            ("\u0080\u0082\u0085",),
            ("  Jean   Dupont  ",),
            ("NaN",),
            (None,),
        ],
        ["value"],
    )

    result = [
        r["value"]
        for r in df.select(
            p.clean_string("value").alias("value")
        ).collect()
    ]

    assert result[0] == "Jean'Dupont"
    assert result[1] == "E'..."
    assert result[2] == "Jean Dupont"
    assert result[3] == ""
    assert result[4] == ""


def test_clean_string_sqlserver(spark_session):
    df = spark_session.createDataFrame(
        [("Jean\u2019 Dupont",), (None,)],
        ["value"],
    )

    result = [
        r["value"]
        for r in df.select(
            p.clean_string("value", "sqlserver").alias("value")
        ).collect()
    ]

    assert result == ["Jean' Dupont", ""]


def test_clean_numeric(spark_session):
    df = spark_session.createDataFrame(
        [
            ("125.50 €",),
            ("abc",),
            (None,),
            ("-25.5",),
        ],
        ["value"],
    )

    result = [
        r["value"]
        for r in df.select(
            p.clean_numeric("value").alias("value")
        ).collect()
    ]

    assert result == [125.5, 0.0, 0.0, -25.5]


def test_clean_date(spark_session):
    df = spark_session.createDataFrame(
        [
            ("2024-02-03",),
            ("03/02/2024",),
            ("2024/02/03",),
            ("invalide",),
            (None,),
        ],
        ["value"],
    )

    result = [
        r["value"]
        for r in df.select(
            p.clean_date("value").alias("value")
        ).collect()
    ]

    assert result[0] == "2024-02-03"
    assert result[1] == "2024-02-03"
    assert result[2] == "2024-02-03"
    assert result[3] is None
    assert result[4] is None


# =========================================================
# 2. UTILITAIRES
# =========================================================

def test_envelope_schema():
    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("id", T.IntegerType())
    ])

    result = p.envelope_schema(schema)

    assert [f.name for f in result.fields] == [
        "op", "after", "before"
    ]


def test_sha_pseudo(spark_session):
    df = spark_session.createDataFrame(
        [
            ("TEST@example.com",),
            ("",),
            (None,),
        ],
        ["email"],
    )

    result = [
        r["hash"]
        for r in df.select(
            p.sha_pseudo(F.col("email")).alias("hash")
        ).collect()
    ]

    assert result[0] is not None
    assert len(result[0]) == 64
    assert result[1] is None
    assert result[2] is None


def test_epoch_conversions(spark_session):
    df = spark_session.createDataFrame(
        [(1, 1704067200000, 1704067200000000)],
        ["days", "ms", "us"],
    )

    result = df.select(
        p.epoch_days_str("days").alias("days"),
        p.epoch_ms_str("ms").alias("ms"),
        p.epoch_us_str("us").alias("us"),
    ).first()

    assert result["days"] == "1970-01-02"
    assert result["ms"].startswith("2024-01-01")
    assert result["us"].startswith("2024-01-01")


def test_union_or_none(spark_session):
    a = spark_session.createDataFrame([(1,)], ["id"])
    b = spark_session.createDataFrame([(2,)], ["id"])

    assert p.union_or_none(None, b).count() == 1
    assert p.union_or_none(a, None).count() == 1
    assert p.union_or_none(a, b).count() == 2
    assert p.union_or_none(None, None) is None


def test_latest(spark_session):
    df = spark_session.createDataFrame(
        [
            (1, "ancien", "2024-01-01 10:00:00", "u"),
            (1, "nouveau", "2024-01-02 10:00:00", "u"),
        ],
        ["id", "value", "event_ts", "op"],
    ).withColumn("event_ts", F.to_timestamp("event_ts"))

    result = p.latest(df, "id").first()

    assert result["value"] == "nouveau"
    assert "event_ts" not in p.latest(df, "id").columns
    assert "op" not in p.latest(df, "id").columns


def test_bronze_read_error():
    spark = MagicMock()
    filesystem = (
        spark._jvm.org.apache.hadoop.fs.Path.return_value
        .getFileSystem.return_value
    )
    filesystem.exists.return_value = True
    statuses = filesystem.listFiles.return_value
    statuses.hasNext.side_effect = [True, False]
    file_status = statuses.next.return_value
    file_status.isFile.return_value = True
    file_status.getPath.return_value.getName.return_value = "part-1.parquet"
    spark.read.parquet.side_effect = OSError("HDFS unavailable")

    assert p.bronze(spark, "topic") is None


# =========================================================
# 3. CLIENTS
# =========================================================

def test_build_clients_missing_sources(monkeypatch, spark_session):
    monkeypatch.setattr(p, "bronze", lambda spark, name: None)

    assert p.build_clients(spark_session) is None


def test_build_clients_ab(monkeypatch, spark_session):
    df = spark_session.createDataFrame(
        [
            (
                1, "Dupont", "Jean", 365,
                "jean@test.fr", "0600000000",
                "75001", "ACTIF",
                "2024-01-01 10:00:00", "c"
            )
        ],
        [
            "ab_client_id", "ab_nom", "ab_prenom",
            "ab_date_naissance", "ab_email",
            "ab_telephone", "ab_code_postal",
            "ab_statut_client", "event_ts", "op"
        ],
    ).withColumn("event_ts", F.to_timestamp("event_ts"))

    monkeypatch.setattr(
        p, "bronze",
        lambda spark, name: df if name == "dbab_ab_ab_client" else None
    )

    result = p.build_clients(spark_session).collect()

    assert len(result) == 1
    assert result[0]["source"] == "ABASSURANCE"


def test_build_clients_ap(monkeypatch, spark_session):
    df = spark_session.createDataFrame(
        [
            (
                10, "Jean Dupont", "1980-01-01",
                "jean@test.fr", "0600000000",
                "75001", "ACTIF",
                "2024-01-01 10:00:00", "c"
            )
        ],
        [
            "ap_user_id", "ap_full_name", "ap_birth_date",
            "ap_mail_address", "ap_phone_number",
            "ap_zip_code", "ap_customer_status",
            "event_ts", "op"
        ],
    ).withColumn("event_ts", F.to_timestamp("event_ts"))

    monkeypatch.setattr(
        p, "bronze",
        lambda spark, name: df if name == "dbap_ap_ap_users" else None
    )

    result = p.build_clients(spark_session).collect()

    assert len(result) == 1
    assert result[0]["source"] == "ASSUREPLUS"
    assert result[0]["nom"] == "Jean"


# =========================================================
# 4. DOUBLONS
# =========================================================

def test_build_doublons_none():
    assert p.build_doublons(None) is None


def test_build_doublons_detects_duplicates(spark_session):
    df = spark_session.createDataFrame(
        [
            ("ABASSURANCE", "1", "Dupont", "Jean", "1980-01-01", "75001"),
            ("ASSUREPLUS", "10", "dupont", "jean", "1980-01-01", "75001"),
        ],
        [
            "source", "id_source", "nom", "prenom",
            "date_naissance", "code_postal"
        ],
    )

    result = p.build_doublons(df).collect()

    assert len(result) == 1
    assert result[0]["id_ab"] == "1"
    assert result[0]["id_ap"] == "10"


# =========================================================
# 5. CONTRATS / SINISTRES / PAIEMENTS
# =========================================================

@pytest.mark.parametrize(
    "builder",
    [
        p.build_contrats,
        p.build_sinistres,
        p.build_paiements,
    ],
)
def test_builders_no_bronze(builder, monkeypatch, spark_session):
    monkeypatch.setattr(p, "bronze", lambda spark, name: None)

    assert builder(spark_session) is None


# =========================================================
# 6. GOLD
# =========================================================

def fake_dataframe():
    df = MagicMock()
    writer = df.write
    writer.mode.return_value = writer
    writer.format.return_value = writer
    writer.save.return_value = None
    return df


def test_recompute_gold_all_tables(monkeypatch):
    df = fake_dataframe()
    doublons = fake_dataframe()

    monkeypatch.setattr(p, "build_clients", lambda spark: df)
    monkeypatch.setattr(p, "build_contrats", lambda spark: df)
    monkeypatch.setattr(p, "build_sinistres", lambda spark: df)
    monkeypatch.setattr(p, "build_paiements", lambda spark: df)
    monkeypatch.setattr(p, "build_doublons", lambda clients: doublons)

    p.recompute_gold(MagicMock())

    assert df.write.save.call_count == 4
    doublons.write.save.assert_called_once()


def test_recompute_gold_missing_tables(monkeypatch):
    for name in [
        "build_clients",
        "build_contrats",
        "build_sinistres",
        "build_paiements",
    ]:
        monkeypatch.setattr(p, name, lambda spark: None)

    p.recompute_gold(MagicMock())


def test_recompute_gold_write_error(monkeypatch):
    df = fake_dataframe()
    df.write.save.side_effect = OSError("HDFS unavailable")

    monkeypatch.setattr(p, "build_clients", lambda spark: df)
    monkeypatch.setattr(p, "build_contrats", lambda spark: None)
    monkeypatch.setattr(p, "build_sinistres", lambda spark: None)
    monkeypatch.setattr(p, "build_paiements", lambda spark: None)
    monkeypatch.setattr(p, "build_doublons", lambda clients: None)

    with pytest.raises(RuntimeError, match="clients"):
        p.recompute_gold(MagicMock())


def test_recompute_gold_doublons_none(monkeypatch):
    df = fake_dataframe()

    monkeypatch.setattr(p, "build_clients", lambda spark: df)
    monkeypatch.setattr(p, "build_contrats", lambda spark: None)
    monkeypatch.setattr(p, "build_sinistres", lambda spark: None)
    monkeypatch.setattr(p, "build_paiements", lambda spark: None)
    monkeypatch.setattr(p, "build_doublons", lambda clients: None)

    p.recompute_gold(MagicMock())


# =========================================================
# 7. MAIN
# =========================================================

def configure_builder(monkeypatch, spark):
    builder = MagicMock()
    builder.appName.return_value = builder
    builder.config.return_value = builder
    builder.getOrCreate.return_value = spark

    monkeypatch.setattr(p.SparkSession, "builder", builder)


def test_main_keyboard_interrupt(monkeypatch):
    spark = MagicMock()
    spark.streams.awaitAnyTermination.side_effect = KeyboardInterrupt()

    configure_builder(monkeypatch, spark)

    queries = []

    def start_writer(spark, topic):
        query = MagicMock()
        query.isActive = True
        query.name = topic
        queries.append(query)
        return query

    gold = MagicMock()
    gold.isActive = True
    gold.name = "gold"
    queries.append(gold)

    monkeypatch.setattr(p, "start_hdfs_writer", start_writer)
    monkeypatch.setattr(p, "start_gold", lambda spark: gold)

    p.main()

    assert len(queries) == 9
    for query in queries:
        query.stop.assert_called_once()

    spark.stop.assert_called_once()


def test_main_startup_error(monkeypatch):
    spark = MagicMock()
    configure_builder(monkeypatch, spark)

    monkeypatch.setattr(
        p,
        "start_hdfs_writer",
        lambda spark, topic: (_ for _ in ()).throw(
            RuntimeError("startup failed")
        ),
    )

    with pytest.raises(RuntimeError, match="startup failed"):
        p.main()

    spark.stop.assert_called_once()
