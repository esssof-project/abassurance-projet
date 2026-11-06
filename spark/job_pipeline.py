"""Job Spark : lecture Kafka -> data cleaning -> matrice de doublons ->
UUID client global -> Data Lake HDFS (Parquet) + Datamart Postgres (tables gold)."""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType)

KAFKA = "kafka:9092"
HDFS = "hdfs://namenode:9000"
PG_URL = "jdbc:postgresql://postgres:5432/datamart"
PG_PROPS = {"user": "admin", "password": "Admin123!", "driver": "org.postgresql.Driver"}

SCHEMAS = {
    "topic-clients": StructType([
        StructField("source", StringType()), StructField("id_source", StringType()),
        StructField("nom", StringType()), StructField("prenom", StringType()),
        StructField("date_naissance", StringType()), StructField("email_pseudo", StringType()),
        StructField("telephone_pseudo", StringType()), StructField("code_postal", StringType()),
        StructField("statut", StringType())]),
    "topic-contrats": StructType([
        StructField("source", StringType()), StructField("ref_contrat", StringType()),
        StructField("id_client_source", StringType()), StructField("type_assurance", StringType()),
        StructField("date_debut", StringType()), StructField("prime_annuelle", DoubleType()),
        StructField("statut", StringType())]),
    "topic-sinistres": StructType([
        StructField("source", StringType()), StructField("id_sinistre", StringType()),
        StructField("ref_contrat", StringType()), StructField("date_sinistre", StringType()),
        StructField("montant_estime", DoubleType()), StructField("statut", StringType())]),
    "topic-paiements": StructType([
        StructField("source", StringType()), StructField("id_paiement", StringType()),
        StructField("ref_contrat", StringType()), StructField("date_paiement", StringType()),
        StructField("montant", DoubleType()), StructField("mode_paiement", StringType())]),
}


def read_topic(spark, topic):
    return (spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), SCHEMAS[topic]).alias("d"))
        .select("d.*"))


def to_pg(df, table):
    df.write.jdbc(url=PG_URL, table=table, mode="overwrite", properties=PG_PROPS)


def main():
    spark = (SparkSession.builder.appName("AbAssurance-Pipeline")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.postgresql:postgresql:42.7.2")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    clients = read_topic(spark, "topic-clients")
    contrats = read_topic(spark, "topic-contrats")
    sinistres = read_topic(spark, "topic-sinistres")
    paiements = read_topic(spark, "topic-paiements")

    # --- Data cleaning : trim, dedoublonnage intra-source, normalisation CP ---
    clients = (clients.dropDuplicates(["source", "id_source"])
        .withColumn("code_postal", F.regexp_replace("code_postal", "\\s", "")))

    # --- MATRICE DE DOUBLONS : clients presents chez AB et AP ---
    # Correctif du bug identifie (Dossier 3, Q3) : filtrer les empreintes NULL
    # avant la jointure, sinon un email absent produirait un faux doublon sur None.
    ab = (clients.filter("source='ABASSURANCE'")
        .filter(F.col("email_pseudo").isNotNull())
        .withColumnRenamed("id_source", "id_ab"))
    ap = (clients.filter("source='ASSUREPLUS'")
        .filter(F.col("email_pseudo").isNotNull())
        .withColumnRenamed("id_source", "id_ap"))
    doublons = ab.join(ap, on=["email_pseudo"], how="inner") \
                 .select("id_ab", "id_ap",
                         F.col("nom_ab").alias("nom") if "nom_ab" in ab.columns else "nom",
                         "prenom", "code_postal")
    to_pg(doublons, "gold_doublons")

    # --- Client consolide 360 avec UUID global (bloc 2 : UUIDCLIENTGLOBAL) ---
    w = Window.partitionBy("email_pseudo").orderBy("source")
    clients_360 = (clients.withColumn("uuid_client_global", F.expr("uuid()"))
        .withColumn("rn", F.row_number().over(w)).filter("rn=1").drop("rn"))
    clients_360.write.mode("overwrite").parquet(HDFS + "/datalake/gold/clients_360")

    # --- STATISTIQUES ---
    repart_clients = clients.groupBy("source", "code_postal") \
        .count().withColumnRenamed("count", "nb_clients")
    to_pg(repart_clients, "gold_repartition_clients")

    stats_contrats = contrats.groupBy("source", "type_assurance", "statut") \
        .agg(F.count("*").alias("nb_contrats"),
             F.sum("prime_annuelle").alias("total_primes"))
    to_pg(stats_contrats, "gold_stats_contrats")

    stats_sinistres = sinistres.groupBy("source", "statut") \
        .agg(F.count("*").alias("nb_sinistres"),
             F.sum("montant_estime").alias("montant_total"))
    to_pg(stats_sinistres, "gold_stats_sinistres")

    stats_financieres = paiements.groupBy("source", "mode_paiement") \
        .agg(F.count("*").alias("nb_paiements"),
             F.sum("montant").alias("ca_total"))
    to_pg(stats_financieres, "gold_stats_financieres")

    print("Pipeline termine : HDFS (Data Lake) + Datamart Postgres alimentes.")
    spark.stop()


if __name__ == "__main__":
    main()
