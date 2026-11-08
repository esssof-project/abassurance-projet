"""Job Spark BATCH (demonstration du data flow du Bloc 1) :
lecture des topics du producer -> data cleaning -> matrice de doublons ->
UUID client global -> Data Lake HDFS (Parquet).
NB : en mode temps reel, c'est streaming_pipeline.py (Debezium CDC) qui
ecrit dans HDFS ; ce job demontre le socle batch + Hadoop."""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType)

KAFKA = "kafka:9092"
HDFS = "hdfs://namenode:9000"

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


def main():
    spark = (SparkSession.builder.appName("AbAssurance-Pipeline-Batch")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    clients = read_topic(spark, "topic-clients")

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
                 .select("id_ab", "id_ap", "nom", "prenom", "code_postal")
    doublons.write.mode("overwrite").parquet(HDFS + "/datalake/gold/doublons")

    # --- Client consolide 360 avec UUID global (bloc 2 : UUIDCLIENTGLOBAL) ---
    w = Window.partitionBy("email_pseudo").orderBy("source")
    clients_360 = (clients.withColumn("uuid_client_global", F.expr("uuid()"))
        .withColumn("rn", F.row_number().over(w)).filter("rn=1").drop("rn"))
    clients_360.write.mode("overwrite").parquet(HDFS + "/datalake/gold/clients_360")

    print("Job batch termine : Data Lake HDFS alimente "
          "(doublons + clients_360 au format Parquet).")
    spark.stop()


if __name__ == "__main__":
    main()
