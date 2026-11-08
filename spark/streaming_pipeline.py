"""Spark Structured Streaming : CDC Debezium (Kafka) -> HDFS (Parquet)."""

from pyspark.sql import SparkSession, functions as F, types as T

KAFKA = "kafka:9092"
HDFS = "hdfs://namenode:9000"

# Schemas "after" des evenements Debezium.
# NB decimal.handling.mode=double : NUMERIC -> double ;
#    DATE -> int (jours depuis 1970-01-01), TIMESTAMP -> bigint (millis).
AFTER = {
    "dbab.ab.ab_client": [
        ("ab_client_id", T.IntegerType()),
        ("ab_nom", T.StringType()),
        ("ab_prenom", T.StringType()),
        ("ab_date_naissance", T.IntegerType()),
        ("ab_email", T.StringType()),
        ("ab_telephone", T.StringType()),
        ("ab_code_postal", T.StringType()),
        ("ab_statut_client", T.StringType()),
    ],
    "dbab.ab.ab_contrat": [
        ("ab_policy_number", T.StringType()),
        ("ab_client_id", T.IntegerType()),
        ("ab_type_assurance", T.StringType()),
        ("ab_date_debut", T.IntegerType()),
        ("ab_prime_annuelle", T.DoubleType()),
        ("ab_statut_contrat", T.StringType()),
    ],
    "dbab.ab.ab_sinistre": [
        ("ab_claim_id", T.IntegerType()),
        ("ab_policy_number", T.StringType()),
        ("ab_date_sinistre", T.LongType()),
        ("ab_montant_estime", T.DoubleType()),
        ("ab_statut_sinistre", T.StringType()),
    ],
    "dbab.ab.ab_paiement": [
        ("ab_payment_id", T.IntegerType()),
        ("ab_policy_number", T.StringType()),
        ("ab_date_paiement", T.IntegerType()),
        ("ab_montant", T.DoubleType()),
        ("ab_mode_paiement", T.StringType()),
    ],
    "dbap.ap.ap_users": [
        ("ap_user_id", T.IntegerType()),
        ("ap_full_name", T.StringType()),
        ("ap_birth_date", T.StringType()),
        ("ap_mail_address", T.StringType()),
        ("ap_phone_number", T.StringType()),
        ("ap_zip_code", T.StringType()),
        ("ap_customer_status", T.StringType()),
    ],
    "dbap.ap.ap_contracts": [
        ("ap_contract_ref", T.StringType()),
        ("ap_user_id", T.IntegerType()),
        ("ap_product_code", T.StringType()),
        ("ap_start_date", T.LongType()),
        ("ap_monthly_premium", T.DoubleType()),
        ("ap_contract_state", T.StringType()),
    ],
    "dbap.ap.ap_claims": [
        ("ap_sinistre_num", T.IntegerType()),
        ("ap_contract_ref", T.StringType()),
        ("ap_incident_date", T.StringType()),
        ("ap_estimated_amount", T.DoubleType()),
        ("ap_claim_status", T.StringType()),
    ],
    "dbap.ap.ap_payments": [
        ("ap_payment_ref", T.StringType()),
        ("ap_contract_ref", T.StringType()),
        ("ap_payment_datetime", T.LongType()),
        ("ap_amount_paid", T.DoubleType()),
        ("ap_payment_channel", T.StringType()),
    ],
}


def envelope_schema(after_schema):
    """Enveloppe Debezium : op + after + before (les autres champs sont ignores)."""
    return T.StructType([
        T.StructField("op", T.StringType()),
        T.StructField("after", after_schema),
        T.StructField("before", after_schema),
    ])


def start_hdfs_writer(spark, topic):
    """Demarre un flux Kafka -> HDFS : evenements parses et aplatis en Parquet.
    Chaque micro-batch ecrit un fichier Parquet dans /datalake/cdc/<topic> ;
    le Dataviz deduplique ensuite par cle (dernier evenement = etat courant)."""
    after_schema = T.StructType([T.StructField(n, t) for n, t in AFTER[topic]])
    name = topic.replace(".", "_")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA)
           .option("subscribe", topic)
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "false")
           .load()
           .select(F.col("value").cast("string").alias("v"),
                   F.col("timestamp").alias("event_ts")))

    # Deux formats possibles selon la config du worker Kafka Connect :
    # enveloppe directe {op, after, ...} (schemas.enable=false) ou
    # wrapper {"schema":..., "payload":{op, after, ...}} (schemas.enable=true).
    wrapped = F.from_json("v", T.StructType([
        T.StructField("payload", envelope_schema(after_schema))])).getField("payload")
    direct = F.from_json("v", envelope_schema(after_schema))
    parsed = raw.select("event_ts", F.coalesce(wrapped, direct).alias("e"))

    flat = parsed.select("event_ts",
                         F.col("e.op").alias("op"),
                         *[F.col(f"e.after.{n}").alias(n) for n, _ in AFTER[topic]])

    return (flat.writeStream
            .format("parquet")
            .option("path", f"{HDFS}/datalake/cdc/{name}")
            .option("checkpointLocation", f"/tmp/checkpoints/{name}")
            .trigger(processingTime="5 seconds")
            .start())


GOLD = f"{HDFS}/datalake/gold"


# =============================================================================
# ZONE GOLD : consolidation + indicateurs calcules dans Spark.
# La zone bronze (evenements CDC bruts) reste la source de verite ; la gold
# est recalculee apres chaque batch CDC et lue telle quelle par le Dataviz
# (qui retombe sur son calcul bronze si la gold n'existe pas encore).
# =============================================================================

def sha_pseudo(col):
    """Pseudonymisation identique au Dataviz : sha256(lower(trim(v))), NULL si vide."""
    s = F.trim(F.coalesce(col.cast("string"), F.lit("")))
    return F.when(F.length(s) > 0, F.sha2(F.lower(s), 256))


def bronze(spark, name):
    """Lit toute la zone bronze d'un topic (None si le dossier est absent)."""
    try:
        return spark.read.parquet(f"{HDFS}/datalake/cdc/{name}")
    except Exception:
        return None


def latest(df, pk):
    """Etat courant : dernier evenement par cle primaire."""
    from pyspark.sql.window import Window
    w = Window.partitionBy(pk).orderBy(F.col("event_ts").desc())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1).drop("_rn", "event_ts", "op"))


def epoch_days_str(colname):
    """Jours depuis 1970 (Debezium DATE) -> 'YYYY-MM-DD'."""
    return F.date_format(
        F.expr(f"date_add(DATE '1970-01-01', CAST({colname} AS INT))"), "yyyy-MM-dd")


def epoch_us_str(colname):
    """Microsecondes depuis 1970 (Debezium Timestamp) -> 'YYYY-MM-DD HH:mm:ss'."""
    return F.date_format((F.col(colname) / 1000000).cast("timestamp"), "yyyy-MM-dd HH:mm:ss")


def union_or_none(a, b):
    if a is None:
        return b
    if b is None:
        return a
    return a.unionByName(b)


def build_clients(spark):
    """Clients consolides AB + AP : meme modele unifie que le Dataviz."""
    ab = bronze(spark, "dbab_ab_ab_client")
    ap = bronze(spark, "dbap_ap_ap_users")
    ab_df = None
    if ab is not None:
        ab_df = (latest(ab, "ab_client_id")
                 .select(F.lit("ABASSURANCE").alias("source"),
                         F.col("ab_client_id").cast("string").alias("id_source"),
                         F.col("ab_nom").alias("nom"),
                         F.col("ab_prenom").alias("prenom"),
                         epoch_days_str("ab_date_naissance").alias("date_naissance"),
                         sha_pseudo(F.col("ab_email")).alias("email_pseudo"),
                         sha_pseudo(F.col("ab_telephone")).alias("telephone_pseudo"),
                         F.col("ab_code_postal").alias("code_postal"),
                         F.col("ab_statut_client").alias("statut")))
    ap_df = None
    if ap is not None:
        parts = F.split(F.coalesce(F.col("ap_full_name"), F.lit("")), " ")
        ap_df = (latest(ap, "ap_user_id")
                 .select(F.lit("ASSUREPLUS").alias("source"),
                         F.col("ap_user_id").cast("string").alias("id_source"),
                         parts.getItem(0).alias("nom"),
                         F.coalesce(parts.getItem(1), F.lit("")).alias("prenom"),
                         F.coalesce(F.col("ap_birth_date"), F.lit("")).alias("date_naissance"),
                         sha_pseudo(F.col("ap_mail_address")).alias("email_pseudo"),
                         sha_pseudo(F.col("ap_phone_number")).alias("telephone_pseudo"),
                         F.col("ap_zip_code").alias("code_postal"),
                         F.col("ap_customer_status").alias("statut")))
    return union_or_none(ab_df, ap_df)


def build_doublons(clients):
    """Matrice de doublons inter-assureurs (meme email pseudonymise)."""
    if clients is None:
        return None
    ab = clients.filter((F.col("source") == "ABASSURANCE") & F.col("email_pseudo").isNotNull())
    ap = clients.filter((F.col("source") == "ASSUREPLUS") & F.col("email_pseudo").isNotNull()) \
                .withColumnRenamed("id_source", "id_ap")
    m = ab.join(ap, "email_pseudo")
    return m.select(F.col("id_source").alias("id_ab"), F.col("id_ap"),
                    F.col("nom"), F.col("prenom"), F.col("code_postal"))


def build_contrats(spark):
    ab = bronze(spark, "dbab_ab_ab_contrat")
    ap = bronze(spark, "dbap_ap_ap_contracts")
    ab_df = None
    if ab is not None:
        ab_df = (latest(ab, "ab_policy_number")
                 .select(F.lit("ABASSURANCE").alias("source"),
                         F.col("ab_policy_number").alias("ref_contrat"),
                         F.col("ab_client_id").cast("string").alias("id_client_source"),
                         F.col("ab_type_assurance").alias("type_assurance"),
                         epoch_days_str("ab_date_debut").alias("date_debut"),
                         F.col("ab_prime_annuelle").cast("double").alias("prime_annuelle"),
                         F.col("ab_statut_contrat").alias("statut")))
    ap_df = None
    if ap is not None:
        ap_df = (latest(ap, "ap_contract_ref")
                 .select(F.lit("ASSUREPLUS").alias("source"),
                         F.col("ap_contract_ref").alias("ref_contrat"),
                         F.col("ap_user_id").cast("string").alias("id_client_source"),
                         F.col("ap_product_code").alias("type_assurance"),
                         epoch_us_str("ap_start_date").alias("date_debut"),
                         (F.col("ap_monthly_premium").cast("double") * 12).alias("prime_annuelle"),
                         F.col("ap_contract_state").alias("statut")))
    return union_or_none(ab_df, ap_df)


def build_sinistres(spark):
    ab = bronze(spark, "dbab_ab_ab_sinistre")
    ap = bronze(spark, "dbap_ap_ap_claims")
    ab_df = None
    if ab is not None:
        ab_df = (latest(ab, "ab_claim_id")
                 .select(F.lit("ABASSURANCE").alias("source"),
                         F.col("ab_claim_id").cast("string").alias("id_sinistre"),
                         F.col("ab_policy_number").alias("ref_contrat"),
                         epoch_us_str("ab_date_sinistre").alias("date_sinistre"),
                         F.col("ab_montant_estime").cast("double").alias("montant_estime"),
                         F.col("ab_statut_sinistre").alias("statut")))
    ap_df = None
    if ap is not None:
        ap_df = (latest(ap, "ap_sinistre_num")
                 .select(F.lit("ASSUREPLUS").alias("source"),
                         F.col("ap_sinistre_num").cast("string").alias("id_sinistre"),
                         F.col("ap_contract_ref").alias("ref_contrat"),
                         F.coalesce(F.col("ap_incident_date"), F.lit("")).alias("date_sinistre"),
                         F.col("ap_estimated_amount").cast("double").alias("montant_estime"),
                         F.col("ap_claim_status").alias("statut")))
    return union_or_none(ab_df, ap_df)


def build_paiements(spark):
    ab = bronze(spark, "dbab_ab_ab_paiement")
    ap = bronze(spark, "dbap_ap_ap_payments")
    ab_df = None
    if ab is not None:
        ab_df = (latest(ab, "ab_payment_id")
                 .select(F.lit("ABASSURANCE").alias("source"),
                         F.col("ab_payment_id").cast("string").alias("id_paiement"),
                         F.col("ab_policy_number").alias("ref_contrat"),
                         epoch_days_str("ab_date_paiement").alias("date_paiement"),
                         F.col("ab_montant").cast("double").alias("montant"),
                         F.col("ab_mode_paiement").alias("mode_paiement")))
    ap_df = None
    if ap is not None:
        ap_df = (latest(ap, "ap_payment_ref")
                 .select(F.lit("ASSUREPLUS").alias("source"),
                         F.col("ap_payment_ref").alias("id_paiement"),
                         F.col("ap_contract_ref").alias("ref_contrat"),
                         epoch_us_str("ap_payment_datetime").alias("date_paiement"),
                         F.col("ap_amount_paid").cast("double").alias("montant"),
                         F.col("ap_payment_channel").alias("mode_paiement")))
    return union_or_none(ab_df, ap_df)


def recompute_gold(spark):
    """Reecrit les tables gold depuis la zone bronze (appelee a chaque batch)."""
    written = []
    for name, builder in [("clients", lambda: build_clients(spark)),
                          ("contrats", lambda: build_contrats(spark)),
                          ("sinistres", lambda: build_sinistres(spark)),
                          ("paiements", lambda: build_paiements(spark))]:
        try:
            df = builder()
            if df is None:
                continue
            df.write.mode("overwrite").parquet(f"{GOLD}/{name}")
            written.append(name)
            if name == "clients":
                d = build_doublons(df)
                if d is not None:
                    d.write.mode("overwrite").parquet(f"{GOLD}/doublons")
                    written.append("doublons")
        except Exception as e:
            # fichier bronze partiel en cours d'ecriture : retry au batch suivant
            print(f"[gold] {name} reporte au prochain batch : {e}")
    print(f"[gold] tables a jour : {', '.join(written) if written else 'aucune'}")


def start_gold(spark):
    """Flux declencheur : l'arrivee d'evenements CDC relance le calcul gold.
    Le contenu du batch est ignore : la bronze reste la source de verite."""
    def recompute(batch_df, batch_id):
        try:
            recompute_gold(spark)
        except Exception as e:
            print(f"[gold] recompute echoue : {e}")
    q = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", KAFKA)
         .option("subscribe", ",".join(AFTER.keys()))
         .option("startingOffsets", "earliest")
         .option("failOnDataLoss", "false")
         .load())
    return (q.writeStream
            .foreachBatch(recompute)
            .option("checkpointLocation", "/tmp/checkpoints/gold")
            .trigger(processingTime="15 seconds")
            .start())


def main():
    spark = (SparkSession.builder.appName("AbAssurance-Streaming-HDFS")
             .config("spark.jars.ivy", "/tmp/.ivy2")
             .config("spark.jars.packages",
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    for topic in AFTER:
        start_hdfs_writer(spark, topic)
        print(f"Flux actif : {topic} -> {HDFS}/datalake/cdc/{topic.replace('.', '_')}")
    start_gold(spark)
    print(f"Gold actif : consolidation + doublons -> {GOLD} (recalcul a chaque batch).")

    print("Spark Streaming actif : CDC Debezium -> HDFS Parquet (5 s de latence).")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
