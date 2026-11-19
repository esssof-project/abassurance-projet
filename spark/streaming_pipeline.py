
"""Spark Structured Streaming : CDC Debezium (Kafka) -> HDFS (Parquet)."""

from pyspark.sql import SparkSession, functions as F, types as T


KAFKA = "kafka:9092"
HDFS = "hdfs://namenode:9000"
GOLD = f"{HDFS}/datalake/gold"


# =============================================================================
# 1. NETTOYAGE DES DONNEES
# =============================================================================

def clean_string(col, source="oracle"):
    """Nettoie une colonne Spark et normalise les caractères."""

    c = F.col(col) if isinstance(col, str) else col
    cleaned = c.cast("string")

    # Normalisation des apostrophes et guillemets.
    cleaned = F.regexp_replace(cleaned, r'[\u2018\u2019]', "'")
    cleaned = F.regexp_replace(cleaned, r'[\u201C\u201D]', '"')
    cleaned = F.regexp_replace(cleaned, r'[\u201E\u201F]', '"')

    # Corrections des caractères Windows-1252 mal décodés.
    if source == "oracle":
        replacements = [
            (r'\u0080', 'E'),
            (r'\u0082', "'"),
            (r'\u0084', '"'),
            (r'\u0085', '...'),
            (r'\u0086', '"'),
            (r'\u0087', '"'),
            (r'\u0088', '^'),
            (r'\u0089', '%'),
            (r'\u008A', 'S'),
            (r'\u008B', '<'),
            (r'\u008C', 'OE'),
            (r'\u008E', 'Z'),
            (r'\u0091', "'"),
            (r'\u0092', "'"),
            (r'\u0093', '"'),
            (r'\u0094', '"'),
            (r'\u0095', '*'),
            (r'\u0096', '-'),
            (r'\u0097', '-'),
            (r'\u0098', '~'),
            (r'\u0099', 'TM'),
            (r'\u009A', 's'),
            (r'\u009B', '>'),
            (r'\u009C', 'oe'),
            (r'\u009E', 'z'),
            (r'\u009F', 'Y'),
        ]

        for pattern, replacement in replacements:
            cleaned = F.regexp_replace(
                cleaned, pattern, replacement
            )

    # Suppression des caractères de contrôle restants.
    cleaned = F.regexp_replace(
        cleaned, r'[\x00-\x1F\x7F-\x9F]', ''
    )

    # Normalisation des espaces.
    cleaned = F.regexp_replace(cleaned, r'\s+', ' ')
    cleaned = F.trim(cleaned)

    return F.when(
        c.isNull()
        | (F.lower(cleaned) == "nan")
        | (F.length(cleaned) == 0),
        F.lit("")
    ).otherwise(cleaned)


def clean_numeric(col):
    """Nettoie une colonne numérique et retourne un double."""

    c = F.col(col) if isinstance(col, str) else col

    cleaned_str = F.regexp_replace(
        c.cast("string"),
        r'[^0-9.-]',
        ''
    )

    numeric_value = cleaned_str.cast("double")

    return F.coalesce(numeric_value, F.lit(0.0))


def clean_date(col):
    """Normalise plusieurs formats de date en YYYY-MM-DD."""

    c = F.col(col) if isinstance(col, str) else col

    date_formats = [
        "yyyy-MM-dd",
        "dd/MM/yyyy",
        "MM/dd/yyyy",
        "dd-MM-yyyy",
        "yyyy/MM/dd",
    ]

    parsed_dates = [
        F.to_date(c, fmt)
        for fmt in date_formats
    ]

    result = F.coalesce(*parsed_dates)

    return F.date_format(result, "yyyy-MM-dd")


# =============================================================================
# 2. SCHEMAS DEBEZIUM
# =============================================================================

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
    """Schéma de l'enveloppe Debezium."""

    return T.StructType([
        T.StructField("op", T.StringType()),
        T.StructField("after", after_schema),
        T.StructField("before", after_schema),
    ])


# =============================================================================
# 3. ZONE BRONZE : KAFKA -> HDFS
# =============================================================================

def start_hdfs_writer(spark, topic):
    """Lit un topic Kafka et écrit les événements en Parquet dans HDFS."""

    after_schema = T.StructType([
        T.StructField(name, dtype)
        for name, dtype in AFTER[topic]
    ])

    name = topic.replace(".", "_")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.col("value").cast("string").alias("v"),
            F.col("timestamp").alias("event_ts")
        )
    )

    # Format Debezium avec wrapper payload.
    wrapped = F.from_json(
        "v",
        T.StructType([
            T.StructField(
                "payload",
                envelope_schema(after_schema)
            )
        ])
    ).getField("payload")

    # Format Debezium direct.
    direct = F.from_json(
        "v",
        envelope_schema(after_schema)
    )

    parsed = raw.select(
        "event_ts",
        F.coalesce(wrapped, direct).alias("e")
    )

    flat = parsed.select(
        "event_ts",
        F.col("e.op").alias("op"),
        *[
            F.col(f"e.after.{name}").alias(name)
            for name, _ in AFTER[topic]
        ]
    )

    output_path = f"{HDFS}/datalake/cdc/{name}"

    return (
        flat.writeStream
        .queryName(f"bronze_{name}")
        .format("parquet")
        .option("path", output_path)
        .option(
            "checkpointLocation",
            f"/tmp/checkpoints/{name}"
        )
        .trigger(processingTime="5 seconds")
        .start()
    )


# =============================================================================
# 4. FONCTIONS UTILITAIRES GOLD
# =============================================================================

def sha_pseudo(col):
    """Pseudonymisation SHA-256 des données sensibles."""

    s = F.trim(
        F.coalesce(col.cast("string"), F.lit(""))
    )

    return F.when(
        F.length(s) > 0,
        F.sha2(F.lower(s), 256)
    )


def bronze(spark, name):
    """Lit un dossier Bronze. Retourne None si absent."""

    path = f"{HDFS}/datalake/cdc/{name}"

    try:
        return spark.read.parquet(path)

    except Exception as e:
        print(f"[bronze] Lecture impossible {path}: {e}")
        return None


def latest(df, pk):
    """Conserve le dernier événement CDC par clé primaire."""

    from pyspark.sql.window import Window

    w = Window.partitionBy(pk).orderBy(
        F.col("event_ts").desc()
    )

    return (
        df.withColumn(
            "_rn",
            F.row_number().over(w)
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn", "event_ts", "op")
    )


def epoch_days_str(colname):
    """Convertit les jours depuis 1970 en YYYY-MM-DD."""

    return F.date_format(
        F.expr(
            f"date_add(DATE '1970-01-01', "
            f"CAST({colname} AS INT))"
        ),
        "yyyy-MM-dd"
    )


def epoch_ms_str(colname):
    """Convertit les millisecondes depuis 1970 en timestamp."""

    return F.date_format(
        (
            F.col(colname).cast("double") / 1000
        ).cast("timestamp"),
        "yyyy-MM-dd HH:mm:ss"
    )


def union_or_none(a, b):
    """Unionne deux DataFrames lorsque disponibles."""

    if a is None:
        return b

    if b is None:
        return a

    return a.unionByName(b)


# =============================================================================
# 5. CONSOLIDATION DES CLIENTS
# =============================================================================

def build_clients(spark):
    """Consolide les clients ABASSURANCE et ASSUREPLUS."""

    ab = bronze(spark, "dbab_ab_ab_client")
    ap = bronze(spark, "dbap_ap_ap_users")

    ab_df = None
    ap_df = None

    # -------------------------
    # Clients ABASSURANCE
    # -------------------------

    if ab is not None:
        ab_df = (
            latest(ab, "ab_client_id")
            .select(
                F.lit("ABASSURANCE").alias("source"),

                F.col("ab_client_id")
                .cast("string")
                .alias("id_source"),

                clean_string(
                    F.col("ab_nom"), "oracle"
                ).alias("nom"),

                clean_string(
                    F.col("ab_prenom"), "oracle"
                ).alias("prenom"),

                epoch_days_str(
                    "ab_date_naissance"
                ).alias("date_naissance"),

                sha_pseudo(
                    clean_string(
                        F.col("ab_email"), "oracle"
                    )
                ).alias("email_pseudo"),

                sha_pseudo(
                    clean_string(
                        F.col("ab_telephone"), "oracle"
                    )
                ).alias("telephone_pseudo"),

                clean_string(
                    F.col("ab_code_postal"), "oracle"
                ).alias("code_postal"),

                clean_string(
                    F.col("ab_statut_client"), "oracle"
                ).alias("statut")
            )
        )

    # -------------------------
    # Clients ASSUREPLUS
    # -------------------------

    if ap is not None:
        # Séparation du nom complet.
        parts = F.split(
            F.trim(
                F.coalesce(
                    clean_string(
                        F.col("ap_full_name"), "sqlserver"
                    ),
                    F.lit("")
                )
            ),
            r"\s+"
        )

        ap_df = (
            latest(ap, "ap_user_id")
            .select(
                F.lit("ASSUREPLUS").alias("source"),

                F.col("ap_user_id")
                .cast("string")
                .alias("id_source"),

                clean_string(
                    parts.getItem(0), "sqlserver"
                ).alias("nom"),

                clean_string(
                    F.coalesce(
                        parts.getItem(1),
                        F.lit("")
                    ),
                    "sqlserver"
                ).alias("prenom"),

                clean_date(
                    F.col("ap_birth_date")
                ).alias("date_naissance"),

                sha_pseudo(
                    clean_string(
                        F.col("ap_mail_address"), "sqlserver"
                    )
                ).alias("email_pseudo"),

                sha_pseudo(
                    clean_string(
                        F.col("ap_phone_number"), "sqlserver"
                    )
                ).alias("telephone_pseudo"),

                clean_string(
                    F.col("ap_zip_code"), "sqlserver"
                ).alias("code_postal"),

                clean_string(
                    F.col("ap_customer_status"), "sqlserver"
                ).alias("statut")
            )
        )

    return union_or_none(ab_df, ap_df)


# =============================================================================
# 6. DETECTION DES DOUBLONS
# =============================================================================

def build_doublons(clients):
    """Détecte les clients communs aux deux assureurs."""

    if clients is None:
        return None

    def dedup_key(df, prefix):
        key = F.concat_ws(
            "|",

            F.coalesce(
                F.lower(
                    F.trim(F.col(f"{prefix}_nom"))
                ),
                F.lit("")
            ),

            F.coalesce(
                F.lower(
                    F.trim(F.col(f"{prefix}_prenom"))
                ),
                F.lit("")
            ),

            F.coalesce(
                F.col(
                    f"{prefix}_date_naissance"
                ).cast("string"),
                F.lit("")
            ),

            F.coalesce(
                F.col(
                    f"{prefix}_code_postal"
                ).cast("string"),
                F.lit("")
            )
        )

        return df.withColumn(
            "dedup_key",
            F.sha2(key, 256)
        )

    # Clients ABASSURANCE.
    ab = (
        clients
        .filter(F.col("source") == "ABASSURANCE")
        .select(
            F.col("id_source").alias("ab_id"),
            F.col("nom").alias("ab_nom"),
            F.col("prenom").alias("ab_prenom"),
            F.col("date_naissance").alias(
                "ab_date_naissance"
            ),
            F.col("code_postal").alias(
                "ab_code_postal"
            )
        )
    )

    # Clients ASSUREPLUS.
    ap = (
        clients
        .filter(F.col("source") == "ASSUREPLUS")
        .select(
            F.col("id_source").alias("ap_id"),
            F.col("nom").alias("ap_nom"),
            F.col("prenom").alias("ap_prenom"),
            F.col("date_naissance").alias(
                "ap_date_naissance"
            ),
            F.col("code_postal").alias(
                "ap_code_postal"
            )
        )
    )

    ab = dedup_key(ab, "ab").alias("ab")
    ap = dedup_key(ap, "ap").alias("ap")

    return (
        ab.join(
            ap,
            F.col("ab.dedup_key") == F.col("ap.dedup_key"),
            "inner"
        )
        .select(
            F.col("ab.ab_id").alias("id_ab"),
            F.col("ap.ap_id").alias("id_ap"),
            F.col("ab.ab_nom").alias("nom"),
            F.col("ab.ab_prenom").alias("prenom"),
            F.col("ab.ab_date_naissance").alias(
                "date_naissance"
            ),
            F.col("ab.ab_code_postal").alias(
                "code_postal"
            )
        )
        .distinct()
    )


# =============================================================================
# 7. CONSOLIDATION DES CONTRATS
# =============================================================================

def build_contrats(spark):
    ab = bronze(spark, "dbab_ab_ab_contrat")
    ap = bronze(spark, "dbap_ap_ap_contracts")

    ab_df = None
    ap_df = None

    if ab is not None:
        ab_df = (
            latest(ab, "ab_policy_number")
            .select(
                F.lit("ABASSURANCE").alias("source"),
                F.col("ab_policy_number").alias("ref_contrat"),
                F.col("ab_client_id").cast("string").alias(
                    "id_client_source"
                ),
                clean_string(
                    F.col("ab_type_assurance"), "oracle"
                ).alias("type_assurance"),
                epoch_days_str(
                    "ab_date_debut"
                ).alias("date_debut"),
                F.col("ab_prime_annuelle")
                .cast("double")
                .alias("prime_annuelle"),
                clean_string(
                    F.col("ab_statut_contrat"), "oracle"
                ).alias("statut")
            )
        )

    if ap is not None:
        ap_df = (
            latest(ap, "ap_contract_ref")
            .select(
                F.lit("ASSUREPLUS").alias("source"),
                F.col("ap_contract_ref").alias("ref_contrat"),
                F.col("ap_user_id").cast("string").alias(
                    "id_client_source"
                ),
                clean_string(
                    F.col("ap_product_code"), "sqlserver"
                ).alias("type_assurance"),
                epoch_ms_str(
                    "ap_start_date"
                ).alias("date_debut"),
                (
                    F.col("ap_monthly_premium").cast("double") * 12
                ).alias("prime_annuelle"),
                clean_string(
                    F.col("ap_contract_state"), "sqlserver"
                ).alias("statut")
            )
        )

    return union_or_none(ab_df, ap_df)


# =============================================================================
# 8. CONSOLIDATION DES SINISTRES
# =============================================================================

def build_sinistres(spark):
    ab = bronze(spark, "dbab_ab_ab_sinistre")
    ap = bronze(spark, "dbap_ap_ap_claims")

    ab_df = None
    ap_df = None

    if ab is not None:
        ab_df = (
            latest(ab, "ab_claim_id")
            .select(
                F.lit("ABASSURANCE").alias("source"),
                F.col("ab_claim_id").cast("string").alias(
                    "id_sinistre"
                ),
                F.col("ab_policy_number").alias("ref_contrat"),
                epoch_ms_str(
                    "ab_date_sinistre"
                ).alias("date_sinistre"),
                F.col("ab_montant_estime")
                .cast("double")
                .alias("montant_estime"),
                clean_string(
                    F.col("ab_statut_sinistre"), "oracle"
                ).alias("statut")
            )
        )

    if ap is not None:
        ap_df = (
            latest(ap, "ap_sinistre_num")
            .select(
                F.lit("ASSUREPLUS").alias("source"),
                F.col("ap_sinistre_num").cast("string").alias(
                    "id_sinistre"
                ),
                F.col("ap_contract_ref").alias("ref_contrat"),
                clean_string(
                    F.col("ap_incident_date"), "sqlserver"
                ).alias("date_sinistre"),
                F.col("ap_estimated_amount")
                .cast("double")
                .alias("montant_estime"),
                clean_string(
                    F.col("ap_claim_status"), "sqlserver"
                ).alias("statut")
            )
        )

    return union_or_none(ab_df, ap_df)


# =============================================================================
# 9. CONSOLIDATION DES PAIEMENTS
# =============================================================================

def build_paiements(spark):
    ab = bronze(spark, "dbab_ab_ab_paiement")
    ap = bronze(spark, "dbap_ap_ap_payments")

    ab_df = None
    ap_df = None

    if ab is not None:
        ab_df = (
            latest(ab, "ab_payment_id")
            .select(
                F.lit("ABASSURANCE").alias("source"),
                F.col("ab_payment_id").cast("string").alias(
                    "id_paiement"
                ),
                F.col("ab_policy_number").alias("ref_contrat"),
                epoch_days_str(
                    "ab_date_paiement"
                ).alias("date_paiement"),
                F.col("ab_montant").cast("double").alias(
                    "montant"
                ),
                clean_string(
                    F.col("ab_mode_paiement"), "oracle"
                ).alias("mode_paiement")
            )
        )

    if ap is not None:
        ap_df = (
            latest(ap, "ap_payment_ref")
            .select(
                F.lit("ASSUREPLUS").alias("source"),
                F.col("ap_payment_ref").alias("id_paiement"),
                F.col("ap_contract_ref").alias("ref_contrat"),
                epoch_ms_str(
                    "ap_payment_datetime"
                ).alias("date_paiement"),
                F.col("ap_amount_paid").cast("double").alias(
                    "montant"
                ),
                clean_string(
                    F.col("ap_payment_channel"), "sqlserver"
                ).alias("mode_paiement")
            )
        )

    return union_or_none(ab_df, ap_df)


# =============================================================================
# 10. RECALCUL GOLD
# =============================================================================

def recompute_gold(spark):
    """
    Recalcule les tables Gold depuis Bronze.

    Les erreurs sont enregistrées et remontées à la fin.
    Une table absente de Bronze n'écrase pas une ancienne table Gold.
    """

    builders = {
        "clients": build_clients,
        "contrats": build_contrats,
        "sinistres": build_sinistres,
        "paiements": build_paiements,
    }

    written = []
    failed = []

    print("[gold] Début du recalcul Gold")

    for name, builder in builders.items():

        try:
            print(f"[gold] Construction : {name}")

            df = builder(spark)

            if df is None:
                print(
                    f"[gold] {name} ignorée : "
                    "aucune donnée Bronze disponible"
                )
                continue

            output_path = f"{GOLD}/{name}"

            (
                df.write
                .mode("overwrite")
                .format("parquet")
                .save(output_path)
            )

            written.append(name)

            print(
                f"[gold] Table {name} écrite : {output_path}"
            )

            # Calcul des doublons après l'écriture des clients.
            if name == "clients":

                print("[gold] Calcul des doublons")

                doublons_df = build_doublons(df)

                if doublons_df is not None:
                    doublons_path = f"{GOLD}/doublons"

                    (
                        doublons_df.write
                        .mode("overwrite")
                        .format("parquet")
                        .save(doublons_path)
                    )

                    written.append("doublons")

                    print(
                        f"[gold] Table doublons écrite : "
                        f"{doublons_path}"
                    )

        except Exception as e:
            failed.append(name)

            print(
                f"[gold] ERREUR sur la table {name} : {e}"
            )

            import traceback
            traceback.print_exc()

    print("[gold] Bilan du recalcul")
    print(f"[gold] Tables écrites : {written}")
    print(f"[gold] Tables en erreur : {failed}")

    if failed:
        raise RuntimeError(
            "Échec du recalcul Gold : "
            + ", ".join(failed)
        )

    print("[gold] Recalcul terminé avec succès")


# =============================================================================
# 11. FLUX DECLENCHEUR GOLD
# =============================================================================

def start_gold(spark):
    """
    Flux déclencheur :
    chaque micro-batch Kafka relance le recalcul Gold.
    """

    def recompute(batch_df, batch_id):
        print(f"[gold] Début du batch {batch_id}")

        recompute_gold(spark)

        print(f"[gold] Batch {batch_id} terminé")

    q = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", ",".join(AFTER.keys()))
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    return (
        q.writeStream
        .queryName("gold_recomputation")
        .foreachBatch(recompute)
        .option(
            "checkpointLocation",
            "/tmp/checkpoints/gold"
        )
        .trigger(processingTime="15 seconds")
        .start()
    )


# =============================================================================
# 12. MAIN : DEMARRAGE ET ARRET DES FLUX
# =============================================================================

def main():

    spark = (
        SparkSession.builder
        .appName("AbAssurance-Streaming-HDFS")
        .config("spark.jars.ivy", "/tmp/.ivy2")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        )
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    queries = []

    try:
        # Démarrage des huit flux Bronze.
        for topic in AFTER:

            query = start_hdfs_writer(spark, topic)
            queries.append(query)

            print(
                f"Flux actif : {topic} -> "
                f"{HDFS}/datalake/cdc/"
                f"{topic.replace('.', '_')}"
            )

        # Démarrage du flux Gold.
        gold_query = start_gold(spark)
        queries.append(gold_query)

        print(
            f"Gold actif : consolidation + doublons -> {GOLD}"
        )

        print(
            "Spark Streaming actif : "
            "CDC Debezium -> HDFS Parquet."
        )

        # Attente de la terminaison d'un flux.
        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        print("[streaming] Arrêt manuel demandé")

    except Exception as e:
        print(f"[streaming] ERREUR : {e}")

        # Afficher les erreurs individuelles des requêtes.
        for query in queries:
            try:
                if query.exception() is not None:
                    print(
                        f"[streaming] Erreur requête "
                        f"{query.name} : {query.exception()}"
                    )
            except Exception:
                pass

        raise

    finally:
        print("[streaming] Arrêt des requêtes")

        for query in queries:
            try:
                if query.isActive:
                    query.stop()
                    print(
                        f"[streaming] Requête arrêtée : {query.name}"
                    )
            except Exception as e:
                print(
                    f"[streaming] Erreur arrêt requête : {e}"
                )

        try:
            spark.stop()
        except Exception as e:
            print(f"[streaming] Erreur arrêt Spark : {e}")

        print("[streaming] Application arrêtée")


if __name__ == "__main__":
    main()
