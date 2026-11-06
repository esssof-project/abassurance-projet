"""Producer Talend-like : extraction des deux bases sources
(AbAssurance schema AB_* / AssurePlus schema AP_*),
normalisation vers le modele unifie, pseudonymisation RGPD,
publication vers Apache Kafka. Les imports lourds sont lazy
pour permettre les tests unitaires sans les dependances DB/Kafka."""

import os
import json
import hashlib


def pseudo(value):
    """Pseudonymisation RGPD (US 1.1) des donnees sensibles avant Kafka."""
    if not value:
        return None
    return hashlib.sha256(value.lower().strip().encode()).hexdigest()


def is_valid_email_pseudo(h):
    """Indique si une empreinte email est utilisable pour la jointure de doublons."""
    return bool(h)


def extract_abassur(conn_factory=None):
    """Extraction + normalisation AbAssurance (base source AbAssurance).
    Pour la demo : PostgreSQL simulant le schema Oracle AB_*.
    conn_factory injecte une connexion (pattern utile pour les tests)."""
    conn = conn_factory.connect()
    cur = conn.cursor()
    cur.execute("""SELECT AB_CLIENT_ID, AB_NOM, AB_PRENOM, AB_DATE_NAISSANCE, AB_EMAIL,
                          AB_TELEPHONE, AB_CODE_POSTAL, AB_STATUT_CLIENT FROM ab.AB_CLIENT""")
    for r in cur:
        yield "topic-clients", {
            "source": "ABASSURANCE", "id_source": r[0],
            "nom": r[1], "prenom": r[2], "date_naissance": r[3],
            "email_pseudo": pseudo(r[4]), "telephone_pseudo": pseudo(r[5]),
            "code_postal": r[6], "statut": r[7]}
    cur.execute("""SELECT AB_POLICY_NUMBER, AB_CLIENT_ID, AB_TYPE_ASSURANCE, AB_DATE_DEBUT,
                          AB_PRIME_ANNUELLE, AB_STATUT_CONTRAT FROM ab.AB_CONTRAT""")
    for r in cur:
        yield "topic-contrats", {"source": "ABASSURANCE", "ref_contrat": r[0],
            "id_client_source": r[1], "type_assurance": r[2], "date_debut": r[3],
            "prime_annuelle": float(r[4]), "statut": r[5]}
    cur.execute("""SELECT AB_CLAIM_ID, AB_POLICY_NUMBER, AB_DATE_SINISTRE,
                          AB_MONTANT_ESTIME, AB_STATUT_SINISTRE FROM ab.AB_SINISTRE""")
    for r in cur:
        yield "topic-sinistres", {"source": "ABASSURANCE", "id_sinistre": r[0],
            "ref_contrat": r[1], "date_sinistre": r[2],
            "montant_estime": float(r[3]), "statut": r[4]}
    cur.execute("""SELECT AB_PAYMENT_ID, AB_POLICY_NUMBER, AB_DATE_PAIEMENT,
                          AB_MONTANT, AB_MODE_PAIEMENT FROM ab.AB_PAIEMENT""")
    for r in cur:
        yield "topic-paiements", {"source": "ABASSURANCE", "id_paiement": r[0],
            "ref_contrat": r[1], "date_paiement": r[2],
            "montant": float(r[3]), "mode_paiement": r[4]}
    conn.close()


def extract_assureplus(conn_factory=None):
    """Extraction + normalisation AssurePlus (base source AssurePlus).
    Pour la demo : PostgreSQL simulant le schema SQL Server AP_*."""
    conn = conn_factory.connect()
    cur = conn.cursor()
    cur.execute("""SELECT AP_USER_ID, AP_FULL_NAME, AP_BIRTH_DATE, AP_MAIL_ADDRESS,
                          AP_PHONE_NUMBER, AP_ZIP_CODE, AP_CUSTOMER_STATUS FROM ap.AP_USERS""")
    for r in cur:
        nom_prenom = r[1].split(" ", 1)
        yield "topic-clients", {
            "source": "ASSUREPLUS", "id_source": r[0],
            "nom": nom_prenom[0], "prenom": nom_prenom[1] if len(nom_prenom) > 1 else "",
            "date_naissance": r[2], "email_pseudo": pseudo(r[3]),
            "telephone_pseudo": pseudo(r[4]), "code_postal": r[5], "statut": r[6]}
    cur.execute("""SELECT AP_CONTRACT_REF, AP_USER_ID, AP_PRODUCT_CODE, AP_START_DATE,
                          AP_MONTHLY_PREMIUM, AP_CONTRACT_STATE FROM ap.AP_CONTRACTS""")
    for r in cur:
        yield "topic-contrats", {"source": "ASSUREPLUS", "ref_contrat": r[0],
            "id_client_source": r[1], "type_assurance": r[2], "date_debut": r[3],
            "prime_annuelle": float(r[4]) * 12, "statut": r[5]}
    cur.execute("""SELECT AP_SINISTRE_NUM, AP_CONTRACT_REF, AP_INCIDENT_DATE,
                          AP_ESTIMATED_AMOUNT, AP_CLAIM_STATUS FROM ap.AP_CLAIMS""")
    for r in cur:
        yield "topic-sinistres", {"source": "ASSUREPLUS", "id_sinistre": r[0],
            "ref_contrat": r[1], "date_sinistre": r[2],
            "montant_estime": float(r[3]), "statut": r[4]}
    cur.execute("""SELECT AP_PAYMENT_REF, AP_CONTRACT_REF, AP_PAYMENT_DATETIME,
                          AP_AMOUNT_PAID, AP_PAYMENT_CHANNEL FROM ap.AP_PAYMENTS""")
    for r in cur:
        yield "topic-paiements", {"source": "ASSUREPLUS", "id_paiement": str(r[0]),
            "ref_contrat": r[1], "date_paiement": r[2],
            "montant": float(r[3]), "mode_paiement": r[4]}
    conn.close()


def main():
    import psycopg2
    from kafka import KafkaProducer

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    producer = KafkaProducer(bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"))

    class AbFactory:
        @staticmethod
        def connect():
            return psycopg2.connect(host=os.getenv("AB_DB_HOST", "db-abassur"),
                                     dbname=os.getenv("AB_DB_NAME", "abassurance"),
                                     user=os.getenv("DB_USER", "demo"),
                                     password=os.getenv("DB_PASSWORD", "Demo123!"))

    class ApFactory:
        @staticmethod
        def connect():
            return psycopg2.connect(host=os.getenv("AP_DB_HOST", "db-assureplus"),
                                     dbname=os.getenv("AP_DB_NAME", "assureplus"),
                                     user=os.getenv("DB_USER", "demo"),
                                     password=os.getenv("DB_PASSWORD", "Demo123!"))

    messages = list(extract_abassur(AbFactory)) + list(extract_assureplus(ApFactory))
    for topic, msg in messages:
        producer.send(topic, msg)
    producer.flush()
    print(f"{len(messages)} messages publies vers Kafka.")


if __name__ == "__main__":
    main()
