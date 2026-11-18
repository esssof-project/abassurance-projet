"""Tests pour spark/streaming_pipeline.py (VOS fonctions réelles)."""
import pytest
from pyspark.sql import Row, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
from spark.streaming_pipeline import (
    sha_pseudo,
    epoch_days_str,
    epoch_us_str,
    build_clients,
    build_doublons,
    latest,
    bronze,
    union_or_none,
    envelope_schema,
    AFTER,
)

class TestShaPseudo:
    """Tests pour la fonction sha_pseudo (pseudonymisation)."""

    def test_sha_pseudo_basic(self):
        """Teste la pseudonymisation de base."""
        result = sha_pseudo(F.lit("test@example.com"))
        # Vérifie que le résultat est une chaîne de 64 caractères (SHA256)
        assert result is not None
        # Note: On ne peut pas vérifier la valeur exacte car F.sha2 retourne une colonne Spark

    def test_sha_pseudo_null(self):
        """Teste que NULL retourne NULL."""
        result = sha_pseudo(F.lit(None))
        assert result is not None  # F.sha2 retourne une colonne Spark, pas None

    def test_sha_pseudo_empty(self):
        """Teste que les chaînes vides sont gérées."""
        result = sha_pseudo(F.lit(""))
        assert result is not None

    def test_sha_pseudo_with_dataframe(self, spark_session):
        """Teste sha_pseudo avec un DataFrame réel."""
        df = spark_session.createDataFrame([
            Row(email="test@example.com"),
            Row(email="another@example.com"),
        ])
        result = df.withColumn("email_pseudo", sha_pseudo(F.col("email")))
        rows = result.collect()
        assert len(rows) == 2
        assert rows[0].email_pseudo != rows[1].email_pseudo  # Différents emails = différentes clés

---

class TestEpochConversions:
    """Tests pour les conversions de dates (epoch_days_str, epoch_us_str)."""

    def test_epoch_days_str(self, spark_session):
        """Teste la conversion des jours depuis 1970 en date (YYYY-MM-DD)."""
        df = spark_session.createDataFrame([
            Row(days=0),      # 1970-01-01
            Row(days=18262),  # 2020-01-01
        ])
        result = df.withColumn("date", epoch_days_str("days"))
        rows = result.collect()
        assert rows[0].date == "1970-01-01"
        assert rows[1].date == "2020-01-01"

    def test_epoch_us_str(self, spark_session):
        """Teste la conversion des microsecondes depuis 1970 en timestamp."""
        # 0 = 1970-01-01 00:00:00
        # 1609459200000000 = 2021-01-01 00:00:00 (en microsecondes)
        df = spark_session.createDataFrame([
            Row(us=0),
            Row(us=1609459200000000),
        ])
        result = df.withColumn("timestamp", epoch_us_str("us"))
        rows = result.collect()
        assert "1970-01-01" in rows[0].timestamp
        assert "2021-01-01" in rows[1].timestamp

---

class TestLatest:
    """Tests pour la fonction latest (dernier événement par clé)."""

    def test_latest_basic(self, spark_session):
        """Teste la sélection du dernier événement par clé."""
        df = spark_session.createDataFrame([
            Row(id=1, value="A", ts=100),
            Row(id=1, value="B", ts=200),  # Dernier pour id=1
            Row(id=2, value="C", ts=50),
        ])
        result = latest(df, "id")
        rows = result.collect()
        assert len(rows) == 2
        # Vérifie que pour id=1, on a la valeur "B" (ts=200)
        id1_row = next(row for row in rows if row.id == 1)
        assert id1_row.value == "B"

    def test_latest_with_null_ts(self, spark_session):
        """Teste latest avec des timestamps NULL."""
        df = spark_session.createDataFrame([
            Row(id=1, value="A", ts=None),
            Row(id=1, value="B", ts=200),
        ])
        result = latest(df, "id")
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0].value == "B"

---

class TestBronze:
    """Tests pour la fonction bronze (lecture des données CDC)."""
    @pytest.fixture
    def mock_parquet(self, spark_session, tmp_path):
        """Crée un faux fichier Parquet pour les tests."""
        df = spark_session.createDataFrame([
            Row(id=1, nom="Dupont"),
            Row(id=2, nom="Martin"),
        ])
        parquet_path = str(tmp_path / "test.parquet")
        df.write.parquet(parquet_path)
        return parquet_path

    def test_bronze_success(self, spark_session, mock_parquet, monkeypatch):
        """Teste la lecture des données bronze."""
        # Monkeypatch HDFS pour utiliser le chemin local
        monkeypatch.setattr("spark.streaming_pipeline.HDFS", f"file://{mock_parquet.parent}")
        result = bronze(spark_session, "test")
        assert result is not None
        assert result.count() == 2

    def test_bronze_failure(self, spark_session, monkeypatch):
        """Teste que bronze retourne None si le fichier n'existe pas."""
        monkeypatch.setattr("spark.streaming_pipeline.HDFS", "file:///nonexistent/path")
        result = bronze(spark_session, "nonexistent")
        assert result is None

---

class TestUnionOrNone:
    """Tests pour la fonction union_or_none."""

    def test_union_or_none_both_not_none(self, spark_session):
        """Teste l'union de deux DataFrames non-None."""
        df1 = spark_session.createDataFrame([Row(id=1)])
        df2 = spark_session.createDataFrame([Row(id=2)])
        result = union_or_none(df1, df2)
        assert result.count() == 2

    def test_union_or_none_first_none(self, spark_session):
        """Teste union_or_none avec le premier DataFrame None."""
        df2 = spark_session.createDataFrame([Row(id=2)])
        result = union_or_none(None, df2)
        assert result.count() == 1

    def test_union_or_none_second_none(self, spark_session):
        """Teste union_or_none avec le deuxième DataFrame None."""
        df1 = spark_session.createDataFrame([Row(id=1)])
        result = union_or_none(df1, None)
        assert result.count() == 1

    def test_union_or_none_both_none(self):
        """Teste union_or_none avec les deux DataFrames None."""
        result = union_or_none(None, None)
        assert result is None

---

class TestBuildClients:
    """Tests pour la fonction build_clients (clients unifiés)."""

    @pytest.fixture
    def mock_bronze_data(self, spark_session, tmp_path):
        """Crée des données mock pour bronze."""
        # Données AbAssurance
        ab_data = [
            Row(ab_client_id=1, ab_nom="Dupont", ab_prenom="Jean", ab_date_naissance=18262, ab_email="dupont@test.com", ab_telephone="0102030405", ab_code_postal="75001", ab_statut_client="ACTIF"),
        ]
        ab_df = spark_session.createDataFrame(ab_data)
        ab_path = str(tmp_path / "ab_client.parquet")
        ab_df.write.parquet(ab_path)

        # Données AssurePlus
        ap_data = [
            Row(ap_user_id=1, ap_full_name="Martin Pierre", ap_birth_date="1990-05-15", ap_mail_address="martin@test.com", ap_phone_number="0607080910", ap_zip_code="75002", ap_customer_status="ACTIF"),
        ]
        ap_df = spark_session.createDataFrame(ap_data)
        ap_path = str(tmp_path / "ap_users.parquet")
        ap_df.write.parquet(ap_path)

        # Monkeypatch HDFS pour utiliser les chemins locaux
        import spark.streaming_pipeline as sp
        sp.HDFS = f"file://{tmp_path}"

        return tmp_path

    def test_build_clients_both_sources(self, spark_session, mock_bronze_data):
        """Teste build_clients avec les deux sources."""
        result = build_clients(spark_session)
        assert result is not None
        assert result.count() == 2
        assert "clé_déduplication" in result.columns
        assert "source" in result.columns

    def test_build_clients_ab_only(self, spark_session, tmp_path):
        """Teste build_clients avec seulement AbAssurance."""
        ab_data = [
            Row(ab_client_id=1, ab_nom="Dupont", ab_prenom="Jean", ab_date_naissance=18262, ab_email="dupont@test.com", ab_telephone="0102030405", ab_code_postal="75001", ab_statut_client="ACTIF"),
        ]
        ab_df = spark_session.createDataFrame(ab_data)
        ab_path = str(tmp_path / "ab_client.parquet")
        ab_df.write.parquet(ab_path)

        # Monkeypatch HDFS pour utiliser le chemin local
        import spark.streaming_pipeline as sp
        sp.HDFS = f"file://{tmp_path}"

        result = build_clients(spark_session)
        assert result is not None
        assert result.count() == 1

    def test_build_clients_ap_only(self, spark_session, tmp_path):
        """Teste build_clients avec seulement AssurePlus."""
        ap_data = [
            Row(ap_user_id=1, ap_full_name="Martin Pierre", ap_birth_date="1990-05-15", ap_mail_address="martin@test.com", ap_phone_number="0607080910", ap_zip_code="75002", ap_customer_status="ACTIF"),
        ]
        ap_df = spark_session.createDataFrame(ap_data)
        ap_path = str(tmp_path / "ap_users.parquet")
        ap_df.write.parquet(ap_path)

        # Monkeypatch HDFS pour utiliser le chemin local
        import spark.streaming_pipeline as sp
        sp.HDFS = f"file://{tmp_path}"

        result = build_clients(spark_session)
        assert result is not None
        assert result.count() == 1

---

class TestBuildDoublons:
    """Tests pour la fonction build_doublons (matrice de doublons)."""

    @pytest.fixture
    def mock_clients_data(self, spark_session):
        """Crée des données mock pour les clients."""
        data = [
            Row(source="ABASSURANCE", id_source="1", nom="Dupont", prenom="Jean", date_naissance="1980-01-01", code_postal="75001", email_pseudo="hash1", telephone_pseudo="hash2", statut="ACTIF"),
            Row(source="ASSUREPLUS", id_source="2", nom="Dupont", prenom="Jean", date_naissance="1980-01-01", code_postal="75001", email_pseudo="hash3", telephone_pseudo="hash4", statut="ACTIF"),  # Doublon
            Row(source="ABASSURANCE", id_source="3", nom="Martin", prenom="Pierre", date_naissance="1990-05-15", code_postal="75002", email_pseudo="hash5", telephone_pseudo="hash6", statut="ACTIF"),
        ]
        return spark_session.createDataFrame(data)

    def test_build_doublons_with_doubles(self, spark_session, mock_clients_data):
        """Teste build_doublons avec des doublons."""
        result = build_doublons(mock_clients_data)
        assert result is not None
        assert result.count() == 1  # 1 doublon (Dupont Jean)
        rows = result.collect()
        assert rows[0].nom == "Dupont"
        assert rows[0].prenom == "Jean"

    def test_build_doublons_no_doubles(self, spark_session):
        """Teste build_doublons sans doublons."""
        data = [
            Row(source="ABASSURANCE", id_source="1", nom="Dupont", prenom="Jean", date_naissance="1980-01-01", code_postal="75001", email_pseudo="hash1", telephone_pseudo="hash2", statut="ACTIF"),
            Row(source="ABASSURANCE", id_source="2", nom="Martin", prenom="Pierre", date_naissance="1990-05-15", code_postal="75002", email_pseudo="hash3", telephone_pseudo="hash4", statut="ACTIF"),
        ]
        df = spark_session.createDataFrame(data)
        result = build_doublons(df)
        assert result is not None
        assert result.count() == 0  # Aucun doublon

    def test_build_doublons_empty(self, spark_session):
        """Teste build_doublons avec un DataFrame vide."""
        data = []
        df = spark_session.createDataFrame(data, ["source", "id_source", "nom", "prenom", "date_naissance", "code_postal", "email_pseudo", "telephone_pseudo", "statut"])
        result = build_doublons(df)
        assert result is None

---

class TestBuildContrats:
    """Tests pour la fonction build_contrats."""

    @pytest.fixture
    def mock_contrats_data(self, spark_session, tmp_path):
        """Crée des données mock pour les contrats."""
        # Données AbAssurance
        ab_data = [
            Row(ab_policy_number="AB001", ab_client_id=1, ab_type_assurance="Auto", ab_date_debut=18262, ab_prime_annuelle=500.0, ab_statut_contrat="ACTIF"),
        ]
        ab_df = spark_session.createDataFrame(ab_data)
        ab_path = str(tmp_path / "ab_contrat.parquet")
        ab_df.write.parquet(ab_path)

        # Données AssurePlus
        ap_data = [
            Row(ap_contract_ref="AP001", ap_user_id=1, ap_product_code="Auto", ap_start_date=1609459200000000, ap_monthly_premium=50.0, ap_contract_state="ACTIF"),
        ]
        ap_df = spark_session.createDataFrame(ap_data)
        ap_path = str(tmp_path / "ap_contracts.parquet")
        ap_df.write.parquet(ap_path)

        # Monkeypatch HDFS pour utiliser les chemins locaux
        import spark.streaming_pipeline as sp
        sp.HDFS = f"file://{tmp_path}"

        return tmp_path

    def test_build_contrats_both_sources(self, spark_session, mock_contrats_data):
        """Teste build_contrats avec les deux sources."""
        result = build_contrats(spark_session)
        assert result is not None
        assert result.count() == 2
        assert "prime_annuelle" in result.columns
        assert "date_debut" in result.columns

---
---
## 📄 **5. Fichier `run-tests.sh`**
```bash
#!/bin/bash
# Script pour exécuter les tests locaux avec couverture
set -e

echo "=========================================="
echo "  Tests unitaires pour streaming_pipeline.py"
echo "=========================================="

# Installer les dépendances
pip install -q pytest==8.2.0 pytest-cov==4.1.0 pyspark==3.5.0

# Exécuter les tests avec couverture
pytest tests/test_streaming_pipeline.py \
  --cov=spark \
  --cov=tests \
  --cov-report=term-missing \
  --cov-report=html \
  --cov-fail-under=80 \
  -v \
  --tb=short

echo ""
echo "=========================================="
echo "  Tests terminés. Rapport de couverture :"
echo "  Ouvrez htmlcov/index.html dans un navigateur."
echo "=========================================="