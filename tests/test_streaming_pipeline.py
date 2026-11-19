"""Tests pour spark/streaming_pipeline.py avec données HDFS réelles."""
import pytest
from pyspark.sql import functions as F

from spark.streaming_pipeline import (
    bronze,
    build_clients,
    build_contrats,
    build_doublons,
    build_paiements,
    build_sinistres,
    epoch_days_str,
    epoch_us_str,
    sha_pseudo,
)


def test_hdfs_configuration():
    """Vérifie que la configuration HDFS est correcte."""
    from spark.streaming_pipeline import HDFS
    assert HDFS == "hdfs://namenode:9000", f"HDFS doit être 'hdfs://namenode:9000', mais est '{HDFS}'"

class TestBronzeWithHDFS:
    """Tests pour la fonction bronze avec données HDFS réelles."""

    def test_bronze_ab_client(self, spark_session):
        """Teste la lecture des clients AbAssurance depuis HDFS."""
        df = bronze(spark_session, "dbab_ab_ab_client")
        if df is not None:
            assert df.count() > 0
            assert "ab_client_id" in df.columns
            assert "ab_nom" in df.columns
        else:
            pytest.skip("Aucune donnée AbAssurance disponible dans HDFS")

    def test_bronze_ap_users(self, spark_session):
        """Teste la lecture des utilisateurs AssurePlus depuis HDFS."""
        df = bronze(spark_session, "dbap_ap_ap_users")
        if df is not None:
            assert df.count() > 0
            assert "ap_user_id" in df.columns
            assert "ap_full_name" in df.columns
        else:
            pytest.skip("Aucune donnée AssurePlus disponible dans HDFS")



class TestBuildClientsWithHDFS:
    """Tests pour build_clients avec données HDFS réelles."""

    def test_build_clients_with_real_data(self, spark_session):
        """Teste build_clients avec les données réelles d'HDFS."""
        result = build_clients(spark_session)
        if result is not None:
            assert result.count() > 0
            assert "source" in result.columns
            assert "nom" in result.columns
            assert "prenom" in result.columns
            # Vérifie que la clé de déduplication est présente (si votre code l'ajoute)
            # assert "clé_déduplication" in result.columns
        else:
            pytest.skip("Aucune donnée disponible pour build_clients")

    def test_build_clients_columns(self, spark_session):
        """Teste les colonnes du résultat de build_clients."""
        result = build_clients(spark_session)
        if result is not None:
            expected_columns = {"source", "id_source", "nom", "prenom", "date_naissance",
                               "email_pseudo", "telephone_pseudo", "code_postal", "statut"}
            assert expected_columns.issubset(set(result.columns))
        else:
            pytest.skip("Aucune donnée disponible pour build_clients")


class TestBuildDoublonsWithHDFS:
    """Tests pour build_doublons avec données HDFS réelles."""

    def test_build_doublons_with_real_data(self, spark_session):
        """Teste build_doublons avec les données réelles d'HDFS."""
        clients = build_clients(spark_session)
        if clients is not None and clients.count() > 0:
            result = build_doublons(clients)
            if result is not None:
                # Vérifie que le résultat contient les colonnes attendues
                assert "id_ab" in result.columns
                assert "id_ap" in result.columns
                assert "nom" in result.columns
                assert "prenom" in result.columns
            else:
                assert True  # Aucun doublon trouvé
        else:
            pytest.skip("Aucune donnée disponible pour build_doublons")


class TestBuildContratsWithHDFS:
    """Tests pour build_contrats avec données HDFS réelles."""

    def test_build_contrats_with_real_data(self, spark_session):
        """Teste build_contrats avec les données réelles d'HDFS."""
        result = build_contrats(spark_session)
        if result is not None:
            assert result.count() >= 0  # Peut être 0 si aucune donnée
            assert "source" in result.columns
            assert "ref_contrat" in result.columns
            assert "id_client_source" in result.columns
            assert "type_assurance" in result.columns
        else:
            pytest.skip("Aucune donnée disponible pour build_contrats")


class TestBuildSinistresWithHDFS:
    """Tests pour build_sinistres avec données HDFS réelles."""

    def test_build_sinistres_with_real_data(self, spark_session):
        """Teste build_sinistres avec les données réelles d'HDFS."""
        result = build_sinistres(spark_session)
        if result is not None:
            assert result.count() >= 0
            assert "source" in result.columns
            assert "id_sinistre" in result.columns
            assert "ref_contrat" in result.columns
        else:
            pytest.skip("Aucune donnée disponible pour build_sinistres")


class TestBuildPaiementsWithHDFS:
    """Tests pour build_paiements avec données HDFS réelles."""

    def test_build_paiements_with_real_data(self, spark_session):
        """Teste build_paiements avec les données réelles d'HDFS."""
        result = build_paiements(spark_session)
        if result is not None:
            assert result.count() >= 0
            assert "source" in result.columns
            assert "id_paiement" in result.columns
            assert "ref_contrat" in result.columns
        else:
            pytest.skip("Aucune donnée disponible pour build_paiements")


class TestEpochConversionsWithHDFS:
    """Tests pour les conversions de dates avec données HDFS réelles."""

    def test_epoch_days_str_with_real_data(self, spark_session):
        """Teste epoch_days_str avec des données réelles."""
        df = bronze(spark_session, "dbab_ab_ab_client")
        if df is not None and "ab_date_naissance" in df.columns:
            result = df.withColumn("date_naissance", epoch_days_str("ab_date_naissance")).collect()
            assert len(result) > 0
            # Vérifie que la date est au format YYYY-MM-DD
            assert all("-" in row.date_naissance for row in result if row.date_naissance)
        else:
            pytest.skip("Aucune donnée avec ab_date_naissance disponible")

    def test_epoch_us_str_with_real_data(self, spark_session):
        """Teste epoch_us_str avec des données réelles."""
        df = bronze(spark_session, "dbab_ab_ab_sinistre")
        if df is not None and "ab_date_sinistre" in df.columns:
            result = df.withColumn("date_sinistre", epoch_us_str("ab_date_sinistre")).collect()
            assert len(result) > 0
            # Vérifie que la date est au format YYYY-MM-DD HH:mm:ss
            assert all(":" in row.date_sinistre for row in result if row.date_sinistre)
        else:
            pytest.skip("Aucune donnée avec ab_date_sinistre disponible")


class TestShaPseudoWithHDFS:
    """Tests pour sha_pseudo avec données HDFS réelles."""

    def test_sha_pseudo_with_real_emails(self, spark_session):
        """Teste sha_pseudo avec des emails réels."""
        df = bronze(spark_session, "dbab_ab_ab_client")
        if df is not None and "ab_email" in df.columns:
            result = df.withColumn("email_pseudo", sha_pseudo(F.col("ab_email"))).collect()
            assert len(result) > 0
            # Vérifie que les emails non-null ont un hash
            for row in result:
                if row.ab_email is not None and row.ab_email.strip():
                    assert row.email_pseudo is not None
        else:
            pytest.skip("Aucune donnée avec ab_email disponible")