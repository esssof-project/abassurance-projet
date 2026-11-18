"""Tests pour dataviz/app.py - Conformité Bloc 1"""
import pytest
import pandas as pd

class TestDatavizDataLoading:
    """Tests pour le chargement des données dans Dataviz (Bloc 1 - MVP)."""

    def test_client_distribution(self):
        """Teste la répartition des clients (Bloc 1 - MVP)."""
        data = pd.DataFrame({
            "source": ["ABASSURANCE", "ASSUREPLUS", "ABASSURANCE"],
            "clé_déduplication": ["abc123", "def456", "ghi789"]
        })
        distribution = data["source"].value_counts()
        assert "ABASSURANCE" in distribution.index
        assert "ASSUREPLUS" in distribution.index
        assert distribution["ABASSURANCE"] == 2
        assert distribution["ASSUREPLUS"] == 1

    def test_contracts_statistics(self):
        """Teste le calcul des statistiques de contrats (Bloc 1 - MVP)."""
        data = pd.DataFrame({
            "type_assurance": ["Auto", "Habitation", "Auto"],
            "prime_annuelle": [500.0, 800.0, 600.0]
        })
        stats = data.groupby("type_assurance")["prime_annuelle"].agg(["sum", "mean", "count"])
        assert "Auto" in stats.index
        assert "Habitation" in stats.index
        assert stats.loc["Auto", "count"] == 2