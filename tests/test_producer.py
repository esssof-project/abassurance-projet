"""Tests pour producer/main.py"""
import pytest
from unittest.mock import patch, MagicMock
from producer.main import extract_ab_clients, transform_client, pseudo


class TestPseudo:
    def test_pseudo_deterministic(self):
        assert pseudo("test@example.com") == pseudo("Test@Example.com")

    def test_pseudo_none(self):
        assert pseudo(None) is None
        assert pseudo("") is None


class TestExtractAbClients:
    @patch('producer.main.psycopg2.connect')
    def test_extract_success(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            (1, "Dupont", "Jean", 19800101, "jean@ab.com", "0123456789", "75001", "ACTIF")
        ]
        mock_cursor.description = [
            ("ab_client_id",), ("ab_nom",), ("ab_prenom",), ("ab_date_naissance",),
            ("ab_email",), ("ab_telephone",), ("ab_code_postal",), ("ab_statut_client",)
        ]
        mock_connect.return_value = mock_conn
        
        clients = extract_ab_clients("localhost", "abassurance", "demo", "test")
        assert len(clients) == 1
        assert clients[0]["nom"] == "Dupont"


class TestTransformClient:
    def test_transform_ab(self):
        data = {
            "ab_client_id": 1, "ab_nom": "Dupont", "ab_prenom": "Jean",
            "ab_date_naissance": 19800101, "ab_code_postal": "75001"
        }
        result = transform_client(data, "ABASSURANCE")
        assert result["nom"] == "Dupont"
        assert result["source"] == "ABASSURANCE"

    def test_transform_ap(self):
        data = {
            "ap_user_id": 1, "ap_full_name": "Dupont Jean",
            "ap_birth_date": "1980-01-01", "ap_zip_code": "75001"
        }
        result = transform_client(data, "ASSUREPLUS")
        assert result["nom"] == "Dupont"
        assert result["prenom"] == "Jean"
        assert result["source"] == "ASSUREPLUS"
