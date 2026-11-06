import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "producer"))
from producer import pseudo, is_valid_email_pseudo


def test_pseudo_deterministic():
    """La pseudonymisation est deterministe : meme email => meme hash (RGPD)."""
    assert pseudo("jean.dupont@email.fr") == pseudo("Jean.Dupont@Email.fr")


def test_pseudo_none():
    """Un email vide ne doit pas produire de hash trompeur."""
    assert pseudo(None) is None
    assert pseudo("") is None


def test_pseudo_different_inputs():
    assert pseudo("a@b.fr") != pseudo("c@d.fr")


def test_is_valid_email_pseudo():
    """Correctif du bug Dossier 3 : une empreinte NULL est invalide pour la jointure."""
    assert is_valid_email_pseudo(pseudo("jean.dupont@email.fr")) is True
    assert is_valid_email_pseudo(pseudo(None)) is False
