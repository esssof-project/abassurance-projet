import pandas as pd


def build_doublons(clients):
    """Replique testable de la logique de matrice de doublons du job Spark :
    jointure interne sur l'empreinte email entre les deux sources,
    avec filtrage des empreintes NULL (correctif du bug identifie)."""
    df = clients.dropna(subset=["email_pseudo"])
    ab = df[df.source == "ABASSURANCE"].rename(columns={"id_source": "id_ab"})
    ap = df[df.source == "ASSUREPLUS"].rename(columns={"id_source": "id_ap"})
    return ab.merge(ap, on="email_pseudo", suffixes=("_ab", "_ap"))[
        ["id_ab", "id_ap", "nom_ab", "prenom_ab"]]


def test_doublon_detecte():
    clients = pd.DataFrame([
        {"source": "ABASSURANCE", "id_source": 1, "nom_ab": "DUPONT",
         "prenom_ab": "Jean", "email_pseudo": "hash-jean"},
        {"source": "ASSUREPLUS", "id_source": 1, "nom_ab": "DUPONT",
         "prenom_ab": "Jean", "email_pseudo": "hash-jean"},
    ])
    assert len(build_doublons(clients)) == 1


def test_pas_de_faux_doublon():
    clients = pd.DataFrame([
        {"source": "ABASSURANCE", "id_source": 1, "nom_ab": "DUPONT",
         "prenom_ab": "Jean", "email_pseudo": "hash-jean"},
        {"source": "ASSUREPLUS", "id_source": 2, "nom_ab": "MARTIN",
         "prenom_ab": "Sophie", "email_pseudo": "hash-sophie"},
    ])
    assert len(build_doublons(clients)) == 0


def test_bug_null_email():
    """BUG POTENTIEL (Dossier 3, Q3) : un email NULL chez les deux assureurs
    produirait un faux doublon (jointure None == None). Le filtrage le corrige."""
    clients = pd.DataFrame([
        {"source": "ABASSURANCE", "id_source": 3, "nom_ab": "X",
         "prenom_ab": "Y", "email_pseudo": None},
        {"source": "ASSUREPLUS", "id_source": 4, "nom_ab": "Z",
         "prenom_ab": "W", "email_pseudo": None},
    ])
    assert len(build_doublons(clients)) == 0
