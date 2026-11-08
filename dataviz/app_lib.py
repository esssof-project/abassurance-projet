"""Bibliotheque Dataviz : lecture du Data Lake HDFS (WebHDFS -> Parquet),
consolidation des evenements CDC en etat courant, calcul des indicateurs.
Utilisee a la fois par l'application Streamlit (app.py) et par le smoke
test CLI (check.py)."""

import io
import hashlib
import requests
import pandas as pd
from datetime import date, datetime, timedelta

NAMENODE = "http://namenode:9870"
BASE = NAMENODE + "/webhdfs/v1"
CDC = "/datalake/cdc"
GOLD = "/datalake/gold"


def pseudo(value):
    """Pseudonymisation RGPD appliquee a la lecture (donnees brutes du lake)."""
    if not value:
        return None
    return hashlib.sha256(str(value).lower().strip().encode()).hexdigest()


def list_dir(path):
    """Liste les fichiers Parquet d'un dossier HDFS via WebHDFS.
    Exclut les fichiers vides (0 octet) : Spark Streaming rend visibles des
    fichiers .parquet vides tant qu'ils sont en cours d'ecriture."""
    r = requests.get(f"{BASE}{path}", params={"op": "LISTSTATUS"}, timeout=10)
    r.raise_for_status()
    return [f["pathSuffix"] for f
            in r.json()["FileStatuses"]["FileStatus"]
            if f["pathSuffix"].endswith(".parquet") and f.get("length", 0) > 0]


def read_topic(name):
    """Lit tous les Parquet d'un dossier CDC (evenements Debezium aplatis ecrits
    par Spark Streaming : event_ts + op + colonnes de la table source)."""
    import pyarrow.parquet as pq
    frames = []
    for f in list_dir(f"{CDC}/{name}"):
        r = requests.get(f"{BASE}{CDC}/{name}/{f}", params={"op": "OPEN"}, timeout=10)
        if not r.content:
            continue  # fichier devenu vide entre le listing et la lecture
        try:
            frames.append(pq.read_table(io.BytesIO(r.content)).to_pandas())
        except Exception:
            continue  # fichier partiel/corrompu en cours d'ecriture : on l'ignore
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def latest(df, pk):
    """Etat courant : dernier evenement par cle. Les deletes (op='d') et
    les evenements non parsables (cle nulle) sont exclus."""
    if df.empty or pk not in df.columns:
        return pd.DataFrame()
    df = df[df[pk].notna()]
    if df.empty:
        return df
    df = df.sort_values("event_ts").drop_duplicates(subset=[pk], keep="last")
    return df[df["op"] != "d"]


def to_float(v):
    """Conversion tolerante : null/NaN/NaT -> 0.0 (champ absent d'un evenement)."""
    if v is None or pd.isna(v):
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def to_str(v):
    """Conversion tolerante : null/NaN/NaT -> None."""
    if v is None or pd.isna(v):
        return None
    return str(v)


def to_id(v):
    """Identifiant canonique en chaine : 1.0 -> '1' (evite 'NaT'/'nan')."""
    if v is None or pd.isna(v):
        return None
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def epoch_days(v):
    """Debezium DATE = jours depuis 1970-01-01 -> ISO."""
    if v is None or pd.isna(v):
        return None
    return (date(1970, 1, 1) + timedelta(days=int(v))).isoformat()


def epoch_us(v):
    """Debezium MicroTimestamp = MICROSECONDES epoch -> ISO."""
    if v is None or pd.isna(v):
        return None
    return datetime.fromtimestamp(int(v) / 1_000_000.0).isoformat()


def read_gold(name):
    """Lit une table consolidee pre-calculee par Spark (zone gold).
    Retourne None si la gold n'existe pas (encore) : l'appelant retombe
    alors sur le calcul historique depuis la zone bronze."""
    import pyarrow.parquet as pq
    try:
        files = list_dir(f"{GOLD}/{name}")
    except Exception:
        return None
    frames = []
    for f in files:
        try:
            r = requests.get(f"{BASE}{GOLD}/{name}/{f}", params={"op": "OPEN"}, timeout=10)
            if r.content:
                frames.append(pq.read_table(io.BytesIO(r.content)).to_pandas())
        except Exception:
            continue
    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def load_clients():
    """Consolidation des clients AB + AP en modele unifie pseudonymise.
    Zone gold Spark si disponible, sinon calcul depuis la zone bronze."""
    g = read_gold("clients")
    if g is not None and not g.empty:
        return g
    ab = latest(read_topic("dbab_ab_ab_client"), "ab_client_id")
    ap = latest(read_topic("dbap_ap_ap_users"), "ap_user_id")
    rows = []
    for _, r in ab.iterrows():
        rows.append({"source": "ABASSURANCE", "id_source": to_id(r["ab_client_id"]),
                     "nom": to_str(r["ab_nom"]), "prenom": to_str(r["ab_prenom"]),
                     "date_naissance": epoch_days(r["ab_date_naissance"]),
                     "email_pseudo": pseudo(r["ab_email"]),
                     "telephone_pseudo": pseudo(r["ab_telephone"]),
                     "code_postal": to_str(r["ab_code_postal"]),
                     "statut": to_str(r["ab_statut_client"])})
    for _, r in ap.iterrows():
        parts = to_str(r["ap_full_name"]).split(" ", 1)
        rows.append({"source": "ASSUREPLUS", "id_source": to_id(r["ap_user_id"]),
                     "nom": parts[0], "prenom": parts[1] if len(parts) > 1 else "",
                     "date_naissance": to_str(r["ap_birth_date"]),
                     "email_pseudo": pseudo(r["ap_mail_address"]),
                     "telephone_pseudo": pseudo(r["ap_phone_number"]),
                     "code_postal": to_str(r["ap_zip_code"]),
                     "statut": to_str(r["ap_customer_status"])})
    return pd.DataFrame(rows)


def load_contrats():
    g = read_gold("contrats")
    if g is not None and not g.empty:
        return g
    ab = latest(read_topic("dbab_ab_ab_contrat"), "ab_policy_number")
    ap = latest(read_topic("dbap_ap_ap_contracts"), "ap_contract_ref")
    rows = []
    for _, r in ab.iterrows():
        rows.append({"source": "ABASSURANCE", "ref_contrat": to_str(r["ab_policy_number"]),
                     "id_client_source": to_id(r["ab_client_id"]),
                     "type_assurance": to_str(r["ab_type_assurance"]),
                     "date_debut": epoch_days(r["ab_date_debut"]),
                     "prime_annuelle": to_float(r["ab_prime_annuelle"]),
                     "statut": to_str(r["ab_statut_contrat"])})
    for _, r in ap.iterrows():
        rows.append({"source": "ASSUREPLUS", "ref_contrat": to_str(r["ap_contract_ref"]),
                     "id_client_source": to_id(r["ap_user_id"]),
                     "type_assurance": to_str(r["ap_product_code"]),
                     "date_debut": epoch_us(r["ap_start_date"]),
                     "prime_annuelle": to_float(r["ap_monthly_premium"]) * 12,
                     "statut": to_str(r["ap_contract_state"])})
    return pd.DataFrame(rows)


def load_sinistres():
    g = read_gold("sinistres")
    if g is not None and not g.empty:
        return g
    ab = latest(read_topic("dbab_ab_ab_sinistre"), "ab_claim_id")
    ap = latest(read_topic("dbap_ap_ap_claims"), "ap_sinistre_num")
    rows = []
    for _, r in ab.iterrows():
        rows.append({"source": "ABASSURANCE", "id_sinistre": to_id(r["ab_claim_id"]),
                     "ref_contrat": to_str(r["ab_policy_number"]),
                     "date_sinistre": epoch_us(r["ab_date_sinistre"]),
                     "montant_estime": to_float(r["ab_montant_estime"]),
                     "statut": to_str(r["ab_statut_sinistre"])})
    for _, r in ap.iterrows():
        rows.append({"source": "ASSUREPLUS", "id_sinistre": to_id(r["ap_sinistre_num"]),
                     "ref_contrat": to_str(r["ap_contract_ref"]),
                     "date_sinistre": to_str(r["ap_incident_date"]),
                     "montant_estime": to_float(r["ap_estimated_amount"]),
                     "statut": to_str(r["ap_claim_status"])})
    return pd.DataFrame(rows)


def load_paiements():
    g = read_gold("paiements")
    if g is not None and not g.empty:
        return g
    ab = latest(read_topic("dbab_ab_ab_paiement"), "ab_payment_id")
    ap = latest(read_topic("dbap_ap_ap_payments"), "ap_payment_ref")
    rows = []
    for _, r in ab.iterrows():
        rows.append({"source": "ABASSURANCE", "id_paiement": to_id(r["ab_payment_id"]),
                     "ref_contrat": to_str(r["ab_policy_number"]),
                     "date_paiement": epoch_days(r["ab_date_paiement"]),
                     "montant": to_float(r["ab_montant"]),
                     "mode_paiement": to_str(r["ab_mode_paiement"])})
    for _, r in ap.iterrows():
        rows.append({"source": "ASSUREPLUS", "id_paiement": to_str(r["ap_payment_ref"]),
                     "ref_contrat": to_str(r["ap_contract_ref"]),
                     "date_paiement": epoch_us(r["ap_payment_datetime"]),
                     "montant": to_float(r["ap_amount_paid"]),
                     "mode_paiement": to_str(r["ap_payment_channel"])})
    return pd.DataFrame(rows)


def doublons(clients):
    """Matrice de doublons : clients presents chez AB et AP (meme email).
    Zone gold Spark si disponible, sinon calcul depuis les clients."""
    g = read_gold("doublons")
    if g is not None and not g.empty:
        return g
    if clients.empty:
        return pd.DataFrame()
    ab = clients[(clients.source == "ABASSURANCE") & clients.email_pseudo.notna()]
    ap = clients[(clients.source == "ASSUREPLUS") & clients.email_pseudo.notna()]
    m = ab.merge(ap, on="email_pseudo", suffixes=("_ab", "_ap"))
    return m[["id_source_ab", "id_source_ap", "nom_ab", "prenom_ab",
              "code_postal_ab"]].rename(columns={
        "id_source_ab": "id_ab", "id_source_ap": "id_ap",
        "nom_ab": "nom", "prenom_ab": "prenom", "code_postal_ab": "code_postal"})


def repartition(clients):
    if clients.empty:
        return pd.DataFrame()
    return clients.groupby(["source", "code_postal"]).size().reset_index(name="nb_clients")


def stats_contrats(contrats):
    if contrats.empty:
        return pd.DataFrame()
    return contrats.groupby(["source", "type_assurance", "statut"]).agg(
        nb_contrats=("ref_contrat", "count"),
        total_primes=("prime_annuelle", "sum")).reset_index()


def stats_sinistres(sinistres):
    if sinistres.empty:
        return pd.DataFrame()
    return sinistres.groupby(["source", "statut"]).agg(
        nb_sinistres=("id_sinistre", "count"),
        montant_total=("montant_estime", "sum")).reset_index()


def stats_financieres(paiements):
    if paiements.empty:
        return pd.DataFrame()
    return paiements.groupby(["source", "mode_paiement"]).agg(
        nb_paiements=("id_paiement", "count"),
        ca_total=("montant", "sum")).reset_index()
