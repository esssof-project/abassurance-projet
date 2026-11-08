"""Dashboard AbAssurance : indicateurs consolides des deux assureurs,
lus directement du Data Lake HDFS (architecture Hadoop -> Dataviz).
Auto-rafraichissement 10 s : les modifications des bases sources
(CDC Debezium -> Kafka -> Spark Streaming -> HDFS) apparaissent seules."""

import streamlit as st
from streamlit_autorefresh import st_autorefresh
import plotly.express as px

import app_lib as lib

st.set_page_config(page_title="AbAssurance Analytics", page_icon=":bar_chart:", layout="wide")
st_autorefresh(interval=10000, key="cdc")  # temps reel : relecture HDFS toutes les 10 s

st.title("AbAssurance — Data Analytics")
st.caption("Source : Data Lake Hadoop HDFS (Parquet CDC Debezium) · auto-refresh 10 s")

clients = lib.load_clients()
contrats = lib.load_contrats()
sinistres = lib.load_sinistres()
paiements = lib.load_paiements()

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Clients ABASSURANCE", 0 if clients.empty else int((clients.source == "ABASSURANCE").sum()))
k2.metric("Clients ASSUREPLUS", 0 if clients.empty else int((clients.source == "ASSUREPLUS").sum()))
k3.metric("Doublons detectes", len(lib.doublons(clients)))
k4.metric("Contrats actifs", 0 if contrats.empty else int((contrats.statut == "ACTIF").sum()))
k5.metric("CA paiements (EUR)",
          0 if paiements.empty else round(float(paiements.montant.sum()), 2))

left, right = st.columns(2)

with left:
    st.subheader("Matrice de doublons (clients AB / AP)")
    st.dataframe(lib.doublons(clients), use_container_width=True)
    st.subheader("Repartition clients par source / code postal")
    rep = lib.repartition(clients)
    if not rep.empty:
        fig = px.bar(rep, x=rep["code_postal"].astype(str), y="nb_clients",
                      color="source", barmode="group")
        fig.update_xaxes(type="category")   # codes postaux affiches en entier (pas 75K)
        st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("Primes par type d'assurance")
    sc = lib.stats_contrats(contrats)
    if not sc.empty:
        st.plotly_chart(px.bar(sc, x="type_assurance", y="total_primes",
                               color="source"),
                        use_container_width=True)
    st.subheader("Sinistres par statut")
    ss = lib.stats_sinistres(sinistres)
    if not ss.empty:
        st.plotly_chart(px.pie(ss, names="statut", values="nb_sinistres", hole=0.4),
                        use_container_width=True)

with st.expander("Detail : indicateurs financiers (CA par mode de paiement)"):
    sf = lib.stats_financieres(paiements)
    if not sf.empty:
        st.dataframe(sf, use_container_width=True)
