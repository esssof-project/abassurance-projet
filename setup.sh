#!/usr/bin/env bash
set -e

echo "==============================================="
echo "  AbAssurance Big Data - Deploiement automatique"
echo "==============================================="

echo "[1/7] Construction et demarrage des conteneurs"
docker compose up -d --build

echo "[2/7] Attente des bases sources (PostgreSQL legers, ~10 s)..."
until docker exec db-abassur pg_isready -U demo -d abassurance >/dev/null 2>&1; do
  echo -n "."; sleep 2
done; echo " OK"
until docker exec db-assureplus pg_isready -U demo -d assureplus >/dev/null 2>&1; do
  echo -n "."; sleep 2
done; echo " OK"

echo "[3/7] Attente de Kafka..."
until docker exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka:9092 >/dev/null 2>&1; do
  echo -n "."; sleep 3
done; echo " OK"

echo "[4/7] Attente de Kafka Connect (Debezium, ~30-60 s)..."
until curl -s http://localhost:8083/connectors >/dev/null 2>&1; do
  echo -n "."; sleep 3
done
# Le REST repond AVANT que le plugin Debezium soit charge : on attend
# que PostgresConnector apparaisse dans /connector-plugins avant de POSTer.
until curl -s http://localhost:8083/connector-plugins 2>/dev/null | grep -q PostgresConnector; do
  echo -n "."; sleep 3
done; echo " OK"

echo "[5/7] Enregistrement des connecteurs CDC (Debezium)"
register() {
  if curl -s http://localhost:8083/connectors | grep -q "\"$1\""; then
    echo "  $1 : deja enregistre"
    return 0
  fi
  for attempt in 1 2 3 4 5; do
    RESP=$(curl -s -X POST -H 'Content-Type: application/json' \
      -d @"$2" http://localhost:8083/connectors)
    if echo "$RESP" | grep -q "error_code"; then
      echo "  $1 : tentative $attempt/5 en echec : $RESP"
      sleep 10
    else
      echo "  $1 : enregistre (snapshot initial + CDC temps reel)"
      return 0
    fi
  done
  echo "  $1 : ECHEC d'enregistrement apres 5 tentatives"
  exit 1
}
register source-abassur    connect/source-abassur.json
register source-assureplus connect/source-assureplus.json

# Verification : les connecteurs doivent passer en RUNNING (snapshot lance)
echo "  Attente de l'etat RUNNING des connecteurs..."
for c in source-abassur source-assureplus; do
  until curl -s http://localhost:8083/connectors/$c/status 2>/dev/null | grep -q '"state":"RUNNING"'; do
    echo -n "."; sleep 3
  done
done; echo " OK"

echo "[6/7] Attente des 8 topics CDC puis redemarrage du streaming"
for t in dbab.ab.ab_client dbab.ab.ab_contrat dbab.ab.ab_sinistre dbab.ab.ab_paiement \
         dbap.ap.ap_users dbap.ap.ap_contracts dbap.ap.ap_claims dbap.ap.ap_payments; do
  until docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list 2>/dev/null | grep -q "^$t$"; do
    echo -n "."; sleep 3
  done
done; echo " OK"
docker exec spark-streaming sh -c "rm -rf /tmp/checkpoints" 2>/dev/null || true
docker compose restart spark-streaming

echo "[7/7] Attente des premiers Parquet CDC dans HDFS (~60 s)..."
for d in dbab_ab_ab_client dbap_ap_ap_users; do
  until curl -s "http://localhost:9870/webhdfs/v1/datalake/cdc/$d?op=LISTSTATUS" 2>/dev/null | grep -q pathSuffix; do
    echo -n "."; sleep 5
  done
done; echo " OK"

echo "      Attente de la zone gold (consolidation calculee par Spark)..."
until curl -s "http://localhost:9870/webhdfs/v1/datalake/gold/clients?op=LISTSTATUS" 2>/dev/null | grep -q pathSuffix; do
  echo -n "."; sleep 5
done; echo " OK"

echo ""
echo "==============================================="
echo "  Plateforme prete :"
echo "  Dataviz (dashboards) : https://dashboard.abassurance.local"
echo "    (si non fait : echo '127.0.0.1 dashboard.abassurance.local' | sudo tee -a /etc/hosts)"
echo "    (certificat TLS interne : accepter l'avertissement du navigateur)"
echo "  Kafka Connect : http://localhost:8083/connectors"
echo "  HDFS UI : http://localhost:9870 | Spark UI : http://localhost:8080"
echo ""
echo "  TEMPS REEL : modifiez une ligne dans une base source, ex :"
echo "  docker exec -i db-abassur psql -U demo -d abassurance <<'EOSQL'"
echo "  UPDATE ab.AB_CLIENT SET AB_STATUT_CLIENT='SUSPENDU' WHERE AB_CLIENT_ID=1;"
echo "  EOSQL"
echo "  -> sous ~15 s, le Dataviz montre la nouvelle valeur (auto-refresh 10 s)."
echo "==============================================="
