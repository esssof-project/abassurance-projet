#!/usr/bin/env bash
FAIL=0
check() { if [ "$1" -eq 0 ]; then echo "  [OK] $2"; else echo "  [KO] $2"; FAIL=1; fi; }

echo "[1] Conteneurs"
for c in db-abassur db-assureplus kafka kafka-connect namenode datanode spark-master spark-worker spark-streaming dataviz caddy; do
  docker ps --format '{{.Names}}' | grep -q "^$c$"
  check $? "$c en cours d'execution"
done

echo "[2] Connecteurs CDC Debezium"
for c in source-abassur source-assureplus; do
  curl -s http://localhost:8083/connectors | grep -q "\"$c\""
  check $? "connecteur $c enregistre"
done
curl -s http://localhost:8083/connectors/source-abassur/status | grep -q '"state":"RUNNING"'
check $? "source-abassur RUNNING"
curl -s http://localhost:8083/connectors/source-assureplus/status | grep -q '"state":"RUNNING"'
check $? "source-assureplus RUNNING"

echo "[3] Topics CDC dans Kafka"
for t in dbab.ab.ab_client dbab.ab.ab_contrat dbab.ab.ab_sinistre dbab.ab.ab_paiement dbap.ap.ap_users dbap.ap.ap_contracts dbap.ap.ap_claims dbap.ap.ap_payments; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list | grep -q "^$t$"
  check $? "topic $t"
done

echo "[4] Data Lake HDFS : evenements CDC en Parquet"
for d in dbab_ab_ab_client dbab_ab_ab_contrat dbab_ab_ab_sinistre dbab_ab_ab_paiement dbap_ap_ap_users dbap_ap_ap_contracts dbap_ap_ap_claims dbap_ap_ap_payments; do
  N=$(curl -s "http://localhost:9870/webhdfs/v1/datalake/cdc/$d?op=LISTSTATUS" 2>/dev/null | grep -o pathSuffix | wc -l)
  [ "${N:-0}" -gt 0 ]; check $? "HDFS /datalake/cdc/$d alimente ($N fichiers Parquet)"
echo "[4b] Zone gold : consolidation pre-calculee par Spark"
for d in clients contrats sinistres paiements doublons; do
  N=$(curl -s "http://localhost:9870/webhdfs/v1/datalake/gold/$d?op=LISTSTATUS" 2>/dev/null | grep -o pathSuffix | wc -l)
  [ "${N:-0}" -gt 0 ]; check $? "HDFS /datalake/gold/$d alimente ($N fichiers Parquet)"
done
done

echo "[5] Indicateurs consolides (lecture HDFS par le Dataviz)"
N=$(docker exec dataviz python check.py clients); echo "  clients consolides : $N"
[ "${N:-0}" -ge 4 ]; check $? "au moins 4 clients (2 AB + 2 AP)"
D=$(docker exec dataviz python check.py doublons)
[ "${D:-0}" -ge 1 ]; check $? "doublon DUPONT Jean detecte ($D)"

echo "[6] TEMPS REEL : mise a jour CDC -> HDFS -> Dataviz"
docker exec -i db-abassur psql -U demo -d abassurance <<'EOSQL'
UPDATE ab.AB_CLIENT SET AB_STATUT_CLIENT='TEST-CDC' WHERE AB_CLIENT_ID=1;
EOSQL
echo "  Attente de la propagation (20 s)..."; sleep 20
R=$(docker exec dataviz python check.py statut ABASSURANCE 1)
[ "$R" = "TEST-CDC" ]; check $? "mise a jour temps reel visible dans le Dataviz (statut=$R)"
docker exec -i db-abassur psql -U demo -d abassurance <<'EOSQL'
UPDATE ab.AB_CLIENT SET AB_STATUT_CLIENT='ACTIF' WHERE AB_CLIENT_ID=1;
EOSQL

echo "[7] Dataviz web (HTTPS via Caddy)"
curl -sk https://dashboard.abassurance.local/_stcore/health | grep -q ok
check $? "Dataviz repond en HTTPS (https://dashboard.abassurance.local)"

echo ""
if [ "$FAIL" -eq 0 ]; then
  echo "SMOKE TEST COMPLET : architecture BDD -> Kafka -> Spark -> Hadoop -> Dataviz (HTTPS) operationnelle"
else
  echo "Des points ont echoue - verifie les logs: docker compose logs <service>"
  exit 1
fi
