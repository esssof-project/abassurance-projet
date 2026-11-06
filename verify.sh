#!/usr/bin/env bash
FAIL=0
check() { if [ "$1" -eq 0 ]; then echo "  [OK] $2"; else echo "  [KO] $2"; FAIL=1; fi; }

echo "[1] Conteneurs"
for c in db-abassur db-assureplus kafka namenode datanode spark-master spark-worker postgres-datamart metabase caddy; do
  docker ps --format '{{.Names}}' | grep -q "^$c$"
  check $? "$c en cours d'execution"
done

echo "[2] Topics Kafka"
for t in topic-clients topic-contrats topic-sinistres topic-paiements; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list | grep -q "^$t$"
  check $? "topic $t"
done
echo "  Messages dans topic-clients : $(docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic topic-clients --from-beginning --max-messages 100 --timeout-ms 5000 2>/dev/null | wc -l)"

echo "[3] Data Lake HDFS (Parquet)"
docker exec namenode hdfs dfs -ls -R /datalake 2>/dev/null | grep -q parquet
check $? "fichiers Parquet presents dans /datalake"

echo "[4] Datamart Postgres (tables gold)"
for tbl in gold_doublons gold_repartition_clients gold_stats_contrats gold_stats_sinistres gold_stats_financieres; do
  N=$(docker exec postgres-datamart psql -U admin -d datamart -tAc \
      "SELECT COUNT(*) FROM $tbl" 2>/dev/null)
  [ -n "$N" ] && [ "$N" -gt 0 ]; check $? "$tbl ($N lignes)"
done

echo "[5] Matrice de doublons (DUPONT Jean AB/AP doit etre detecte)"
docker exec postgres-datamart psql -U admin -d datamart -tAc \
  "SELECT COUNT(*) FROM gold_doublons" | grep -qE "[1-9]"
check $? "au moins un doublon AB/AP identifie"

echo "[6] Dashboard HTTPS Metabase"
curl -sk https://dashboard.abassurance.local/api/health | grep -q ok
check $? "Metabase repond en HTTPS"

echo ""
if [ "$FAIL" -eq 0 ]; then
  echo "SMOKE TEST COMPLET : toute la chaine est operationnelle"
else
  echo "Des points ont echoue - verifie les logs: docker compose logs <service>"
  exit 1
fi
