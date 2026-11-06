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

echo "[3/6] Attente de Kafka..."
until docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --list >/dev/null 2>&1; do
  echo -n "."; sleep 3
done; echo " OK"

echo "[4/6] Creation des topics Kafka"
for t in topic-clients topic-contrats topic-sinistres topic-paiements; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
    --create --if-not-exists --topic "$t" --partitions 3 --replication-factor 1
done

echo "[5/6] Ingestion (producer Talend-like -> Kafka)"
docker compose up producer --exit-code-from producer

echo "[6/6] Traitement Spark (dedoublonnage + stats -> HDFS + Datamart)"
docker exec -u spark spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.2 \
  /opt/spark-apps/job_pipeline.py

echo ""
echo "==============================================="
echo "  Plateforme prete :"
echo "  Dashboard   : https://dashboard.abassurance.local"
echo "  HDFS UI     : http://localhost:9870"
echo "  Spark UI    : http://localhost:8080"
echo "  SonarQube   : http://localhost:9000 (admin/admin)"
echo ""
echo "  Pense a ajouter dans /etc/hosts :"
echo "  127.0.0.1  dashboard.abassurance.local"
echo "==============================================="
