#!/usr/bin/env bash
# Mode batch (demonstration du data flow Bloc 1 : Talend -> Kafka -> Spark -> HDFS)
set -e
echo "[1/2] Ingestion producer -> Kafka"
docker compose up producer --exit-code-from producer
echo "[2/2] Job Spark batch -> HDFS (doublons + clients_360 en Parquet)"
docker exec -u spark spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark-apps/job_pipeline.py
