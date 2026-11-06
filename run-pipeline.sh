#!/usr/bin/env bash
set -e
echo "[1/2] Ingestion producer -> Kafka"
docker compose up producer --exit-code-from producer
echo "[2/2] Job Spark -> HDFS + Datamart"
docker exec -u spark spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.2 \
  /opt/spark-apps/job_pipeline.py
