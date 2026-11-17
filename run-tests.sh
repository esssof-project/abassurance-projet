#!/bin/bash
# Script de tests unitaires avec couverture de code
set -e

echo "=========================================="
echo "Tests unitaires + Couverture de code"
echo "=========================================="

echo ""
echo "Exécution des tests..."
pytest tests/test_spark_pipeline.py tests/test_streaming_pipeline.py tests/test_dataviz.py \
  --cov=spark \
  --cov=dataviz \
  --cov-report=term-missing \
  --cov-report=xml \
  --cov-fail-under=80 \
  -v \
  --tb=short

echo ""
echo "=========================================="
echo "Tests terminés"
echo "=========================================="