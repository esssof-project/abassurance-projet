#!/bin/bash
# Script pour exécuter les tests avec données HDFS
set -e

# Exporter le PYTHONPATH pour que Python trouve le module spark
export PYTHONPATH=$(pwd):$PYTHONPATH

echo "=========================================="
echo "  Tests avec données HDFS réelles"
echo "=========================================="

# Installer les dépendances
pip install -q pytest pytest-cov pyspark hdfs

# Exécuter les tests avec couverture
pytest tests/test_streaming_pipeline_hdfs.py \
  --cov=spark \
  --cov=tests \
  --cov-report=term-missing \
  --cov-report=html \
  --cov-fail-under=70 \
  -v \
  --tb=short

echo ""
echo "=========================================="
echo "  Tests terminés. Rapport de couverture :"
echo "  Ouvrez htmlcov/index.html dans un navigateur."
echo "=========================================="