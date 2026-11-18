#!/bin/bash
# Script pour exécuter les tests locaux avec couverture
set -e

# Exporter le PYTHONPATH pour que Python trouve le module spark
export PYTHONPATH=$(pwd):$PYTHONPATH

echo "=========================================="
echo "  Tests unitaires pour streaming_pipeline.py"
echo "=========================================="

# Installer les dépendances
pip install -q pytest==8.2.0 pytest-cov==4.1.0 pyspark==3.5.0

# Exécuter les tests avec couverture
pytest tests/test_streaming_pipeline.py \
  --cov=spark \
  --cov=tests \
  --cov-report=term-missing \
  --cov-report=html \
  --cov-fail-under=80 \
  -v \
  --tb=short

echo ""
echo "=========================================="
echo "  Tests terminés. Rapport de couverture :"
echo "  Ouvrez htmlcov/index.html dans un navigateur."
echo "=========================================="