
#!/bin/bash

# Arrêter le script en cas d'erreur
set -e

# Se placer à la racine du projet
cd "$(dirname "$0")"

# Permettre à Python de trouver le module spark
export PYTHONPATH="$(pwd):${PYTHONPATH}"

echo "=========================================="
echo "  TESTS SPARK AVEC DONNEES HDFS"
echo "=========================================="

# Installer les dépendances
python -m pip install -q pytest pytest-cov pyspark hdfs

# Vérifier les fichiers de tests détectés
echo ""
echo "========== TESTS DETECTES ================"

python -m pytest --collect-only -q tests/

# Nettoyer les anciens rapports
rm -rf htmlcov
rm -f coverage.xml

# Exécuter TOUS les fichiers de tests
echo ""
echo "========== EXECUTION DES TESTS ==========="

python -m pytest tests/ \
    --cov=spark.streaming_pipeline \
    --cov-report=term-missing \
    --cov-report=html \
    --cov-report=xml \
    -v \
    --tb=short

echo ""
echo "=========================================="
echo "  TESTS TERMINES"
echo "  Rapport : htmlcov/index.html"
echo "=========================================="
