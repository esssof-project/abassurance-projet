
#!/bin/bash
set -e

cd "$(dirname "$0")"

export PYTHONPATH="$(pwd):${PYTHONPATH}"

echo "======================================"
echo "Installation des dépendances"
echo "======================================"

python -m pip install -q pytest pytest-cov pyspark hdfs

echo "======================================"
echo "Vérification des tests"
echo "======================================"

python -m pytest --collect-only -q tests/

echo "======================================"
echo "Exécution des tests"
echo "======================================"

rm -rf htmlcov
rm -f coverage.xml

python -m pytest tests/ \
    --cov=spark.streaming_pipeline \
    --cov-report=term-missing \
    --cov-report=html \
    --cov-report=xml \
    -v \
    --tb=short

echo "======================================"
echo "Tests terminés avec succès"
echo "Rapport HTML : htmlcov/index.html"
echo "======================================"
