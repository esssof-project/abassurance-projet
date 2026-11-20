#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export PYTHONPATH="$(pwd):${PYTHONPATH:-}"
REPORT_DIR="${TEST_REPORT_DIR:-.}"
mkdir -p "$REPORT_DIR"

# Vérifie d'abord que pytest trouve les tests existants et supplémentaires.
python -m pytest --collect-only -q -p no:cacheprovider tests/

# La couverture porte uniquement sur le module Streaming actif.
# -s affiche aussi les diagnostics Spark/HDFS des tests qui sont ignorés.
python -m pytest tests/ \
  --cov=spark.streaming_pipeline \
  --cov-config=.coveragerc \
  --cov-report=term-missing \
  --cov-report="html:$REPORT_DIR/htmlcov" \
  --cov-report="xml:$REPORT_DIR/coverage.xml" \
  --cov-branch -p no:cacheprovider -s -v --tb=short

echo
echo "Rapport HTML : $REPORT_DIR/htmlcov/index.html"
echo "Rapport XML : $REPORT_DIR/coverage.xml"
