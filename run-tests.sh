#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
export PYTHONPATH="$(pwd):${PYTHONPATH:-}"

# Vérifie d'abord que pytest trouve les tests existants et supplémentaires.
python -m pytest --collect-only -q tests/

# La couverture porte uniquement sur le module Streaming actif.
python -m pytest tests/ \
  --cov=spark.streaming_pipeline \
  --cov-report=term-missing \
  --cov-report=html \
  --cov-report=xml \
  --cov-branch   -v --tb=short

echo
echo "Rapport HTML : htmlcov/index.html"
