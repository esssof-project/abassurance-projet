#!/usr/bin/env bash
set -e
echo "=== Tests unitaires avec couverture (DoD Bloc 2 : >= 80%) ==="
docker run --rm -v "$PWD":/app -w /app python:3.11-slim bash -c \
  "pip install -q -r tests/requirements.txt && \
   pytest tests/ --cov=producer --cov=tests --cov-report=term-missing --cov-report=xml -v"
echo ""
echo "Rapport de couverture genere : coverage.xml (a importer dans SonarQube)"
