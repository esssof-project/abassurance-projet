#!/usr/bin/env bash
set -e
echo "=== Tests unitaires avec couverture (DoD Bloc 2 : >= 80%) ==="
docker run --rm -v "$PWD":/app -w /app python:3.11-slim bash -c \
  "pip install -q -r tests/requirements.txt && \
   pytest tests/ --cov=producer --cov=tests --cov-report=term-missing --cov-report=xml -v"

echo ""
echo "=== Analyse statique : ruff (qualite de code) ==="
docker run --rm -v "$PWD":/app -w /app python:3.11-slim bash -c \
  "pip install -q ruff && ruff check producer/ spark/ dataviz/ tests/"

echo ""
echo "=== Analyse de securite : bandit (fuites de PII, code vulnerable) ==="
docker run --rm -v "$PWD":/app -w /app python:3.11-slim bash -c \
  "pip install -q bandit && bandit -r producer/ spark/ dataviz/ -ll"

echo ""
echo "Rapports generes : coverage.xml (SonarCloud), analyses ruff + bandit ci-dessus"
