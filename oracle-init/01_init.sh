#!/bin/bash
# Script pour initialiser la base AbAssurance avec les variables d'environnement

# Charger les variables depuis .env
if [ -f /docker-entrypoint-initdb.d/.env ]; then
  set -a
  source /docker-entrypoint-initdb.d/.env
  set +a
fi

# Remplacer les variables dans le fichier SQL
envsubst < /docker-entrypoint-initdb.d/01_init.sql.template > /tmp/01_init.sql

# Exécuter le script SQL avec psql
psql -U ${DB_USER:-demo} -d abassurance -f /tmp/01_init.sql