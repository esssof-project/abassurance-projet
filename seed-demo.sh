#!/usr/bin/env bash
# Genere ~1000 clients par assureur (donnees de demonstration soutenance)
# Les insertions sont capturees par le CDC : le dashboard se met a jour seul.
set -e
echo "[1/2] AbAssurance : 1000 clients + contrats + sinistres + paiements..."
docker exec -i db-abassur psql -U demo -d abassurance -q < datagen/seed_abassur.sql
echo "  OK"
echo "[2/2] AssurePlus : 1000 users + contrats + sinistres + paiements..."
docker exec -i db-assureplus psql -U demo -d assureplus -q < datagen/seed_assureplus.sql
echo "  OK"
echo ""
echo "Le CDC capture ces ~7000 insertions en temps reel (~30 s d'ingestion)."
echo "Puis https://dashboard.abassurance.local montre :"
echo "  - ~2000 clients consolides (1000 AB + 1000 AP)"
echo "  - ~100 doublons inter-assureurs (emails communs)"
echo "Verification : docker exec dataviz python check.py clients"
