#!/usr/bin/env bash
set -Eeuo pipefail

# Se placer dans le dossier du projet
ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

# Créer .env s'il n'existe pas
if [[ ! -f .env ]]; then
    if [[ -f .env.example ]]; then
        cp .env.example .env
        echo "Fichier .env créé depuis .env.example"
    else
        echo "ERREUR : .env.example introuvable"
        exit 1
    fi
fi

# Charger les variables
set -a
source .env
set +a

# Vérifier les variables obligatoires
: "${DB_USER:=demo}"
: "${DB_PASSWORD:?DB_PASSWORD doit être renseigné dans .env}"

export DB_USER DB_PASSWORD

echo "Configuration chargée."

# Créer un dossier temporaire pour les fichiers JSON avec variables injectées
mkdir -p /tmp/connect_configs

# Remplacer les variables dans les fichiers JSON avant enregistrement
for file in connect/*.json; do
  if [[ -f "$file" ]]; then
    filename=$(basename "$file")
    envsubst < "$file" > "/tmp/connect_configs/$filename"
  fi
done

echo "==============================================="
echo "  AbAssurance Big Data - Deploiement automatique"
echo "==============================================="

echo "[1/7] Construction et demarrage des conteneurs"
docker compose up -d --build

echo "[2/7] Attente des bases sources (PostgreSQL legers, ~10 s)..."
until docker exec db-abassur pg_isready \
  -U "$DB_USER" -d abassurance >/dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo " OK"

until docker exec db-assureplus pg_isready \
  -U "$DB_USER" -d assureplus >/dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo " OK"

echo "[3/7] Attente de Kafka..."
until docker exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka:9092 >/dev/null 2>&1; do
  echo -n "."; sleep 3
done; echo " OK"

echo "[4/7] Attente de Kafka Connect (Debezium, ~30-60 s)..."
until curl -s http://localhost:8083/connectors >/dev/null 2>&1; do
  echo -n "."; sleep 3
done
# Le REST repond AVANT que le plugin Debezium soit charge : on attend
# que PostgresConnector apparaisse dans /connector-plugins avant de POSTer.
until curl -s http://localhost:8083/connector-plugins 2>/dev/null | grep -q PostgresConnector; do
  echo -n "."; sleep 3
done; echo " OK"

# Enregistrer les connecteurs
echo "[5/7] Enregistrement des connecteurs CDC (Debezium)..."
for file in /tmp/connect_configs/*.json; do
  if [[ -f "$file" ]]; then
    echo "  Enregistrement de $(basename "$file")..."
    curl -X POST -H "Content-Type: application/json" --data @"$file" http://localhost:8083/connectors
  fi
done

# Verification : les connecteurs doivent passer en RUNNING (snapshot lance)
echo "  Attente de l'etat RUNNING des connecteurs..."
for c in source-abassur source-assureplus; do
  until curl -s http://localhost:8083/connectors/$c/status 2>/dev/null | grep -q '"state":"RUNNING"'; do
    echo -n "."; sleep 3
  done
done; echo " OK"

echo "[6/7] Attente des 8 topics CDC puis redemarrage du streaming"
for t in dbab.ab.ab_client dbab.ab.ab_contrat dbab.ab.ab_sinistre dbab.ab.ab_paiement \
         dbap.ap.ap_users dbap.ap.ap_contracts dbap.ap.ap_claims dbap.ap.ap_payments; do
  until docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list 2>/dev/null | grep -q "^$t$"; do
    echo -n "."; sleep 3
  done
done; echo " OK"
docker exec spark-streaming sh -c "rm -rf /tmp/checkpoints" 2>/dev/null || true
docker compose restart spark-streaming

echo "[7/7] Attente des premiers Parquet CDC dans HDFS (~60 s)..."
for d in dbab_ab_ab_client dbap_ap_ap_users; do
  until curl -s "http://localhost:9870/webhdfs/v1/datalake/cdc/$d?op=LISTSTATUS" 2>/dev/null | grep -q pathSuffix; do
    echo -n "."; sleep 5
  done
done; echo " OK"

echo "      Attente de la zone gold (consolidation calculee par Spark)..."
until curl -s "http://localhost:9870/webhdfs/v1/datalake/gold/clients?op=LISTSTATUS" 2>/dev/null | grep -q pathSuffix; do
  echo -n "."; sleep 5
done; echo " OK"

echo ""
echo "==============================================="
echo "  Plateforme prete :"
echo "  Dataviz (dashboards) : https://ec2-15-236-126-88.eu-west-3.compute.amazonaws.com"
echo "    (certificat TLS interne : accepter l'avertissement du navigateur)"
echo "  Kafka Connect : http://localhost:8083/connectors"
echo "  HDFS UI : http://localhost:9870 | Spark UI : http://localhost:8080"
echo ""
echo "  TEMPS REEL : modifiez une ligne dans une base source, ex :"
echo "  docker exec -i db-abassur psql -U demo -d abassurance <<'EOSQL'"
echo "  UPDATE ab.AB_CLIENT SET AB_STATUT_CLIENT='SUSPENDU' WHERE AB_CLIENT_ID=1;"
echo "  EOSQL"
echo "  -> sous ~15 s, le Dataviz montre la nouvelle valeur (auto-refresh 10 s)."
echo "==============================================="
