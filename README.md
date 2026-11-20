# AbAssurance Big Data Pipeline — Bloc 3

Architecture :

 BDD sources -> Debezium CDC (temps reel, equivalent open source de Talend/GoldenGate) -> Kafka -> Spark Streaming -> Hadoop HDFS (Parquet) -> Dataviz Python (Streamlit + Plotly, lecture WebHDFS, auto-refresh 10 s).

## Demarrage

    ./setup.sh          # deploiement complet + CDC temps reel actif
    ./run-tests.sh      # tests unitaires + coverage + ruff + bandit
    ./verify.sh         # smoke test : chaine complete + propagation temps reel
    ./seed-demo.sh      # ajout de 1000 clients/assureur + ~100 doublons à travers des requetes insert

## Temps reel sans redemarrage

Le service spark-streaming tourne en continu et ecrit chaque evenement CDC
en Parquet dans HDFS. Modifiez une ligne dans une base source : Debezium
publie l'evenement dans Kafka, Spark Streaming l'ecrit dans HDFS (~5 s), et
le Dataviz (auto-refresh 10 s) montre la nouvelle valeur ~15 s plus tard.
Aucun conteneur a relancer : c'est l'architecture Hadoop -> Dataviz demandee.

## Convention de branches Git

- main      : code stable, uniquement merge via Pull Request relue
- develop   : branche d'integration continue
- feature/* : nouvelle fonctionnalite (ex: feature/cdc-debezium)
- fix/*     : correction de bug (ex: fix/null-email-doublons)
- hotfix/*  : correctif urgent en production depuis main

## Access

- Dataviz (dashboards) : https://ec2-15-236-126-88.eu-west-3.compute.amazonaws.com
  (certificat TLS interne Caddy : accepter l'avertissement du navigateur)
- Kafka Connect : http://localhost:8083/connectors
- HDFS UI : http://localhost:9870 | Spark UI : http://localhost:8080

## Qualite de code

- En local : `./run-tests.sh` (tests Spark, coverage et Ruff). Pour lancer les tests depuis l'hôte EC2, le NameNode publie son RPC sur `127.0.0.1:9001` afin d'éviter le port 9000 de SonarQube : `HDFS_URL=hdfs://127.0.0.1:9001 ./run-tests.sh`.
- CI : GitHub Actions exécute Ruff et les tests ; Dataviz est exclue des métriques de couverture, mais reste analysée par SonarQube. L'analyse SonarQube est lancée sur les pushes vers `main`.

### Déployer SonarQube Server

1. Sur le serveur Linux, installer Docker Compose et configurer le paramètre noyau requis :
   `sudo sysctl -w vm.max_map_count=524288`
   Pour le rendre permanent, ajouter `vm.max_map_count=524288` dans `/etc/sysctl.d/99-sonarqube.conf`, puis appliquer avec `sudo sysctl --system`.
2. Copier `.env.example` vers `.env` si nécessaire et définir `SONAR_DB_PASSWORD` avec un mot de passe aléatoire dédié. Ne pas committer `.env`.
3. Démarrer d'abord la plateforme principale (`docker compose up -d`) afin que Caddy crée son réseau Docker, puis démarrer SonarQube et sa base persistante :
   `docker compose -f sonarqube-compose.yml up -d`
   Redémarrer Caddy pour appliquer sa nouvelle route avec `docker compose restart caddy`. Suivre le démarrage avec `docker compose -f sonarqube-compose.yml logs -f sonarqube`. L'interface est publiée sous `https://ec2-15-236-126-88.eu-west-3.compute.amazonaws.com/sonarqube` et localement sur `http://127.0.0.1:9000/sonarqube` ; changer le mot de passe initial `admin` dès la première connexion.
4. Caddy utilise actuellement une autorité TLS interne. Récupérer son certificat racine avec `docker exec caddy cat /data/caddy/pki/authorities/local/root.crt`, puis enregistrer le contenu PEM comme secret GitHub Actions `SONAR_CA_CERT`. Le scanner l'ajoutera à son truststore temporaire (la vérification TLS reste active).
5. Dans SonarQube, créer le projet avec la clé `abassurance-projet` et générer un jeton d'analyse. Dans les secrets Actions du dépôt, définir `SONAR_HOST_URL` à `https://ec2-15-236-126-88.eu-west-3.compute.amazonaws.com/sonarqube`, `SONAR_TOKEN` avec ce jeton, et `SONAR_CA_CERT` avec le certificat racine. Ne pas exposer directement le port HTTP 9000 à Internet ; il est lié à `127.0.0.1`.

La version Community analyse la branche principale, mais ne fournit pas l'analyse des pull requests/branches. Pour scanner les PR ou `develop`, une édition SonarQube qui prend en charge l'analyse multi-branches est nécessaire.

**Prérequis de la plateforme** : copier `.env.example` vers `.env` sur le serveur et renseigner les mots de passe avant de démarrer les conteneurs.
