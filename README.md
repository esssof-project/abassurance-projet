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

- En local : `./run-tests.sh` (Test des fonctions reel spark et hdfs).
- Sur le repo : GitHub Actions + SonarCloud (test code avec ruff).

**prérequie** :  merci de renommer le fichier .env.exemple par .env dans le serveur et rensegnier le mot de passe avant de lancer le build des contenaines  docker
