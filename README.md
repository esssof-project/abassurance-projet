# AbAssurance Big Data Pipeline — Bloc 3

<<<<<<< HEAD
Architecture : bases sources AbAssurance (schema AB_*) + AssurePlus (schema AP_*)
-> Producer (Talend-like) -> Kafka -> Spark -> Hadoop HDFS (Data Lake) +
-> Dataviz (HTTPS via Caddy).
=======
Architecture demandee (BDD -> Talend -> Kafka -> Talend/Spark -> Hadoop -> Dataviz) :
>>>>>>> 0df17ab (new project with dataviz)

  TEMPS REEL : BDD sources -> Debezium CDC (temps reel, equivalent open source
  de Talend/GoldenGate) -> Kafka -> Spark Streaming -> Hadoop HDFS (Parquet)
  -> Dataviz Python (Streamlit + Plotly, lecture WebHDFS, auto-refresh 10 s).

  BATCH (demonstration Bloc 1) : BDD sources -> producer Talend-like -> Kafka
  -> Spark batch -> HDFS (/datalake/gold : doublons + clients_360 en Parquet).

## Demarrage
    ./setup.sh          # deploiement complet + CDC temps reel actif
    ./run-pipeline.sh   # (optionnel) demo du mode batch -> HDFS gold
    ./run-tests.sh      # tests unitaires + coverage + ruff + bandit
    ./verify.sh         # smoke test : chaine complete + propagation temps reel
    ./seed-demo.sh      # (demo soutenance) 1000 clients/assureur + ~100 doublons

## Temps reel sans redemarrage
Le service spark-streaming tourne en continu et ecrit chaque evenement CDC
en Parquet dans HDFS. Modifiez une ligne dans une base source : Debezium
publie l'evenement dans Kafka, Spark Streaming l'ecrit dans HDFS (~5 s), et
le Dataviz (auto-refresh 10 s) montre la nouvelle valeur ~15 s plus tard.
Aucun conteneur a relancer : c'est l'architecture Hadoop -> Dataviz demandee.

## Bases sources (version demo legere)
Les bases Oracle et SQL Server de l'enonce sont simulees par 2 PostgreSQL
alpines (~90 Mo) reproduisant les schemas AB_* et AP_*. Le CDC utilise la
replication logique PostgreSQL (wal_level=logical) + Debezium, equivalent
open source et gratuit de GoldenGate / SQL Server CDC. En production, seuls
les 2 services de bases et les 2 connecteurs changent : le reste de la chaine
(Kafka, Spark, HDFS, Dataviz) reste strictement identique.

## Convention de branches Git
- main      : code stable, uniquement merge via Pull Request relue
- develop   : branche d'integration continue
- feature/* : nouvelle fonctionnalite (ex: feature/cdc-debezium)
- fix/*     : correction de bug (ex: fix/null-email-doublons)
- hotfix/*  : correctif urgent en production depuis main

## Access
- Dataviz (dashboards) : https://dashboard.abassurance.local
  (ajouter "127.0.0.1 dashboard.abassurance.local" dans /etc/hosts ;
   certificat TLS interne Caddy : accepter l'avertissement du navigateur)
- Kafka Connect : http://localhost:8083/connectors
- HDFS UI : http://localhost:9870 | Spark UI : http://localhost:8080

## Qualite de code (Dossier 3)
- En local : `./run-tests.sh` (pytest+coverage, ruff, bandit).
- Sur le repo : GitHub Actions + SonarCloud (gratuit en repo public).
  Definir le secret SONAR_TOKEN dans les parametres du repo GitHub.
