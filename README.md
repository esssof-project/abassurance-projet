# AbAssurance Big Data Pipeline — Bloc 3

Architecture : bases sources AbAssurance (schema AB_*) + AssurePlus (schema AP_*)
-> Producer (Talend-like) -> Kafka -> Spark -> Hadoop HDFS (Data Lake) +
Postgres (Datamart) -> Metabase (HTTPS via Caddy).

## Bases sources (version demo legere)
Pour la demo, les bases sources Oracle et SQL Server de l'enonce sont simulees
par 2 instances PostgreSQL alpines (~90 Mo, ~30 Mo RAM chacune) reproduisant
a l'identique les schemas AB_* et AP_* de l'annexe. En production, il suffit
de remplacer les 2 services db-abassur / db-assureplus et les 2 factory de
connexion du producer (psycopg2 -> oracledb / pymssql) : le reste de la
chaine (Kafka, Spark, Hadoop, Dataviz) reste strictement identique.

## Demarrage
    ./setup.sh          # deploiement + ingestion + traitement complet
    ./run-pipeline.sh   # relancer ingestion + job Spark
    ./run-tests.sh      # tests unitaires + coverage (>= 80 % exigé par la DoD)
    ./verify.sh         # smoke test de bout en bout

## Convention de branches Git
- main      : code stable, uniquement mergé via Pull Request relue
- develop   : branche d'intégration continue
- feature/* : nouvelle fonctionnalité (ex: feature/matrice-doublons)
- fix/*     : correction de bug (ex: fix/null-email-doublons)
- hotfix/*  : correctif urgent en production depuis main

## Access
- Dashboard Metabase : https://dashboard.abassurance.local
- HDFS UI : http://localhost:9870 | Spark UI : http://localhost:8080
- SonarQube : http://localhost:9000 (admin/admin)
