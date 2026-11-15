# Modèle Conceptuel de Données (MCD) - AbAssurance Bloc 3

## Schéma Unifié Clients

### Entité : CLIENT_UNIFIE

Le modèle conceptuel unifié permet de fusionner les données des deux assureurs (AbAssurance et AssurePlus) en un seul schéma cohérent avec déduplication.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        CLIENT_UNIFIE                                  │
├─────────────────────────────────────────────────────────────────────────┤
│ PK  │ uuid_client_global              UUID         Identifiant unique   │
│ FK  │ id_source                       VARCHAR(50)  ID source (AB/AP)     │
│     │ source                          VARCHAR(20)  Source (ABASSURANCE/ │
│     │                                                ASSUREPLUS)        │
│     │ nom                             VARCHAR(100) Nom du client         │
│     │ prenom                          VARCHAR(100) Prénom du client      │
│     │ date_naissance                  DATE         Date de naissance    │
│     │ email_pseudo                    VARCHAR(64)  Email pseudonymisé   │
│     │ telephone_pseudo                VARCHAR(64)  Téléphone pseudonym. │
│     │ code_postal                     VARCHAR(10)  Code postal          │
│     │ statut                          VARCHAR(50)  Statut client        │
│ UQ  │ (nom, prenom, date_naissance,  -      Clé de déduplication    │
│     │  code_postal)                                              │
└─────────────────────────────────────────────────────────────────────────┘
```

### Clé de déduplication

La **clé de déduplication** est composée des champs :
- **nom** (normalisé : majuscule, trim)
- **prenom** (normalisé : majuscule, trim)
- **date_naissance** (format YYYY-MM-DD)
- **code_postal** (nettoyé : suppression des espaces)

Cette combinaison permet d'identifier de manière unique un client à travers les deux systèmes sources, même lorsque l'ID technique diffère.

### Règles de gestion

1. **Identifiant unique** : `uuid_client_global` est généré via UUID v4 pour chaque client distinct
2. **Déduplication** : Deux enregistrements sont considérés comme des doublons si leur clé de déduplication est identique
3. **Pseudonymisation** : Les champs sensibles (email, téléphone) sont pseudonymisés via SHA-256 avant stockage
4. **Priorité des sources** : En cas de conflit, la source ABASSURANCE a la priorité (regle métier)

## Schéma des tables de correspondances

### Table : DOUBLONS

Stocke les correspondances entre clients des deux assureurs.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DOUBLONS                                      │
├─────────────────────────────────────────────────────────────────────────┤
│ PK  │ id_ab                           VARCHAR(50)  ID AbAssurance         │
│ PK  │ id_ap                           VARCHAR(50)  ID AssurePlus          │
│     │ nom                             VARCHAR(100) Nom commun             │
│     │ prenom                          VARCHAR(100) Prénom commun          │
│     │ code_postal                     VARCHAR(10)  Code postal            │
│     │ date_detection                  TIMESTAMP    Date de détection     │
└─────────────────────────────────────────────────────────────────────────┘
```

### Table : CONTRATS_UNIFIE

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        CONTRATS_UNIFIE                               │
├─────────────────────────────────────────────────────────────────────────┤
│ PK  │ ref_contrat                     VARCHAR(50)  Référence contrat    │
│ FK  │ id_client_source                VARCHAR(50)  ID client source     │
│     │ source                          VARCHAR(20)  Source               │
│     │ type_assurance                  VARCHAR(100) Type d'assurance     │
│     │ date_debut                      DATE         Date début           │
│     │ prime_annuelle                  DOUBLE        Prime annuelle      │
│     │ statut                          VARCHAR(50)  Statut               │
└─────────────────────────────────────────────────────────────────────────┘
```

## Architecture Data Lake

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    HDFS Data Lake (Hadoop)                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  /datalake/                                                             │
│  ├── bronze/                                                           │
│  │   └── cdc/          # Données brutes CDC (Debezium)                 │
│  │       ├── dbab_ab_ab_client/                                       │
│  │       ├── dbab_ab_ab_contrat/                                      │
│  │       ├── dbab_ab_ab_sinistre/                                     │
│  │       ├── dbab_ab_ab_paiement/                                     │
│  │       ├── dbap_ap_ap_users/                                        │
│  │       ├── dbap_ap_ap_contracts/                                     │
│  │       ├── dbap_ap_ap_claims/                                       │
│  │       └── dbap_ap_ap_payments/                                     │
│  │                                                                     │
│  └── gold/                                                             │
│      ├── clients/          # Clients unifiés avec UUID global         │
│      ├── contrats/         # Contrats unifiés                          │
│      ├── sinistres/        # Sinistres unifiés                        │
│      ├── paiements/        # Paiements unifiés                        │
│      └── doublons/         # Table de correspondance doublons          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Dictionnaire des données

### CLIENT_UNIFIE

| Champ | Type | Format | Description | Source AB | Source AP |
|-------|------|--------|-------------|-----------|-----------|
| uuid_client_global | UUID | UUID v4 | Identifiant unique client | Généré | Généré |
| id_source | VARCHAR(50) | Texte | ID technique source | AB_CLIENT_ID | AP_USER_ID |
| source | VARCHAR(20) | Texte | Source des données | ABASSURANCE | ASSUREPLUS |
| nom | VARCHAR(100) | Texte | Nom de famille | AB_NOM | AP_FULL_NAME (split) |
| prenom | VARCHAR(100) | Texte | Prénom | AB_PRENOM | AP_FULL_NAME (split) |
| date_naissance | DATE | YYYY-MM-DD | Date de naissance | AB_DATE_NAISSANCE | AP_BIRTH_DATE |
| email_pseudo | VARCHAR(64) | SHA-256 | Email pseudonymisé | AB_EMAIL | AP_MAIL_ADDRESS |
| telephone_pseudo | VARCHAR(64) | SHA-256 | Téléphone pseudonymisé | AB_TELEPHONE | AP_PHONE_NUMBER |
| code_postal | VARCHAR(10) | Texte | Code postal | AB_CODE_POSTAL | AP_ZIP_CODE |
| statut | VARCHAR(50) | Texte | Statut client | AB_STATUT_CLIENT | AP_CUSTOMER_STATUS |

### Clé de déduplication

La clé composite `(nom, prenom, date_naissance, code_postal)` permet de :
1. Identifier les doublons entre les deux assureurs
2. Créer une vue unifiée des clients
3. Éviter les doublons lors de l'intégration

**Algorithme de déduplication :**
```
1. Normaliser tous les champs (trim, upper/lower selon le cas)
2. Nettoyer le code postal (supprimer les espaces)
3. Créer une empreinte SHA-256 sur la concaténation des 4 champs
4. Regrouper par cette empreinte
5. Générer un UUID unique pour chaque groupe
```

## Exemple de requête de déduplication (Spark)

```python
from pyspark.sql import functions as F

# Normalisation et création de la clé de déduplication
clients_normalized = (clients
    .withColumn("nom", F.upper(F.trim(F.col("nom"))))
    .withColumn("prenom", F.upper(F.trim(F.col("prenom"))))
    .withColumn("code_postal", F.regexp_replace("code_postal", "\\s", ""))
    .withColumn("dedup_key", 
        F.concat_ws("|", "nom", "prenom", "date_naissance", "code_postal"))
    .withColumn("uuid_client_global", F.expr("uuid()"))
)

# Détection des doublons
from pyspark.sql.window import Window
w = Window.partitionBy("dedup_key").orderBy("source")
clients_dedup = (clients_normalized
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn", "dedup_key")
)
```

## Diagramme Merise (Optionnel)

```
CLIENT_UNIFIE
  └── 0,N CONTRATS_UNIFIE
        └── 0,N SINISTRES_UNIFIE
        └── 0,N PAIEMENTS_UNIFIE
```

---

**Version** : 1.0  
**Date** : 2026-11-15  
**Auteur** : Sofiane SESSAKHI  
**Contexte** : Bloc 3 - Correction évaluation
