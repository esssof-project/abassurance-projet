# Pull Request : Corrections Bloc 3

## Contexte
Corrections demandées par l'évaluateur pour valider le Bloc 3.

## Changements apportés

### 1. ✅ MCD (Modèle Conceptuel de Données)
**Problème** : Une table de correspondance ne suffit pas. Il faut un schéma unifié avec un identifiant unique et une clé de déduplication sur le nom, prénom, date de naissance et code postal.

**Solution** : 
- Ajout de `docs/MCD.md` avec :
  - Schéma unifié `CLIENT_UNIFIE` avec `uuid_client_global` comme PK
  - Clé de déduplication composite : `(nom, prenom, date_naissance, code_postal)`
  - Architecture Data Lake bronze/gold documentée
  - Dictionnaire des données complet
  - Exemples de requêtes Spark pour la déduplication

### 2. ✅ Merge par Pull Request
**Problème** : Votre dépôt ne correspond pas à ce que vous écrivez. Vous annoncez des merges par Pull Request, mais je n'en ai pas vu dans le dépôt.

**Solution** :
- Création de la branche `feature/bloc3-corrections`
- Tous les commits de correction sont sur cette branche
- **Pour finaliser** : Créer une PR depuis cette branche vers `main` sur GitHub

### 3. ✅ Mots de passe en dur
**Problème** : Retirez les mots de passe du code.

**Solution** :
- **producer.py** : Suppression des valeurs par défaut des mots de passe
  - Avant : `password=os.getenv("DB_PASSWORD", "Demo123!")`
  - Après : `password=os.getenv("DB_PASSWORD")`
- **docker-compose.yml** : Suppression des valeurs par défaut des mots de passe
  - Avant : `POSTGRES_PASSWORD: Demo123!`
  - Après : `POSTGRES_PASSWORD:`
- **Ajout** : Fichier `.env.example` pour documenter la configuration nécessaire

### 4. ✅ Couverture de test
**Problème** : Vous annoncez 80%, mais votre rapport indique 41% uniquement sur le producer. Vos tests de doublons portent sur une copie en Pandas. Testez vos vraies fonctions Spark et visez au moins 70% en totalité.

**Solution** :
- **Ajout** : `tests/test_spark_pipeline.py` avec tests complets des vraies fonctions Spark :
  - Test des schemas Kafka
  - Test du nettoyage des données (déduplication intra-source)
  - Test de la détection des doublons inter-assureurs
  - Test du bug NULL email (correction incluse)
  - Test de la génération UUID
  - Test de la pseudonymisation SHA-256
  - Test des fonctions de conversion de dates
  - Test des opérations d'union
  - Test de la création de la clé de déduplication

- **Mise à jour** : `tests/test_pipeline.py` pour utiliser Spark au lieu de Pandas
- **Mise à jour** : `tests/requirements.txt` pour inclure pyspark
- **Mise à jour** : `run-tests.sh` pour inclure spark et dataviz dans la couverture

## Comment tester

### Localement
```bash
# Installer les dépendances
pip install -r tests/requirements.txt

# Exécuter les tests
./run-tests.sh
```

### Avec Docker
```bash
# Construire et démarrer les conteneurs
docker-compose up -d

# Exécuter les tests dans un conteneur
./run-tests.sh
```

## Vérification de la couverture

La couverture est maintenant calculée sur :
- `producer/` (module de production)
- `spark/` (pipelines Spark)
- `dataviz/` (module de visualisation)

Objectif : ≥ 70% de couverture globale

## Fichiers modifiés

- `docs/MCD.md` (NOUVEAU)
- `.env.example` (NOUVEAU)
- `producer/producer.py` (MODIFIÉ)
- `docker-compose.yml` (MODIFIÉ)
- `tests/test_spark_pipeline.py` (NOUVEAU)
- `tests/test_pipeline.py` (MODIFIÉ)
- `tests/requirements.txt` (MODIFIÉ)
- `run-tests.sh` (MODIFIÉ)

## Prochaines étapes

1. **Créer la Pull Request** sur GitHub :
   - Aller sur : https://github.com/esssof-project/abassurance-projet/pull/new/feature/bloc3-corrections
   - Titre : "Corrections Bloc 3 : MCD, PR, mots de passe, couverture tests"
   - Description : Copier le contenu de ce fichier
   - Assigner l'évaluateur comme reviewer

2. **Attendre la validation** de la PR

3. **Merger vers main** après approbation

---
**Note** : Tous les commits de cette branche corrigeant les 4 problèmes identifiés par l'évaluateur.
