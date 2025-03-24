# Réponses du test

## _Utilisation de la solution (étape 1 à 3)_

### Étape 1: Préparation de l'environnement

```bash
# Création de l'environnement virtuel
python -m venv venv

# Activation de l'environnement virtuel
# Sur Linux/macOS:
source venv/bin/activate
# Sur Windows:
# venv\Scripts\activate

# Installation des dépendances
pip install -r requirements.txt
```

### Étape 2: Flux de données avec Dagster

J'ai implémenté un flux de données basé sur Dagster pour récupérer manuelllement les données de l'API. La solution comprend:

1. Une implémentation basée sur les assets Dagster pour extraire les données
2. Un job qui peut être déclenché manuellement ou programmé quotidiennement
3. Une logique pour sauvegarder les données extraites dans des fichiers JSON horodatés

#### Structure du flux de données:
- `assets.py`: Définit les assets pour extraire et stocker les données
- `definitions.py`: Configure le job Dagster et les définitions nécessaires

#### Pour exécuter le flux de données:

1. Lancez d'abord le serveur FastAPI:
```bash
cd src/moovitamix_fastapi
python -m uvicorn main:app
```

2. Dans un autre terminal, exécutez le job Dagster via l'une des méthodes suivantes:

**Option 2: Utilisation de l'interface Dagster**
```bash
# À partir de la racine du projet
cd src/moovitamix_dagster
dagster dev -f assets.py
```

Vous pouvez accéder à l'interface Dagster via le lien `http://localhost:3000/` et déclencher manuellement le job `data_ingestion_job`.

Les données extraites seront sauvegardées dans le dossier `data/` à la racine du projet, avec un horodatage dans les noms de fichiers.

Pour une exécution quotidienne, on pourrait ajouter un schedule Dagster.

### Étape 3: Tests unitaires

Pour exécuter les tests:
```bash
# À exécuter depuis la racine du projet
python -m pytest test/test_moovitamix_dagster.py
```

## Questions (étapes 4 à 7)
### Étape 4 : Détailler le schéma de la base de données que vous utiliseriez pour stocker les informations récupérées des trois sources de données mentionnées plus tôt. Quel système de base de données recommanderiez-vous pour répondre à ces besoins et pourquoi?

Pour cette application de streaming musical, je recommanderais une approche hybride :

- **PostgreSQL** pour les données transactionnelles principales (SQL)
- **MongoDB** pour les fonctionnalités analytiques et de recommandations (NoSQL)

Cette approche hybride permet de tirer parti des forces des deux paradigmes de bases de données :

- Utiliser **PostgreSQL** pour les données métier principales où la cohérence et les relations entre les entités sont fixes et critiques.
- Utiliser **MongoDB** pour les analyses, les recommandations et le suivi du comportement des utilisateurs, où la flexibilité et l'évolutivité sont plus importantes.
### Étape 5 : Suivi de la santé du pipeline de données

Pour surveiller la santé du pipeline de données dans son exécution quotidienne, je propose une solution basée sur l'intégration de **Dagster**, **Prometheus**, et **Grafana**. Cette combinaison permet de collecter, stocker et visualiser les métriques clés du pipeline.

Some of the key metrics for Monitoring: 

1. Business Metrics
- User Growth: Rate of new user additions
- Content Growth: Rate of new track additions
- Engagement Metrics: Changes in listening patterns

2. Performance Metrics
- API Response Time: Time taken for each API endpoint request
- Resource Utilization: CPU, memory, and disk usage during pipeline execution
- Asset Duration: Execution time for each individual asset

3. Reliability Metrics
- Asset Materialization Success: Success/failure rate for each asset
- Record Counts: Number of records processed for tracks, users, and listen_history
- Data Freshness: Time since last successful data refresh
- Schema Validation: Count of records failing schema validation

We can also implement a Alert Strategy: 
- API connectivity issues
- Pipelines failures
- Data freshness exceding 24h

### Étape 6 Dessinez et/ou expliquez comment vous procèderiez pour automatiser le calcul des recommandations.

_votre réponse ici_

### Étape 7 Dessinez et/ou expliquez comment vous procèderiez pour automatiser le réentrainement du modèle de recommandation.

_votre réponse ici_
