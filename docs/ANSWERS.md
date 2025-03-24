# Réponses du test

## _Utilisation de la solution (étape 1 à 3)_

### Étape 1: Préparation de l'environnement

```bash
# Création de l'environnement virtuel
python -m venv venv

# Activation de l'environnement virtuel
# Sur Linux/macOS:
source venv/bin/activate


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
### Étape 4
Détailler le schéma de la base de données que vous utiliseriez pour stocker les informations récupérées des trois sources de données mentionnées plus tôt. Quel système de base de données recommanderiez-vous pour répondre à ces besoins et pourquoi?


Pour cette application de streaming musical, je recommanderais une approche hybride :

- **PostgreSQL** pour les données transactionnelles principales (SQL)
- **MongoDB** pour les fonctionnalités analytiques et de recommandations (NoSQL)

Cette approche hybride permet de tirer parti des forces des deux paradigmes de bases de données :

- Utiliser **PostgreSQL** pour les données métier principales où la cohérence et les relations entre les entités sont fixes et critiques.
- Utiliser **MongoDB** pour les analyses, les recommandations et le suivi du comportement des utilisateurs, où la flexibilité et l'évolutivité sont plus importantes.

### Étape 5
Suivi de la santé du pipeline de données

Pour surveiller la santé du pipeline de données dans son exécution quotidienne, je propose une solution basée sur l'intégration de **Dagster**, **Prometheus** et **Grafana**. Cette combinaison permet de collecter, stocker et visualiser les métriques clés du pipeline.

#### Principales métriques à surveiller :

1. **Métriques métier** :
    - Croissance des utilisateurs : Taux d'ajout de nouveaux utilisateurs.
    - Croissance du contenu : Taux d'ajout de nouveaux morceaux.
    - Engagement : Évolution des habitudes d'écoute.

2. **Métriques de performance** :
    - Temps de réponse de l'API : Durée des requêtes pour chaque point de terminaison.
    - Utilisation des ressources : Consommation de CPU, mémoire et disque pendant l'exécution du pipeline.
    - Durée des assets : Temps d'exécution pour chaque asset individuel.

3. **Métriques de fiabilité** :
    - Succès de matérialisation des assets : Taux de succès/échec pour chaque asset.
    - Comptage des enregistrements : Nombre d'enregistrements traités pour les morceaux, utilisateurs et historique d'écoute.
    - Fraîcheur des données : Temps écoulé depuis la dernière actualisation réussie des données.
    - Validation du schéma : Nombre d'enregistrements échouant à la validation du schéma.

#### Stratégie d'alerte :

Des alertes peuvent être configurées pour les cas suivants :
- Problèmes de connectivité avec l'API.
- Échecs dans l'exécution des pipelines.
- Fraîcheur des données dépassant 24 heures.

### Étape 6
Dessinez et/ou expliquez comment vous procèderiez pour automatiser le calcul des recommandations.

En s'appuyant sur notre pipeline existant basé sur Dagster, j'ajouterais de nouveaux assets dédiés au traitement des recommandations. Ces assets extrairaient des caractéristiques significatives à partir des données disponibles (par exemple : historique d'écoute, préférences des utilisateurs, similarité entre les morceaux). Ensuite, j'implémenterais des stratégies de recommandation, telles que :

- Recommander des morceaux similaires à ceux appréciés par l'utilisateur.
- Proposer des playlists personnalisées basées sur les habitudes d'écoute.

Le pipeline de recommandations serait configuré pour s'exécuter selon un calendrier approprié (par exemple : génération quotidienne de playlists personnalisées pour chaque utilisateur: daily mix) ou déclenché par des événements spécifiques (par exemple : lorsqu'un utilisateur demande une station basée sur une track ou artist).

### Étape 7 
Dessinez et/ou expliquez comment vous procèderiez pour automatiser le réentrainement du modèle de recommandation.

Pour automatiser le réentrainement des modèles de recommandation, je mettrais en place un système capable de surveiller les performances des modèles, de déclencher un réentrainement lorsque nécessaire (par exemple : dégradation des performances) ou selon un calendrier prédéfini. Ce système gérerait également le déploiement des modèles mis à jour.

Ce processus s'intégrerait dans notre pipeline de données existant et comprendrait :

- **Collecte des données** : Utilisation des assets Dagster existants pour collecter les données nécessaires (morceaux, utilisateurs, historique d'écoute).
- **Stockage** : Utilisation d'une base de données NoSQL pour stocker les métriques et les données liées aux recommandations.
- **Surveillance** : Intégration avec Prometheus et Grafana pour suivre les performances des modèles (par exemple : précision des recommandations, taux d'engagement des utilisateurs).


### 📌 **Remarques finales**  
- Ce travail m’a pris entre **3 heures** à réaliser excluant le temps que j'ai passé à essayer airflow. 
- J’ai utilisé **ChatGPT** et **GitHub Copilot** pour m'aider à générer rapidement du texte et du code.
- **Tous les sujets abordés et idées exprimés ici sont mes propres idées et réflexions.**
