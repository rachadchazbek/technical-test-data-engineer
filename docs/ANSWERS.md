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

## Questions (étapes 4 à 7)

### Étape 4

_votre réponse ici_

### Étape 5

_votre réponse ici_

### Étape 6

_votre réponse ici_

### Étape 7

_votre réponse ici_
