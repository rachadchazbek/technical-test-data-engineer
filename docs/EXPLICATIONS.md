# Explications et remarques

## Choix de l'outil d'orchestration 

Pour choisir l'outil d'orchestration approprié, j'ai comparé plusieurs solutions populaires en me basant sur ces articles:
- [Azure Data Factory vs Apache Airflow](https://www.xenonstack.com/blog/azure-data-factory-vs-apache-airflow)
- [Airflow vs ADF vs Databricks](https://medium.com/indiciumtech/what-is-the-best-tool-apache-airflow-azure-data-factory-or-databricks-workflows-1d2e39422c07)
- [Dagster vs Airflow](https://dagster.io/blog/dagster-airflow)

J'ai initialement opté pour Apache Airflow, mais j'ai rencontré des difficultés lors de la configuration locale, notamment avec les dépendances Docker. 

J'ai trouvé que Dagster a Principled architecture: Designed from the ground up for modern data workflows, Dagster has better local development, testing, and debugging.


En cherchant des alternatives, j'ai découvert Dagster. Après avoir consulté la documentation, j'ai pu implémenter une solution fonctionnelle en moins de 30 minutes. Bien que Dagster puisse avoir des limitations que je n'ai pas encore découvertes, il s'est avéré parfaitement adapté pour mes besoins d'orchestration simples, permettant une matérialisation efficace des assets et une mise en place rapide du pipeline.

## Implémentation des tests unitaires

### Tests de la fonction d'extraction de données (TestFetchApiData)

- **test_fetch_api_data_success** : Vérifie que la fonction récupère correctement les données depuis un endpoint API et traite la réponse JSON.
- **test_fetch_api_data_error_handling** : S'assure que la fonction gère correctement les erreurs HTTP en levant les exceptions appropriées.

### Tests des assets de récupération (TestAssetsExecution)

- **test_tracks_asset** : Vérifie que l'asset `tracks` récupère et traite correctement les données d'API.
- **test_users_asset** : S'assure que l'asset `users` fonctionne comme prévu avec les données mockées.
- **test_listen_history_asset** : Teste le bon fonctionnement de l'asset `listen_history`.

### Tests des assets de sauvegarde (TestSaveAssets)

- **test_save_tracks** : Vérifie que les données des pistes sont correctement sauvegardées dans un fichier JSON.
- **test_save_users** : S'assure que les données des utilisateurs sont écrites dans le système de fichiers.
- **test_save_listen_history** : Teste la sauvegarde des données d'historique d'écoute.


# Why PostgreSQL for core data:
- Strong data integrity with ACID compliance
- Well-defined relationships between users, tracks, and listening history
- Efficient querying for common operations (user lookups, track details)
- Support for complex joins when generating reports
- Mature ecosystem with excellent tooling

# Why MongoDB for analytics:
- Flexible schema for evolving data requirements
- Better handling of semi-structured data
- Efficient storage of listening history as arrays
- Horizontal scaling for high-volume write operations
- Better performance for read-heavy analytics workloads

# Monitoring
[Prometheus](https://prometheus.io/)
[Grafana](https://grafana.com/?src=ggl-s&mdm=cpc&cnt=99878325494&camp=b-grafana-exac-amer&trm=grafana

