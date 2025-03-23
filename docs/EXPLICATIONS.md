# Explications et remarques

## Choix de l'outil d'orchestration 

Pour choisir l'outil d'orchestration approprié, j'ai comparé plusieurs solutions populaires en me basant sur ces articles:
- [Azure Data Factory vs Apache Airflow](https://www.xenonstack.com/blog/azure-data-factory-vs-apache-airflow)
- [Airflow vs ADF vs Databricks](https://medium.com/indiciumtech/what-is-the-best-tool-apache-airflow-azure-data-factory-or-databricks-workflows-1d2e39422c07)

J'ai initialement opté pour Apache Airflow, mais j'ai rencontré des difficultés lors de la configuration locale, notamment avec les dépendances Docker.

En cherchant des alternatives, j'ai découvert Dagster. Après avoir consulté la documentation, j'ai pu implémenter une solution fonctionnelle en moins de 30 minutes. Bien que Dagster puisse avoir des limitations que je n'ai pas encore découvertes, il s'est avéré parfaitement adapté pour mes besoins d'orchestration simples, permettant une matérialisation efficace des assets et une mise en place rapide du pipeline.
