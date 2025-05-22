# Airflow DAG

![Airflow DAG](../assets/airflow_dag.png)

Apache Airflow is used to orchestrate the project's monthly data workflows. The DAG performs the following key tasks:

- Ingests historical flight data from the U.S. Department of Transportation, weather data from Meteostat, and related metadata into Google Cloud Storage (GCS).

- Triggers Spark jobs on Google Cloud Dataproc for large-scale data processing, transforming raw data into structured datasets optimized for different use cases: Machine Learning, data analysis, and visualization.

- Loads the transformed data into Google BigQuery for downstream analytics and visualization.

