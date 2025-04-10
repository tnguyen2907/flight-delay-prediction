import os

from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator, BigQueryDeleteTableOperator

from utils import load_sql

def append_bq_dashboard_tg():
    with TaskGroup('append_bq_dashboard') as append_bq_dashboard_tg:
        
        # Create temporary external table        
        task_create_temp_table = BigQueryCreateExternalTableOperator(
            task_id='create_temp_bq_table',
            table_resource= {
                "tableReference": {
                    "projectId": os.environ['GCP_PROJECT_ID'],
                    "datasetId": "flight_delay_pred_dataset",
                    "tableId": "temp_dashboard"
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": ["gs://flight-delay-pred-data/processed/data_{{ ti.xcom_pull(task_ids='get_latest_year_month', key='year') }}_{{ ti.xcom_pull(task_ids='get_latest_year_month', key='month') }}.parquet/*"],
                    "autoDetect": True
                },
                "location": 'us-east1'
            },
            gcp_conn_id='google_cloud',
            google_cloud_storage_conn_id='google_cloud'
        )
        
        append_dashboard_sql = load_sql('/opt/airflow/sql/monthly_dashboard_append.sql').format(gcp_project_id=os.environ['GCP_PROJECT_ID'])
        
        task_append_dashboard = BigQueryInsertJobOperator(
            task_id='append_bq_dashboard',
            configuration={
                "query": {
                    "query": append_dashboard_sql,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id='google_cloud',
            location='us-east1'
        )
        
        task_drop_temp_table = BigQueryDeleteTableOperator(
            task_id='drop_temp_bq_table',
            deletion_dataset_table=f"{os.environ['GCP_PROJECT_ID']}.flight_delay_pred_dataset.temp_dashboard",
            ignore_if_missing=True,
            location='us-east1',
            gcp_conn_id='google_cloud'
        )        
        
        task_create_temp_table >> task_append_dashboard >> task_drop_temp_table
        
        return append_bq_dashboard_tg