from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
import pandas as pd
import os

from google.cloud import storage

from get_airport_coords import get_airport_coords_tg
from get_flight_data import get_flight_data_tg
from get_airline_name import get_airline_name_tg
from get_weather_data import get_weather_data_tg
from get_weather_code_meteostat import get_weather_code_meteostat_tg
from append_bq_dashboard import append_bq_dashboard_tg
from aggregate import agg_tg
from utils import submit_dataproc_job_op

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='monthly_etl_dag',
    description='Monthly ETL for flight and weather data',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 10, 10),
    catchup=False,
) as dag:
    def get_latest_year_month(**kwargs):
        storage_client = storage.Client()
        
        bucket_name = 'flight-delay-pred-data'
        file_path = "dashboard/pre_agged_data.csv"
        
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        
        blob.download_to_filename('/opt/airflow/data/dashboard/pre_agged_data.csv')
        df = pd.read_csv('/opt/airflow/data/dashboard/pre_agged_data.csv')
        max_flight_date = datetime.strptime(df['MaxFlightDate'].iloc[0], "%Y-%m-%d")
        year = max_flight_date.year
        month = max_flight_date.month
        
        # Increment month
        if month < 12:
            month += 1
        else:
            month = 1
            year += 1
            
        kwargs['ti'].xcom_push(key='year', value=year)
        kwargs['ti'].xcom_push(key='month', value=month)
    
    task_get_latest_year_month = PythonOperator(
        task_id='get_latest_year_month',
        python_callable=get_latest_year_month,
    )
    
    task_group_airport_coords = get_airport_coords_tg()
    task_group_airline_name = get_airline_name_tg()
    task_group_weather_code_meteostat = get_weather_code_meteostat_tg()
    
    def etl_tg():            
        with TaskGroup('etl') as etl_month_tg:                
            # Weather data
            task_group_weather_data = get_weather_data_tg()
            task_process_weather_data = submit_dataproc_job_op('process-weather-data', 'gs://flight-delay-pred-data/spark/jobs/process_weather_data.py')
            task_group_airport_coords >> task_group_weather_data
            [task_group_weather_data, task_group_weather_code_meteostat] >> task_process_weather_data
            

            # Flight data
            task_group_flight_data = get_flight_data_tg()
            task_process_flight_data = submit_dataproc_job_op('process-flight-data', 'gs://flight-delay-pred-data/spark/jobs/process_flight_data.py')
            [task_group_flight_data, task_group_airline_name] >> task_process_flight_data

            # Join weather and flight data
            task_join_weather_flight = submit_dataproc_job_op('join-weather-flight', 'gs://flight-delay-pred-data/spark/jobs/join_weather_flight.py')
            [task_process_weather_data, task_process_flight_data] >> task_join_weather_flight
            
            return etl_month_tg
    
    task_group_etl = etl_tg()
    
    task_group_append_bq_dashboard = append_bq_dashboard_tg()
    task_group_agg = agg_tg()
    task_get_latest_year_month >> task_group_etl >> task_group_append_bq_dashboard >> task_group_agg
