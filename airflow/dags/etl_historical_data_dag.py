from airflow import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from get_airport_coords import get_airport_coords_tg
from get_flight_data import get_flight_data_tg
from get_airline_name import get_airline_name_tg
from get_weather_data import get_weather_data_tg
from get_weather_code_meteostat import get_weather_code_meteostat_tg
from utils import submit_spark_job_op

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='etl_historical_data',
    description='ETL for historical flight and weather data',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 10, 10),
    catchup=False,
) as dag:
    task_group_airport_coords = get_airport_coords_tg()
    task_group_airline_name = get_airline_name_tg()
    task_group_weather_code_meteostat = get_weather_code_meteostat_tg()

    years = [2014]
    months = range(1, 2)
    with TaskGroup('Main') as main_tg:
        for year in years:
            with TaskGroup(f'etl_{year}') as etl_year_tg:
                for month in months:
                    if year == 2024 and month > 8:
                        break
                    with TaskGroup(f'etl_{year}_{month}') as etl_month_tg:
                        # Weather data
                        task_group_weather_data = get_weather_data_tg(year, month)
                        task_process_weather_data = submit_spark_job_op(f'process_weather_data_{year}_{month}', '/opt/airflow/spark/jobs/process_weather_data.py', {'year': str(year), 'month': str(month)})
                        task_group_airport_coords >> task_group_weather_data
                        [task_group_weather_data, task_group_weather_code_meteostat] >> task_process_weather_data

                        # Flight data
                        task_group_flight_data = get_flight_data_tg(year, month)
                        task_process_flight_data = submit_spark_job_op(f'process_flight_data_{year}_{month}', '/opt/airflow/spark/jobs/process_flight_data.py', {'year': str(year), 'month': str(month)})
                        [task_group_flight_data, task_group_airline_name] >> task_process_flight_data

                        # Join weather and flight data
                        task_join_weather_flight = submit_spark_job_op(f'join_weather_flight_{year}_{month}', '/opt/airflow/spark/jobs/join_weather_flight.py', {'year': str(year), 'month': str(month)})
                        [task_process_weather_data, task_process_flight_data] >> task_join_weather_flight
    

