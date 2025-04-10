import os
import json
import pandas as pd

from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from utils import load_sql, upload_to_gcs_op


def agg_tg():
    with TaskGroup('agg') as agg_tg:
        def fetch_agg_data():
            bq_hook = BigQueryHook(gcp_conn_id='google_cloud', use_legacy_sql=False, location='us-east1')

            total_agg_sql = load_sql('/opt/airflow/sql/total_agg.sql').format(gcp_project_id=os.environ['GCP_PROJECT_ID'])
            min_flight_date, max_flight_date, total_flights, total_delayed_flights, total_delayed_minutes, total_carrier_delay_minutes, total_weather_delay_minutes, total_nas_delay_minutes, total_security_delay_minutes, total_late_aircraft_delay_minutes, avg_departure_delayed_minutes = bq_hook.get_records(sql=total_agg_sql)[0]


            pre_agged_data = {
                "DelayType": ["Carrier", "Weather", "National Airspace System", "Security", "Late Aircraft"],
                "DelayTypeMinutes": [total_carrier_delay_minutes, total_weather_delay_minutes, total_nas_delay_minutes, total_security_delay_minutes, total_late_aircraft_delay_minutes],
                "MinFlightDate": [min_flight_date] * 5,
                "MaxFlightDate": [max_flight_date] * 5,
                "TotalFlights": [total_flights] * 5,
                "TotalDelayedFlights": [total_delayed_flights] * 5,
                "TotalDelayedMinutes": [total_delayed_minutes] * 5,
                "AvgDepartureDelayedMinutes": [avg_departure_delayed_minutes] * 5,
            }

            df = pd.DataFrame(pre_agged_data)
            df.to_csv("/opt/airflow/data/dashboard/pre_agged_data.csv", index=False)


        task_fetch_agg_data = PythonOperator(
            task_id='bq_fetch_agg_data',
            python_callable=fetch_agg_data,
        )

        task_upload_to_gcs = upload_to_gcs_op('pre_agged_data', 'flight-delay-pred-data', '/opt/airflow/data/dashboard/pre_agged_data.csv', 'dashboard/pre_agged_data.csv')

        def get_distinct():
            bq_hook = BigQueryHook(gcp_conn_id='google_cloud', use_legacy_sql=False, location='us-east1')

            distinct_sql = "SELECT DISTINCT {} FROM `{}.flight_delay_pred_dataset.dashboard` ORDER BY {}"
            cols = ["AirlineName", "OriginAirport", "DestinationAirport"]

            for col in cols:
                sql = distinct_sql.format(col, os.environ['GCP_PROJECT_ID'], col)
                data = bq_hook.get_records(sql=sql)
                # Save data to txt, create dir if not exists
                os.makedirs("/opt/airflow/data/app", exist_ok=True)
                with open(f"/opt/airflow/data/app/{col}.txt", "w") as f:
                    for row in data:
                        f.write(f"{row[0]}\n")

        task_get_distinct = PythonOperator(
            task_id='bq_get_distinct',
            python_callable=get_distinct,
        )
        
        with TaskGroup('upload_app_data_to_gcs') as upload_app_data_to_gcs:
            task_upload_airline_data = upload_to_gcs_op('airline_data', 'flight-delay-pred-data', '/opt/airflow/data/app/AirlineName.txt', 'app/AirlineName.txt')
            task_upload_origin_data = upload_to_gcs_op('origin_airport_data', 'flight-delay-pred-data', '/opt/airflow/data/app/OriginAirport.txt', 'app/OriginAirport.txt')
            task_upload_destination_data = upload_to_gcs_op('destination_airport_data', 'flight-delay-pred-data', '/opt/airflow/data/app/DestinationAirport.txt', 'app/DestinationAirport.txt')        
                
            [task_upload_airline_data, task_upload_origin_data, task_upload_destination_data]
            
        def get_airport_to_state_mapping():
            bq_hook = BigQueryHook(gcp_conn_id='google_cloud', use_legacy_sql=False, location='us-east1')

            airport_to_state_sql = f"SELECT DISTINCT OriginAirport, OriginState FROM `{os.environ['GCP_PROJECT_ID']}.flight_delay_pred_dataset.dashboard` ORDER BY OriginAirport"
            data = bq_hook.get_records(sql=airport_to_state_sql)
            # Save as json
            airport_to_state = {row[0]: row[1] for row in data}
            with open("/opt/airflow/data/app/airport_to_state.json", "w") as f:
                json.dump(airport_to_state, f)
                
        task_get_airport_to_state_mapping = PythonOperator(
            task_id='bq_get_airport_to_state_mapping',
            python_callable=get_airport_to_state_mapping,
        )
        
        task_upload_airport_to_state_mapping = upload_to_gcs_op('airport_to_state_mapping', 'flight-delay-pred-data', '/opt/airflow/data/app/airport_to_state.json', 'app/airport_to_state.json')

        task_get_airport_to_state_mapping >> task_upload_airport_to_state_mapping
        task_get_distinct >> upload_app_data_to_gcs
        task_fetch_agg_data >> task_upload_to_gcs
        
        return agg_tg