from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import re

from utils import download_op, upload_to_gcs_op

def clean_and_save_airline_name(file_path):
    df_airline = pd.read_csv(file_path)
    df_airline = df_airline.drop_duplicates(subset=['Code'], keep='last')
    df_airline["Description"] = df_airline["Description"].apply(lambda x: re.sub(r"\s*\(\d{4}.*\)", "", x))

    df_airline.to_csv(file_path, index=False)

def get_airline_name_tg():
    file_path="/opt/airflow/data/raw/airline_lookup.csv"
    download_url="https://www.transtats.bts.gov/Download_Lookup.asp?Y11x72=Y_PNeeVRe_UVfgbel"
    with TaskGroup('get_airline_name') as get_airline_name_tg:
        task_download_airline_name = download_op('airline_name', file_path, download_url)

        task_clean_and_save_airline_name = PythonOperator(
            task_id='clean_airline_name',
            python_callable=clean_and_save_airline_name,
            op_kwargs={'file_path': file_path},
        )

        task_upload_to_gcs_airline_name = upload_to_gcs_op('airline_name', "flight-delay-pred-data", file_path, 'raw/airline_lookup.csv')

        task_download_airline_name >> task_clean_and_save_airline_name >> task_upload_to_gcs_airline_name

    return get_airline_name_tg