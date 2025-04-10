from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator

import pandas as pd

from utils import download_unzip_tg, upload_to_gcs_op

manual_coords = {
    "ABY": (36.0, -86.0),
    "BQK": (31.0, -81.0),
    "HAR": (40.0, -79.0),
    "SFB": (29.0, -81.0),
}

def clean_and_save_airport_coords():
    df = pd.read_csv('/opt/airflow/data/raw/GlobalAirportDatabase.txt', sep=':', header=None)
    df = df[[1, 4, 14, 15]]
    df.columns = ['IATA_code', "Country", "Latitude",  "Longitude"]
    df = df[df['IATA_code'].notnull()]
    df = df[df['Country'] == 'USA']
    df = df[['IATA_code', 'Latitude', 'Longitude']]
    df = df.groupby('IATA_code').sum()
    for iata, coords in manual_coords.items():
        df.loc[iata]['Latitude'] = coords[0]
        df.loc[iata]['Longitude'] = coords[1]
    df.reset_index(inplace=True)
    df.to_csv('/opt/airflow/data/raw/airport_coords.csv', index=False)
    
def get_airport_coords_tg():
    file_path="/opt/airflow/data/raw/GlobalAirportDatabase.zip"
    download_url="https://www.partow.net/downloads/GlobalAirportDatabase.zip"
    with TaskGroup('get_airport_coords') as get_airport_coords_tg:
        task_group_download_unzip_airport_coords = download_unzip_tg('airport_coords', file_path, download_url, user_agent=True)

        task_clean_and_save_airport_coords = PythonOperator(
            task_id='clean_airport_coords',
            python_callable=clean_and_save_airport_coords,
        )

        task_upload_to_gcs_airport_coords = upload_to_gcs_op('airport_coords', "flight-delay-pred-data", '/opt/airflow/data/raw/airport_coords.csv', 'raw/airport_coords.csv')

        task_group_download_unzip_airport_coords >> task_clean_and_save_airport_coords >> task_upload_to_gcs_airport_coords

    return get_airport_coords_tg
